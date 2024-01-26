package cmd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/history"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/dumpling"
	"github.com/pingcap-inc/tidb2dw/pkg/metrics"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/dumpling/export"
	tiflow_config "github.com/pingcap/tiflow/pkg/config"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

type RunMode enumflag.Flag

const (
	RunModeFull RunMode = iota
	RunModeSnapshotOnly
	RunModeIncrementalOnly
	RunModeCloud
	RunModeHistory
)

var RunModeIds = map[RunMode][]string{
	RunModeFull:            {"full"},
	RunModeSnapshotOnly:    {"snapshot-only"},
	RunModeIncrementalOnly: {"incremental-only"},
	RunModeCloud:           {"cloud"},
	RunModeHistory:         {"history"},
}

// o => create changefeed =>   dump snapshot   => load snapshot => incremental load
//
//	^                     ^                    ^ 				 ^
//	|			          |				       |				 |
//	+------ init ---------+ changefeed created + snapshot dumped + snapshot loaded --
type Stage string

const (
	StageInit              Stage = "init"
	StageChangefeedCreated Stage = "changefeed-created"
	StageSnapshotDumped    Stage = "snapshot-dumped"
	StageSnapshotLoaded    Stage = "snapshot-loaded"
)

var DumplingCsvOutputDialectMap = map[string]export.CSVDialect{
	"":          export.CSVDialectDefault,
	"default":   export.CSVDialectDefault,
	"bigquery":  export.CSVDialectBigQuery,
	"snowflake": export.CSVDialectSnowflake,
	"redshift":  export.CSVDialectRedshift,
}

var CdcCsvBinaryEncodingMethodMap = map[string]string{
	"":          tiflow_config.BinaryEncodingHex,
	"default":   tiflow_config.BinaryEncodingHex,
	"bigquery":  tiflow_config.BinaryEncodingBase64,
	"snowflake": tiflow_config.BinaryEncodingHex,
	"redshift":  tiflow_config.BinaryEncodingHex,
}

func checkStage(storage storage.ExternalStorage) (Stage, error) {
	stage := StageInit
	ctx := context.Background()
	if exist, err := storage.FileExists(ctx, "increment/metadata"); err != nil || !exist {
		return stage, errors.Wrap(err, "Failed to check increment metadata")
	} else {
		stage = StageChangefeedCreated
	}
	if exist, err := storage.FileExists(ctx, "snapshot/metadata"); err != nil || !exist {
		return stage, errors.Wrap(err, "Failed to check snapshot metadata")
	} else {
		stage = StageSnapshotDumped
	}
	if exist, err := storage.FileExists(ctx, "snapshot/loadinfo"); err != nil || !exist {
		return stage, errors.Wrap(err, "Failed to check snapshot loadinfo")
	} else {
		stage = StageSnapshotLoaded
	}
	return stage, nil
}

func getGCSURIWithCredentials(storagePath string, credentialsFilePath string) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to parse workspace path")
	}

	if uri.Scheme != "gcs" && uri.Scheme != "gs" {
		return nil, errors.New("Not a gcs storage")
	}

	// bigquery does not support gcs scheme
	if uri.Scheme == "gcs" {
		uri.Scheme = "gs"
	}

	// append credentials file path to query string
	if credentialsFilePath != "" {
		values := url.Values{}
		values.Add("credentials-file", credentialsFilePath)
		uri.RawQuery = values.Encode()
	}
	return uri, nil
}

func getS3URIWithCredentials(storagePath string, cred *credentials.Value) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to parse workspace path")
	}

	if uri.Scheme != "s3" {
		return nil, errors.New("Not a s3 storage")
	}

	// append credentials to query string
	values := url.Values{}
	values.Add("access-key", cred.AccessKeyID)
	values.Add("secret-access-key", cred.SecretAccessKey)
	if cred.SessionToken != "" {
		values.Add("session-token", cred.SessionToken)
	}
	uri.RawQuery = values.Encode()
	return uri, nil
}

func genSnapshotAndIncrementURIs(storageURI *url.URL) (*url.URL, *url.URL, error) {
	// create snapshot and increment uri from storage uri, append snapshot and increment path to path
	snapshotURI := *storageURI
	incrementURI := *storageURI

	var err error

	snapshotURI.Path, err = url.JoinPath(storageURI.Path, "snapshot")
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to join workspace path")
	}
	incrementURI.Path, err = url.JoinPath(storageURI.Path, "increment")
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to join workspace path")
	}
	return &snapshotURI, &incrementURI, nil
}

func resolveAWSCredential(storagePath string) (*credentials.Value, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if uri.Scheme == "s3" {
		creds := credentials.NewEnvCredentials()
		credValue, err := creds.Get()
		return &credValue, err
	}
	return nil, errors.New("Not a s3 storage")
}

func Export(
	tidbConfig *tidbsql.TiDBConfig,
	tables []string,
	storageURI *url.URL,
	snapshotURI *url.URL,
	incrementURI *url.URL,
	snapshotConcurrency int,
	cdcHost string,
	cdcPort int,
	cdcFlushInterval time.Duration,
	cdcFileSize int,
	csvOutputDialect string,
	mode RunMode,
) (
	stage Stage,
	err error) {
	ctx := context.Background()
	storage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return "", errors.Trace(err)
	}
	stage, err = checkStage(storage)
	if err != nil {
		return "", errors.Trace(err)
	}
	log.Info("Start Export", zap.String("stage", string(stage)), zap.String("mode", RunModeIds[mode][0]))

	startTSO := uint64(0)
	if mode == RunModeFull {
		startTSO, err = tidbsql.GetCurrentTSO(tidbConfig)
		if err != nil {
			return "", errors.Annotate(err, "Failed to get current TSO")
		}
	}

	onSnapshotDumpProgress := func(dumpedRows, totalRows int64) {
		log.Info("Snapshot dump progress", zap.Int64("dumpedRows", dumpedRows), zap.Int64("estimatedTotalRows", totalRows))
	}

	switch stage {
	case StageInit:
		if mode != RunModeSnapshotOnly && mode != RunModeCloud {
			cdcConnector, err := cdc.NewCDCConnector(
				cdcHost, cdcPort, tables, startTSO, incrementURI, cdcFlushInterval, cdcFileSize,
				CdcCsvBinaryEncodingMethodMap[csvOutputDialect],
			)
			if err != nil {
				return "", errors.Trace(err)
			}
			if err = cdcConnector.CreateChangefeed(); err != nil {
				return "", errors.Trace(err)
			}
		}
		fallthrough
	case StageChangefeedCreated:
		if mode != RunModeIncrementalOnly && mode != RunModeCloud {
			if err := dumpling.RunDump(
				tidbConfig, snapshotConcurrency, snapshotURI, fmt.Sprint(startTSO), tables,
				DumplingCsvOutputDialectMap[csvOutputDialect], onSnapshotDumpProgress,
			); err != nil {
				return "", errors.Trace(err)
			}
		}
	}

	return stage, nil
}

func Replicate(
	tidbConfig *tidbsql.TiDBConfig,
	tables []string,
	storageURI *url.URL,
	snapshotURI *url.URL,
	incrementURI *url.URL,
	snapshotConcurrency int,
	cdcHost string,
	cdcPort int,
	cdcFlushInterval time.Duration,
	cdcFileSize int,
	snapConnectorMap map[string]coreinterfaces.Connector,
	increConnectorMap map[string]coreinterfaces.Connector,
	csvOutputDialect string,
	mode RunMode,
) error {
	metrics.TableNumGauge.Add(float64(len(tables)))
	stage, err := Export(tidbConfig, tables, storageURI, snapshotURI, incrementURI,
		snapshotConcurrency, cdcHost, cdcPort, cdcFlushInterval, cdcFileSize, csvOutputDialect, mode)
	if err != nil {
		return errors.Trace(err)
	}

	var wg sync.WaitGroup
	for _, table := range tables {
		wg.Add(1)
		go func(table string) {
			defer wg.Done()
			ctx := context.Background()
			if mode != RunModeIncrementalOnly && stage != StageSnapshotLoaded {
				apiservice.GlobalInstance.APIInfo.SetTableStage(table, apiservice.TableStageLoadingSnapshot)
				if err = replicate.StartReplicateSnapshot(ctx, snapConnectorMap[table], table, tidbConfig, snapshotURI); err != nil {
					apiservice.GlobalInstance.APIInfo.SetTableFatalError(table, err)
					metrics.AddCounter(metrics.ErrorCounter, 1, table)
					return
				}
			}
			if mode != RunModeSnapshotOnly {
				apiservice.GlobalInstance.APIInfo.SetTableStage(table, apiservice.TableStageLoadingIncremental)
				if err = replicate.StartReplicateIncrement(ctx, increConnectorMap[table], table, incrementURI, cdcFlushInterval/5); err != nil {
					apiservice.GlobalInstance.APIInfo.SetTableFatalError(table, err)
					metrics.AddCounter(metrics.ErrorCounter, 1, table)
					return
				}
			}
			apiservice.GlobalInstance.APIInfo.SetTableStage(table, apiservice.TableStageFinished)
		}(table)
	}

	wg.Wait()
	return nil
}

func Generate(
	tables []string,
	storageURI *url.URL,
	connectorMap map[string]coreinterfaces.Connector,
	timezone *time.Location,
	startTS uint64,
	endTS uint64,
	csvOutputDialect string) error {
	// 遍历 s3 上的地址
	for _, tableFQN := range tables {
		sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
		err := history.GenerateHistoryEvents(sourceDatabase, sourceTable, storageURI, connectorMap[tableFQN], timezone, startTS, endTS, csvOutputDialect)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func runWithServer(startServer bool, addr string, body func()) {
	if !startServer {
		body()
		return
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Start API service failed", zap.Error(err))
		return
	}

	log.Info("API service started", zap.String("address", addr))

	go func() {
		body()
	}()

	apiservice.GlobalInstance.Serve(l)
}
