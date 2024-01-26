package cmd

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const layout = "2006-01-02 15:04:05"

func checkFormat(input, layout string) error {
	_, err := time.Parse(layout, input)
	if err != nil {
		return errors.New("Time format incorrect, should be '1970-01-01 00:00:00'")
	}
	return nil
}

func transformTimeZone(timezone string) (*time.Location, error) {
	if timezone == "system" {
		return time.Local, nil
	}
	matched, _ := regexp.MatchString(`UTC[+-]\d+`, timezone)
	if !matched {
		return nil, errors.New("Time zone format incorrect, should be 'UTC+x' or 'UTC-x'")
	}

	var loc *time.Location

	if strings.HasPrefix(timezone, "UTC+") {
		offset, _ := strconv.Atoi(strings.TrimPrefix(timezone, "UTC+"))
		if offset >= 0 && offset <= 12 {
			loc = time.FixedZone("UTC", offset*3600)
		} else {
			return nil, errors.New("Time zone format incorrect, should be 'UTC+x' or 'UTC-x', x should be between 0 and 12")
		}
	} else {
		offset, _ := strconv.Atoi(strings.TrimPrefix(timezone, "UTC-"))
		if offset >= 0 && offset <= 12 {
			loc = time.FixedZone("UTC", -offset*3600)
		} else {
			return nil, errors.New("Time zone format incorrect, should be 'UTC+x' or 'UTC-x', x should be between 0 and 12")
		}
	}

	return loc, nil
}

func convertToTimestamp(startTime, endTime string, tc *time.Location) (uint64, uint64) {
	startTimestamp, _ := time.ParseInLocation(layout, startTime, tc)
	endTimestamp, _ := time.ParseInLocation(layout, endTime, tc)

	fmt.Println("startTimestamp: ", startTimestamp, "endTimestamp: ", endTimestamp)

	return oracle.ComposeTS(startTimestamp.Unix(), 0), oracle.ComposeTS(endTimestamp.Unix(), 0) // 可能精度有问题
}

func NewSnowflakeCmd() *cobra.Command {
	var (
		tidbConfigFromCli      tidbsql.TiDBConfig
		snowflakeConfigFromCli snowsql.SnowflakeConfig
		tables                 []string
		snapshotConcurrency    int
		storagePath            string
		cdcHost                string
		cdcPort                int
		cdcFlushInterval       time.Duration
		cdcFileSize            int
		timezone               string
		logFile                string
		logLevel               string
		awsAccessKey           string
		awsSecretKey           string
		startTime              string
		endTime                string
		credValue              *credentials.Value

		mode          RunMode
		apiListenHost string
		apiListenPort int
	)

	run := func() error {
		err := logutil.InitLogger(&logutil.Config{
			Level: logLevel,
			File:  logFile,
		})
		if err != nil {
			return errors.Trace(err)
		}

		if mode == RunModeHistory {
			// check whether the start-time/end-time/timezone is valid
			err := checkFormat(startTime, layout)
			if err != nil {
				return errors.Trace(err)
			}
			err = checkFormat(endTime, layout)
			if err != nil {
				return errors.Trace(err)
			}

			tc, err := transformTimeZone(timezone)
			if err != nil {
				return errors.Trace(err)
			}

			startTimestamp, endTimestamp := convertToTimestamp(startTime, endTime, tc)

			// check storageURI/ak/sk is valid

			if awsAccessKey != "" && awsSecretKey != "" {
				credValue = &credentials.Value{
					AccessKeyID:     awsAccessKey,
					SecretAccessKey: awsSecretKey,
				}
			} else {
				return errors.New("please provide aws access key and secret key")
			}

			storageURI, err := getS3URIWithCredentials(storagePath, credValue)
			if err != nil {
				return errors.Trace(err)
			}

			fmt.Printf("storageURI is %s", storageURI.String())

			connectorMap := make(map[string]coreinterfaces.Connector)
			for _, tableFQN := range tables {
				sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
				if err := snowsql.CreateHistoryTables(&snowflakeConfigFromCli, sourceDatabase, sourceTable); err != nil {
					return errors.Trace(err)
				}
				fmt.Printf("create history tables for %s-%s", sourceDatabase, sourceTable)
				connector, err := snowsql.NewSnowflakeConnector(
					&snowflakeConfigFromCli,
					fmt.Sprintf("snapshot_external_%s_%s", sourceDatabase, sourceTable),
					storageURI,
					credValue,
				)
				connectorMap[tableFQN] = connector
				if err != nil {
					return errors.Trace(err)
				}
			}

			defer func() {
				for _, connector := range connectorMap {
					connector.Close()
				}
			}()

			return Generate(tables, storageURI, connectorMap, tc, startTimestamp, endTimestamp, "snowflake")
		} else {
			if awsAccessKey != "" && awsSecretKey != "" {
				credValue = &credentials.Value{
					AccessKeyID:     awsAccessKey,
					SecretAccessKey: awsSecretKey,
				}
			} else {
				credValue, err = resolveAWSCredential(storagePath)
				if err != nil {
					return errors.Trace(err)
				}
			}

			storageURI, err := getS3URIWithCredentials(storagePath, credValue)
			if err != nil {
				return errors.Trace(err)
			}

			snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(storageURI)
			if err != nil {
				return errors.Trace(err)
			}

			snapConnectorMap := make(map[string]coreinterfaces.Connector)
			increConnectorMap := make(map[string]coreinterfaces.Connector)
			for _, tableFQN := range tables {
				sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
				snapConnector, err := snowsql.NewSnowflakeConnector(
					&snowflakeConfigFromCli,
					fmt.Sprintf("snapshot_external_%s_%s", sourceDatabase, sourceTable),
					snapshotURI,
					credValue,
				)
				if err != nil {
					return errors.Trace(err)
				}
				snapConnectorMap[tableFQN] = snapConnector
				increConnector, err := snowsql.NewSnowflakeConnector(
					&snowflakeConfigFromCli,
					fmt.Sprintf("increment_external_%s_%s", sourceDatabase, sourceTable),
					incrementURI,
					credValue,
				)
				if err != nil {
					return errors.Trace(err)
				}
				increConnectorMap[tableFQN] = increConnector
			}

			defer func() {
				for _, connector := range snapConnectorMap {
					connector.Close()
				}
				for _, connector := range increConnectorMap {
					connector.Close()
				}
			}()

			return Replicate(&tidbConfigFromCli, tables, storageURI, snapshotURI, incrementURI,
				snapshotConcurrency, cdcHost, cdcPort, cdcFlushInterval, cdcFileSize,
				snapConnectorMap, increConnectorMap, "snowflake", mode,
			)
		}

		// 把 start-time 和 end-time 转换为时间戳传下去
		// 加一个校验字段的，如果是 history mode 就必须要有 start-time，end-time，并且能访问 storage（先只支持 s3）
	}

	cmd := &cobra.Command{
		Use:   "snowflake",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake or generate history tables",
		Run: func(_ *cobra.Command, _ []string) {
			runWithServer(mode == RunModeCloud, fmt.Sprintf("%s:%d", apiListenHost, apiListenPort), func() {
				if err := run(); err != nil {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusFatalError(err)
					log.Error("Fatal error running snowflake replication", zap.Error(err))
				} else {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusIdle()
				}
			})
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	// for history mode:
	// ./tidb2dw snowflake
	//           --storage s3://xxxx
	//           --table [<db>.<name>]
	//           --snowflake.account-id xx
	//           --snowflake.user xx
	// 			 --snowflake.pass xx
	//           --snowflake.database xx
	//           --snowflake.schema xx
	// 		     --mode history
	//           --time-zone "system"
	//           --start-time "2021-01-01 00:00:00"
	// 			 --end-time "2021-01-02 00:00:00"
	//           --aws.access-key xxx
	//           --aws.aws.secret-key xxx
	cmd.Flags().Var(enumflag.New(&mode, "mode", RunModeIds, enumflag.EnumCaseInsensitive), "mode", "replication mode: full, snapshot-only, incremental-only, cloud, history")
	cmd.Flags().StringVar(&apiListenHost, "api.host", "0.0.0.0", "API service listen host, only available in --mode=cloud")
	cmd.Flags().IntVar(&apiListenPort, "api.port", 8185, "API service listen port, only available in --mode=cloud")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Host, "tidb.host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.Port, "tidb.port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.User, "tidb.user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Pass, "tidb.pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.SSLCA, "tidb.ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.AccountId, "snowflake.account-id", "", "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Warehouse, "snowflake.warehouse", "COMPUTE_WH", "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.User, "snowflake.user", "", "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Pass, "snowflake.pass", "", "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Database, "snowflake.database", "", "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Schema, "snowflake.schema", "", "snowflake schema")
	cmd.Flags().StringArrayVarP(&tables, "table", "t", []string{}, "tables full qualified name, e.g. -t <db1>.<table1> -t <db2>.<table2>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&storagePath, "storage", "s", "", "storage path: s3://<bucket>/<path> or gcs://<bucket>/<path>")
	cmd.Flags().StringVar(&cdcHost, "cdc.host", "127.0.0.1", "TiCDC server host")
	cmd.Flags().IntVar(&cdcPort, "cdc.port", 8300, "TiCDC server port")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc.flush-interval", 60*time.Second, "")
	cmd.Flags().IntVar(&cdcFileSize, "cdc.file-size", 64*1024*1024, "")
	// cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer") // 这个是不是没用上过？没用上就给我用好了
	cmd.Flags().StringVar(&logFile, "log.file", "", "log file path")
	cmd.Flags().StringVar(&logLevel, "log.level", "info", "log level")
	cmd.Flags().StringVar(&awsAccessKey, "aws.access-key", "", "aws access key")
	cmd.Flags().StringVar(&awsSecretKey, "aws.secret-key", "", "aws secret key")
	cmd.Flags().StringVar(&timezone, "time-zone", "system", "time-zone, default is local time-zone, if you don't want to use the local time-zone, please set it to UTC-x or UTC+x")
	cmd.Flags().StringVar(&startTime, "start-time", "", "start time of history data, format like 1970-01-01 00:00:00")
	cmd.Flags().StringVar(&endTime, "end-time", "", "end time of history data, format like 1970-01-01 00:00:00")

	cmd.MarkFlagRequired("storage")

	return cmd
}
