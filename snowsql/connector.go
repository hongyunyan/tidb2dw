package snowsql

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	_ "github.com/snowflakedb/gosnowflake"
	"go.uber.org/zap"
)

// A Wrapper of snowflake connection.
// All snowflake related operations should be done through this struct.
type SnowflakeConnector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName string
}

func NewSnowflakeConnector(db *sql.DB, stageName string, upstreamURI *url.URL, credentials credentials.Value) (*SnowflakeConnector, error) {
	// create stage
	var err error
	if upstreamURI.Host == "" {
		err = CreateInternalStage(db, stageName)
	} else {
		stageUrl := fmt.Sprintf("%s://%s%s", upstreamURI.Scheme, upstreamURI.Host, upstreamURI.Path)
		err = CreateExternalStage(db, stageName, stageUrl, credentials)
	}
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	return &SnowflakeConnector{db, stageName}, nil
}

func (sc *SnowflakeConnector) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if supported := IsSnowflakeSupportedDDL(tableDef.Type); !supported {
		log.Warn("Snowflake unsupported DDL, just skip", zap.String("query", tableDef.Query), zap.Any("type", tableDef.Type))
		return nil
	}
	query := RewriteDDL(tableDef.Query)
	_, err := sc.db.Exec(query)
	if err != nil {
		return errors.Annotate(err, fmt.Sprintf("Received DDL: %s, rewrite to: %s, but failed to execute", tableDef.Query, query))
	}
	log.Info("Successfully executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", query))
	return nil
}

func (sc *SnowflakeConnector) CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	createTableQuery, err := GenCreateSchema(sourceDatabase, sourceTable, sourceTiDBConn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Snowflake", zap.String("query", createTableQuery))
	_, err = sc.db.Exec(createTableQuery)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

func (sc *SnowflakeConnector) LoadSnapshot(targetTable, filePrefix string) error {
	if err := LoadSnapshotFromStage(sc.db, targetTable, sc.stageName, filePrefix); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully load snapshot", zap.String("table", targetTable), zap.String("filePrefix", filePrefix))
	return nil
}

func (sc *SnowflakeConnector) MergeFile(tableDef cloudstorage.TableDefinition, uri *url.URL, filePath string) error {
	if uri.Scheme == "file" {
		// if the file is local, we need to upload it to stage first
		putQuery := fmt.Sprintf(`PUT file://%s/%s '@%s/%s';`, uri.Path, filePath, sc.stageName, filePath)
		_, err := sc.db.Exec(putQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("put file to stage", zap.String("query", putQuery))
	}

	// merge staged file into table
	mergeQuery := GenMergeInto(tableDef, filePath, sc.stageName)
	_, err := sc.db.Exec(mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("merge staged file into table", zap.String("query", mergeQuery))

	if uri.Scheme == "file" {
		// if the file is local, we need to remove it from stage
		removeQuery := fmt.Sprintf(`REMOVE '@%s/%s';`, sc.stageName, filePath)
		_, err = sc.db.Exec(removeQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("remove file from stage", zap.String("query", removeQuery))
	}

	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (sc *SnowflakeConnector) Close() {
	// drop stage
	if err := DropStage(sc.db, sc.stageName); err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	}

	sc.db.Close()
}
