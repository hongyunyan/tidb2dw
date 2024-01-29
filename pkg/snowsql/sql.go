package snowsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
)

const timeFormat string = "2006-01-02 15:04:05"

func CreateHistoryTables(sfConfig *SnowflakeConfig, databaseName string, tableName string) error { // 代码放哪要再想一下，传 config 到这里总是很奇怪
	db, err := sfConfig.OpenDB()
	if err != nil {
		return errors.Trace(err)
	}

	fmt.Println("success open db")

	dml_sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_%s_dml_history (
		 operator VARCHAR, commit_ts BIGINT, physical_time TIMESTAMP, schema_ts BIGINT, handle_key STRING,
		 pre_value VARIANT, post_value VARIANT);`, databaseName, tableName) // 估计要 if exists

	fmt.Printf("dml_sql is %s", dml_sql)
	if err != nil {
		return err
	}
	_, err = db.Exec(dml_sql)
	if err != nil {
		return err
	}

	fmt.Println("success exec dml_sql")

	ddl_sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_%s_ddl_history (ts BIGINT, 
		 physical_time TIMESTAMP,
		 ddl STRING,
		 pre_schema VARIANT,
		 post_schema VARIANT);`, databaseName, tableName) // ts 要改名成类似 commit-ts 么？

	fmt.Printf("ddl_sql is %s", dml_sql)
	if err != nil {
		return err
	}
	_, err = db.Exec(ddl_sql)

	fmt.Println("success exec ddl_sql")
	return err
}

func CreateExternalStage(db *sql.DB, stageName, s3WorkspaceURL string, cred *credentials.Value) error {
	sql, err := formatter.Format(`
CREATE OR REPLACE STAGE {stageName}
URL = '{url}'
CREDENTIALS = (AWS_KEY_ID = '{awsKeyId}' AWS_SECRET_KEY = '{awsSecretKey}' AWS_TOKEN = '{awsToken}')
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE='\\' BINARY_FORMAT = 'HEX');
	`, formatter.Named{
		"stageName":    utils.EscapeString(stageName),
		"url":          utils.EscapeString(s3WorkspaceURL),
		"awsKeyId":     utils.EscapeString(cred.AccessKeyID),
		"awsSecretKey": utils.EscapeString(cred.SecretAccessKey),
		"awsToken":     utils.EscapeString(cred.SessionToken),
	})
	if err != nil {
		return err
	}
	_, err = db.Exec(sql)
	return err
}

func DropStage(db *sql.DB, stageName string) error {
	sql, err := formatter.Format(`
DROP STAGE IF EXISTS {stageName};
`, formatter.Named{
		"stageName": utils.EscapeString(stageName),
	})
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(sql)
	return err
}

func LoadSnapshotFromStage(db *sql.DB, targetTable, stageName, filePath string) error {
	sql, err := formatter.Format(`
COPY INTO {targetTable}
FROM @{stageName}/{filePath}
FILE_FORMAT = (TYPE = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE='\\' BINARY_FORMAT = 'UTF8');
`, formatter.Named{
		"targetTable": utils.EscapeString(targetTable),
		"stageName":   utils.EscapeString(stageName),
		"filePath":    utils.EscapeString(filePath),
	})
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(sql)
	return err
}

func GetDefaultString(val interface{}) string {
	_, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
	if err != nil {
		return fmt.Sprintf("'%v'", val) // FIXME: escape
	}
	return fmt.Sprintf("%v", val)
}

func GenCreateSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) (string, error) {
	tableColumns, err := tidbsql.GetTiDBTableColumn(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	columnRows := make([]string, 0, len(tableColumns))
	for _, column := range tableColumns {
		row, err := GetSnowflakeColumnString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	snowflakePKColumns, err := tidbsql.GetTiDBTablePKColumns(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return "", errors.Trace(err)
	}

	// TODO: Support unique key

	sqlRows := make([]string, 0, len(columnRows)+1)
	sqlRows = append(sqlRows, columnRows...)
	if len(snowflakePKColumns) > 0 {
		sqlRows = append(sqlRows, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(snowflakePKColumns, ", ")))
	}
	// Add idents
	for i := 0; i < len(sqlRows); i++ {
		sqlRows[i] = fmt.Sprintf("    %s", sqlRows[i])
	}

	sql := []string{}
	sql = append(sql, fmt.Sprintf(`CREATE OR REPLACE TABLE %s (`, sourceTable)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n"), nil
}

func GenMergeInto(tableDef cloudstorage.TableDefinition, filePath string, stageName string) string {
	selectStat := make([]string, 0, len(tableDef.Columns)+1)
	selectStat = append(selectStat, `$1 AS "METADATA$FLAG"`)
	for i, col := range tableDef.Columns {
		if TiDB2SnowflakeTypeMap[strings.ToLower(col.Tp)] == "BINARY" {
			selectStat = append(selectStat, fmt.Sprintf(`TO_BINARY($%d, 'HEX') AS %s`, i+5, col.Name))
		} else {
			selectStat = append(selectStat, fmt.Sprintf(`$%d AS %s`, i+5, col.Name))
		}
	}

	pkColumn := make([]string, 0)
	onStat := make([]string, 0)
	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, col.Name)
			onStat = append(onStat, fmt.Sprintf(`T.%s = S.%s`, col.Name, col.Name))
		}
	}

	updateStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		updateStat = append(updateStat, fmt.Sprintf(`%s = S.%s`, col.Name, col.Name))
	}

	insertStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		insertStat = append(insertStat, col.Name)
	}

	valuesStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		valuesStat = append(valuesStat, fmt.Sprintf(`S.%s`, col.Name))
	}

	// TODO: Remove QUALIFY row_number() after cdc support merge dml or snowflake support deterministic merge
	mergeQuery := fmt.Sprintf(
		`MERGE INTO %s AS T USING
		(
			SELECT
				%s
			FROM '@%s/%s'
			QUALIFY row_number() over (partition by %s order by $4 desc) = 1
		) AS S
		ON
		(
			%s
		)
		WHEN MATCHED AND S.METADATA$FLAG != 'D' THEN UPDATE SET %s
		WHEN MATCHED AND S.METADATA$FLAG = 'D' THEN DELETE
		WHEN NOT MATCHED AND S.METADATA$FLAG != 'D' THEN INSERT (%s) VALUES (%s);`,
		tableDef.Table,
		strings.Join(selectStat, ",\n"),
		stageName,
		filePath,
		strings.Join(pkColumn, ", "),
		strings.Join(onStat, " AND "),
		strings.Join(updateStat, ", "),
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "))

	return mergeQuery
}

func GenInsertDDLItem(tableDef *cloudstorage.TableDefinition, preTableDef *cloudstorage.TableDefinition, timezone *time.Location) (string, error) {
	physicalTime := oracle.GetTimeFromTS(tableDef.TableVersion).In(timezone).Format(timeFormat) // check

	preSchema, err := json.Marshal(preTableDef)
	if err != nil {
		return "", errors.Trace(err)
	}
	log.Info("InsertDDLItem", zap.Any("preTableDef", preTableDef), zap.Any("preSchema", preSchema))

	postSchema, err := json.Marshal(tableDef)
	if err != nil {
		return "", errors.Trace(err)
	}
	log.Info("InsertDDLItem", zap.Any("tableDef", tableDef), zap.Any("postSchema", postSchema))

	insertQuery := fmt.Sprintf(
		`INSERT INTO %s_%s_ddl_history (TS, PHYSICAL_TIME, DDL, PRE_SCHEMA, POST_SCHEMA) select %d, '%s', '%s', TO_VARIANT(PARSE_JSON('%s')), TO_VARIANT(PARSE_JSON('%s'))`,
		tableDef.Schema, tableDef.Table, tableDef.TableVersion, physicalTime, tableDef.Query, preSchema, postSchema)
	return insertQuery, nil
}

func GenInsertDMLItem(record []string, tableDef *cloudstorage.TableDefinition, schemaTs string, timezone *time.Location) (string, error) {
	commitTs := record[3]
	operator := record[0]

	columnValue := make(map[string]string)
	for i, column := range tableDef.Columns {
		columnValue[column.Name] = record[i+6]
	}
	values, err := json.Marshal(columnValue)
	if err != nil {
		return "", errors.Trace(err)
	}

	ts, err := strconv.ParseUint(commitTs, 10, 64)
	if err != nil {
		return "", errors.Trace(err)
	}
	commitPhysicalTime := oracle.GetTimeFromTS(ts).In(timezone).Format(timeFormat) // check

	if operator == "I" {
		insertQuery := fmt.Sprintf(
			`INSERT INTO %s_%s_dml_history (OPERATOR, COMMIT_TS, PHYSICAL_TIME, SCHEMA_TS, HANDLE_KEY, PRE_VALUE, POST_VALUE) select '%s','%s','%s','%s','%s', TO_VARIANT(PARSE_JSON('%s')),TO_VARIANT(PARSE_JSON('%s'))`,
			tableDef.Schema, tableDef.Table, operator, commitTs, commitPhysicalTime, schemaTs, record[5], "{}", values)
		return insertQuery, nil
	} else if operator == "D" {
		insertQuery := fmt.Sprintf(
			`INSERT INTO %s_%s_dml_history (OPERATOR, COMMIT_TS, PHYSICAL_TIME, SCHEMA_TS, HANDLE_KEY, PRE_VALUE, POST_VALUE) select '%s','%s','%s','%s','%s',TO_VARIANT(PARSE_JSON('%s')),TO_VARIANT(PARSE_JSON('%s'))`,
			tableDef.Schema, tableDef.Table, operator, commitTs, commitPhysicalTime, schemaTs, record[5], values, "{}")
		return insertQuery, nil
	}

	return "", errors.New("Unknown operator")
}

func GenInsertUpdateDMLItem(record []string, preRecord []string, tableDef *cloudstorage.TableDefinition, schemaTs string, timezone *time.Location) (string, error) {
	if !(record[0] == "I" && preRecord[0] == "D" && record[3] == preRecord[3]) {
		return "", errors.New("Invalid update record")
	}

	commitTs := record[3]

	postColumnValue := make(map[string]string)
	for i, column := range tableDef.Columns {
		postColumnValue[column.Name] = record[i+6]
	}
	postValues, err := json.Marshal(postColumnValue)
	if err != nil {
		return "", errors.Trace(err)
	}

	preColumnValue := make(map[string]string)
	for i, column := range tableDef.Columns {
		preColumnValue[column.Name] = preRecord[i+6] // 这个用个 const 给我统一掉...
	}
	preValues, err := json.Marshal(preColumnValue)
	if err != nil {
		return "", errors.Trace(err)
	}

	ts, err := strconv.ParseUint(commitTs, 10, 64)
	if err != nil {
		return "", errors.Trace(err)
	}
	commitPhysicalTime := oracle.GetTimeFromTS(ts).In(timezone).Format(timeFormat) // check

	insertQuery := fmt.Sprintf(
		`INSERT INTO %s_%s_dml_history (OPERATOR, COMMIT_TS, PHYSICAL_TIME, SCHEMA_TS, HANDLE_KEY, PRE_VALUE, POST_VALUE) select '%s','%s','%s','%s','%s',TO_VARIANT(PARSE_JSON('%s')),TO_VARIANT(PARSE_JSON('%s'))`,
		tableDef.Schema, tableDef.Table, "U", commitTs, commitPhysicalTime, schemaTs, record[5], preValues, postValues)
	return insertQuery, nil
}
