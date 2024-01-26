package coreinterfaces

import (
	"database/sql"
	"time"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

/// Connector is the interface for Data Warehouse connector
/// One Connector is responsible for one table.
/// Any data warehouse should implement this interface.
/// All Data Warehouse related operations should be done through this.

type Connector interface {
	// InitSchema initializes the schema of the table
	InitSchema(columns []cloudstorage.TableCol) error
	// CopyTableSchema copies the table schema from the source database to the Data Warehouse
	CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error
	// LoadSnapshot loads the snapshot file into the Data Warehouse
	LoadSnapshot(targetTable, filePath string) error
	// ExecDDL executes the DDL statements in Data Warehouse
	ExecDDL(tableDef cloudstorage.TableDefinition) error
	// LoadIncrement loads the increment data into the Data Warehouse
	LoadIncrement(tableDef cloudstorage.TableDefinition, filePath string) error
	// Close closes the connection to the Data Warehouse
	Close()
	// generate ddl-history item and insert into dw
	InsertDDLItem(tableDef *cloudstorage.TableDefinition, preTableDef *cloudstorage.TableDefinition, timezone *time.Location) error
	// generate dml-history item for delete or insert event and insert into dw
	InsertDMLItem(record []string, tableDef *cloudstorage.TableDefinition, schemaTs string, timezone *time.Location) error
	// generate dml-history item for update event and insert into dw
	InsertUpdateDMLItem(record []string, preRecord []string, tableDef *cloudstorage.TableDefinition, schemaTs string, timezone *time.Location) error
}
