package history

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type HistoryGenerateSession struct {
	ctx             context.Context
	storageURI      *url.URL
	externalStorage storage.ExternalStorage
	sourceDatabase  string
	sourceTable     string
	connector       coreinterfaces.Connector
	startTS         uint64
	endTS           uint64
	timezone        *time.Location
	logger          *zap.Logger
}

func NewGenerateSession(
	storageURI *url.URL,
	sourceDatabase string,
	sourceTable string,
	connector coreinterfaces.Connector,
	startTS uint64,
	endTS uint64,
	timezone *time.Location) (*HistoryGenerateSession, error) {

	ctx := context.Background()
	logger := log.L().With(zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	storageURI.Path = storageURI.Path + "/" + sourceDatabase + "/" + sourceTable

	externalStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &HistoryGenerateSession{
		ctx:             ctx,
		storageURI:      storageURI,
		externalStorage: externalStorage,
		sourceDatabase:  sourceDatabase,
		sourceTable:     sourceTable,
		connector:       connector,
		startTS:         startTS,
		endTS:           endTS,
		timezone:        timezone,
		logger:          logger,
	}, nil
}

func (session *HistoryGenerateSession) getDMLDirNames() ([]string, error) {
	dirNames := []string{}
	flag := false // record whether have one dir name is totally the same with startTs, then we don't need add maxTSLessThanStartTS into dir

	maxTSLessThanStartTS := uint64(0) // record the max Ts which is <= startTS, which should be add to the dir
	if err := session.externalStorage.WalkDir(session.ctx, &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		session.logger.Info("WalkDir in storageURI.Path {}, current path is {}", zap.String("storageURI.Path", session.storageURI.Path), zap.String("path", path))
		if strings.HasSuffix(path, "meta") {
			return nil
		}
		// todo:这边需要保证除了 meta 意外以外没有其他不符合格式的名称
		ts, err := strconv.ParseUint(path, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}

		if ts >= session.startTS && ts <= session.endTS {
			dirNames = append(dirNames, path)
		} else if ts < session.startTS && ts > maxTSLessThanStartTS {
			maxTSLessThanStartTS = ts
		}

		if ts == session.startTS {
			flag = true
		}

		return nil
	}); err != nil {
		return nil, errors.Trace(err)
	}

	if !flag && maxTSLessThanStartTS > 0 {
		dirNames = append(dirNames, strconv.FormatUint(maxTSLessThanStartTS, 10))
	}
	return dirNames, nil
}

func (session *HistoryGenerateSession) getSchemaFileNames() ([]string, string, error) {
	schemaFileNames := []string{}
	flag := false                     // record whether have one dir name is totally the same with startTs
	maxTSLessThanStartTS := uint64(0) // record the max Ts which is <= startTS, which should be add to the dir
	prePathName := ""                 // record the schema file name for maxTSLessThanStartTS
	if err := session.externalStorage.WalkDir(session.ctx, &storage.WalkOption{SubDir: "meta", ObjPrefix: "schema_"}, func(path string, size int64) error {
		session.logger.Info("in getSchemaFileNames: WalkDir", zap.String("storageURI.Path", session.storageURI.Path), zap.String("path", path))
		if strings.HasSuffix(path, ".json") {
			slashSplits := strings.Split(path, "/")
			splits := strings.Split(slashSplits[1], "_")
			if len(splits) != 3 {
				return errors.New("schema file name format error")
			}
			ts, err := strconv.ParseUint(splits[1], 10, 64)
			if err != nil {
				return errors.Trace(err)
			}

			if ts >= session.startTS && ts <= session.endTS {
				schemaFileNames = append(schemaFileNames, slashSplits[1])
			} else if ts < session.startTS && ts > maxTSLessThanStartTS {
				maxTSLessThanStartTS = ts
				prePathName = slashSplits[1]
			}

			if ts == session.startTS {
				flag = true
			}
		}
		return nil
	}); err != nil {
		return nil, "", errors.Trace(err)
	}

	if !flag && maxTSLessThanStartTS > 0 {
		return schemaFileNames, prePathName, nil
	}
	return schemaFileNames, "", nil
}

func (session *HistoryGenerateSession) WriteDDLHistory(
	schemaFileNames []string,
	preSchemaFileName string) error {

	var preTableDef cloudstorage.TableDefinition
	subDir := "meta/"

	if preSchemaFileName != "" {
		preSchemaContent, err := session.externalStorage.ReadFile(session.ctx, subDir+preSchemaFileName)
		if err != nil {
			return errors.Trace(err)
		}
		if err = json.Unmarshal(preSchemaContent, &preTableDef); err != nil {
			return errors.Trace(err)
		}
	}

	// 先考虑直接把 tableDef 当作 pre-schema 来用

	for _, schemaFileName := range schemaFileNames {
		var tableDef cloudstorage.TableDefinition
		schemaContent, err := session.externalStorage.ReadFile(session.ctx, subDir+schemaFileName)
		if err != nil {
			return errors.Trace(err)
		}
		if err = json.Unmarshal(schemaContent, &tableDef); err != nil {
			return errors.Trace(err)
		}

		err = session.connector.InsertDDLItem(&tableDef, &preTableDef, session.timezone)
		if err != nil {
			return errors.Trace(err)
		}
		preTableDef = tableDef
	}

	return nil
}

func (session *HistoryGenerateSession) WriteDMLHistory(
	dmlSchemaMap map[string]string) error {

	for dirName, schemaPath := range dmlSchemaMap {
		var tableDef cloudstorage.TableDefinition
		schemaContent, err := session.externalStorage.ReadFile(session.ctx, schemaPath)
		if err != nil {
			return errors.Trace(err)
		}
		if err = json.Unmarshal(schemaContent, &tableDef); err != nil {
			return errors.Trace(err)
		}

		// get dml files
		if err := session.externalStorage.WalkDir(session.ctx, &storage.WalkOption{SubDir: dirName}, func(path string, size int64) error {
			session.logger.Info("in WriteDMLHistory: WalkDir in Path {}", zap.String("path", path))
			if strings.HasSuffix(path, ".csv") {
				csvData, err := session.externalStorage.ReadFile(session.ctx, path)
				if err != nil {
					return errors.Trace(err)
				}

				reader := csv.NewReader(bytes.NewReader(csvData))
				records, err := reader.ReadAll()
				if err != nil {
					return errors.Trace(err)
				}

				preRecord := []string{}
				for _, record := range records {
					if len(record) != tableDef.TotalColumns+5 {
						return errors.New("DML record length not equal to total columns")
					}
					commitTs, err := strconv.ParseUint(record[3], 10, 64)
					if err != nil {
						return errors.Trace(err)
					}
					if (commitTs < session.startTS) || (commitTs > session.endTS) {
						return nil
					}

					isUpdate := record[4]
					operator := record[0]
					if isUpdate == "false" {
						if len(preRecord) != 0 {
							return errors.New("unvalid preRecord " + strings.Join(preRecord, ","))
						}
						err = session.connector.InsertDMLItem(record, &tableDef, dirName, session.timezone)
						if err != nil {
							return errors.Trace(err)
						}
					} else if operator == "D" {
						preRecord = record
					} else if operator == "I" {
						err = session.connector.InsertUpdateDMLItem(record, preRecord, &tableDef, dirName, session.timezone)
						if err != nil {
							return errors.Trace(err)
						}
						preRecord = []string{}
					} else {
						return errors.New("unvalid record " + strings.Join(record, ","))
					}
				}
			}
			return nil
		}); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func GenerateHistoryEvents(
	sourceDatabase string,
	sourceTable string,
	storageURI *url.URL,
	connector coreinterfaces.Connector,
	timezone *time.Location,
	startTS uint64,
	endTS uint64,
	csvOutputDialect string) error {
	// test
	// └── t1
	//     ├── 447241444644093954
	//     │   └── 2024-01-24
	//     │       ├── CDC00000000000000000001.csv
	//     │       ├── CDC00000000000000000002.csv
	//     │       ├── CDC00000000000000000003.csv
	//     │       └── meta
	//     │           └── CDC.index
	//     ├── 447241484988055563
	//     │   └── 2024-01-24
	//     │       ├── CDC00000000000000000001.csv
	//     │       └── meta
	//     │           └── CDC.index
	//     └── meta
	//         ├── schema_447241444644093954_3564574634.json
	//         └── schema_447241484988055563_1535246245.json

	session, err := NewGenerateSession(storageURI, sourceDatabase, sourceTable, connector, startTS, endTS, timezone)
	if err != nil {
		return errors.Trace(err)
	}

	schemaFileNames, preFileNames, err := session.getSchemaFileNames() // preFileNames use to generate the pre-schema, if it's "", means the pre schema is empty
	if err != nil {
		return errors.Trace(err)
	}

	fmt.Println("schemaFileNames: ", schemaFileNames, "preFileNames: ", preFileNames)

	// 写 ddl
	err = session.WriteDDLHistory(schemaFileNames, preFileNames)
	if err != nil {
		return errors.Trace(err)
	}

	// 写 dml
	if preFileNames != "" {
		schemaFileNames = append(schemaFileNames, preFileNames)
	}
	dmlSchemaMap := make(map[string]string)
	for _, schemaNames := range schemaFileNames {
		splits := strings.Split(schemaNames, "_")
		dmlSchemaMap[splits[1]] = "meta/" + schemaNames
	}

	fmt.Println("dmlSchemaMap: ", dmlSchemaMap)

	err = session.WriteDMLHistory(dmlSchemaMap)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
