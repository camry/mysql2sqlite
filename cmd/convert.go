package cmd

import (
    "fmt"
    "sort"
    "strings"

    "github.com/asaskevich/govalidator"
    "github.com/camry/g/gutil"
    "github.com/golang-module/carbon/v2"
    "gorm.io/gorm"
)

type Converter struct {
    serverDbConfig       *DbConfig
    serverDb             *gorm.DB
    serverTable          *Table
    serverTableColumns   []string
    serverTableColumnMap map[string]*MySQL2SQLiteColumn
}

type MySQL2SQLiteColumn struct {
    DataType       string
    SQLiteDataType string
}

// NewConverter 新建转换器。
func NewConverter(serverDbConfig *DbConfig, serverDb *gorm.DB, serverTable *Table) *Converter {
    return &Converter{
        serverDbConfig:       serverDbConfig,
        serverDb:             serverDb,
        serverTable:          serverTable,
        serverTableColumnMap: make(map[string]*MySQL2SQLiteColumn),
    }
}

// Start 启动。
func (c *Converter) Start() {
    defer wg.Done()
    ch <- true

    switch c.serverTable.TableType {
    case "BASE TABLE":
        c.create()
        c.insert()
    case "VIEW":
        // glog.Warnf("表 `%s` 不支持 VIEW 转换。", c.serverTable.TableName)
    }

    <-ch
}

// create SQLite CREATE TABLE 语句。
func (c *Converter) create() {
    var (
        serverColumnData     []Column
        serverStatisticsData []Statistic
    )

    serverTableColumnResult := c.serverDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
        &serverColumnData,
        "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        c.serverDbConfig.Database, c.serverTable.TableName,
    )

    if serverTableColumnResult.RowsAffected > 0 {
        serverStatisticsResult := c.serverDb.Table("STATISTICS").Find(
            &serverStatisticsData,
            "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
            c.serverDbConfig.Database, c.serverTable.TableName,
        )

        var createTableSql, createTableColumnSql, createUniqueIndexSql []string

        createTableSql = append(createTableSql, fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", c.serverTable.TableName))
        createTableSql = append(createTableSql, fmt.Sprintf("CREATE TABLE `%s` (", c.serverTable.TableName))

        // COLUMNS ...
        for _, serverColumn := range serverColumnData {
            dataType := strings.ToUpper(serverColumn.DataType)
            sqliteDataType := c.getDataType(dataType)

            createSql := fmt.Sprintf("  `%s` %s%s",
                serverColumn.ColumnName,
                sqliteDataType,
                c.getNotNull(serverColumn.IsNullable),
            )
            createTableColumnSql = append(createTableColumnSql, createSql)

            c.serverTableColumns = append(c.serverTableColumns, serverColumn.ColumnName)
            c.serverTableColumnMap[serverColumn.ColumnName] = &MySQL2SQLiteColumn{
                DataType:       dataType,
                SQLiteDataType: sqliteDataType,
            }
        }

        // KEY ...
        if serverStatisticsResult.RowsAffected > 0 {
            var serverStatisticIndexNameArray []string
            serverStatisticsDataMap := make(map[string]map[int]Statistic)

            for _, serverStatistic := range serverStatisticsData {
                if _, ok := serverStatisticsDataMap[serverStatistic.IndexName]; ok {
                    serverStatisticsDataMap[serverStatistic.IndexName][serverStatistic.SeqInIndex] = serverStatistic
                } else {
                    serverSeqInIndexStatisticMap := make(map[int]Statistic)
                    serverSeqInIndexStatisticMap[serverStatistic.SeqInIndex] = serverStatistic
                    serverStatisticsDataMap[serverStatistic.IndexName] = serverSeqInIndexStatisticMap
                }

                if !gutil.InArray(serverStatistic.IndexName, serverStatisticIndexNameArray) {
                    serverStatisticIndexNameArray = append(serverStatisticIndexNameArray, serverStatistic.IndexName)
                }
            }

            for _, serverIndexName := range serverStatisticIndexNameArray {
                if 1 != serverStatisticsDataMap[serverIndexName][1].NonUnique {
                    if serverIndexName == "PRIMARY" {
                        createTableColumnSql = append(createTableColumnSql, fmt.Sprintf("  %s", c.getPrimaryKey(serverStatisticsDataMap[serverIndexName])))
                    } else {
                        createUniqueIndexSql = append(createUniqueIndexSql, c.createUniqueKey(serverIndexName, serverStatisticsDataMap[serverIndexName]))
                    }
                }
            }
        }

        if len(createTableColumnSql) > 0 {
            createTableSql = append(createTableSql, strings.Join(createTableColumnSql, ",\n"))
        }
        createTableSql = append(createTableSql, ");")
        if len(createUniqueIndexSql) > 0 {
            createTableSql = append(createTableSql, fmt.Sprintf("%s", strings.Join(createUniqueIndexSql, "\n")))
        }

        insertSql := c.insert()

        if len(insertSql) > 0 {
            createTableSql = append(createTableSql, fmt.Sprintf("%s", strings.Join(insertSql, "\n")))
        }

        lock.Lock()
        sqlTableNames = append(sqlTableNames, c.serverTable.TableName)
        sqlTableMap[c.serverTable.TableName] = strings.Join(createTableSql, "\n")
        lock.Unlock()
    }
}

// insert SQLite INSERT INTO 语句。
func (c *Converter) insert() []string {
    var (
        insertSql []string
        offset    = 0
        limit     = 2000
    )

    for {
        var rows []map[string]any
        result := c.serverDb.Table(fmt.Sprintf("`%s`.`%s`", c.serverDbConfig.Database, c.serverTable.TableName)).Offset(offset).Limit(limit).Find(&rows)
        if result.RowsAffected <= 0 {
            break
        }

        var ks, kv []string
        for _, columnName := range c.serverTableColumns {
            ks = append(ks, fmt.Sprintf("`%s`", columnName))
        }
        for _, row := range rows {
            var vs []string
            for _, columnName := range c.serverTableColumns {
                if col, ok := c.serverTableColumnMap[columnName]; ok {
                    if columnValue, ok1 := row[columnName]; ok1 {
                        if columnValue == nil {
                            vs = append(vs, "NULL")
                        } else {
                            switch col.SQLiteDataType {
                            case "INTEGER", "REAL":
                                vs = append(vs, govalidator.ToString(columnValue))
                            case "TEXT":
                                switch col.DataType {
                                case "DATE", "TIME", "YEAR", "DATETIME", "TIMESTAMP":
                                    vs = append(vs, fmt.Sprintf("'%s'", carbon.Parse(govalidator.ToString(columnValue)).ToDateTimeString()))
                                default:
                                    vs = append(vs, fmt.Sprintf("'%s'", strings.ReplaceAll(govalidator.ToString(columnValue), "'", "''")))
                                }
                            case "BLOB":
                                vs = append(vs, fmt.Sprintf("'%s'", govalidator.ToString(columnValue)))
                            }
                        }
                    }
                }
            }
            kv = append(kv, fmt.Sprintf("(%s)", strings.Join(vs, ",")))
        }
        insertSql = append(insertSql, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
            c.serverTable.TableName,
            strings.Join(ks, ","),
            strings.Join(kv, ","),
        ))

        offset += limit
    }
    return insertSql
}

// getDataType SQLite 数据类型。
func (c *Converter) getDataType(dataType string) string {
    switch dataType {
    case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
        return "INTEGER"
    case "FLOAT", "DOUBLE", "DECIMAL":
        return "REAL"
    case "DATE", "TIME", "YEAR", "DATETIME", "TIMESTAMP", "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
        return "TEXT"
    case "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
        return "BLOB"
    }
    return "TEXT"
}

// getNotNull SQLite NOT NULL 语句。
func (c *Converter) getNotNull(isNullAble string) string {
    if isNullAble == "NO" {
        return " NOT NULL"
    }
    return ""
}

// getPrimaryKey SQLite PRIMARY KEY 语句。
func (c *Converter) getPrimaryKey(statisticMap map[int]Statistic) string {
    var seqInIndexSort []int
    var columnNames []string

    for seqInIndex := range statisticMap {
        seqInIndexSort = append(seqInIndexSort, seqInIndex)
    }

    sort.Ints(seqInIndexSort)

    for _, seqInIndex := range seqInIndexSort {
        columnNames = append(columnNames, fmt.Sprintf("`%s`", statisticMap[seqInIndex].ColumnName))
    }

    return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(columnNames, ","))
}

// createUniqueKey SQLite CREATE UNIQUE INDEX 语句。
func (c *Converter) createUniqueKey(indexName string, statisticMap map[int]Statistic) string {
    lock.Lock()
    if idx, ok := existIndexMap[indexName]; ok {
        idx++
        existIndexMap[indexName] = idx
        indexName = fmt.Sprintf("%s%d", indexName, idx)
    } else {
        existIndexMap[indexName] = 0
    }
    lock.Unlock()

    var seqInIndexSort []int
    var columnNames []string

    for seqInIndex := range statisticMap {
        seqInIndexSort = append(seqInIndexSort, seqInIndex)
    }

    sort.Ints(seqInIndexSort)

    for _, seqInIndex := range seqInIndexSort {
        columnNames = append(columnNames, fmt.Sprintf("`%s`", statisticMap[seqInIndex].ColumnName))
    }

    return fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s` (%s);", indexName, c.serverTable.TableName, strings.Join(columnNames, ","))
}
