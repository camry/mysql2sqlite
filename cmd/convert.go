package cmd

import (
    "fmt"
    "sort"
    "strings"

    "github.com/asaskevich/govalidator"
    "github.com/camry/g/gutil"
    "gorm.io/gorm"
)

type Converter struct {
    serverDbConfig       *DbConfig
    serverDb             *gorm.DB
    serverTable          *Table
    serverTableColumns   []string
    serverTableColumnMap map[string]string
}

// NewConverter 新建转换器。
func NewConverter(serverDbConfig *DbConfig, serverDb *gorm.DB, serverTable *Table) *Converter {
    return &Converter{
        serverDbConfig:       serverDbConfig,
        serverDb:             serverDb,
        serverTable:          serverTable,
        serverTableColumnMap: make(map[string]string),
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

        var createTableSql, createUniqueIndexSql []string

        createTableSql = append(createTableSql, fmt.Sprintf("DROP TABLE IF EXISTS %s;", c.serverTable.TableName))
        createTableSql = append(createTableSql, fmt.Sprintf("CREATE TABLE %s (", c.serverTable.TableName))

        // COLUMNS ...
        for _, serverColumn := range serverColumnData {
            var dot = ""
            if serverColumn != serverColumnData[serverTableColumnResult.RowsAffected-1] || serverStatisticsResult.RowsAffected > 0 {
                dot = ","
            }
            dataType := c.getDataType(serverColumn.DataType)
            createSql := fmt.Sprintf("  %s %s%s",
                serverColumn.ColumnName,
                dataType,
                c.getNotNull(serverColumn.IsNullable),
            )
            createTableSql = append(createTableSql, fmt.Sprintf("%s%s", createSql, dot))

            c.serverTableColumns = append(c.serverTableColumns, serverColumn.ColumnName)
            c.serverTableColumnMap[serverColumn.ColumnName] = dataType
        }

        // KEY ...
        var createKeySql []string

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
                        createKeySql = append(createKeySql, fmt.Sprintf("  %s", c.getPrimaryKey(serverStatisticsDataMap[serverIndexName])))
                    } else {
                        createUniqueIndexSql = append(createUniqueIndexSql, c.createUniqueKey(serverIndexName, serverStatisticsDataMap[serverIndexName]))
                    }
                }
            }
        }

        if len(createKeySql) > 0 {
            createTableSql = append(createTableSql, strings.Join(createKeySql, ",\n"))
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
    var insertSql []string
    var rows []map[string]any
    result := c.serverDb.Table(fmt.Sprintf("`%s`.`%s`", c.serverDbConfig.Database, c.serverTable.TableName)).Find(&rows)
    if result.RowsAffected > 0 {
        var ks, kv []string
        for _, columnName := range c.serverTableColumns {
            ks = append(ks, columnName)
        }
        for _, row := range rows {
            var vs []string
            for _, columnName := range c.serverTableColumns {
                if columnDataType, ok := c.serverTableColumnMap[columnName]; ok {
                    if columnValue, ok1 := row[columnName]; ok1 {
                        if columnValue == nil {
                            vs = append(vs, "NULL")
                        } else {
                            switch columnDataType {
                            case "INTEGER", "REAL":
                                vs = append(vs, govalidator.ToString(columnValue))
                            case "TEXT", "BLOB":
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
    }
    return insertSql
}

// getDataType SQLite 数据类型。
func (c *Converter) getDataType(dataType string) string {
    switch strings.ToUpper(dataType) {
    case "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "INT",
        "INTEGER",
        "BIGINT":
        return "INTEGER"
    case "FLOAT",
        "DOUBLE",
        "DECIMAL":
        return "REAL"
    case "DATE",
        "TIME",
        "YEAR",
        "DATETIME",
        "TIMESTAMP",
        "CHAR",
        "VARCHAR",
        "TINYTEXT",
        "TEXT",
        "MEDIUMTEXT",
        "LONGTEXT":
        return "TEXT"
    case "TINYBLOB",
        "BLOB",
        "MEDIUMBLOB",
        "LONGBLOB":
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
        columnNames = append(columnNames, fmt.Sprintf("%s", statisticMap[seqInIndex].ColumnName))
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
        columnNames = append(columnNames, fmt.Sprintf("%s", statisticMap[seqInIndex].ColumnName))
    }

    return fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);", indexName, c.serverTable.TableName, strings.Join(columnNames, ","))
}
