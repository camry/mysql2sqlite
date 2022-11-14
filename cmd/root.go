package cmd

import (
    "fmt"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "sync"

    "github.com/spf13/cobra"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"
)

const (
    Dsn         = "%s:%s@tcp(%s:%d)/information_schema?timeout=10s&parseTime=true&charset=%s"
    HostPattern = "^(.*)\\:(.*)\\@(.*)\\:(\\d+)$"
    DbPattern   = "^([A-Za-z0-9_]+)$"
)

func Execute() error {
    return rootCmd.Execute()
}

func init() {
    cobra.OnInitialize(initConfig)

    rootCmd.Flags().StringVarP(&server, "server", "s", "", "指定服务器。(格式: <user>:<password>@<host>:<port>)")
    rootCmd.Flags().StringVarP(&db, "db", "d", "", "指定数据库。")

    cobra.CheckErr(rootCmd.MarkFlagRequired("server"))
    cobra.CheckErr(rootCmd.MarkFlagRequired("db"))
}

func initConfig() {
}

var (
    wg   sync.WaitGroup
    lock sync.Mutex
    ch   = make(chan bool, 16)

    server        string
    db            string
    sqlTableNames []string
    sqlTableMap   = make(map[string]string)

    rootCmd = &cobra.Command{
        Use:     "mysql2sqlite",
        Short:   "MySQL convert to SQLite3.",
        Version: "v1.0.0",
        Run: func(cmd *cobra.Command, args []string) {
            serverMatched, err1 := regexp.MatchString(HostPattern, server)
            dbMatched, err2 := regexp.MatchString(DbPattern, db)
            cobra.CheckErr(err1)
            cobra.CheckErr(err2)
            if !serverMatched {
                cobra.CheckErr(fmt.Errorf("服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", server))
            }
            if !dbMatched {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 格式错误。", db))
            }

            var (
                serverUser = strings.Split(server[0:strings.LastIndex(server, "@")], ":")
                serverHost = strings.Split(server[strings.LastIndex(server, "@")+1:], ":")
                err        error
            )
            serverDbConfig := &DbConfig{
                User:     serverUser[0],
                Password: serverUser[1],
                Host:     serverHost[0],
                Charset:  "utf8",
                Database: db,
            }
            serverDbConfig.Port, err = strconv.Atoi(serverHost[1])
            cobra.CheckErr(err)

            serverDb, err := gorm.Open(mysql.New(mysql.Config{
                DSN: fmt.Sprintf(Dsn,
                    serverDbConfig.User, serverDbConfig.Password,
                    serverDbConfig.Host, serverDbConfig.Port,
                    serverDbConfig.Charset,
                ),
            }), &gorm.Config{
                SkipDefaultTransaction: true,
                DisableAutomaticPing:   true,
                Logger:                 logger.Default.LogMode(logger.Silent),
            })
            cobra.CheckErr(err)

            var serverSchema Schema
            serverSchemaResult := serverDb.Table("SCHEMATA").Limit(1).Find(
                &serverSchema,
                "`SCHEMA_NAME` = ?", serverDbConfig.Database,
            )
            if serverSchemaResult.RowsAffected <= 0 {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 不存在。", serverDbConfig.Database))
            }

            var serverTableData []*Table
            serverTableResult := serverDb.Table("TABLES").Order("`TABLE_NAME` ASC").Find(
                &serverTableData,
                "`TABLE_SCHEMA` = ?", serverDbConfig.Database,
            )
            if serverTableResult.RowsAffected <= 0 {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 没有表。", serverDbConfig.Database))
            }

            defer close(ch)
            for _, serverTable := range serverTableData {
                wg.Add(1)
                go NewConverter(serverDbConfig, serverDb, serverTable).Start()
            }
            wg.Wait()

            // Print SQL ...
            if len(sqlTableNames) > 0 && len(sqlTableMap) > 0 {
                fmt.Println("PRAGMA foreign_keys = false;")
                fmt.Println()

                sort.Strings(sqlTableNames)

                for k, sqlTableName := range sqlTableNames {
                    if sql, ok := sqlTableMap[sqlTableName]; ok {
                        if k < len(sqlTableMap)-1 {
                            fmt.Println(sql)
                            fmt.Println()
                        } else {
                            fmt.Println(sql)
                        }
                    }
                }

                fmt.Println()
                fmt.Println("PRAGMA foreign_keys = true;")
            }
        },
    }
)
