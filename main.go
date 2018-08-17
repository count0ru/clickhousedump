package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/kshvakov/clickhouse"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

type partitionDescribe struct {
	partID       string
	tableName    string
	databaseName string
}

func Init(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) {

	Trace = log.New(traceHandle,
		"TRACE: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Info = log.New(infoHandle,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Warning = log.New(warningHandle,
		"WARNING: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile)
}

func freezePartitions(clickhouseConnection *sql.DB, dbName string, showPartitionsOnly bool) (databasePartitions []partitionDescribe) {

	currentPartitions, err := clickhouseConnection.Query("select partition, table, database FROM system.parts WHERE active AND database ='" + dbName + "';")
	if err != nil {
		Error.Println("can't get partitions list for database", dbName)
	}

	Info.Println("searching partitions for",dbName,"database")

	for currentPartitions.Next() {

		var resPartID, resTable, resDatabase string

		if err := currentPartitions.Scan(&resPartID, &resTable, &resDatabase); err != nil {
			Error.Println("can't get partitions IDs")
		}

		if !strings.HasPrefix(resTable, ".") {
			partition := partitionDescribe{
				partID:       resPartID,
				tableName:    resTable,
				databaseName: resDatabase,
			}

			Info.Println(partition.partID, "partition  of table", partition.tableName, "found for", partition.databaseName)
			databasePartitions = append(databasePartitions, partition)
			if !showPartitionsOnly {
				fmt.Println("ALTER TABLE", partition.databaseName + "." + partition.tableName, "FREEZE PARTITION '" + partition.partID + "';")
			}
		}

	}

	return databasePartitions
}

func dumpFiles(inputDirectory string, outDirectory string) error {

	if _, err := os.Stat(inputDirectory); os.IsNotExist(err) {
		Error.Println(inputDirectory, "not found")
		return  err
	}

	if _, err := os.Stat(outDirectory); os.IsNotExist(err) {
		Error.Println(outDirectory, "not found")
		return  err
	}

	return nil
}


func main() {

	Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	var connectionString string

	argHost := flag.String("h", "127.0.0.1", "server hostname")
	argBackup := flag.Bool("backup", false, "backup mode")
	argRestore := flag.Bool("restore", false, "restore mode")
	argDebugOn := flag.Bool("d", false, "show debug info")
	argPort := flag.String("p", "9000", "server port")
	argDatabase := flag.String("db", "", "database name")
    argNoFreeze := flag.Bool("no-freeze", false, "do not freeze, only show partitions")
	argInDirectory := flag.String("in", "", "source directory (/var/lib/clickhouse for backup mode by default)")
	argOutDirectory := flag.String("in", "", "destination directory")
	flag.Parse()

	if *argBackup && !*argRestore {

		fmt.Println("Run in backup mode")

		if *argInDirectory == "" {
			inputDirectory := "/var/lib/clickhouse"
		} else {
			inputDirectory := *argInDirectory
		}

		if *argOutDirectory == "" {
			Error.Fatalln("please set destination directory")
		}

	} else if *argRestore && !*argBackup {
		fmt.Println("Run in restore mode")

	} else if !*argRestore && !*argBackup {
		fmt.Println("Choose mode (restore tor backup)")

	} else {
		Error.Fatalln("Run in only one mode (backup or restore)")

	}

	connectionString = "tcp://" + *argHost + ":" + *argPort +"?username=&compress=true"

	if *argDebugOn {
		connectionString = connectionString + "&debug=true"
	}

	connect, err := sql.Open("clickhouse", connectionString)
	if err != nil {
		Error.Println("can't connect to server")
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			Error.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			Error.Println(err)
		}
	}
	if *argDatabase	== "" {

		databaseList, err := connect.Query("SHOW DATABASES;")
		if err != nil {
			Error.Fatalln("can't get databases list")
		}

		for databaseList.Next() {

			var resDatabase string

			if err := databaseList.Scan(&resDatabase); err != nil {
				Error.Println("can't get partitions IDs")
			}
			freezePartitions(connect, resDatabase, *argNoFreeze)
		}
	} else {
			freezePartitions(connect, *argDatabase, *argNoFreeze)
	}
}
