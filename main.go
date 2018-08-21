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
	"path"
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
				clickhouseConnection.Query("ALTER TABLE", partition.databaseName + "." + partition.tableName, "FREEZE PARTITION '" + partition.partID + "';")
			} else {
			}
		}

	}

	return databasePartitions
}


func copyDirectory(sourceDirectory string, destinationDirectory string) error {

	var err error
	var fileDescriptors []os.FileInfo
	var sourceInfo os.FileInfo

	if sourceInfo, err = os.Stat(sourceDirectory); err != nil {
		return err
	}

	if err = os.MkdirAll(destinationDirectory, sourceInfo.Mode()); err != nil {
		return err
	}

	if fileDescriptors, err = ioutil.ReadDir(sourceDirectory); err != nil {
		return err
	}
	for _, fileDescriptor := range fileDescriptors {
		sourcePath := path.Join(sourceDirectory, fileDescriptor.Name())
		destinationPath := path.Join(destinationDirectory, fileDescriptor.Name())
		if fileDescriptor.IsDir() {
			if err = copyDirectory(sourcePath, destinationPath); err != nil {
				Error.Fatalln(err)
			}
		} else {
			if err = copyFile(sourcePath, destinationPath); err != nil {
				Error.Fatalln(err)
			}
		}
	}
	return nil
}

func copyFile(sourceFile string, destinationFile string) error {

	input, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		Error.Fatalln("cant't open file", sourceFile)
		return err
	}

	err = ioutil.WriteFile(destinationFile, input, 0644)
	if err != nil {
		Error.Fatalln("cant'r create", destinationFile)
		return err
	}

	return nil

}

func DumpData(inDirectory string, outDirectory string, databaseName string) error {

	var err error

	//create backup directory structure
	os.Mkdir(outDirectory + "/partitions", os.ModePerm)
	os.Mkdir(outDirectory + "/partitions/" + databaseName, os.ModePerm)

	os.Mkdir(outDirectory + "/metadata", os.ModePerm)
	os.Mkdir(outDirectory + "/metadata/" + databaseName, os.ModePerm)


	err = copyDirectory(inDirectory + "/shadow/1/data/" + databaseName, outDirectory + "/partitions/" + databaseName)
	if err != nil {
		return  err
	}

	err = copyDirectory(inDirectory + "/metadata/" + databaseName, outDirectory + "/metadata/" + databaseName)
	if err != nil {
		return err
	}

	return nil
}


func main() {

	Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	var (
		connectionString string
	 	inputDirectory string
		outputDirectory string
	)

	//TODO: add cleanup /var/lib/clickhouse/shadow after dump flag
	//TODO: add incremental id
	argHost := flag.String("h", "127.0.0.1", "server hostname")
	argBackup := flag.Bool("backup", false, "backup mode")
	argRestore := flag.Bool("restore", false, "restore mode")
	argDebugOn := flag.Bool("d", false, "show debug info")
	argPort := flag.String("p", "9000", "server port")
	argDatabase := flag.String("db", "", "database name")
    argNoFreeze := flag.Bool("no-freeze", false, "do not freeze, only show partitions")
	argInDirectory := flag.String("in", "", "source directory (/var/lib/clickhouse for backup mode by default)")
	argOutDirectory := flag.String("out", "", "destination directory")
	flag.Parse()

	if *argBackup && !*argRestore {

		fmt.Println("Run in backup mode")


		if *argInDirectory == "" {
			inputDirectory = "/var/lib/clickhouse"
		} else {
			inputDirectory = *argInDirectory
		}

		if *argOutDirectory == "" {
			Error.Fatalln("please set destination directory")
		} else {
			outputDirectory =  *argOutDirectory
		}

	} else if *argRestore && !*argBackup {
		fmt.Println("Run in restore mode")

	} else if !*argRestore && !*argBackup {
		fmt.Println("Choose mode (restore tor backup)")

	} else {
		Error.Fatalln("Run in only one mode (backup or restore)")

	}

	if _, err := os.Stat(inputDirectory); os.IsNotExist(err) {
		Error.Fatalln(inputDirectory, "not found")
	}

	if _, err := os.Stat(outputDirectory); os.IsNotExist(err) {
		Error.Fatalln(outputDirectory, "not found")
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
			err = DumpData(inputDirectory, outputDirectory, *argDatabase)
			if err != nil {
				Error.Fatalln("can't dump data")
			}
	}
}
