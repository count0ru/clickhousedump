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

//check directory is exist
func isDirectoryExist( directoriesList ...string) (error, string) {

	for _, currentDirectory := range directoriesList {
		if _, err := os.Stat(currentDirectory); os.IsNotExist(err) {
			return err, currentDirectory
		}
	}
	return nil, ""
}


// make partitions hardlinks after freeze
func freezePartitions(clickhouseConnection *sql.DB, databaseName string, showPartitionsOnly bool) error {

	var (
		databasePartitions []partitionDescribe
		err error
	)

	currentPartitions, err := clickhouseConnection.Query("select partition, table, database FROM system.parts WHERE active AND database ='" + databaseName + "';")
	if err != nil {
		Error.Printf("can't get partitions list for database %v", databaseName)
	}

	Info.Printf("searching partitions for %v database", databaseName)

	for currentPartitions.Next() {

		var resPartID, resTable, resDatabase string

		if err := currentPartitions.Scan(&resPartID, &resTable, &resDatabase); err != nil {
			Error.Printf("can't get partitions IDs: %v", err)
			return err
		}

		if !strings.HasPrefix(resTable, ".") {
			partition := partitionDescribe{
				partID:       resPartID,
				tableName:    resTable,
				databaseName: resDatabase,
			}

			Info.Println(partition.partID, "partition  of table", partition.tableName, "found for", partition.databaseName)

			databasePartitions = append(databasePartitions, partition)

			if showPartitionsOnly {
				Info.Printf("ALTER TABLE %v.%v FREEZE PARTITION '%v';", partition.databaseName, partition.tableName, partition.partID)
			} else {
				_, err = clickhouseConnection.Exec("ALTER TABLE " + partition.databaseName + "." + partition.tableName + " FREEZE PARTITION '" + partition.partID + "';")
				if err != nil {
					Error.Printf("can't execute partition freeze query: %v", err)
				}
			}

		}

	}

	return nil
}

// recursive copy directory and files
func copyDirectory(sourceDirectory string, destinationDirectory string) error {

	var (
		err error
	    fileDescriptors []os.FileInfo
		sourceInfo os.FileInfo
	)

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

// copy files
func copyFile(sourceFile string, destinationFile string) error {

	input, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		Error.Fatalf("cant't open file: %v", sourceFile)
		return err
	}

	err = ioutil.WriteFile(destinationFile, input, 0644)
	if err != nil {
		Error.Fatalf("cant'r create: %v", destinationFile)
		return err
	}

	return nil

}

// create backup for specify database or for all databases (w/o system)
func makeBackup(
	clickhouseConnection *sql.DB,
	inDirectory string,
	outDirectory string,
	databaseName string,
	noFreezeFlag bool,
	) error {

	var err error

	if databaseName	== "" {
		databaseList, err := clickhouseConnection.Query("SHOW DATABASES;")
		if err != nil {
			Error.Fatalf("can't get databases list: %v", err)
		}

		for databaseList.Next() {

			var resDatabase string

			if err := databaseList.Scan(&resDatabase); err != nil {
				Error.Printf("can't get partitions IDs, %v", err)
				return err
			}

			err = freezePartitions(clickhouseConnection, resDatabase, noFreezeFlag)
			if err != nil {
				Error.Printf("can't freeze partitions: %v", err)
				return err
			}
			if !noFreezeFlag {
				err = dumpData(inDirectory, outDirectory, databaseName)
				if err != nil {
					Error.Printf("can't dump data: %v", err)
					return err
				}
			}
		}
	} else {

			err = freezePartitions(clickhouseConnection, databaseName, noFreezeFlag)
			if err != nil {
				Error.Printf("can't freeze partitions %v", err)
				return err
			}

			if !noFreezeFlag {
				err = dumpData(inDirectory, outDirectory, databaseName)
				if err != nil {
					Error.Printf("can't dump data: %v", err)
					return err
				}
			}
	}

	return nil
}

// create backup directory structure and copy metadata files and freezed partitions from clickhouse_dir/shadow
func createDirectories(directoriesList []string) (error, string) {

	for _, currentDirectory := range directoriesList {
		err := os.Mkdir(currentDirectory, os.ModePerm)
		if err != nil {
			return err, currentDirectory
		}
	}
	return nil, ""

}

func dumpData(inDirectory string, outDirectory string, databaseName string) error {

	var  (
	err error
	directoryList = []string{
		outDirectory + "/partitions",
		outDirectory + "/partitions/" + databaseName,
		outDirectory + "/metadata",
		outDirectory + "/metadata/" + databaseName,
		}
	)

	err, failDirectory := createDirectories(directoryList)
	if err != nil {
		Error.Printf("can't create directory: %v", failDirectory)
	}


	err = copyDirectory(inDirectory + "/shadow/1/data/" + databaseName, outDirectory + "/partitions/" + databaseName)
	if err != nil {
		Info.Println(inDirectory + "/shadow/1/data/" + databaseName, outDirectory + "/partitions/" + databaseName)
		return  err
	}

	err = copyDirectory(inDirectory + "/metadata/" + databaseName, outDirectory + "/metadata/" + databaseName)
	if err != nil {
		Info.Println(inDirectory + "/metadata/" + databaseName, outDirectory + "/metadata/" + databaseName)
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
		err error
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

	connectionString = "tcp://" + *argHost + ":" + *argPort +"?username=&compress=true"

	if *argDebugOn {
		connectionString = connectionString + "&debug=true"
	}

	connect, err := sql.Open("clickhouse", connectionString)
	if err != nil {
		Error.Println("can't connect to server")
	}

	if err = connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			Error.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			Error.Println(err)
		}
	}


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

		err, noDirectory := isDirectoryExist(inputDirectory, outputDirectory)
		if err != nil {
			Error.Fatalln(noDirectory,"not found")
		}

		makeBackup(connect, inputDirectory, outputDirectory, *argDatabase, *argNoFreeze)

	} else if *argRestore && !*argBackup {
		fmt.Println("Run in restore mode")

	} else if !*argRestore && !*argBackup {
		fmt.Println("Choose mode (restore tor backup)")

	} else {
		Error.Fatalln("Run in only one mode (backup or restore)")

	}

}
