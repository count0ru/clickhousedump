package main

import (
	"fileutils"
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/kshvakov/clickhouse"
	"io/ioutil"
	logs "logging"
	"os"
	parts "partutils"
	"restore"
)

var (
	ClickhouseConnectionString string
	Version                    string
	BuildID                    string
	BuildDate                  string
)

type GetDatabasesList struct {
	Result []DataBase
}

type DataBase struct {
	Name string
}

// Get databases list from server
func (gd *GetDatabasesList) Run(databaseConnection *sqlx.DB) error {

	var (
		err       error
		databases []struct {
			DatabaseName string `db:"name"`
		}
	)

	err = databaseConnection.Select(&databases, "show databases;")
	if err != nil {
		return err
	}

	for _, item := range databases {
		gd.Result = append(gd.Result, DataBase{
			Name: item.DatabaseName,
		})
	}

	return nil

}

func main() {

	var (
		err             error
		inputDirectory  string
		outputDirectory string
	)

	logs.Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	argBackup := flag.Bool("backup", false, "backup mode")
	argRestore := flag.Bool("restore", false, "restore mode")
	argHost := flag.String("h", "127.0.0.1", "server hostname")
	argDataBase := flag.String("db", "", "database name")
	argVersion := flag.Bool("version", false, "show version")
	argNoCleanUp := flag.Bool("no-cleanup", false, "do not delete freezed partitions hardlinks after backup")
	argDebugOn := flag.Bool("d", false, "show debug info")
	argPort := flag.String("p", "9000", "server port")
	argNoFreeze := flag.Bool("no-freeze", false, "do not freeze, only show partitions")
	argInDirectory := flag.String("in", "", "source directory (/var/lib/clickhouse for backup mode by default)")
	argOutDirectory := flag.String("out", "", "destination directory")

	flag.Parse()

	ClickhouseConnectionString = "tcp://" + *argHost + ":" + *argPort + "?username=&compress=true"

	if *argVersion {
		logs.Info.Printf("version: %s", Version)
		logs.Info.Printf("build info: %s at %s", BuildID, BuildDate)
		os.Exit(0)
	}

	if *argDebugOn {
		ClickhouseConnectionString = ClickhouseConnectionString + "&debug=true"
	}

	// make connection to clickhouse server
	ClickhouseConnection, err := sqlx.Open("clickhouse", ClickhouseConnectionString)
	if err != nil {
		logs.Error.Fatalf("can't connect to clickouse server, %v", err)
	}

	defer ClickhouseConnection.Close()

	if err = ClickhouseConnection.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logs.Error.Fatalf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			logs.Error.Fatalln(err)
		}
	}

	// determine run mode
	if *argBackup && !*argRestore { //Backup mode

		logs.Info.Println("Run in backup mode")

		if *argInDirectory == "" {
			inputDirectory = "/var/lib/clickhouse"
		} else {
			inputDirectory = *argInDirectory
		}

		if *argOutDirectory == "" {
			logs.Error.Fatalln("please set destination directory")
		} else {
			outputDirectory = *argOutDirectory
		}

		err, noDirectory := fileutils.IsDirectoryInListExist(inputDirectory, outputDirectory)
		if err != nil {
			logs.Error.Fatalf("%v not found", noDirectory)
		}

		if *argDataBase == "" { //backup all databases
			// get partitions list for databases or database (--db argument)
			DatabaseList := GetDatabasesList{}
			err = DatabaseList.Run(ClickhouseConnection)
			if err != nil {
				logs.Error.Printf("can't get database list, %v", err)
			}
			for _, Database := range DatabaseList.Result {
				cmdGetPartitionsList := parts.GetPartitions{Database: Database.Name}
				err = cmdGetPartitionsList.Run(ClickhouseConnection)
				if err != nil {
					logs.Error.Printf("can't get partition list, %v", err)
				}
				cmdFreezePartitions := parts.FreezePartitions{
					Partitions:           cmdGetPartitionsList.Result,
					SourceDirectory:      inputDirectory,
					DestinationDirectory: outputDirectory,
					NoFreezeFlag:         *argNoFreeze,
				}
				err = cmdFreezePartitions.Run(ClickhouseConnection)
				if err != nil {
					logs.Error.Printf("can't freeze partition, %v", err)
				}
			}
		} else { //backup specify database
			cmdGetPartitionsList := parts.GetPartitions{Database: *argDataBase}
			err = cmdGetPartitionsList.Run(ClickhouseConnection)
			if err != nil {
				logs.Error.Printf("can't get partition list, %v", err)
			}
			cmdFreezePartitions := parts.FreezePartitions{
				Partitions:           cmdGetPartitionsList.Result,
				SourceDirectory:      inputDirectory,
				DestinationDirectory: outputDirectory,
				NoFreezeFlag:         *argNoFreeze,
			}
			err = cmdFreezePartitions.Run(ClickhouseConnection)
			if err != nil {
				logs.Error.Printf("can't freeze partition, %v", err)
			}
		}

		// clean up backup directory
		if !*argNoCleanUp {
			logs.Info.Printf("clean up %v", inputDirectory+"/shadow/backup")
			os.RemoveAll(inputDirectory + "/shadow/backup")
		}
	} else if *argRestore && !*argBackup {

		fmt.Println("Run in restore mode")

		if *argInDirectory == "" {
			logs.Error.Fatalln("please set source directory")
		} else {
			inputDirectory = *argInDirectory
		}

		if *argOutDirectory == "" {
			outputDirectory = "/var/lib/clickhouse"
		} else {
			outputDirectory = *argOutDirectory
		}

		if *argDataBase == "" {
			logs.Error.Fatalln("please set database for restore")
		}

		err, noDirectory := fileutils.IsDirectoryInListExist(inputDirectory, outputDirectory)
		if err != nil {
			logs.Error.Fatalf("%v not found", noDirectory)
		}

		cmdRestoreDatabase := restore.RestoreDatabase{
			DatabaseName:         *argDataBase,
			SourceDirectory:      inputDirectory,
			DestinationDirectory: outputDirectory,
		}
		err = cmdRestoreDatabase.Run(ClickhouseConnection)
		if err != nil {
			logs.Error.Printf("can't restore database, %v", err)
		}

	} else if !*argRestore && !*argBackup {
		fmt.Println("run with --help for help")
	} else {
		logs.Error.Fatalln("Run in only one mode (backup or restore)")
	}

}
