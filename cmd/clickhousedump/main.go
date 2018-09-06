package main

import (
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/kshvakov/clickhouse"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
)

var (
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

var (
	ClickhouseConnectionString string
	NoFreezeFlag               bool
)

type partitionDescribe struct {
	databaseName string
	tableName    string
	partID       string
}

type dataBase struct {
	name string
}

type GetPartitions struct {
	Database string
	Result   []partitionDescribe
}

type FreezePartitions struct {
	Partitions           []partitionDescribe
	SourceDirectory      string
	DestinationDirectory string
}

type restoreDatabase struct {
	DatabaseName         string
	SourceDirectory      string
	DestinationDirectory string
}

type GetDatabasesList struct {
	Result []dataBase
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

// Recursive copy directory and files
func copyDirectory(sourceDirectory string, destinationDirectory string) error {

	var (
		err             error
		fileDescriptors []os.FileInfo
		sourceInfo      os.FileInfo
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

// Copy files
func copyFile(sourceFile string, destinationFile string) error {

	var err error

	fromFile, err := os.Open(sourceFile)
	if err != nil {
		return err
	}

	defer fromFile.Close()

	toFile, err := os.OpenFile(destinationFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer toFile.Close()

	_, err = io.Copy(toFile, fromFile)
	if err != nil {
		return err
	}

	return nil
}

// Replace string in all files in directory
func replaceStringInDirectoryFiles(filesPath string, oldString string, newString string) error {
	var (
		err             error
		fileDescriptors []os.FileInfo
	)

	if fileDescriptors, err = ioutil.ReadDir(filesPath); err != nil {
		return err
	}

	for _, fileDescriptor := range fileDescriptors {
		if !fileDescriptor.IsDir() && strings.HasSuffix(fileDescriptor.Name(), ".sql") {

			fileContent, err := ioutil.ReadFile(filesPath + "/" + fileDescriptor.Name())
			if err != nil {
				return err
			}

			newContent := strings.Replace(
				string(fileContent),
				oldString,
				newString,
				-1,
			)

			err = ioutil.WriteFile(filesPath+"/"+fileDescriptor.Name(), []byte(newContent), 0)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Create list of directories
func createDirectories(directoriesList []string) (error, string) {
	for _, currentDirectory := range directoriesList {
		directoryExists, err := isExists(currentDirectory)
		if err != nil {
			if !directoryExists {
				err := os.Mkdir(currentDirectory, os.ModePerm)
				if err != nil {
					return err, currentDirectory
				}
			}
		}
	}
	return nil, ""
}

// Check single directory or file exist
func isExists(filePath string) (bool, error) {
	var err error
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false, err
	}
	return true, err
}

// Check partition exist in partition list
func isPartExists(currentPartitions []partitionDescribe, newPart partitionDescribe) bool {
	for _, partitionID := range currentPartitions {
		if partitionID.partID == newPart.partID {
			return true
		}

	}
	return false
}

// Get partition list from directory with parts
func getPartitionsListFromDir(sourceDirectory string, destinationDirectory string, databaseName string, tableName string) ([]partitionDescribe, error) {

	var (
		err     error
		partsFD []os.FileInfo
		result  []partitionDescribe
	)

	Info.Println(sourceDirectory + "/partitions/" + databaseName + "/" + tableName)
	if partsFD, err = ioutil.ReadDir(sourceDirectory + "/partitions/" + databaseName + "/" + tableName); err != nil {
		Info.Println(err)
	}
	for _, partDescriptor := range partsFD {
		if partDescriptor.IsDir() && partDescriptor.Name() != "detached" {

			// copy partition files to detached  directory
			Info.Printf("copy partition from %v to %v",
				sourceDirectory+"/partitions/"+databaseName+"/"+tableName,
				destinationDirectory+"/data/"+databaseName+"/"+tableName+"/detached")
			err = copyDirectory(
				sourceDirectory+"/partitions/"+databaseName+"/"+tableName,
				destinationDirectory+"/data/"+databaseName+"/"+tableName+"/detached")
			if err != nil {
				return result, err
			}
			// append partition to result part list
			if !isPartExists(result,
				partitionDescribe{
					databaseName: databaseName,
					tableName:    tableName,
					partID:       partDescriptor.Name()[:6],
				}) {
				result = append(result,
					partitionDescribe{
						databaseName: databaseName,
						tableName:    tableName,
						partID:       partDescriptor.Name()[:6],
					})
			}
		}
	}

	return result, nil

}

// Check directory list is exist
func isDirectoryInListExist(directoriesList ...string) (error, string) {
	Info.Println(directoriesList)
	for _, currentDirectory := range directoriesList {
		_, err := isExists(currentDirectory)
		if err != nil {
			return err, currentDirectory
		}
	}
	return nil, ""
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
		gd.Result = append(gd.Result, dataBase{
			name: item.DatabaseName,
		})
	}

	return nil

}

// Freeze partitions and create hardlink in $CLICKHOUSE_DIRECTORY/shadow
func (fz *FreezePartitions) Run(databaseConnection *sqlx.DB) error {

	for _, partition := range fz.Partitions {

		if NoFreezeFlag {
			Info.Printf("ALTER TABLE %v.%v FREEZE PARTITION '%v' WITH NAME 'backup';",
				partition.databaseName,
				partition.tableName,
				partition.partID,
			)
		} else {

			// freeze partitions
			_, err := databaseConnection.Exec(
				fmt.Sprintf(
					"ALTER TABLE %v.%v FREEZE PARTITION '%v' WITH NAME 'backup';",
					partition.databaseName,
					partition.tableName,
					partition.partID,
				))
			if err != nil {
				return err
			}

			// copy partition files and metadata
			inDirectory := fz.SourceDirectory
			outDirectory := fz.DestinationDirectory

			directoryList := []string{
				outDirectory + "/partitions",
				outDirectory + "/partitions/" + partition.databaseName,
				outDirectory + "/metadata",
				outDirectory + "/metadata/" + partition.databaseName,
			}

			err, failDirectory := createDirectories(directoryList)
			if err != nil {
				Error.Printf("can't create directory: %v", failDirectory)
				return err
			}

			// copy partition files
			Info.Printf("copy data from %v to %v",
				inDirectory+"/shadow/backup/data/"+partition.databaseName,
				outDirectory+"/partitions/"+partition.databaseName)
			err = copyDirectory(
				inDirectory+"/shadow/backup/data/"+partition.databaseName,
				outDirectory+"/partitions/"+partition.databaseName)
			if err != nil {
				return err
			}

			// copy metadata files
			Info.Printf("copy data from %v to %v",
				inDirectory+"/metadata/"+partition.databaseName,
				outDirectory+"/metadata/"+partition.databaseName)
			err = copyDirectory(
				inDirectory+"/metadata/"+partition.databaseName,
				outDirectory+"/metadata/"+partition.databaseName)
			if err != nil {
				return err
			}

			// replace ATTACH TABLE to CREATE TABLE in metadata files
			err = replaceStringInDirectoryFiles(
				outDirectory+"/metadata/"+partition.databaseName,
				"ATTACH",
				"CREATE",
			)
			if err != nil {
				Error.Printf("can't replace string in metadata files, %v", err)
			}
		}
	}

	return nil

}

// Restore database
func (rb *restoreDatabase) Run(databaseConnection *sqlx.DB) error {

	type metadataFiles struct {
		fileName,
		objectName,
		objectType,
		metaData string
	}
	var (
		err             error
		fileDescriptors []os.FileInfo
		metaFiles       []metadataFiles
	)

	Info.Printf("try to create database %v", rb.DatabaseName)
	_, err = databaseConnection.Exec(fmt.Sprintf("CREATE DATABASE %v", rb.DatabaseName))
	if err != nil {
		Error.Printf("failed to create database %v", rb.DatabaseName)
		return err
	} else {
		Info.Println("success")
	}

	if fileDescriptors, err = ioutil.ReadDir(rb.SourceDirectory + "/metadata/" + rb.DatabaseName); err != nil {
		return err
	}
	if err != nil {
		Error.Printf("can't replace string in metadata files, %v", err)
	} else {
		Info.Println("success")
	}

	for _, fileDescriptor := range fileDescriptors {
		if !fileDescriptor.IsDir() && strings.HasSuffix(fileDescriptor.Name(), ".sql") {

			Info.Printf("try to read from metadata file %v", fileDescriptor.Name())
			fileContent, err := ioutil.ReadFile(rb.SourceDirectory + "/metadata/" + rb.DatabaseName + "/" + fileDescriptor.Name())
			if err != nil {
				Info.Printf("cant't read from metadata file %v", fileDescriptor.Name())
				return err
			} else {
				Info.Println("success")
				if strings.HasPrefix(string(fileContent[:]), "CREATE TABLE") { // if object is TABLE
					metaFiles = append(metaFiles, metadataFiles{
						fileDescriptor.Name(),
						strings.Replace(fileDescriptor.Name(), ".sql", "", -1),
						"table",
						string(fileContent[:]),
					})
				} else if strings.HasPrefix(string(fileContent[:]), "CREATE MATERIALIZED VIEW") { // if object is view
					metaFiles = append(metaFiles, metadataFiles{
						fileDescriptor.Name(),
						strings.Replace(fileDescriptor.Name(), ".sql", "", -1),
						"view",
						string(fileContent[:]),
					})
				} else { // is other type object
					metaFiles = append(metaFiles, metadataFiles{
						fileDescriptor.Name(),
						strings.Replace(fileDescriptor.Name(), ".sql", "", -1),
						"other",
						string(fileContent[:]),
					})
				}

			}
		}
	}

	// create only tables first
	for _, metadataFile := range metaFiles {
		if metadataFile.objectType == "table" {
			Info.Printf("try to apply metadata from file %v", metadataFile.fileName)
			_, err = databaseConnection.Exec(
				strings.Replace(
					metadataFile.metaData,
					"CREATE TABLE ",
					"CREATE TABLE "+rb.DatabaseName+".", -1))
			if err != nil {
				Info.Printf("cant't apply metadata file %v", metadataFile.fileName)
				return err
			} else {
				Info.Println("success")
			}

			Info.Printf("try to attach partitions for %v", rb.DatabaseName+"."+metadataFile.fileName)
			partitionsList, err := getPartitionsListFromDir(
				rb.SourceDirectory,
				rb.DestinationDirectory,
				rb.DatabaseName,
				metadataFile.objectName,
			)
			if err != nil {
				Info.Printf("cant't get partitions list for attach from backup directory for table %v.%v", rb.DatabaseName, metadataFile.objectName)
				return err
			} else {
				Info.Println("success")
			}

			for _, attachedPart := range partitionsList {
				// attach partition
				Info.Printf("ALTER TABLE %v.%v ATTACH PARTITION '%v'",
					attachedPart.databaseName,
					attachedPart.tableName,
					attachedPart.partID)
				_, err = databaseConnection.Exec(
					"ALTER TABLE " + attachedPart.databaseName + "." + attachedPart.tableName + " ATTACH PARTITION '" + attachedPart.partID + "';")
				if err != nil {
					Info.Printf("cant't attach partition %v to %v table in %v database, %v",
						attachedPart.partID,
						attachedPart.tableName,
						attachedPart.databaseName, err)
					return err
				} else {
					Info.Println("success")
				}
			}
		}
	}
	// create another objects
	for _, metadataFile := range metaFiles {
		if metadataFile.objectType != "table" {
			Info.Printf("try to apply metadata from file %v", metadataFile.fileName)
			_, err = databaseConnection.Exec(
				strings.Replace(
					metadataFile.metaData,
					metadataFile.objectName,
					rb.DatabaseName+"."+metadataFile.objectName, -1))
			if err != nil {
				Info.Printf("cant't apply metadata file %v", metadataFile.fileName)
				return err
			} else {
				Info.Println("success")
			}
		}
	}

	return nil

}

// Get list of partitions for tables
func (gp *GetPartitions) Run(databaseConnection *sqlx.DB) error {

	var (
		err        error
		partitions []struct {
			Partition string `db:"partition"`
			Table     string `db:"table"`
			Database  string `db:"database"`
		}
	)

	err = databaseConnection.Select(&partitions,
		fmt.Sprintf("select "+
			"DISTINCT partition, "+
			"table, "+
			"database "+
			"FROM system.parts WHERE active AND database ='%v';", gp.Database))
	if err != nil {
		return err
	}

	for _, item := range partitions {
		if !strings.HasPrefix(item.Table, ".") {
			Info.Printf("found %v partition of %v table in %v database", item.Partition, item.Table, item.Database)
			gp.Result = append(gp.Result, partitionDescribe{
				partID:       item.Partition,
				tableName:    item.Table,
				databaseName: item.Database,
			})
		}
	}

	return nil

}

func main() {

	var (
		err             error
		inputDirectory  string
		outputDirectory string
	)

	Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	argBackup := flag.Bool("backup", false, "backup mode")
	argRestore := flag.Bool("restore", false, "restore mode")
	argHost := flag.String("h", "127.0.0.1", "server hostname")
	argDataBase := flag.String("db", "", "database name")
	argDebugOn := flag.Bool("d", false, "show debug info")
	argPort := flag.String("p", "9000", "server port")
	argNoFreeze := flag.Bool("no-freeze", false, "do not freeze, only show partitions")
	argInDirectory := flag.String("in", "", "source directory (/var/lib/clickhouse for backup mode by default)")
	argOutDirectory := flag.String("out", "", "destination directory")

	flag.Parse()

	NoFreezeFlag = *argNoFreeze
	ClickhouseConnectionString = "tcp://" + *argHost + ":" + *argPort + "?username=&compress=true"

	if *argDebugOn {
		ClickhouseConnectionString = ClickhouseConnectionString + "&debug=true"
	}

	// make connection to clickhouse server
	clickhouseConnection, err := sqlx.Open("clickhouse", ClickhouseConnectionString)
	if err != nil {
		Error.Fatalf("can't connect to clickouse server, %v", err)
	}

	defer clickhouseConnection.Close()

	if err = clickhouseConnection.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			Error.Fatalf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			Error.Fatalln(err)
		}
	}

	// determine run mode
	if *argBackup && !*argRestore { //Backup mode

		Info.Println("Run in backup mode")

		if *argInDirectory == "" {
			inputDirectory = "/var/lib/clickhouse"
		} else {
			inputDirectory = *argInDirectory
		}

		if *argOutDirectory == "" {
			Error.Fatalln("please set destination directory")
		} else {
			outputDirectory = *argOutDirectory
		}

		err, noDirectory := isDirectoryInListExist(inputDirectory, outputDirectory)
		if err != nil {
			Error.Fatalf("%v not found", noDirectory)
		}

		var partitionsList []partitionDescribe

		// get partitions list for databases or database (--db argument)
		if *argDataBase == "" {
			databaseList := GetDatabasesList{}
			err = databaseList.Run(clickhouseConnection)
			if err != nil {
				Error.Printf("can't get database list, %v", err)
			}
			for _, database := range databaseList.Result {
				cmdGetPartitionsList := GetPartitions{Database: database.name}
				err = cmdGetPartitionsList.Run(clickhouseConnection)
				if err != nil {
					Error.Printf("can't get partition list, %v", err)
				}
				partitionsList = cmdGetPartitionsList.Result
			}
		} else {
			cmdGetPartitionsList := GetPartitions{Database: *argDataBase}
			err = cmdGetPartitionsList.Run(clickhouseConnection)
			if err != nil {
				Error.Printf("can't get partition list, %v", err)
			}
			partitionsList = cmdGetPartitionsList.Result
		}

		cmdFreezePartitions := FreezePartitions{
			Partitions:           partitionsList,
			SourceDirectory:      inputDirectory,
			DestinationDirectory: outputDirectory,
		}
		err = cmdFreezePartitions.Run(clickhouseConnection)
		if err != nil {
			Error.Printf("can't freeze partition, %v", err)
		}
	} else if *argRestore && !*argBackup {

		fmt.Println("Run in restore mode")

		if *argInDirectory == "" {
			Error.Fatalln("please set source directory")
		} else {
			inputDirectory = *argInDirectory
		}

		if *argOutDirectory == "" {
			outputDirectory = "/var/lib/clickhouse"
		} else {
			outputDirectory = *argOutDirectory
		}

		if *argDataBase == "" {
			Error.Fatalln("please set database for restore")
		}

		err, noDirectory := isDirectoryInListExist(inputDirectory, outputDirectory)
		if err != nil {
			Error.Fatalf("%v not found", noDirectory)
		}

		cmdRestoreDatabase := restoreDatabase{
			*argDataBase,
			inputDirectory,
			outputDirectory,
		}
		err = cmdRestoreDatabase.Run(clickhouseConnection)
		if err != nil {
			Error.Printf("can't restore database, %v", err)
		}

	} else if !*argRestore && !*argBackup {
		fmt.Println("Choose mode (restore tor backup)")

	} else {
		Error.Fatalln("Run in only one mode (backup or restore)")
	}

	fmt.Println("done")
}
