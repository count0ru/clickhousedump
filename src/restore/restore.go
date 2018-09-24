package restore

import (
	"fileutils"
	"fmt"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	logs "logging"
	"os"
	parts "partutils"
	"strings"
)

type RestoreDatabase struct {
	DatabaseName         string
	SourceDirectory      string
	DestinationDirectory string
}

// Restore database
func (rb *RestoreDatabase) Run(databaseConnection *sqlx.DB) error {

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

	logs.Info.Printf("try to create database %v", rb.DatabaseName)
	_, err = databaseConnection.Exec(fmt.Sprintf("CREATE DATABASE %v", rb.DatabaseName))
	if err != nil {
		logs.Error.Printf("failed to create database %v", rb.DatabaseName)
		return err
	} else {
		logs.Info.Println("success")
	}

	if fileDescriptors, err = ioutil.ReadDir(rb.SourceDirectory + "/metadata/" + rb.DatabaseName); err != nil {
		return err
	}
	if err != nil {
		logs.Error.Printf("can't replace string in metadata files, %v", err)
	} else {
		logs.Info.Println("success")
	}

	for _, fileDescriptor := range fileDescriptors {
		if !fileDescriptor.IsDir() && (strings.HasSuffix(fileDescriptor.Name(), ".sql") || !strings.HasSuffix(fileDescriptor.Name(), "%2E")) {

			logs.Info.Printf("try to read from metadata file %v", fileDescriptor.Name())
			fileContent, err := ioutil.ReadFile(rb.SourceDirectory + "/metadata/" + rb.DatabaseName + "/" + fileDescriptor.Name())
			if err != nil {
				logs.Info.Printf("cant't read from metadata file %v", fileDescriptor.Name())
				return err
			} else {
				logs.Info.Println("success")
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
			logs.Info.Printf("try to apply metadata from file %v", metadataFile.fileName)
			_, err = databaseConnection.Exec(
				strings.Replace(
					metadataFile.metaData,
					"CREATE TABLE ",
					"CREATE TABLE "+rb.DatabaseName+".", -1))
			if err != nil {
				logs.Info.Printf("cant't apply metadata file %v", metadataFile.fileName)
				return err
			} else {
				logs.Info.Println("success")
			}

			logs.Info.Printf("try to attach partitions for %v", rb.DatabaseName+"."+metadataFile.fileName)
			cmdGetPartitionsListFromDir := parts.GetPartitionsListFromDir{
				SourceDirectory:      rb.SourceDirectory,
				DestinationDirectory: rb.DestinationDirectory,
				DatabaseName:         rb.DatabaseName,
				TableName:            metadataFile.objectName,
			}
			err = cmdGetPartitionsListFromDir.Run()
			if err != nil {
				logs.Error.Printf("can't get partition list for attach, %v", err)
			}
			partitionsList := cmdGetPartitionsListFromDir.Result
			for _, attachedPart := range partitionsList {
				// attach partition
				logs.Info.Printf("ALTER TABLE %v.%v ATTACH PARTITION '%v'",
					attachedPart.DatabaseName,
					attachedPart.TableName,
					attachedPart.PartID)
				_, err = databaseConnection.Exec(
					"ALTER TABLE " + attachedPart.DatabaseName + "." + attachedPart.TableName + " ATTACH PARTITION '" + attachedPart.PartID + "';")
				if err != nil {
					logs.Info.Printf("cant't attach partition %v to %v table in %v database, %v",
						attachedPart.PartID,
						attachedPart.TableName,
						attachedPart.DatabaseName, err)
					return err
				} else {
					logs.Info.Println("success")
				}
			}
		}
	}
	// create another objects
	for _, metadataFile := range metaFiles {
		if metadataFile.objectType != "table" {
			logs.Info.Printf("try to apply metadata from file %v", metadataFile.fileName)
			_, err = databaseConnection.Exec(
				strings.Replace(
					metadataFile.metaData,
					metadataFile.objectName,
					rb.DatabaseName+"."+metadataFile.objectName, -1))
			if err != nil {
				logs.Info.Printf("cant't apply metadata file %v", metadataFile.fileName)
				return err
			} else {
				logs.Info.Println("success")
			}
		}
	}

	return nil

}
