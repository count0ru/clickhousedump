package partutils

import (
	"clickhousedump/pkg/fileutils"
	logs "clickhousedump/pkg/logging"
	"fmt"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"os"
	"strings"
)

var (
	NoFreezeFlag               bool
)

type PartitionDescribe struct {
	DatabaseName string
	TableName    string
	PartID       string
}

type GetPartitionsListFromDir struct {
	SourceDirectory      string
	DestinationDirectory string
	DatabaseName         string
	TableName            string
	Result               []PartitionDescribe
}


type GetPartitions struct {
	Database string
	Result   []PartitionDescribe
}

type FreezePartitions struct {
	Partitions           []PartitionDescribe
	SourceDirectory      string
	DestinationDirectory string
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
			logs.Info.Printf("found %v partition of %v table in %v database", item.Partition, item.Table, item.Database)
			gp.Result = append(gp.Result, PartitionDescribe{
				PartID:       item.Partition,
				TableName:    item.Table,
				DatabaseName: item.Database,
			})
		}
	}

	return nil

}

// Check partition exist in partition list
func IsPartExists(currentPartitions []PartitionDescribe, newPart PartitionDescribe) bool {
	for _, partitionID := range currentPartitions {
		if partitionID.PartID == newPart.PartID {
			return true
		}

	}
	return false
}

// Get partition list from directory with parts
func (gl *GetPartitionsListFromDir) Run() error {
	var (
		err     error
		partsFD []os.FileInfo
		result  []PartitionDescribe
	)

	logs.Info.Println(gl.SourceDirectory + "/partitions/" + gl.DatabaseName + "/" + gl.TableName)
	if partsFD, err = ioutil.ReadDir(gl.SourceDirectory + "/partitions/" + gl.DatabaseName + "/" + gl.TableName); err != nil {
		logs.Info.Println(err)
	}
	for _, partDescriptor := range partsFD {
		if partDescriptor.IsDir() && partDescriptor.Name() != "detached" {

			// copy partition files to detached  directory
			logs.Info.Printf("copy partition from %v to %v",
				gl.SourceDirectory+"/partitions/"+gl.DatabaseName+"/"+gl.TableName,
				gl.DestinationDirectory+"/data/"+gl.DatabaseName+"/"+gl.TableName+"/detached")
			err = fileutils.CopyDirectory(
				gl.SourceDirectory+"/partitions/"+gl.DatabaseName+"/"+gl.TableName,
				gl.DestinationDirectory+"/data/"+gl.DatabaseName+"/"+gl.TableName+"/detached")
			if err != nil {
				gl.Result = result
				return err
			}
			// append partition to result part list
			if !IsPartExists(result,
				PartitionDescribe{
					DatabaseName: gl.DatabaseName,
					TableName:    gl.TableName,
					PartID:       partDescriptor.Name()[:6],
				}) {
				result = append(result,
					PartitionDescribe{
						DatabaseName: gl.DatabaseName,
						TableName:    gl.TableName,
						PartID:       partDescriptor.Name()[:6],
					})
			}
		}
	}

	gl.Result = result
	return nil

}

// Freeze partitions and create hardlink in $CLICKHOUSE_DIRECTORY/shadow
func (fz *FreezePartitions) Run(databaseConnection *sqlx.DB) error {

	for _, partition := range fz.Partitions {

		if NoFreezeFlag {
			logs.Info.Printf("ALTER TABLE %v.%v FREEZE PARTITION '%v' WITH NAME 'backup';",
				partition.DatabaseName,
				partition.TableName,
				partition.PartID,
			)
		} else {

			// freeze partitions
			_, err := databaseConnection.Exec(
				fmt.Sprintf(
					"ALTER TABLE %v.%v FREEZE PARTITION '%v' WITH NAME 'backup';",
					partition.DatabaseName,
					partition.TableName,
					partition.PartID,
				))
			if err != nil {
				return err
			}

			// copy partition files and metadata
			inDirectory := fz.SourceDirectory
			outDirectory := fz.DestinationDirectory

			directoryList := []string{
				outDirectory + "/partitions",
				outDirectory + "/partitions/" + partition.DatabaseName,
				outDirectory + "/metadata",
				outDirectory + "/metadata/" + partition.DatabaseName,
			}

			err, failDirectory := fileutils.CreateDirectories(directoryList)
			if err != nil {
				logs.Error.Printf("can't create directory: %v", failDirectory)
				return err
			}

			// copy partition files
			logs.Info.Printf("copy data from %v to %v",
				inDirectory+"/shadow/backup/data/"+partition.DatabaseName,
				outDirectory+"/partitions/"+partition.DatabaseName)
			err = fileutils.CopyDirectory(
				inDirectory+"/shadow/backup/data/"+partition.DatabaseName,
				outDirectory+"/partitions/"+partition.DatabaseName)
			if err != nil {
				return err
			}

			// copy metadata files
			logs.Info.Printf("copy data from %v to %v",
				inDirectory+"/metadata/"+partition.DatabaseName,
				outDirectory+"/metadata/"+partition.DatabaseName)
			err = fileutils.CopyDirectory(
				inDirectory+"/metadata/"+partition.DatabaseName,
				outDirectory+"/metadata/"+partition.DatabaseName)
			if err != nil {
				return err
			}

			// replace ATTACH TABLE to CREATE TABLE in metadata files
			err = fileutils.ReplaceStringInDirectoryFiles(
				outDirectory+"/metadata/"+partition.DatabaseName,
				"ATTACH",
				"CREATE",
			)
			if err != nil {
				logs.Error.Printf("can't replace string in metadata files, %v", err)
			}
		}
	}

	return nil

}
