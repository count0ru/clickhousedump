package fileutils

import (
	"io"
	"io/ioutil"
	logs "logging"
	"os"
	"path"
	"strings"
)

// Recursive copy directory and files
func CopyDirectory(sourceDirectory string, destinationDirectory string) error {

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
		if !strings.HasPrefix(fileDescriptor.Name(), "%2Einner%2E") {
			sourcePath := path.Join(sourceDirectory, fileDescriptor.Name())
			destinationPath := path.Join(destinationDirectory, fileDescriptor.Name())
			if fileDescriptor.IsDir() {
				if err = CopyDirectory(sourcePath, destinationPath); err != nil {
					logs.Error.Fatalln(err)
				}
			} else {
				if err = CopyFile(sourcePath, destinationPath); err != nil {
					logs.Error.Fatalln(err)
				}
			}
		}
	}
	return nil
}

// Copy files
func CopyFile(sourceFile string, destinationFile string) error {
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
func ReplaceStringInDirectoryFiles(filesPath string, oldString string, newString string) error {
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

// Check directory list is exist
func IsDirectoryInListExist(directoriesList ...string) (error, string) {
	for _, currentDirectory := range directoriesList {
		_, err := IsExists(currentDirectory)
		if err != nil {
			return err, currentDirectory
		}
	}
	return nil, ""
}

// Create list of directories
func CreateDirectories(directoriesList []string) (error, string) {
	for _, currentDirectory := range directoriesList {
		directoryExists, err := IsExists(currentDirectory)
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
func IsExists(filePath string) (bool, error) {
	var err error
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false, err
	}
	return true, err
}
