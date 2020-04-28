// -*- tab-width: 2 -*-

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

func readWillDeleteFile(filename string) (*[]string, error) {

	var filter []string = make([]string, 0, 64)
	if len(filename) == 0 {
		fmt.Println("No will delete file specified, using empty", filename)
		return &filter, nil
	}
	binaryFilename, err := os.Executable()
	if err != nil {
		panic(err)
	}
	filePath := path.Join(path.Dir(binaryFilename), filename)
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Warning: can't open will delete file, using empty,",
			filename, filePath, err.Error())
		return &filter, err
	}
	fmt.Println("Using will delete file", filePath)
	defer file.Close()
	fileReader := bufio.NewReader(file)
	err = addWillDeleteListFromReader(fileReader, &filter)
	return &filter, nil
}

func addWillDeleteListFromReader(reader io.Reader, filter *[]string) error {

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if err := scanner.Err(); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading will delete list", err)
			return err
		}
		if len(line) > 0 && line[:1] == "#" { // TODO  space space #
			continue
		}
		value := strings.TrimSpace(line)
		if len(value) > 0 { // don't allow '' or it will match everything
			fmt.Println("Adding will delete entry", value)
			*filter = append(*filter, value)
		}
	}
	return nil
}

// filterObjectPasses returns true if the object can be removed
func filterObjectPasses(b string, k string, filter *[]string) bool {

	if !theConfig["oneBucket"].BoolVal {
		return false // never delete when we are scanning all the buckets
	}

	// wildcard
	if theConfig["listFilesMatchingPrefix"].StrVal == "*" &&
		len(theConfig["ListFilesMatchingExclude"].StrVal) > 0 &&
		!strings.Contains(k, theConfig["ListFilesMatchingExclude"].StrVal) {
		return true // only that special case of matching
	}

	// now tickier cases
	// 1.  In matching files but not in exclude list
	if strings.HasPrefix(k, theConfig["listFilesMatchingPrefix"].StrVal) &&
		len(theConfig["ListFilesMatchingExclude"].StrVal) > 0 &&
		!strings.Contains(k, theConfig["listFilesMatchingExclude"].StrVal) { // but never if the exclude thing matches

		return true
	}
	if filter != nil {
		for _, s := range *filter {
			if strings.HasPrefix(k, s) {
				return true // so delete all the things in the delete file
			}
		}
	}
	return false
}
