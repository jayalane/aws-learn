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

func readFilter(filename string) (*[]string, error) {

	var filter []string = make([]string, 0, 64)
	if len(filename) == 0 {
		fmt.Println("No config file specified, using empty", filename)
		return &filter, nil
	}
	binaryFilename, err := os.Executable()
	if err != nil {
		panic(err)
	}
	filePath := path.Join(path.Dir(binaryFilename), filename)
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Warning: can't open filter file, using empty,",
			filename, filePath, err.Error())
		return &filter, err
	}
	fmt.Println("Using filter file", filePath)
	defer file.Close()
	fileReader := bufio.NewReader(file)
	err = addFilterFromReader(fileReader, &filter)
	return &filter, nil
}

func addFilterFromReader(reader io.Reader, filter *[]string) error {

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if err := scanner.Err(); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading filter", err)
			return err
		}
		if len(line) > 0 && line[:1] == "#" { // TODO  space space #
			continue
		}
		value := strings.TrimSpace(line)
		fmt.Println("Adding filter", line)
		*filter = append(*filter, value)
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
		!strings.Contains(k, theConfig["listFilesMatchingExclude"].StrVal) { // but never if the exclude thing ma}

		return true
	}
	if filter != nil {
		for _, s := range *filter {
			if strings.Contains(k, s) {
				fmt.Println("Filter matches", s, k)
				return true // so delete all the filter paths
			}
		}
	}
	return false
}
