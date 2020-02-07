// -*- tab-width: 2 -*-

package main

import (
	"encoding/csv"
	"os"
)

// ReadCsv accepts a file and returns its content as a multi-dimentional type
// with lines and each column. Only parses to string type.
func readCsv(filename string) (*map[string]string, error) {

	// Open CSV file
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)

	for _, l := range lines {
		m[l[0]] = l[1]
	}
	return &m, nil
}
