// -*- tab-width: 2 -*-

package main

import (
	"bufio"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
)

const (
	scannerBufSize = 1024 * 1024 // 1MB max line length for wide TSVs
	perCent        = 100
)

// findFieldIndex looks for fieldName in a tab-separated header line.
// Returns the index if found, or -1.
func findFieldIndex(headerLine string, fieldName string) int {
	for i, col := range strings.Split(headerLine, "\t") {
		if strings.EqualFold(strings.TrimSpace(col), fieldName) {
			return i
		}
	}

	return -1
}

// streamAndCountNulls streams an S3 object line by line as a TSV,
// counting how many times the field at fieldIndex is NULL vs non-NULL.
// If fieldName is non-empty, the first line is treated as a header
// and used to look up the column index by name.
func streamAndCountNulls(bucket, key string, fieldIndex int, fieldName string, sess *session.Session) { //nolint:cyclop
	svc := s3.New(sess)

	count.Incr("aws-get-object")

	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		logCountErrTag(err, "GetObject failed "+bucket+"/"+key, bucket)

		return
	}

	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, scannerBufSize)
	scanner.Buffer(buf, scannerBufSize)

	lineNum := 0
	baseName := path.Base(key)

	// If a field name is configured, try to find it in the header row.
	if fieldName != "" && scanner.Scan() {
		lineNum++

		idx := findFieldIndex(scanner.Text(), fieldName)
		if idx >= 0 {
			fieldIndex = idx
			fmt.Println("Found column", fieldName, "at index", fieldIndex, "in", bucket, key)
			count.Incr("null-check-header-found")
		} else {
			fmt.Println("Column", fieldName, "not found in header, using index", fieldIndex, "in", bucket, key)
			count.Incr("null-check-header-not-found")
		}
	}

	fileNull := 0
	fileTotal := 0

	for scanner.Scan() {
		lineNum++

		count.Incr("row")
		count.Incr("row-" + bucket)
		count.Incr("row-" + baseName)

		fields := strings.Split(scanner.Text(), "\t")
		if fieldIndex >= len(fields) {
			count.Incr("null-check-field-missing")
			count.Incr("null-check-field-missing-" + bucket)
			count.Incr("null-check-field-missing-" + baseName)

			continue
		}

		fileTotal++

		val := fields[fieldIndex]
		if val == "" || strings.EqualFold(val, "NULL") || val == "\\N" {
			fileNull++

			count.Incr("field-null")
			count.Incr("field-null-" + bucket)
			count.Incr("field-null-" + baseName)
		} else {
			count.Incr("field-non-null")
			count.Incr("field-non-null-" + bucket)
			count.Incr("field-non-null-" + baseName)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error scanning object", bucket, key, err)
		count.Incr("null-check-scan-error")

		return
	}

	if fileTotal > 0 {
		pct := (fileNull * perCent) / fileTotal

		count.IncrDelta("null-pct-"+baseName, int64(pct))
	}

	fmt.Println("Scanned", lineNum, "lines from", bucket, key)
	count.IncrDelta("null-check-lines", int64(lineNum))
}
