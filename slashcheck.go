// -*- tab-width: 2 -*-

package main

import (
	"bufio"
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
)

// streamAndCheckSlashes streams an S3 object line by line as a TSV and reports,
// per column, how many rows contain a stray backslash (\). A "stray backslash" is
// any field that contains the \ character — including fields that are exactly "\"
// and fields ending with \. The first line is treated as a header row if present,
// so results are reported under the column name rather than a numeric index.
func streamAndCheckSlashes(bucket, key string, hasHeader bool, sess *session.Session) { //nolint:cyclop,gocognit
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

	baseName := path.Base(key)

	var headers []string

	if hasHeader && scanner.Scan() {
		headers = strings.Split(scanner.Text(), ",")
		for i := range headers {
			headers[i] = strings.TrimSpace(headers[i])
		}

		count.Incr("slash-check-header-found")
		fmt.Println("Header has", len(headers), "columns in", bucket, key)
	}

	lineNum := 0
	rowsWithSlash := 0
	headerPrinted := false

	for scanner.Scan() {
		lineNum++

		count.Incr("slash-check-row")
		count.Incr("slash-check-row-" + baseName)

		line := scanner.Text()
		fields := strings.Split(line, ",")
		rowHadSlash := false

		for i, val := range fields {
			if !strings.Contains(val, "\\") {
				continue
			}

			rowHadSlash = true
			colName := columnLabel(headers, i)

			count.Incr("slash-in-col-" + colName)
			count.Incr("slash-in-col-" + colName + "-" + baseName)

			// Flag the especially problematic pattern: a field that is just "\".
			if val == "\\" {
				count.Incr("slash-only-col-" + colName)
				count.Incr("slash-only-col-" + colName + "-" + baseName)
			}

			// Field ending with \ — in a CSV this escapes the following delimiter.
			if strings.HasSuffix(val, "\\") {
				count.Incr("slash-trailing-col-" + colName)
				count.Incr("slash-trailing-col-" + colName + "-" + baseName)
			}
		}

		if rowHadSlash {
			rowsWithSlash++

			count.Incr("slash-rows")
			count.Incr("slash-rows-" + baseName)

			if !headerPrinted {
				fmt.Println("=== OFFENDING ROWS in", bucket+"/"+key, "===")
				fmt.Println("HEADER:", strings.Join(headers, ","))

				headerPrinted = true
			}

			fmt.Printf("FILE=%s LINE=%d ROW=%s\n", baseName, lineNum, line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error scanning object", bucket, key, err)
		count.Incr("slash-check-scan-error")

		return
	}

	fmt.Println("Slash-scan:", lineNum, "lines,", rowsWithSlash, "rows with a stray \\ in", bucket, key)
	count.IncrDelta("slash-check-lines", int64(lineNum))
}

// columnLabel returns the header column name for the given index,
// or "col<index>" if there is no header or the index is out of range.
func columnLabel(headers []string, i int) string {
	if i < len(headers) && headers[i] != "" {
		return headers[i]
	}

	return "col" + strconv.Itoa(i)
}
