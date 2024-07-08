// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	count "github.com/jayalane/go-counter"
)

// logCountErr checks aws error and logs and registers stats.
func logCountErr(err error, msg string) bool { //nolint:unparam
	return logCountErrTag(err, msg, "")
}

// logCountErrTag checks aws error and logs and registers stats.
func logCountErrTag(err error, msg string, tag string) bool { //nolint:cyclop
	fmt.Println("Got error on ", msg, err, tag)

	is403 := false

	var reqerr awserr.RequestFailure

	if errors.As(err, &reqerr) { //nolint:nestif
		fmt.Println("Got err", reqerr)

		switch {
		case reqerr.StatusCode() == http.StatusNotFound:
			if tag != "" {
				count.Incr("404-error-" + tag)
			}

			count.Incr("404 error")
		case reqerr.StatusCode() == http.StatusForbidden:
			fmt.Println("Got 403 error", reqerr)

			if strings.Contains(reqerr.Message(),
				"The security token included in the request is expired") {
				panic("Exiting due to AWS token expired, refresh creds")
			}

			is403 = true

			if tag != "" {
				count.Incr("403-error-" + tag)
			}

			count.Incr("403 error")
		default:
			fmt.Println("Got request error on", msg, err, reqerr, reqerr.OrigErr())

			if strings.Contains(reqerr.Message(), "send request failed") {
				time.Sleep(sendSlowDownSeconds * time.Second) // slow down
			}

			if strings.Contains(reqerr.Message(), "ExpiredToken") {
				panic("Exiting due to AWS token expired, refresh creds")
			}
		}
	} else {
		var netErr net.Error

		if errors.As(err, &netErr) {
			fmt.Println("Error is type", reflect.TypeOf(netErr.Timeout()))
			fmt.Println("Got net error on ", msg, netErr)
		} else {
			fmt.Println("Got error on ", msg, err)
		}
	}

	return is403
}
