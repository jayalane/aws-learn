// -*- tab-width: 2 -*-

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	count "github.com/jayalane/go-counter"
	"net"
	"reflect"
	"strings"
	"time"
)

// func logCountErr checks aws error and logs and registers stats
func logCountErr(err error, msg string) bool {
	fmt.Println("Got error on ", msg, err)
	is403 := false

	if reqerr, ok := err.(awserr.RequestFailure); ok {
		fmt.Println("Got err", reqerr)
		if reqerr.StatusCode() == 404 {
			count.Incr("404 error")
		} else if reqerr.StatusCode() == 403 {
			fmt.Println("Got 403 error", reqerr)
			is403 = true
			count.Incr("403 error")
		} else {
			fmt.Println("Got request error on", msg, err, reqerr, reqerr.OrigErr())
			if strings.Contains(reqerr.Message(), "send request failed") {
				time.Sleep(10 * time.Second) // slow down
			}
		}
	} else {
		if netErr, ok := err.(net.Error); ok {
			fmt.Println("Error is type", reflect.TypeOf(netErr.Temporary()))
			fmt.Println("Got net error on ", msg, netErr)
		} else {
			fmt.Println("Got error on ", msg, err)
		}
	}
	return is403

}
