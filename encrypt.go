// -*- tab-width: 2 -*-

package main

// this file uses copy.go routines

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"strings"
)

// given a bucket and head check if the encryption is ok
func isObjectEncOk(b string, head s3.HeadObjectOutput) bool {
	keyID := ""
	hasKeyID := false
	theCtx.keyRW.RLock()
	keyID, hasKeyID = theCtx.keyIDMap[b]
	theCtx.keyRW.RUnlock()

	if hasKeyID {
		// must be encrypted and right key id
		if head.ServerSideEncryption == nil {
			count.Incr("encrypt-no-server-side-fail")
			return false
		}
		if *head.ServerSideEncryption != "aws:kms" {
			count.Incr("encrypt-no-server-side-kms")
			return false
		}
		if !strings.Contains(*head.SSEKMSKeyId, keyID) {
			count.Incr("encrypt-keymismatch-fail")
			return false
		}
		count.Incr("encrypt-check-ok-kms")
		return true
	}
	// no keyID so just needs to be something
	if head.ServerSideEncryption == nil {
		count.Incr("encrypt-no-sse-fail")
		return false
	}
	count.Incr("encrypt-no-key-id-for-bucket-ok")
	return true
}

// given a bucket, an object, and a session, reencrypt it
func reencryptObject(bucketName string,
	objectName string,
	sess *session.Session) bool {

	if strings.HasSuffix(objectName, "%%%") {
		count.Incr("skip-percents")
		return false
	}

	count.Incr("start-encrypt")
	// first copy setup
	_, err := copyOnce(
		bucketName+"/"+objectName,
		objectName+"%%%",
		bucketName,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got err", err.Error(), bucketName, objectName)
		return true
	}
	count.Incr("one-copy")

	// second copy setup
	_, err = copyOnce(
		bucketName+"/"+objectName+"%25%25%25",
		objectName,
		bucketName,
		sess)

	if err != nil {
		// logging done
		fmt.Println("Got 2nd err", err.Error(), bucketName, objectName)
		return true
	}
	count.Incr("two-copy")
	// then delete the tmp
	_, err = deleteObject(
		objectName+"%%%",
		bucketName,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got delete err", err.Error(), bucketName, objectName)
		return true
	}
	// check for ok?
	count.Incr("encrypted-ok")
	return false

}
