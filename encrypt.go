// -*- tab-width: 2 -*-

package main

// this file uses copy.go routines

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
)

// keyName returns the hash key for the given bucket/object combo.
func keyName(bucket string,
	object string,
) string {
	bs := fmt.Sprintf("%s-%s", bucket, object)

	return bs
}

// given a bucket and head check if the encryption is ok.
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
// returns true if the error is retryable.
func reencryptObject(bucketName string,
	objectName string,
	keyNeeded bool,
	sess *session.Session,
) bool {
	if strings.HasSuffix(objectName, "%%%") {
		count.Incr("skip-percents")

		return false
	}

	if theConfig["reCopyFiles"].BoolVal {
		// keep track.  re-encrypt, the state is in the object
		// for recopying all it is not (maybe mod time but ...
		sb := keyName(bucketName, objectName)

		if theCtx.doneObjects.InSet(sb) {
			count.Incr("skip-done-copy")

			return false
		}
	}

	keyID := ""
	hasKeyID := false

	theCtx.keyRW.RLock()

	keyID, hasKeyID = theCtx.keyIDMap[bucketName]

	theCtx.keyRW.RUnlock()

	if !hasKeyID && keyNeeded {
		count.Incr("skip-encryp-no-keyid")

		return false // not retryable
	}

	count.Incr("start-encrypt")

	// first copy setup
	_, err := copyOnce(
		bucketName+"/"+objectName,
		objectName+"%%%",
		bucketName,
		keyNeeded,
		keyID,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got err", err.Error(), bucketName, objectName)
		count.Incr("one-copy-failed")

		return true
	}

	count.Incr("one-copy")

	// second copy setup
	_, err = copyOnce(
		bucketName+"/"+objectName+"%25%25%25",
		objectName,
		bucketName,
		keyNeeded,
		keyID,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got 2nd err", err.Error(), bucketName, objectName)
		count.Incr("two-copy-failed")

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
		count.Incr("delete-failed")

		return true
	}

	// check for ok?
	count.Incr("encrypted-ok")

	if theConfig["reCopyFiles"].BoolVal {
		// reCopy lacks state in S3
		sb := keyName(bucketName, objectName)

		theCtx.doneObjects.Add(sb) // eventually on disk
	}

	return false // actually is retriable :) (i.e. these operations are idempotent
}
