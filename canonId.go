// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
)

func parseBucketList(bObj *s3.ListBucketsOutput) (string, error) {
	if bObj == nil {
		return "", errors.New("no bucket list") //nolint:err113
	}

	log.Println("Returning canonical ID", *bObj.Owner.ID)

	return *bObj.Owner.ID, nil
}

// lookupCanonID does all the logic to get the
// account canonical ID from the s3 list-bukcets.
func lookupCanonID(
	acct string,
	sess *session.Session,
) (string, error) {
	log.Println("Looking up canonical id for ", acct)

	svc := s3.New(sess)

	count.Incr("aws-listbuckets-canon")

	bObj, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		logCountErr(err, "listBuckets failed"+acct)

		return "", err
	}

	return parseBucketList(bObj)
}

// getCanonIDMaybeCall checks the cache of canonical IDs
// and will make a session and call AWS if needed.
func getCanonIDMaybeCall(acct string) (string, bool) {
	theCtx.canonRW.RLock()

	canonID, ok := theCtx.canonIDMap[acct]

	theCtx.canonRW.RUnlock()

	if !ok {
		return canonID, ok
	}

	sess := getSessForAcct(acct)

	lookupCanonicalIDForAcct(acct, sess)
	theCtx.canonRW.RLock()

	canonID, ok = theCtx.canonIDMap[acct]

	theCtx.canonRW.RUnlock()

	return canonID, ok
}
