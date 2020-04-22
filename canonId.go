// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"log"
)

func parseBucketList(bObj *s3.ListBucketsOutput) (string, error) {
	if bObj == nil {
		return "", errors.New("No bucket list")
	}
	log.Println("Returning canonical ID", *bObj.Owner.ID)
	return *bObj.Owner.ID, nil
}

// lookupCanonID does all the logic to get the
// account canonical ID from the s3 list-bukcets
func lookupCanonID(acct string,
	sess *session.Session) (string, error) {
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
