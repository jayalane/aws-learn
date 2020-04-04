// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func parseBucketList(bObj *s3.ListBucketsOutput) (string, error) {
	if bObj == nil {
		return "", errors.New("No bucket list")
	}
	fmt.Println("Returning canonical ID", *bObj.Owner.ID)
	return *bObj.Owner.ID, nil
}

// lookupCanonID does all the logic to get the
// account canonical ID from the s3 list-bukcets
func lookupCanonID(acct string,
	sess *session.Session) (string, error) {
	fmt.Println("Looking up canonical id for ", acct)
	svc := s3.New(sess)
	bObj, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		logCountErr(err, "listBuckets failed"+acct)
		return "", err
	}
	return parseBucketList(bObj)

}
