// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"os"
)

func getCanonID(acct string) (string, bool) {
	theCtx.canonRW.RLock()
	canonID, ok := theCtx.canonIDMap[acct]
	theCtx.canonRW.RUnlock()
	return canonID, ok

}

func tryAlernativeSession() *session.Session {
	sess := getSessForAcct(theConfig["aclOwnerAcct"].StrVal)
	return sess
}

// addACL adds in the canonical ID as with full access
func fixACL(acl s3.GetObjectAclOutput,
	bucket string,
	obj string,
	acct string) (s3.AccessControlPolicy, error) {

	canonID, ok := getCanonID(acct)
	if !ok {
		return s3.AccessControlPolicy{}, errors.New("Can't get canonical ID")
	}

	newACL := s3.AccessControlPolicy{}
	newACL.Owner = acl.Owner

	for _, g := range acl.Grants {
		newACL.Grants = append(newACL.Grants, g)
	}

	newG := s3.Grant{}
	newGrantee := s3.Grantee{}
	newGrantee.ID = &canonID // thank god it's not c
	t := s3.TypeCanonicalUser
	newGrantee.Type = &t
	perm := "FULL_CONTROL" // this should be in AWS API
	newG.Grantee = &newGrantee
	newG.Permission = &perm

	newACL.Grants = append(newACL.Grants, &newG)

	return newACL, nil
}

// checkACL returns true if the ACL is ok, false otherwise
func checkACL(acl s3.GetObjectAclOutput,
	bucket string,
	obj string,
	acct string) bool {
	canonID, ok := getCanonID(acct)
	if !ok {
		return true // fail open
	}
	if (acl.Owner.ID != nil) && (*acl.Owner.ID == canonID) {
		return false
	}
	found := false
	for _, g := range acl.Grants {
		if (g.Grantee.ID != nil) && (*g.Grantee.ID == canonID) {
			found = true
		}
	}
	if !found {
		fmt.Println("Wrong ACL!", acl, bucket, obj)
		return true
	}
	return false
}

// handleAcl does all the logic for Acl get/set
// no error - it will print out any errors
func handleACL(bucket string,
	obj string,
	acct string,
	sess *session.Session) {

	tryAgain := false
	var err error
	getACL := &s3.GetObjectAclOutput{}
	var svc *s3.S3
	for {
		if tryAgain {
			sess = tryAlernativeSession()
		}
		svc = s3.New(sess)
		count.Incr("aws-get-object-acl")
		getACL, err = svc.GetObjectAcl(&s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj),
		})

		if err != nil {
			is403 := logCountErr(err, "GetObjectAcl failed"+bucket+"/"+obj)
			if tryAgain {
				return
			}
			if is403 {
				tryAgain = true
			} else {
				return // already logged
			}
		} else {
			break
		}
	}
	doIt := checkACL(*getACL, bucket, obj, acct)
	if doIt {
		fmt.Println("ERROR: Got result", doIt, acct, bucket, obj, *getACL)
	}
	if doIt && theConfig["setToDangerToForceACL"].StrVal == "danger" {
		// todo retry -- but actually we have already retried
		newACL, err := fixACL(*getACL, bucket, obj, acct)
		fmt.Println("NewACL/OldACL", *getACL, newACL)
		if err != nil {
			logCountErr(err, "Get new ACL failed"+bucket+"/"+obj)
			return
		}
		count.Incr("aws-put-object-acl")
		_, err = svc.PutObjectAcl(&s3.PutObjectAclInput{
			AccessControlPolicy: &newACL,
			Bucket:              aws.String(bucket),
			Key:                 aws.String(obj),
		})
		if err != nil {
			logCountErr(err, "PutObjectAcl failed"+bucket+"/"+obj)
			return
		}
		fmt.Println("Successfully fixed", acct, bucket, obj)
		getACL, err = svc.GetObjectAcl(&s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj),
		})
		if getACL != nil {
			fmt.Println("refetched acl", obj, *getACL)
		} else {
			fmt.Println("refetched acl err", err)
		}
		os.Exit(2)
	}

}
