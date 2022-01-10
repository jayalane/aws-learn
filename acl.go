// -*- tab-width: 2 -*-

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"strings"
)

func tryAlternativeSession(tryAgain int, obj string) *session.Session {
	if tryAgain == 1 {
		sess := getSessForAcct(theConfig["aclOwnerAcct"].StrVal)
		return sess
	}
	objS := strings.Split(obj, "/")
	sess := getSessForAcct(objS[0])
	fmt.Println("Try again with", obj, objS[0], sess)
	return sess
}

// addACL adds in the canonical ID as with full access
func fixACL(
	acl s3.GetObjectAclOutput,
	bucket string,
	obj string,
	acct string) (s3.AccessControlPolicy, error) {

	canonID, ok := getCanonIDMaybeCall(acct)
	if !ok {
		return s3.AccessControlPolicy{}, fmt.Errorf("can't get canonical ID for %s", acct)
	}

	newACL := s3.AccessControlPolicy{}
	newACL.Owner = acl.Owner

	newACL.Grants = append(newACL.Grants, acl.Grants...)

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
	desiredOwnerAcct string) bool {
	// fmt.Println("Checking ACL for", bucket, obj, desired_own_acct)
	canonID, ok := getCanonIDMaybeCall(desiredOwnerAcct)
	if !ok {
		return true // fail open
	}
	if (acl.Owner.ID != nil) && (*acl.Owner.ID == canonID) {
		fmt.Println("bad owner for", bucket, obj, desiredOwnerAcct)
		return true // not sure this will work
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

// checkACL returns true if the ACL is ok, false otherwise
func checkThreeACL(
	acl s3.GetObjectAclOutput,
	bucket string,
	obj string,
	desiredOwnerAcct string) bool {

	fmt.Println("Checking ACL for", bucket, obj, desiredOwnerAcct)
	canonID, ok := getCanonIDMaybeCall(desiredOwnerAcct)
	if !ok {
		return true // fail open
	}
	if (acl.Owner.ID != nil) && (*acl.Owner.ID == canonID) {
		fmt.Println("bad owner for", bucket, obj, desiredOwnerAcct)
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
func handleACL(
	bucket string,
	obj string,
	bucketAcct string,
	sess *session.Session) {
	tryAgain := 0
	var err error
	var getACL *s3.GetObjectAclOutput
	var svc *s3.S3
	for {
		if tryAgain > 0 {
			sess = tryAlternativeSession(tryAgain, obj)
			if sess == nil {
				if tryAgain == 2 {
					panic("Doing acl checks but not powerful enough - use root account creds")
				}
				tryAgain = tryAgain + 1
				continue
			}
		}
		svc = s3.New(sess)
		count.Incr("aws-get-object-acl")
		getACL, err = svc.GetObjectAcl(&s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj),
		})

		if err != nil {
			is403 := logCountErrTag(err, "GetObjectAcl failed"+bucket+"/"+obj, bucket)
			if tryAgain == 2 {
				fmt.Println("Try again failed - check aclOwnerAcct config.txt setting", bucket, obj, err)
				return
			}
			if is403 {
				tryAgain = tryAgain + 1
			} else {
				return // already logged
			}
		} else {
			break
		}
	}
	doIt := checkACL(*getACL, bucket, obj, bucketAcct)
	if doIt {
		fmt.Println("ERROR: Got result", doIt, bucketAcct, bucket, obj, *getACL)
		count.Incr("bad-acl-found")
	}
	if doIt && theConfig["setToDangerToForceACL"].StrVal == "danger" {
		// todo retry -- but actually we have already retried
		newACL, err := fixACL(*getACL, bucket, obj, bucketAcct)
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
		fmt.Println("Successfully fixed", bucketAcct, bucket, obj)
		getACL, err = svc.GetObjectAcl(&s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj),
		})
		if getACL != nil {
			fmt.Println("refetched acl", obj, *getACL)
		} else {
			fmt.Println("refetched acl err", err)
		}
	}
}

// handleThreeAcl does all the logic for Acl get/set
// no error - it will print out any errors
func handleThreeACL(
	bucket string,
	obj string,
	bucketAcct string,
	sess *session.Session) {

	var err error
	var getACL *s3.GetObjectAclOutput
	var svc *s3.S3 = s3.New(sess)
	count.Incr("aws-get-object-3acl")

	getACL, err = svc.GetObjectAcl(&s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(obj),
	})

	if err != nil {
		_ = logCountErrTag(err, "GetObjectAcl failed"+bucket+"/"+obj, bucket)
		return // already logged
	}
	for _, readAcct := range strings.Split((theConfig)["threeAcctAclReader"].StrVal, ",") {

		doIt := checkThreeACL(*getACL, bucket, obj, readAcct)
		if doIt {
			fmt.Println("ERROR: Got result", doIt, bucket, obj, *getACL)
			count.Incr("bad-3acl-found")
		}
		if doIt && theConfig["setToDangerToForceACL"].StrVal == "danger" {
			newACL, err := fixACL(*getACL, bucket, obj, readAcct)
			fmt.Println("NewACL/OldACL", *getACL, newACL)
			if err != nil {
				logCountErr(err, "Get new ACL failed"+bucket+"/"+obj)
				continue
			}
			count.Incr("aws-put-object-3acl")
			_, err = svc.PutObjectAcl(&s3.PutObjectAclInput{
				AccessControlPolicy: &newACL,
				Bucket:              aws.String(bucket),
				Key:                 aws.String(obj),
			})
			if err != nil {
				logCountErr(err, "PutObjectAcl failed"+bucket+"/"+obj)
				return
			}
			fmt.Println("Successfully fixed", readAcct, bucket, obj)
			getACL, err = svc.GetObjectAcl(&s3.GetObjectAclInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(obj),
			})
			if getACL != nil {
				fmt.Println("refetched acl", obj, *getACL)
			} else {
				fmt.Println("refetched acl err", err)
			}
		}
	}

}
