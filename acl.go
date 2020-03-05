// -*- tab-width: 2 -*-

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func checkACL(acl s3.GetObjectAclOutput,
	bucket string,
	object string,
	acct string) bool {

	canonID, ok := theCtx.canonIDMap[acct]
	if !ok {
		return false
	}
	/*	if (acl.Owner.ID != nil) && (*acl.Owner.ID != canonID) {
		fmt.Println("canon ID is", canonID)
		fmt.Println("Acl is", acl)
		fmt.Println("True!", acl, bucket, object)
		return false
	}*/
	found := false
	for _, g := range acl.Grants {
		if (g.Grantee.ID != nil) && (*g.Grantee.ID == canonID) {
			found = true
		}
	}
	if !found {
		fmt.Println("canon ID is", canonID)
		fmt.Println("Acl is", acl)
		fmt.Println("True!", acl, bucket, object)
		return false
	}
	return false
}

// handleAcl does all the logic for Acl get/set
// no error - it will print out any errors
func handleACL(bucket string,
	obj string,
	acct string,
	sess *session.Session) {

	svc := s3.New(sess)
	getACL, err := svc.GetObjectAcl(&s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(obj),
	})
	if err != nil {
		logCountErr(err, "GetObjectAcl failed"+bucket+"/"+obj)
		return
	}
	doIt := checkACL(*getACL, bucket, obj, acct)
	if doIt && theConfig["setToDangerToForceACL"].BoolVal {
		_, err = svc.PutObjectAcl(&s3.PutObjectAclInput{
			ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
			Bucket: aws.String(bucket),
			Key:    aws.String(obj),
		})
		if err != nil {
			logCountErr(err, "PutObjectAcl failed"+bucket+"/"+obj)
			return
		}
	}

}
