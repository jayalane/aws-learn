// -*- tab-width: 2 -*-

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"os"
	"time"
)

var total int = 0

func copyOnce(source string,
	dest string,
	bucketName string,
	sess *session.Session) (*s3.CopyObjectOutput, error) {
	svc := s3.New(sess)
	input := &s3.CopyObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(dest),
		CopySource:           aws.String(source),
		ServerSideEncryption: aws.String(s3.ServerSideEncryptionAwsKms),
	}
	output, err := svc.CopyObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeObjectNotInActiveTierError:
				fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		fmt.Println("Got error ******** ", bucketName, source, dest)
		count.Incr("error-copy")
		return nil, err
	}
	return output, nil
}

// delete object
func deleteObject(objectName string,
	bucketName string,
	sess *session.Session) (*s3.DeleteObjectOutput, error) {
	svc := s3.New(sess)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	output, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeObjectNotInActiveTierError:
				fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		fmt.Println("Got error on delete ******** ", bucketName, objectName)
		count.Incr("error-copy")
		return nil, err
	}
	return output, nil
}

// given a bucket, an object, and a session, reencrypt it
func reencryptBucket(bucketName string,
	objectName string,
	sess *session.Session) {
	fmt.Println("Reencrypt called with ", bucketName, objectName)
	if total > 0 {
		return
	}
	total = 1
	// TODO
	count.Incr("start-copy")
	start := time.Now()
	defer fmt.Println("Time elapsed", time.Since(start))
	// first copy setup
	_, err := copyOnce(
		bucketName+"/"+objectName,
		objectName+"%%%",
		bucketName,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got err", err.Error(), bucketName, objectName)
		os.Exit(3)
		return
	}

	// second copy setup
	_, err = copyOnce(
		bucketName+"/"+objectName+"%25%25%25",
		objectName,
		bucketName,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got 2nd err", err.Error(), bucketName, objectName)
		os.Exit(3)
		return
	}
	// then delete the tmp
	_, err = deleteObject(
		objectName+"%%%",
		bucketName,
		sess)
	if err != nil {
		// logging done
		fmt.Println("Got delete err", err.Error(), bucketName, objectName)
		os.Exit(3)
		return
	}
	// check for ok?
	os.Exit(3)

}
