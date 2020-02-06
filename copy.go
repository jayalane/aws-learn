// -*- tab-width: 2 -*-

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"strings"
	"time"
)

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
		SSEKMSKeyId:          aws.String(theConfig["oneBucketKMSKeyId"].StrVal),
	}
	n := 0.0
	for {
		n = n + 1.0
		output, err := svc.CopyObject(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeObjectNotInActiveTierError:
					fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
				case s3.ErrCodeNoSuchKey:
					if n < 5 {
						fmt.Println("Key not found, retrying", aerr.Error(), source)
						time.Sleep(time.Duration(n*5000) * time.Millisecond)
						continue
					} else {
						return nil, nil
					}
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				fmt.Println(err.Error())
			}
			if strings.Contains(err.Error(), "SlowDown") {
				fmt.Println("Got slow down, sleeping for a few minutes")
				time.Sleep(120 * time.Second)
				count.Incr("slow-down")
			}
			fmt.Println("Got error ******** ", bucketName, source, dest)
			count.Incr("error-copy")
			return nil, err
		}
		return output, nil
	}
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
		if strings.Contains(err.Error(), "SlowDown") {
			fmt.Println("Got slow down, sleeping for a few minutes")
			count.Incr("slow-down")
			time.Sleep(60 * time.Second)
		}
		return nil, err
	}
	return output, nil
}

// given a bucket, an object, and a session, reencrypt it
func reencryptBucket(bucketName string,
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
