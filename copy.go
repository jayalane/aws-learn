// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
)

const (
	delayBaseMsecs        = 5000
	slowDownSeconds       = 120
	deleteSlowDownSeconds = 60
	sendSlowDownSeconds   = 10
)

func copyOnce(source string,
	dest string,
	bucketName string,
	keyNeeded bool,
	keyID string,
	sess *session.Session,
) (*s3.CopyObjectOutput, error) { // nolint:unparam
	svc := s3.New(sess)

	var input *s3.CopyObjectInput

	if keyNeeded {
		input = &s3.CopyObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(dest),
			CopySource:           aws.String(source),
			ServerSideEncryption: aws.String(s3.ServerSideEncryptionAwsKms),
			SSEKMSKeyId:          &keyID,
		}
	} else {
		input = &s3.CopyObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(dest),
			CopySource:           aws.String(source),
			ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
		}
	}

	n := 0.0

	for {
		n += 1.0

		count.Incr("aws-copy")

		output, err := svc.CopyObject(input)
		if err != nil { //nolint:nestif
			var aerr awserr.Error

			if errors.As(err, &aerr) {
				switch aerr.Code() {
				case s3.ErrCodeObjectNotInActiveTierError:
					fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
				case s3.ErrCodeNoSuchKey:
					if n < maxNotFoundTries {
						fmt.Println("Key not found, retrying", aerr.Error(), source)
						time.Sleep(time.Duration(n*delayBaseMsecs) * time.Millisecond)

						continue
					} else {
						return nil, err
					}
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				fmt.Println(err.Error())
			}

			if strings.Contains(err.Error(), "SlowDown") {
				log.Println("Got slow down, sleeping for a few minutes")
				fmt.Println("Got slow down, sleeping for a few minutes")
				time.Sleep(slowDownSeconds * time.Second)
				count.Incr("slow-down")
			}

			fmt.Println("Got error ******** ", bucketName, source, dest)
			count.Incr("error-copy")

			return nil, err
		}

		return output, nil
	}
}

// deleteObject deletes object from the specified bucket using the provided session.
func deleteObject(objectName string,
	bucketName string,
	sess *session.Session,
) (*s3.DeleteObjectOutput, error) { //nolint:unparam
	svc := s3.New(sess)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	count.Incr("aws-delete")

	output, err := svc.DeleteObject(input)
	if err != nil {
		var aerr awserr.Error

		if errors.As(err, &aerr) {
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

		count.Incr("error-delete")

		if strings.Contains(err.Error(), "SlowDown") {
			fmt.Println("Got slow down, sleeping for a few minutes")
			count.Incr("slow-down")
			time.Sleep(deleteSlowDownSeconds * time.Second)
		}

		return nil, err
	}

	return output, nil
}
