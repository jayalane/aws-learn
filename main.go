// -*- tab-width: 2 -*-

package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/organizations"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"github.com/jayalane/go-tinyconfig"
	"log"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

var theConfig config.Config
var defaultConfig = `#
oneBucket = false
oneBucketName = bucket_name
oneBucketReencrypt = false
oneBucketKMSKeyId = none
checkOrgAccounts = false
listFilesMatching = dlv
justListFiles = false
setToDangerToDeleteMatching = no
numAccountHandlers = 1
numBucketHandlers = 10
numObjectHandlers = 1000
# comments
`

// info about an object to check
type objectChanItem struct {
	acctID string
	bucket string
	object string
}

// info about a bucket to check
type bucketChanItem struct {
	acctID string
	bucket string
}

// global state
type context struct {
	bucketChan  chan bucketChanItem
	objectChan  chan objectChanItem
	accountChan chan string
	done        chan int
	credsRW     sync.RWMutex
	creds       map[string]*credentials.Credentials
	wg          *sync.WaitGroup
	keyIDMap    map[string]string
	keyRW       sync.RWMutex
}

var theCtx context

// first a few utilities

// given a bucket and head check if the encryption is ok
func isBucketEncOk(b string, head s3.HeadObjectOutput) bool {
	keyID := ""
	hasKeyID := false
	theCtx.keyRW.RLock()
	keyID, hasKeyID = theCtx.keyIDMap[b]
	theCtx.keyRW.RUnlock()
	if hasKeyID {
		// must be encrypted and right key id
		if head.ServerSideEncryption == nil {
			return false
		}
		if *head.ServerSideEncryption != "aws:kms" {
			return false
		}
		if !strings.Contains(*head.SSEKMSKeyId, keyID) {
			return false
		}
		return true
	}
	// no keyID so just needs to be something
	if head.ServerSideEncryption == nil {
		return false
	}
	return true
}

// given an account, gets a session
func getSessForAcct(a string) *session.Session {
	theCtx.credsRW.RLock()
	defer theCtx.credsRW.RUnlock()
	if val, ok := theCtx.creds[a]; ok {
		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String("us-east-1"),
			Credentials: val})
		if err != nil {
			fmt.Println("Error getting session for acct id", a, err)
			return nil
		}
		return sess
	}
	return nil
}

func getDefaultKey(b *string, svc *s3.S3) (string, error) {
	if b == nil || svc == nil {
		return "", errors.New("No bucket or svc")
	}
	Input := s3.GetBucketEncryptionInput{
		Bucket: b,
	}
	out, err := svc.GetBucketEncryption(&Input)
	if err != nil {
		if reqerr, ok := err.(awserr.RequestFailure); ok {
			if reqerr.StatusCode() == 404 {
				count.Incr("404 error")
			} else if reqerr.StatusCode() == 403 {
				count.Incr("403 error")
			} else {
				fmt.Println("Got request error on object", b, err, reqerr, reqerr.OrigErr())
			}
		} else {
			if netErr, ok := err.(net.Error); ok {
				fmt.Println("Error is type", reflect.TypeOf(netErr.Temporary()))
				fmt.Println("Got net error on bucket", b, netErr)
			} else {
				fmt.Println("Got error on object", b, err)
			}
		}
		return "", err
	}
	keyID := ""
	if out != nil {
		// this is complex API for some reason
		if out.ServerSideEncryptionConfiguration == nil {
			return "", errors.New("No encryption setting")
		}
		for _, v := range out.ServerSideEncryptionConfiguration.Rules {
			if v.ApplyServerSideEncryptionByDefault != nil {
				if "AES256" == aws.StringValue(v.ApplyServerSideEncryptionByDefault.SSEAlgorithm) {
					return "AES256", nil
				}
				keyID = aws.StringValue(v.ApplyServerSideEncryptionByDefault.KMSMasterKeyID)
			}
		}
	}
	return keyID, nil
}

// write id to map with sync
func setBucketKey(b *string, id string) {
	if b != nil && id != "" {
		theCtx.keyRW.Lock()
		theCtx.keyIDMap[*b] = id
		theCtx.keyRW.Unlock()
	}
}

// given a master session and an account ID, generate an assumed role credentials
func getCredentials(session session.Session, acctID string) *credentials.Credentials {
	a := "arn:aws:iam::" + acctID + ":role/OrganizationAccountAccessRole"
	creds := stscreds.NewCredentials(&session, a)
	return creds
}

// then a few go routines

// go routine to get objects and see if they are encrypted
func handleObject() {
	initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	sess := &session.Session{}
	if err != nil {
		panic(fmt.Sprintf("Can't get session for master %s", err.Error()))
	}
	for {
		select {
		case kb := <-theCtx.objectChan:
			if kb.acctID == "0" {
				sess = initSess
			} else {
				sess = getSessForAcct(kb.acctID)
				if sess == nil {
					fmt.Println("Can't log into AWS!")
					theCtx.wg.Done()
					continue
				}
			}
			svc := s3.New(sess)
			k := kb.object
			b := kb.bucket
			if theConfig["justListFiles"].BoolVal {
				if strings.Contains(k, theConfig["listFilesMatching"].StrVal) {
					fmt.Printf(
						"Found match s3://%s/%s\n",
						b,
						k)
					if theConfig["setToDangerToDeleteMatching"].StrVal == "danger" {
						fmt.Println("Going to delete", k)
						_, err = deleteObject(k, b, sess)
						if err != nil {
							fmt.Println("Error deleting", k, err.Error())
						}
					}
				}
				theCtx.wg.Done()
				continue
			}
			req := &s3.HeadObjectInput{Key: aws.String(k),
				Bucket: aws.String(b)}
			count.Incr("total-object")
			head, err := svc.HeadObject(req)
			if err != nil {
				if reqerr, ok := err.(awserr.RequestFailure); ok {
					if reqerr.StatusCode() == 404 {
						count.Incr("404 error")
					} else if reqerr.StatusCode() == 403 {
						count.Incr("403 error")
					} else {
						fmt.Println("Got request error on object", k, err, reqerr, reqerr.OrigErr())
					}
				} else {
					if netErr, ok := err.(net.Error); ok {
						fmt.Println("Error is type", reflect.TypeOf(netErr.Temporary()))
						fmt.Println("Got net error on object", k, netErr)
					} else {
						fmt.Println("Got error on object", k, err)
					}
				}
			} else { // head succeeded
				tooBig := false
				if head.ContentLength != nil {
					count.IncrDelta("object-length", *head.ContentLength)
					if *head.ContentLength > 5368709000 {
						fmt.Println("Big object", *aws.String(b), *aws.String(k))
						tooBig = true
					}
				}
				if theConfig["oneBucketReencrypt"].BoolVal {
					// we will be reencrypting
					if !isBucketEncOk(b, *head) {
						if !tooBig {
							retry := reencryptBucket(b, k, sess)
							if retry {
								count.Incr("retry-object")
								theCtx.objectChan <- kb
							}
						}
					}
				} else {
					if !isBucketEncOk(b, *head) {
						count.Incr("unencrypted")
						if rand.Float64() < (1.0 / (float64(count.ReadSync("unencrypted")))) {
							if nil == head.ServerSideEncryption {
								fmt.Println("ERROR: no encryption", b, k)
							} else if *head.ServerSideEncryption == "aws:kms" {
								fmt.Println("ERROR: ", b, k, *head.ServerSideEncryption, *head.SSEKMSKeyId)
							} else {
								fmt.Println("ERROR: ", b, k, *head.ServerSideEncryption)
							}
						}
					}
				}
			}
			theCtx.wg.Done()

		case <-time.After(60 * time.Second):
			fmt.Println("Giving up on objects after 1 minute with no traffic")
			return
		}
	}
}

// go routine to get buckets and list their objects
func handleBucket() {
	initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	sess := &session.Session{}
	if err != nil {
		panic(fmt.Sprintf("Can't get session for master %s", err.Error()))
	}
	for {
		select {
		case b := <-theCtx.bucketChan:
			fmt.Println("Got a bucket", b.bucket)
			if b.acctID == "0" {
				sess = initSess
			} else {
				sess = getSessForAcct(b.acctID)
				if sess == nil {
					fmt.Println("Can't log into AWS!")
					theCtx.wg.Done()
					continue
				}
			}
			count.Incr("total-bucket")
			svc := s3.New(sess)
			// get default key
			key, err := getDefaultKey(aws.String(b.bucket), svc)
			if err != nil && key != "" {
				setBucketKey(aws.String(b.bucket), key)
			}
			// start list objects
			req := &s3.ListObjectsV2Input{Bucket: aws.String(b.bucket)}
			svc.ListObjectsV2Pages(req, func(resp *s3.ListObjectsV2Output, lastPage bool) bool {
				for _, content := range resp.Contents {
					key := *content.Key
					theCtx.wg.Add(1) // Done in handleObject
					theCtx.objectChan <- objectChanItem{b.acctID, b.bucket, key}
				}
				return true
			})
			theCtx.wg.Done()
		case <-time.After(60 * time.Second):
			fmt.Println("Giving up on buckets after 1 minute with no traffic")
			return
		}
	}
}

// go routine to get accounts and list their buckets
func handleAccount() {
	initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	sess := &session.Session{}
	if err != nil {
		panic(fmt.Sprintf("Can't get session for master %s", err.Error()))
	}
	for {
		select {
		case a := <-theCtx.accountChan:
			if a == "0" {
				sess = initSess
			} else {
				fmt.Println("Got an account", a)
				creds := getCredentials(*sess, a)
				theCtx.credsRW.Lock()
				theCtx.creds[a] = creds
				theCtx.credsRW.Unlock()
				sess, err = session.NewSession(&aws.Config{Region: aws.String("us-east-1"),
					Credentials: creds})
				if err != nil {
					fmt.Println("Couldn't use credentials for acct", a, err)
					continue
				}
			}
			svc := s3.New(sess)
			fmt.Println("Got an s3 thing", a, svc)
			result, err := svc.ListBuckets(nil)
			if err != nil {
				fmt.Println("Can't list buckets!", err)
			}
			for _, b := range result.Buckets {

				if theConfig["oneBucket"].BoolVal == false || theConfig["oneBucketName"].StrVal == aws.StringValue(b.Name) {
					theCtx.wg.Add(1) // done in handleBucket
					fmt.Println("Got a bucket", aws.StringValue(b.Name))
					theCtx.bucketChan <- bucketChanItem{a, *b.Name}
				}
			}
			theCtx.wg.Done() // Add(1) in main

		case <-time.After(60 * time.Second):
			fmt.Println("Giving up on buckets after 1 minute with no traffic")
			return
		}
	}
}

func main() {
	// stats
	count.InitCounters()
	// config
	if len(os.Args) > 1 && os.Args[1] == "--dumpConfig" {
		log.Println(defaultConfig)
		return
	}
	var err error
	theConfig, err = config.ReadConfig("config.txt", defaultConfig)
	log.Println("Config", theConfig)
	if err != nil {
		log.Println("Error opening config.txt", err.Error())
		if theConfig == nil {
			os.Exit(11)
		}
	}
	// init the globals
	theCtx.wg = new(sync.WaitGroup)
	theCtx.accountChan = make(chan string, 100)
	theCtx.bucketChan = make(chan bucketChanItem, 100)
	theCtx.objectChan = make(chan objectChanItem, 1000000)
	theCtx.creds = make(map[string]*credentials.Credentials)
	theCtx.credsRW = sync.RWMutex{}
	theCtx.keyIDMap = make(map[string]string)
	theCtx.keyRW = sync.RWMutex{}

	// start go routines
	go handleAccount()
	for i := 0; i < theConfig["numBucketHandlers"].IntVal; i++ {
		go handleBucket()
	}
	for i := 0; i < theConfig["numObjectHandlers"].IntVal; i++ {
		go handleObject()
	}

	// now the work
	// log into master account
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(fmt.Sprintf("Can't log into AWS! %s", err))
	}
	if theConfig["checkOrgAccounts"].BoolVal {
		svc := organizations.New(sess)
		// to get all the accounts
		input := &organizations.ListAccountsInput{}
		la, err := svc.ListAccounts(input)
		if err != nil {
			fmt.Println("Got an Organization error: ", err, err.Error())
			return
		}
		for { // to handle paginatin - break is down in the "no next token"
			fmt.Println("Result", la)

			for _, r := range la.Accounts {
				if *r.Status == "ACTIVE" {
					theCtx.accountChan <- *r.Id
					theCtx.wg.Add(1) // done in handleBucket
				}
			}
			fmt.Println("next token", la.NextToken)
			if la.NextToken != nil {
				fmt.Println("Got NextToken")
				in := &organizations.ListAccountsInput{NextToken: la.NextToken}
				la, err = svc.ListAccounts(in)
				if err != nil {
					fmt.Println("Got an Organization error: ", err, err.Error())
					break
				}
			} else { // no more data
				break
			}
		}
	} else {
		theCtx.accountChan <- "0"
		theCtx.wg.Add(1) // done in handleBucket
	}
	theCtx.wg.Wait()
	count.LogCounters()
	//	<-theCtx.done
}
