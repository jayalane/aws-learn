// -*- tab-width: 2 -*-
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/organizations"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	set "github.com/jayalane/go-persist-set"
	config "github.com/jayalane/go-tinyconfig"
)

var (
	theConfig     config.Config
	defaultConfig = `#
logAllObjects = false
oneBucket = false
oneBucketName = bucket_name
oneBucketReencrypt = false
checkReplica = false
checkEtag = false
reCopyFile = false
setToDangerToReCopy = asdfasd
checkAcl = false
aclOwnerAcct = true
checkOrgAccounts = true
listFilesMatchingSuffix = %%%/
listFilesMatchingPrefix = %%%
listFilesMatchingExclude = %%%
useDeleteAnywayFile =
justListFiles = false
setToDangerToReencrypt = no
reencryptToTargetBucket = 
setToDangerToDeleteMatching = no
setToDangerToForceACL = no
numAccountHandlers = 1
numBucketHandlers = 10
numObjectHandlers = 1000
profListen = localhost:6060
# comments
`
)

const (
	danger             = "danger"
	minutesInHour      = 60
	errExit            = 11
	smallChannelBuffer = 100
	largeChannelBuffer = 1_000_000
)

// info about an object to check.
type objectChanItem struct {
	acctID string
	bucket string
	object string
	wg     *sync.WaitGroup
}

// info about a bucket to check.
type bucketChanItem struct {
	acctID string
	bucket string
}

// context holds the global state.
type context struct {
	doneObjects *set.DB
	lastObj     uint64
	filter      *[]string
	bucketChan  chan bucketChanItem
	objectChan  chan objectChanItem
	accountChan chan string
	credsRW     sync.RWMutex
	creds       map[string]*credentials.Credentials
	wg          *sync.WaitGroup
	canonIDMap  map[string]string
	canonRW     sync.RWMutex
	keyIDMap    map[string]string
	keyRW       sync.RWMutex
}

var theCtx context

// first a few utilities

// getReplicaBucket given a bucket, replace us-east-1 with us-west-2.
func getReplicaBucket(b string) string {
	return strings.ReplaceAll(b, "us-east-1", "us-west-2")
}

// given an account, gets a session.
func getSessForAcct(a string) *session.Session {
	if a == "0" {
		initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
		if err != nil {
			panic("Can't get session for master" + err.Error())
		}

		return initSess
	}

	theCtx.credsRW.RLock()
	defer theCtx.credsRW.RUnlock()

	var creds *credentials.Credentials

	var ok bool

	if creds, ok = theCtx.creds[a]; !ok {
		initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})

		count.Incr("aws-newsession-root")

		if err != nil {
			panic("Can't get session for master" + err.Error())
		}

		creds = getCredentials(*initSess, a)
	}

	count.Incr("aws-newsession-acct")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: creds,
	})
	if err != nil {
		fmt.Println("Can't get session for master", err.Error())
		count.Incr("aws-newsession-error")

		return nil
	}

	return sess
}

// lookupCanonicalIDForAcct given an account, gets a Canonical ID.
func lookupCanonicalIDForAcct(a string, sess *session.Session) {
	theCtx.canonRW.Lock()
	defer theCtx.canonRW.Unlock()

	if _, ok := theCtx.canonIDMap[a]; !ok {
		cID, err := lookupCanonID(a, sess)
		if err != nil {
			log.Println("Error getting canonical ID for acct id", a, err)
		}

		theCtx.canonIDMap[a] = cID
	}
}

func getDefaultKey(b *string, svc *s3.S3) (string, error) { //nolint:cyclop
	if b == nil || svc == nil {
		return "", errors.New("no bucket or svc") //nolint:err113
	}

	Input := s3.GetBucketEncryptionInput{
		Bucket: b,
	}

	count.Incr("aws-get-bucket-enc")

	out, err := svc.GetBucketEncryption(&Input)
	if err != nil { //nolint:nestif
		var reqerr awserr.RequestFailure

		if errors.As(err, &reqerr) {
			switch {
			case reqerr.StatusCode() == http.StatusNotFound:
				count.Incr("404 error")
				count.Incr("404 error" + *b)
			case reqerr.StatusCode() == http.StatusForbidden:
				count.Incr("403 error")
				count.Incr("403 error" + *b)
			default:
				log.Println("Got request error on object", b, err, reqerr, reqerr.OrigErr())
			}
		} else {
			var netErr net.Error

			if errors.As(err, &netErr) {
				log.Println("Error is type", reflect.TypeOf(netErr.Timeout()))
				log.Println("Got net error on bucket", b, netErr)
			} else {
				log.Println("Got error on object", b, err)
			}
		}

		return "", err
	}

	keyID := ""

	if out != nil {
		// this is complex API for some reason
		if out.ServerSideEncryptionConfiguration == nil {
			return "",
				errors.New("no encryption setting") //nolint:err113
		}

		for _, v := range out.ServerSideEncryptionConfiguration.Rules {
			if v.ApplyServerSideEncryptionByDefault != nil {
				if aws.StringValue(v.ApplyServerSideEncryptionByDefault.SSEAlgorithm) == "AES256" {
					return "AES256", nil
				}

				keyID = aws.StringValue(v.ApplyServerSideEncryptionByDefault.KMSMasterKeyID)
			}
		}
	}

	fmt.Println("Got kms key", keyID, " for bucket", *b)
	setBucketKey(b, keyID)

	return keyID, nil
}

// setBucketKey write id to map with sync.
func setBucketKey(b *string, id string) {
	if b != nil && id != "" {
		theCtx.keyRW.Lock()
		theCtx.keyIDMap[*b] = id
		theCtx.keyRW.Unlock()
	}
}

// getCredentials given a master session and an account ID, generate
// an assumed role credentials.
func getCredentials(session session.Session, acctID string) *credentials.Credentials {
	a := "arn:aws:iam::" + acctID + ":role/OrganizationAccountAccessRole"

	count.Incr("aws-sts-new-creds")

	creds := stscreds.NewCredentials(&session, a)

	return creds
}

// then a few go routines

// handleObject is a go routine to get objects and see if they are encrypted.
// It is too complicated, both for one function and for the
// number of different paths thru based on config.
func handleObject() { //nolint:gocognit, cyclop, gocyclo, maintidx
	count.Incr("aws-new-session-init")

	initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})

	var sess *session.Session

	if err != nil {
		panic("Can't get session for master" + err.Error())
	}

	for {
		select {
		case kb := <-theCtx.objectChan:
			count.Incr("object-chan-remove")

			if kb.acctID == "0" {
				sess = initSess
			} else {
				sess = getSessForAcct(kb.acctID)
				if sess == nil {
					log.Println("Can't log into AWS!")
					kb.wg.Done()
					theCtx.wg.Done()

					continue
				}
			}

			svc := s3.New(sess)
			k := kb.object
			b := kb.bucket

			count.Incr("total-object")
			count.Incr("total-object-" + b)

			if theConfig["justListFiles"].BoolVal { //nolint:nestif
				if filterObjectPasses(b, k, theCtx.filter) {
					if theConfig["logAllObjects"].BoolVal {
						fmt.Printf(
							"Found match s3://%s/%s\n",
							b,
							k)
					}

					count.IncrDelta("list-found", 1)
					count.IncrDelta("list-found-"+b, 1)

					if theConfig["setToDangerToDeleteMatching"].StrVal == danger {
						// fmt.Println("Going to delete", k)
						_, err = deleteObject(k, b, sess)
						if err != nil {
							fmt.Println("Error deleting", k, b, err.Error())
						}

						count.IncrDelta("list-deleted", 1)
						count.IncrDelta("list-deleted-"+b, 1)
					}
				} else {
					// fmt.Println("Skipping object", k, b)
					count.IncrDelta("list-skipping", 1)
					count.IncrDelta("list-skipping-"+b, 1)
				}

				kb.wg.Done()
				theCtx.wg.Done()

				continue
			}

			// not just list files
			if theConfig["checkAcl"].BoolVal {
				count.Incr("handle-acl")
				count.Incr("handle-acl-" + b)
				handleACL(b, k, kb.acctID, sess)
				kb.wg.Done()
				theCtx.wg.Done()

				continue
			}

			if theConfig["threeAcl"].BoolVal {
				count.Incr("handle-3acl")
				count.Incr("handle-3acl-" + b)
				handleThreeACL(b, k, kb.acctID, sess)
				kb.wg.Done()
				theCtx.wg.Done()

				continue
			}

			sb := keyName(*aws.String(b), *aws.String(k))
			if theCtx.doneObjects.InSet(sb) {
				count.Incr("skip-done-pre-head")

				continue
			}

			req := &s3.HeadObjectInput{
				Key:    aws.String(k),
				Bucket: aws.String(b),
			}

			count.Incr("aws-head-object")

			head, err := svc.HeadObject(req)
			if err != nil { //nolint:nestif
				logCountErrTag(err, "bucket/object"+k+"/"+b, b)

				continue
			}
			// needed for recopy, re-encrypt, and checkEtag
			tooBig := false
			etag := head.ETag

			// while we are here, check repl status
			switch {
			case head.ReplicationStatus == nil:
				count.Incr("object-replication-empty")
				fmt.Println("Replication empty " + k + "/" + b)
			case *head.ReplicationStatus == "COMPLETED":
				count.Incr("object-replication-completed")
			default:
				count.Incr("object-replication-not-completed")
				count.Incr("object-replication-status-" + *head.ReplicationStatus)
				fmt.Println("Replication status " + k + "/" + b + *head.ReplicationStatus)
			}

			if head.ContentLength != nil {
				count.IncrDelta("object-length", *head.ContentLength)
				count.IncrDelta("object-length-"+b, *head.ContentLength)

				if *head.ContentLength > maxContentLength {
					fmt.Println("Big object", *aws.String(b), *aws.String(k))
					count.Incr("big-object")
					count.Incr("big-object-" + b)

					tooBig = true
				}
			}

			if theConfig["checkReplica"].BoolVal {
				continue
			}

			if theConfig["checkEtag"].BoolVal {
				// fmt.Println("Checking etag", b, k, etag)
				// another head to another bucket
				b2 := getReplicaBucket(b)
				req := &s3.HeadObjectInput{
					Key:    aws.String(k),
					Bucket: aws.String(b2),
				}

				fmt.Println("About to call head", req)
				count.Incr("aws-head-object-etag-repl")

				head, err := svc.HeadObject(req)
				if err != nil {
					logCountErrTag(err, "bucket/object"+k+"/"+b2, b2)
				} else { // head succeeded
					replEtag := head.ETag
					if replEtag == etag {
						continue
					}

					fmt.Print("Object out of sync", k+"/"+b+"/"+b2)
				}

				continue
			}

			switch {
			case theConfig["reCopyFiles"].BoolVal:
				count.Incr("copy-start")
				count.Incr("copy-start-" + b)

				if !(theConfig["setToDangerToReCopy"].StrVal == "danger") {
					continue
				}

				if !tooBig {
					retry := reencryptObject(b, k, false, sess) // false is don't care about key

					if retry {
						count.Incr("retry-copy-object")
						count.Incr("retry-copy-object-" + b)

						theCtx.objectChan <- kb

						kb.wg.Add(1)     // Done in handleObject
						theCtx.wg.Add(1) // Done in handleObject
					}
				}
			case theConfig["oneBucketReencrypt"].BoolVal:
				// we will be reencrypting
				if !isObjectEncOk(b, *head) { //nolint:nestif
					count.Incr("encrypt-bad")
					count.Incr("encrypt-bad-" + b)

					if !(theConfig["setToDangerToReencrypt"].StrVal == "danger") {
						continue
					}

					if !tooBig {
						retry := reencryptObject(b, k, true, sess) // true is must have KMS ID
						if retry {
							count.Incr("retry-object")
							count.Incr("retry-object-" + b)
							theCtx.objectChan <- kb
						}
					}
				} else {
					count.Incr("encrypt-good")
					count.Incr("encrypt-good-" + b)
				}
			default:
				if !isObjectEncOk(b, *head) {
					count.Incr("unencrypted")
					count.Incr("unencrypted-" + b)

					if rand.Float64() < (1.0 / (float64(count.ReadSync("unencrypted")))) { //nolint:gosec
						switch {
						case head.ServerSideEncryption == nil:
							fmt.Println("ERROR: no encryption", b, k)
						case *head.ServerSideEncryption == "aws:kms":
							fmt.Println("ERROR: ", b, k, *head.ServerSideEncryption, *head.SSEKMSKeyId)
						default:
							fmt.Println("ERROR: ", b, k, *head.ServerSideEncryption)
						}
					}
				}
			}

			kb.wg.Done()
			theCtx.wg.Done()

		case <-time.After(time.Minute * minutesInHour):
			log.Println("Exiting object handler after 1 hour with no traffic")

			return
		}
	}
}

func makeTimestamp() uint64 { // from stackoverflow
	return uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
}

// go routine to get buckets and list their objects.
func handleBucket() {
	count.Incr("aws-new-session-bare-2")

	initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})

	var sess *session.Session

	if err != nil {
		panic("Can't get session for master" + err.Error())
	}

	for {
		select {
		case b := <-theCtx.bucketChan:
			atomic.StoreUint64(&theCtx.lastObj, makeTimestamp())
			log.Println("Got a bucket", b.bucket)

			if b.acctID == "0" {
				sess = initSess
			} else {
				sess = getSessForAcct(b.acctID)
				if sess == nil {
					log.Println("Can't log into AWS!")
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

			count.Incr("aws-list-objects-v2")

			wg := new(sync.WaitGroup) // different WG to make bucket wait for objects
			_ = svc.ListObjectsV2Pages(req, func(resp *s3.ListObjectsV2Output, _ bool) bool {
				count.Incr("object-page")

				for _, content := range resp.Contents {
					key := *content.Key

					wg.Add(1)        // Done in handleObject
					theCtx.wg.Add(1) // Done in handleObject
					count.Incr("object-chan-add")
					runtime.Gosched()

					theCtx.objectChan <- objectChanItem{b.acctID, b.bucket, key, wg}
				}

				count.Incr("object-page-exit")

				return true
			})

			theCtx.wg.Done()
		case <-time.After(time.Minute):
			log.Println("Giving up on bucket channel after 1 minute with no traffic")

			return
		}
	}
}

// handleAccount is a go routine to get accounts and list their buckets.
func handleAccount() { //nolint:cyclop
	count.Incr("aws-new-session-bare-2")

	initSess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})

	var sess *session.Session

	if err != nil {
		panic("Can't get session for master" + err.Error())
	}

	gotOne := false

	for {
		select {
		case a := <-theCtx.accountChan:
			if a == "0" {
				sess = initSess

				log.Println("Got default account")
			} else {
				log.Println("Got an account", a)
				creds := getCredentials(*initSess, a)

				theCtx.credsRW.Lock()

				theCtx.creds[a] = creds

				theCtx.credsRW.Unlock()

				count.Incr("aws-new-session-creds-2")

				sess, err = session.NewSession(&aws.Config{
					Region:      aws.String("us-east-1"),
					Credentials: creds,
				})
				if err != nil {
					log.Println("Couldn't use credentials for acct", a, err)

					continue
				}
			}

			log.Println("About to call get canonical id", a)
			lookupCanonicalIDForAcct(a, sess)

			svc := s3.New(sess)

			count.Incr("aws-list-buckets")

			result, err := svc.ListBuckets(nil)
			if err != nil {
				log.Println("Can't list buckets!", err)
			}

			for _, b := range result.Buckets {
				log.Println("Got a bucket", aws.StringValue(b.Name))

				if !theConfig["oneBucket"].BoolVal || theConfig["oneBucketName"].StrVal == aws.StringValue(b.Name) {
					theCtx.wg.Add(1) // done in handleBucket
					log.Println("Got a bucket", aws.StringValue(b.Name))

					theCtx.bucketChan <- bucketChanItem{a, *b.Name}

					gotOne = true
				}
			}

			if !gotOne {
				log.Println("Processing for buckets done with no buckets seen for", a)
			}

			theCtx.wg.Done() // Add(1) in main
		case <-time.After(time.Minute):
			log.Println("Giving up on buckets after 1 minute with no traffic")

			return
		}
	}
}

// main is too complicated.
func main() { //nolint:gocognit,cyclop
	// stats
	count.InitCounters()

	// config
	if len(os.Args) > 1 && os.Args[1] == "--dumpConfig" {
		fmt.Println(defaultConfig)

		return
	}
	// still config
	var err error

	theConfig, err = config.ReadConfig("config.txt", defaultConfig)

	log.Println("Config", theConfig)

	if err != nil {
		log.Println("Error opening config.txt", err.Error())

		if theConfig == nil {
			os.Exit(errExit)
		}
	}

	// save objects we have copied to disk
	if theConfig["reCopyFiles"].BoolVal || theConfig["checkEtag"].BoolVal || theConfig["checkReplica"].BoolVal {
		theCtx.doneObjects = set.New("doneObjects_2")
	}

	// filters for delete only these things under these things
	if len(theConfig["useDeleteAnywayFile"].StrVal) > 0 {
		theCtx.filter, err = readWillDeleteFile(theConfig["useDeleteAnywayFile"].StrVal)
	}

	if theCtx.filter != nil {
		log.Println("Filter", theCtx.filter)
	}

	if err != nil {
		log.Println("Error opening config.txt", err.Error())

		if theConfig == nil {
			os.Exit(errExit)
		}
	}

	// init the globals
	atomic.StoreUint64(&theCtx.lastObj, makeTimestamp())

	theCtx.wg = new(sync.WaitGroup)
	theCtx.accountChan = make(chan string, smallChannelBuffer)
	theCtx.bucketChan = make(chan bucketChanItem, smallChannelBuffer)
	theCtx.objectChan = make(chan objectChanItem, largeChannelBuffer)
	theCtx.creds = make(map[string]*credentials.Credentials)
	theCtx.credsRW = sync.RWMutex{}
	theCtx.keyIDMap = make(map[string]string)
	theCtx.keyRW = sync.RWMutex{}
	theCtx.canonIDMap = make(map[string]string)
	theCtx.canonRW = sync.RWMutex{}

	// start go routines
	go handleAccount()

	for range theConfig["numBucketHandlers"].IntVal {
		go handleBucket()
	}

	for range theConfig["numObjectHandlers"].IntVal {
		go handleObject()
	}

	// now the work
	// log into master account
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(fmt.Sprintf("Can't log into AWS! %s", err))
	}

	if theConfig["checkOrgAccounts"].BoolVal { //nolint:nestif
		svc := organizations.New(sess)
		// to get all the accounts
		input := &organizations.ListAccountsInput{}

		count.Incr("aws-list-accounts-org")

		la, err := svc.ListAccounts(input)
		if err != nil {
			log.Println("Got an Organization error: ", err, err.Error())

			return
		}

		for { // to handle paginatin - break is down in the "no next token"
			for _, r := range la.Accounts {
				fmt.Println("Account", r.Status, r)

				if *r.Status == "ACTIVE" {
					theCtx.accountChan <- *r.Id
					theCtx.wg.Add(1) // done in handleBucket
				}
			}

			if la.NextToken != nil {
				in := &organizations.ListAccountsInput{NextToken: la.NextToken}

				la, err = svc.ListAccounts(in)
				if err != nil {
					log.Println("Got an Organization error: ", err, err.Error())

					break
				}
			} else { // no more data
				break
			}
		}
	} else {
		log.Println("Just doing one account")

		theCtx.accountChan <- "0"

		theCtx.wg.Add(1) // done in handleBucket
	}

	// start the profiler
	go func() {
		if len(theConfig["profListen"].StrVal) > 0 {
			log.Println(http.ListenAndServe(theConfig["profListen"].StrVal, nil))
		}
	}()

	// waiting till done -
	//     this needs to wait a bit because the WG can empty when an object list page is fully processed
	//     and the work per object is very small.
	for makeTimestamp()-atomic.LoadUint64(&theCtx.lastObj) < 10*1000 {
		log.Println("Last activity sleeping 60 seconds", makeTimestamp()-atomic.LoadUint64(&theCtx.lastObj))
		time.Sleep(time.Minute)
		log.Println("Now waiting")
		theCtx.wg.Wait()
	}

	count.LogCounters()
	log.Println("Exiting", makeTimestamp()-atomic.LoadUint64(&theCtx.lastObj))
}
