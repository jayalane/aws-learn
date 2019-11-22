// -*- tab-width: 2 -*-

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	count "github.com/jayalane/go-counter"
	"github.com/jayalane/go-tinyconfig"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

var theConfig config.Config
var defaultConfig = `#
numBucketHandlers = 10
numObjectHandlers = 1000
# comments
`

type objectChanItem struct {
	bucket string
	object string
}

type context struct {
	bucketChan chan string
	objectChan chan objectChanItem
	done       chan int
	wg         *sync.WaitGroup
}

var theCtx context

func handleObject() {

	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(fmt.Sprintf("Can't log into AWS! %s", err))
	}
	svc := s3.New(sess)
	for {
		select {
		case kb := <-theCtx.objectChan:
			k := kb.object
			b := kb.bucket
			req := &s3.HeadObjectInput{Key: aws.String(k),
				Bucket: aws.String(b)}
			count.Incr("total")
			head, err := svc.HeadObject(req)
			if err != nil {
				if reqerr, ok := err.(awserr.RequestFailure); ok {
					if reqerr.StatusCode() == 404 {
						count.Incr("404 error")
					} else if reqerr.StatusCode() == 403 {
						count.Incr("403 error")

					} else {
						fmt.Println("Got request error on object", k, err)
					}
				} else {
					fmt.Println("Got error on object", k, err)
				}
			} else {
				if (head.ServerSideEncryption == nil) || 0 != strings.Compare(*head.ServerSideEncryption,
					"AES256") {
					if rand.Float64() < (1.0 / (float64(count.ReadSync("encrypted")))) {
						fmt.Println("ERROR: ", b, k, head.ServerSideEncryption)
					}
					count.Incr("encrypted")
				}
			}
			theCtx.wg.Done()

		case <-time.After(60 * time.Second):
			fmt.Println("Giving up on objects after 1 minute with no traffic")
			return
		}
	}
}

func handleBucket() {
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(fmt.Sprintf("Can't log into AWS! %s", err))
	}
	svc := s3.New(sess)
	for {
		select {
		case b := <-theCtx.bucketChan:
			fmt.Println("Got a bucket", b)
			req := &s3.ListObjectsV2Input{Bucket: aws.String(b)}
			svc.ListObjectsV2Pages(req, func(resp *s3.ListObjectsV2Output, lastPage bool) bool {
				for _, content := range resp.Contents {
					key := *content.Key
					theCtx.wg.Add(1) // Done in handleObject
					theCtx.objectChan <- objectChanItem{b, key}
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

func main() {
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
	// start the channels
	theCtx.wg = new(sync.WaitGroup)
	theCtx.bucketChan = make(chan string, 100)
	theCtx.objectChan = make(chan objectChanItem, 100000)

	// start go routines
	for i := 0; i < 100; i++ { // theConfig["numBucketHandlers"].IntVal; i++ {
		go handleBucket()
	}
	for i := 0; i < theConfig["numObjectHandlers"].IntVal; i++ {
		go handleObject()
	}
	// now the work
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(fmt.Sprintf("Can't log into AWS! %s", err))
	}
	fmt.Println("Got a session", sess)
	svc := s3.New(sess)
	fmt.Println("Got an s3 thing", sess)
	result, err := svc.ListBuckets(nil)
	if err != nil {
		panic(fmt.Sprintf("Can't list buckets! %s", err))
	}
	for _, b := range result.Buckets {
		theCtx.wg.Add(1) // done in handleBucket
		fmt.Println("Got a bucket", aws.StringValue(b.Name))
		theCtx.bucketChan <- aws.StringValue(b.Name)
	}
	theCtx.wg.Wait()
	count.LogCounters()
	//	<-theCtx.done
}
