package s3core

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func NewClient(endpoint string, key string, secret string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("default"),
		Credentials:      credentials.NewStaticCredentials(key, secret, ""),
		S3ForcePathStyle: aws.Bool(true),
		Endpoint:         aws.String(endpoint),
	})
	if err != nil {
		fmt.Printf("Cannot create new s3 seesion, %v", err)
	}
	client := s3.New(sess)
	return client
}

//create bucket
func CreateBucket(svc *s3.S3, key string) {
	params := &s3.CreateBucketInput{
		Bucket: aws.String(key), // Required
	}

	resp, err := svc.CreateBucket(params)

	if err != nil {
		fmt.Println("create err: ", err.Error())
		return
	}
	fmt.Printf("create done! resp: %s", resp)
	return
}

func SetOBJ(svc *s3.S3, bucket, key, value string) bool {
	//bucketname := bucket

	var putinput *s3.PutObjectInput
	putinput = &s3.PutObjectInput{
		//Body:   strings.NewReader("Hi S3"),
		Body:   strings.NewReader(value),
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, errput := svc.PutObject(putinput)

	if errput != nil {
		return false
	}
	return true
}
