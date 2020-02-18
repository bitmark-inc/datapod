package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/getsentry/sentry-go"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var s3Svc *s3.S3
var httpClient *http.Client

var bitsocialToken string
var bitsocialAPIEndpoint string
var bitsocialBucket string

func cleanS3Folder(dataOwner string) error {
	if dataOwner == "" {
		return fmt.Errorf("invalid data owner")
	}

	for {
		objects, err := s3Svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bitsocialBucket),
			Prefix: aws.String(fmt.Sprintf("%s/", dataOwner)),
		})
		if err != nil {
			return err
		}

		if len(objects.Contents) == 0 {
			return nil
		}

		objectsToDelete := make([]*s3.ObjectIdentifier, 0, 1000)
		for _, object := range objects.Contents {
			objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
				Key: object.Key,
			})
		}

		if _, err := s3Svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String("bitsocial-test"),
			Delete: &s3.Delete{Objects: objectsToDelete},
		}); err != nil {
			return err
		}

		if !*objects.IsTruncated {
			return nil
		}
	}

}

func removeDataOwner(dataOwner string) error {
	req, _ := http.NewRequest(
		"DELETE",
		fmt.Sprintf("%s%s/%s/?cleaned=true",
			strings.TrimRight(bitsocialAPIEndpoint, "/"),
			"/v1/data_owners",
			dataOwner),
		nil)

	req.Header.Set("Authorization", fmt.Sprintf("Token %s", bitsocialToken))

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		if b, err := ioutil.ReadAll(resp.Body); err != nil {
			return err
		} else {
			return fmt.Errorf("%s", b)
		}
	}
	return err
}

func init() {
	sentryDSN := os.Getenv("SENTRY_DSN")
	sentryEnv := os.Getenv("SENTRY_ENV")

	sentry.Init(sentry.ClientOptions{
		Dsn:         sentryDSN,
		Environment: sentryEnv,
	})

	sess := session.New(&aws.Config{Region: aws.String(endpoints.ApNortheast1RegionID)})
	s3Svc = s3.New(sess)

	httpClient = &http.Client{
		Timeout: 10 * time.Second,
	}

	bitsocialToken = os.Getenv("BITSOCIAL_TOKEN")
	bitsocialBucket = os.Getenv("BITSOCIAL_BUCKET")
	bitsocialAPIEndpoint = os.Getenv("BITSOCIAL_API_ENDPOINT") // https://bitsocial-test.bitmark.com/api/
}

func main() {
	dbURI := os.Getenv("BITSOCIAL_DB_URI")
	db, err := gorm.Open("postgres", dbURI)
	if err != nil {
		panic(err)
	}
	log.Println("Start checking 'DELETING' data owners…")
	for {
		rows, err := db.Table("data_owners_dataowner").Select("public_key").Where("status = ?", "DELETING").Rows()
		if err != nil {
			sentry.CaptureException(err)
		}

		for rows.Next() {
			var dataOwnerPublicKey string
			if err := rows.Scan(&dataOwnerPublicKey); err != nil {
				sentry.CaptureException(err)
			}
			log.Printf("Find data owner: %s", dataOwnerPublicKey)

			if err := db.Table("tasks_task").Where(
				map[string]interface{}{
					"status":        1,
					"data_owner_id": dataOwnerPublicKey,
				}).UpdateColumn("status", 90).Error; err != nil {
				sentry.CaptureException(err)
			}

			var taskID string
			r := db.Table("tasks_task").Where(map[string]interface{}{
				"status":        10,
				"data_owner_id": dataOwnerPublicKey,
			}).Select("id").Row()

			if err := r.Scan(&taskID); err != nil {
				if err == sql.ErrNoRows {
					// clean up data owner here
					log.Printf("Clean up s3 data for owner: %s", dataOwnerPublicKey)
					if err := cleanS3Folder(dataOwnerPublicKey); err != nil {
						sentry.CaptureException(err)
						continue
					}

					log.Printf("Remove owner: %s", dataOwnerPublicKey)
					if err := removeDataOwner(dataOwnerPublicKey); err != nil {
						sentry.CaptureException(err)
					}
				} else {
					sentry.CaptureException(err)
				}
			} else {
				log.Printf("There are running tasks for owner: %s", dataOwnerPublicKey)
			}
		}
		rows.Close()
		time.Sleep(10 * time.Second)
	}

}
