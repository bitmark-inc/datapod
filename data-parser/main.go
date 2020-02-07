package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/getsentry/raven-go"
	"github.com/jinzhu/gorm"
	log "github.com/sirupsen/logrus"
	"github.com/t-tiger/gorm-bulk-insert"

	"github.com/datapod/data-parser/schema/facebook"
	"github.com/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	{Name: "friends", Location: "friends", Regexp: regexp.MustCompile("^friends.json"), Schema: facebook.FriendSchemaLoader()},
	{Name: "posts", Location: "posts", Regexp: regexp.MustCompile("your_posts(?P<index>_[0-9]+).json"), Schema: facebook.PostArraySchemaLoader()},
	{Name: "comments", Location: "comments", Regexp: regexp.MustCompile("comments.json"), Schema: facebook.CommentArraySchemaLoader()},
	{Name: "reactions", Location: "likes_and_reactions", Regexp: regexp.MustCompile("posts_and_comments.json"), Schema: facebook.ReactionSchemaLoader()},
}

func handle(fs storage.FileSystem, db *gorm.DB, rootDir, dataOwner string) {
	workingDir := fmt.Sprintf("%s/%s", rootDir, dataOwner)

	parseTimestamp := time.Now().UnixNano() / int64(time.Millisecond) // in milliseconds
	postID := int(parseTimestamp) * 1000000
	postMediaID := int(parseTimestamp) * 1000000
	placeID := int(parseTimestamp) * 1000000
	tagID := int(parseTimestamp) * 1000000

	errLogTags := map[string]string{"data_owner": dataOwner}
	for _, pattern := range patterns {
		errLogTags["data_type"] = pattern.Name

		files, err := pattern.SelectFiles(fs, workingDir)
		if err != nil {
			raven.CaptureErrorAndWait(err, errLogTags)
			// stop processing if it fails to find what to parse
			return
		}
		for _, file := range files {
			data, err := fs.ReadFile(file)
			if err != nil {
				raven.CaptureErrorAndWait(err, errLogTags)
				continue
			}

			if err := pattern.Validate(data); err != nil {
				raven.CaptureErrorAndWait(err, errLogTags)
				continue
			}

			switch pattern.Name {
			case "friends":
				rawFriends := &facebook.RawFriends{}
				json.Unmarshal(data, &rawFriends)
				if err := gormbulk.BulkInsert(db, rawFriends.ORM(parseTimestamp, dataOwner), 1000); err != nil {
					raven.CaptureErrorAndWait(err, errLogTags)
					// friends must exist for inserting tags
					// stop processing if it fails to insert friends
					return
				}
			case "posts":
				rawPosts := facebook.RawPosts{Items: make([]*facebook.RawPost, 0)}
				json.Unmarshal(data, &rawPosts.Items)
				posts, complexPosts := rawPosts.ORM(dataOwner, &postID, &postMediaID, &placeID, &tagID)
				if err := gormbulk.BulkInsert(db, posts, 1000); err != nil {
					raven.CaptureErrorAndWait(err, errLogTags)
					continue
				}
				for _, p := range complexPosts {
					if len(p.Tags) > 0 {
						friends := make([]facebook.FriendORM, 0)
						if err := db.Where("data_owner_id = ?", dataOwner).Find(&friends).Error; err != nil {
							// friends must exist for inserting tags
							// deal with the next post if it fails to find friends of this data owner
							raven.CaptureErrorAndWait(err, errLogTags)
							continue
						}

						friendIDs := make(map[string]int)
						for _, f := range friends {
							friendIDs[f.FriendName] = f.PKID
						}

						for i := range p.Tags {
							p.Tags[i].FriendID = friendIDs[p.Tags[i].Name]
						}
					}

					if err := db.Create(&p).Error; err != nil {
						raven.CaptureErrorAndWait(err, errLogTags)
						continue
					}
				}
			case "comments":
				rawComments := &facebook.RawComments{}
				json.Unmarshal(data, &rawComments)
				if err := gormbulk.BulkInsert(db, rawComments.ORM(parseTimestamp, dataOwner), 1000); err != nil {
					raven.CaptureErrorAndWait(err, errLogTags)
					continue
				}
			case "reactions":
				rawReactions := &facebook.RawReactions{}
				json.Unmarshal(data, &rawReactions)
				if err := gormbulk.BulkInsert(db, rawReactions.ORM(parseTimestamp, dataOwner), 1000); err != nil {
					raven.CaptureErrorAndWait(err, errLogTags)
					continue
				}
			}
		}
	}
}

func main() {
	dbURI := os.Getenv("DB_URI")
	sqsURI := os.Getenv("SQS_URI")
	rootDir := os.Getenv("ROOT_DIR")
	sentryDSN := os.Getenv("SENTRY_DSN")
	sentryEnv := os.Getenv("SENTRY_ENV")

	raven.SetDSN(sentryDSN)
	raven.SetEnvironment(sentryEnv)

	sess, err := session.NewSession(&aws.Config{Region: aws.String(endpoints.ApNortheast1RegionID)})
	if err != nil {
		panic(err)
	}
	db := storage.NewPostgresORMDB(dbURI)
	fs := storage.NewS3FileSystem(sess)
	queue := storage.NewSQS(sess, sqsURI)

	for {
		output, err := queue.Poll()
		if err != nil {
			raven.CaptureErrorAndWait(err, nil)
			continue
		}

		for _, m := range output.Messages {
			var body struct {
				DataOwner string `json:"data_owner"`
			}
			if err := json.Unmarshal([]byte(*m.Body), &body); err != nil {
				raven.CaptureError(fmt.Errorf("unknown message format: %s", *m.Body), nil)
				continue
			}

			// TODO: if the archive is already successfully parsed, ignore this message; otherwise, wipe all data
			log.WithField("data_owner", body.DataOwner).Info("start parsing")
			handle(fs, db, rootDir, body.DataOwner)
			queue.DeleteMessage(m)
		}
	}
}
