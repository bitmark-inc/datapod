package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/getsentry/raven-go"
	"github.com/jinzhu/gorm"
	"github.com/mholt/archiver"
	"github.com/t-tiger/gorm-bulk-insert"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/s3"

	"github.com/datapod/data-parser/schema/facebook"
	"github.com/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	{Name: "friends", Location: "friends", Regexp: regexp.MustCompile("^friends.json"), Schema: facebook.FriendSchemaLoader()},
	{Name: "posts", Location: "posts", Regexp: regexp.MustCompile("your_posts(?P<index>_[0-9]+).json"), Schema: facebook.PostArraySchemaLoader()},
	{Name: "comments", Location: "comments", Regexp: regexp.MustCompile("comments.json"), Schema: facebook.CommentArraySchemaLoader()},
	{Name: "reactions", Location: "likes_and_reactions", Regexp: regexp.MustCompile("posts_and_comments.json"), Schema: facebook.ReactionSchemaLoader()},
}

func handle(afs afs.Service, db *gorm.DB, s3Bucket, workingDir, archiveName, dataOwner string, parseTime time.Time) {
	defer func() {
		afs.Delete(context.Background(), filepath.Join(workingDir, dataOwner))
	}()

	archiveRemoteDir := fmt.Sprintf("s3://%s", filepath.Join(s3Bucket, dataOwner, archiveName))
	archiveLocalDir := filepath.Join(workingDir, dataOwner, "archives")
	dataLocalDir := filepath.Join(workingDir, dataOwner, "data")
	if err := afs.Copy(context.Background(), archiveRemoteDir, archiveLocalDir); err != nil {
		panic(err)
	}
	if err := archiver.Unarchive(filepath.Join(archiveLocalDir, archiveName), dataLocalDir); err != nil {
		panic(err)
	}
	fs := &storage.LocalFileSystem{}

	ts := parseTime.UnixNano() / int64(time.Millisecond) // in milliseconds
	postID := int(ts) * 1000000
	postMediaID := int(ts) * 1000000
	placeID := int(ts) * 1000000
	tagID := int(ts) * 1000000

	errLogTags := map[string]string{"data_owner": dataOwner}
	for _, pattern := range patterns {
		errLogTags["data_type"] = pattern.Name
		subDir := filepath.Join(dataLocalDir, pattern.Location)
		files, err := pattern.SelectFiles(fs, subDir)
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
				if err := gormbulk.BulkInsert(db, rawFriends.ORM(ts, dataOwner), 1000); err != nil {
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
				if err := gormbulk.BulkInsert(db, rawComments.ORM(ts, dataOwner), 1000); err != nil {
					raven.CaptureErrorAndWait(err, errLogTags)
					continue
				}
			case "reactions":
				rawReactions := &facebook.RawReactions{}
				json.Unmarshal(data, &rawReactions)
				if err := gormbulk.BulkInsert(db, rawReactions.ORM(ts, dataOwner), 1000); err != nil {
					raven.CaptureErrorAndWait(err, errLogTags)
					continue
				}
			}
		}
	}
}

func main() {
	dbURI := os.Getenv("DB_URI")
	s3Bucket := os.Getenv("AWS_S3_BUCKET")
	workingDir := os.Getenv("WORKING_DIR")
	sentryDSN := os.Getenv("SENTRY_DSN")
	sentryEnv := os.Getenv("SENTRY_ENV")
	dataOwner := os.Args[1]
	archiveName := os.Args[2]

	raven.SetDSN(sentryDSN)
	raven.SetEnvironment(sentryEnv)

	db := storage.NewPostgresORMDB(dbURI)
	fs := afs.New()

	handle(fs, db, s3Bucket, workingDir, archiveName, dataOwner, time.Now())
}
