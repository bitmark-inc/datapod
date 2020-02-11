package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/getsentry/raven-go"
	"github.com/jinzhu/gorm"
	"github.com/mholt/archiver"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/t-tiger/gorm-bulk-insert"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/s3"

	"github.com/datapod/data-parser/schema/facebook"
	"github.com/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	facebook.FriendsPattern,
	facebook.PostsPattern,
	facebook.ReactionsPattern,
	facebook.CommentsPattern,
}

func handle(afs afs.Service, db *gorm.DB, s3Bucket, workingDir, archiveName, dataOwner string, parseTime time.Time) error {
	defer func() {
		afs.Delete(context.Background(), filepath.Join(workingDir, dataOwner))
	}()

	archiveRemoteDir := fmt.Sprintf("s3://%s", filepath.Join(s3Bucket, archiveName))
	archiveLocalDir := filepath.Join(workingDir, dataOwner, "archives")
	dataLocalDir := filepath.Join(workingDir, dataOwner, "data")

	if err := afs.Copy(context.Background(), archiveRemoteDir, archiveLocalDir); err != nil {
		return err
	}
	if err := archiver.Unarchive(filepath.Join(archiveLocalDir, filepath.Base(archiveName)), dataLocalDir); err != nil {
		return err
	}
	fs := afero.NewOsFs()

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
			return err
		}
		for _, file := range files {
			data, err := afero.ReadFile(fs, file)
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
					return err
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
	return nil
}

func main() {
	postgresURI := os.Getenv("POSTGRES_URI")
	s3Bucket := os.Getenv("AWS_S3_BUCKET")
	sentryDSN := os.Getenv("SENTRY_DSN")
	sentryEnv := os.Getenv("SENTRY_ENV")
	workingDir := os.Getenv("DATA_PARSER_WORKING_DIR")

	raven.SetDSN(sentryDSN)
	raven.SetEnvironment(sentryEnv)

	db := storage.NewPostgresORMDB(postgresURI)
	db = db.Debug()
	fs := afs.New()

	for {
		tasks, err := storage.GetPendingTasks(db)
		if err != nil {
			raven.CaptureError(err, nil)
		}
	TaskList:
		for _, task := range tasks {
			fmt.Printf("%+v", task)
			log.WithFields(log.Fields{"data_owner": task.DataOwnerID}).Info("start parsing")

			// mark the task as RUNNING
			if err := storage.UpdateTaskStatus(db, task, storage.TaskStatusRunning); err != nil {
				raven.CaptureError(err, nil)
				continue TaskList
			}

			err := handle(fs, db, s3Bucket, workingDir, task.Archive.File, task.Archive.DataOwnerID, time.Now())

			status := storage.TaskStatusFinished
			if err != nil {
				fmt.Println(err)
				status = storage.TaskStatusFailed
			}
			if err := storage.UpdateTaskStatus(db, task, status); err != nil {
				raven.CaptureError(err, nil)
				continue TaskList
			}
		}

		time.Sleep(time.Minute)
	}
}
