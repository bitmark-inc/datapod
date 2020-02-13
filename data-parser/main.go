package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/getsentry/raven-go"
	"github.com/jinzhu/gorm"
	"github.com/mholt/archiver"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/t-tiger/gorm-bulk-insert"
	_ "github.com/viant/afsc/s3"

	"github.com/bitmark-inc/datapod/data-parser/schema/facebook"
	"github.com/bitmark-inc/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	facebook.FriendsPattern,
	facebook.PostsPattern,
	facebook.ReactionsPattern,
	facebook.CommentsPattern,
}

func handle(db *gorm.DB, s3Bucket, workingDir string, task *storage.Task, parseTime time.Time) error {
	contextLogger := log.WithFields(log.Fields{"task_id": task.ID})
	contextLogger.Info("task started")

	dataOwner := task.Archive.DataOwnerID
	dataOwnerDir := filepath.Join(workingDir, dataOwner)
	archiveDir := filepath.Join(dataOwnerDir, "archive")
	archivePath := filepath.Join(archiveDir, filepath.Base(task.Archive.File))
	dataDir := filepath.Join(dataOwnerDir, "data")

	fs := afero.NewOsFs()
	file, err := storage.CreateFile(fs, archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	defer fs.RemoveAll(dataOwnerDir)

	if err := storage.DownloadArchive(s3Bucket, task.Archive.File, file); err != nil {
		return err
	}
	contextLogger.Info("archive downloaded")

	if err := archiver.Unarchive(archivePath, dataDir); err != nil {
		return err
	}
	contextLogger.Info("archive unzipped")

	ts := parseTime.UnixNano() / int64(time.Millisecond) // in milliseconds
	postID := int(ts) * 1000000
	postMediaID := int(ts) * 1000000
	placeID := int(ts) * 1000000
	tagID := int(ts) * 1000000

	for _, pattern := range patterns {
		contextLogger.WithField("type", pattern.Name).Info("parsing and inserting records into db")

		subDir := filepath.Join(dataDir, pattern.Location)
		files, err := pattern.SelectFiles(fs, subDir)
		if err != nil {
			return err
		}
		for _, file := range files {
			data, err := afero.ReadFile(fs, file)
			if err != nil {
				return err
			}

			if err := pattern.Validate(data); err != nil {
				return err
			}

			switch pattern.Name {
			case "friends":
				rawFriends := &facebook.RawFriends{}
				json.Unmarshal(data, &rawFriends)
				if err := gormbulk.BulkInsert(db, rawFriends.ORM(ts, dataOwner), 1000); err != nil {
					// friends must exist for inserting tags
					// stop processing if it fails to insert friends
					return err
				}
			case "posts":
				rawPosts := facebook.RawPosts{Items: make([]*facebook.RawPost, 0)}
				json.Unmarshal(data, &rawPosts.Items)
				posts, complexPosts := rawPosts.ORM(dataOwner, &postID, &postMediaID, &placeID, &tagID)
				if err := gormbulk.BulkInsert(db, posts, 1000); err != nil {
					continue
				}
				for _, p := range complexPosts {
					if len(p.Tags) > 0 {
						friends := make([]facebook.FriendORM, 0)
						if err := db.Where("data_owner_id = ?", dataOwner).Find(&friends).Error; err != nil {
							// friends must exist for inserting tags
							// deal with the next post if it fails to find friends of this data owner
							continue
						}

						friendIDs := make(map[string]int)
						for _, f := range friends {
							friendIDs[f.FriendName] = f.PKID
						}

						// TODO: remove tagged people who are not friends
						for i := range p.Tags {
							p.Tags[i].FriendID = friendIDs[p.Tags[i].Name]
						}
					}

					if err := db.Create(&p).Error; err != nil {
						continue
					}
				}
			case "comments":
				rawComments := &facebook.RawComments{}
				json.Unmarshal(data, &rawComments)
				if err := gormbulk.BulkInsert(db, rawComments.ORM(ts, dataOwner), 1000); err != nil {
					continue
				}
			case "reactions":
				rawReactions := &facebook.RawReactions{}
				json.Unmarshal(data, &rawReactions)
				if err := gormbulk.BulkInsert(db, rawReactions.ORM(ts, dataOwner), 1000); err != nil {
					continue
				}
			}
		}
	}

	contextLogger.Info("task finished")
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

	for {
		task, err := storage.GetNextRunningTask(db)
		if err != nil {
			raven.CaptureError(err, nil)
		}

		if task == nil {
			time.Sleep(time.Minute)
			continue
		}

		err = handle(db, s3Bucket, workingDir, task, time.Now())

		status := storage.TaskStatusFinished
		if err != nil {
			status = storage.TaskStatusFailed
		}

		if err := storage.UpdateTaskStatus(db, task, status); err != nil {
			raven.CaptureError(err, nil)
		}
	}
}
