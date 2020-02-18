package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jinzhu/gorm"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/t-tiger/gorm-bulk-insert"

	"github.com/bitmark-inc/datapod/data-parser/schema/facebook"
	"github.com/bitmark-inc/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	facebook.FriendsPattern,
	facebook.PostsPattern,
	facebook.ReactionsPattern,
	facebook.CommentsPattern,
	facebook.MediaPattern,
	facebook.FilesPattern,
}

func init() {
	sentryDSN := os.Getenv("SENTRY_DSN")
	sentryEnv := os.Getenv("SENTRY_ENV")

	sentry.Init(sentry.ClientOptions{
		Dsn:         sentryDSN,
		Environment: sentryEnv,
	})
}

func handle(db *gorm.DB, s3Bucket, workingDir string, task *storage.Task, parseTime time.Time) error {
	contextLogger := log.WithFields(log.Fields{"task_id": task.ID})
	contextLogger.Info("task started")

	// the layout of the local dir for this task:
	// <data-owner> /
	//		archive/
	//			<archive-file-name>.zip
	// 		data/
	// 			about_you/
	// 			ads_and_businesses/
	// 			and more...
	dataOwner := task.Archive.DataOwnerID
	dataOwnerDir := filepath.Join(workingDir, dataOwner)
	archiveDir := filepath.Join(dataOwnerDir, "archive")
	archiveName := filepath.Base(task.Archive.File)
	archivePath := filepath.Join(archiveDir, archiveName)
	dataDir := filepath.Join(dataOwnerDir, "data")

	fs := afero.NewOsFs()
	file, err := storage.CreateFile(fs, archivePath)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	defer file.Close()
	defer fs.RemoveAll(dataOwnerDir)

	if err := storage.DownloadArchiveFromS3(s3Bucket, task.Archive.File, file); err != nil {
		sentry.CaptureException(err)
		return err
	}
	contextLogger.Info("archive downloaded")

	ts := parseTime.UnixNano() / int64(time.Millisecond) // in milliseconds
	postID := int(ts) * 1000000
	postMediaID := int(ts) * 1000000
	placeID := int(ts) * 1000000
	tagID := int(ts) * 1000000

	for _, pattern := range patterns {
		contextLogger.WithField("type", pattern.Name).Info("parsing and inserting records into db")

		if err := storage.ExtractArchive(archivePath, pattern.Location, dataDir); err != nil {
			sentry.CaptureException(err)
			return err
		}

		subDir := filepath.Join(dataDir, pattern.Location)
		if pattern.Name == "media" || pattern.Name == "files" {
			if err := storage.UploadDirToS3(s3Bucket, fmt.Sprintf("%s/fb_archives/%s", dataOwner, task.Archive.ID), subDir); err != nil {
				sentry.CaptureException(err)
				continue
			}
		} else {
			files, err := pattern.SelectFiles(fs, subDir)
			if err != nil {
				sentry.CaptureException(err)
				return err
			}
			for _, file := range files {
				data, err := afero.ReadFile(fs, file)
				if err != nil {
					sentry.CaptureException(err)
					return err
				}

				if err := pattern.Validate(data); err != nil {
					sentry.CaptureException(err)
					return err
				}

				switch pattern.Name {
				case "friends":
					rawFriends := &facebook.RawFriends{}
					json.Unmarshal(data, &rawFriends)
					if err := gormbulk.BulkInsert(db, rawFriends.ORM(ts, dataOwner), 1000); err != nil {
						// friends must exist for inserting tags
						// stop processing if it fails to insert friends
						sentry.CaptureException(err)
						return err
					}
				case "posts":
					rawPosts := facebook.RawPosts{Items: make([]*facebook.RawPost, 0)}
					json.Unmarshal(data, &rawPosts.Items)
					posts, complexPosts := rawPosts.ORM(dataOwner, task.Archive.ID, &postID, &postMediaID, &placeID, &tagID)
					if err := gormbulk.BulkInsert(db, posts, 1000); err != nil {
						sentry.CaptureException(err)
						continue
					}
					for _, p := range complexPosts {
						if len(p.Tags) > 0 {
							friends := make([]facebook.FriendORM, 0)
							if err := db.Where("data_owner_id = ?", dataOwner).Find(&friends).Error; err != nil {
								// friends must exist for inserting tags
								// deal with the next post if it fails to find friends of this data owner
								sentry.CaptureException(err)
								continue
							}

							friendIDs := make(map[string]int)
							for _, f := range friends {
								friendIDs[f.FriendName] = f.PKID
							}

							// FIXME: non-friends couldn't be tagged
							c := 0 // valid tag count
							for i := range p.Tags {
								friendID, ok := friendIDs[p.Tags[i].Name]
								if ok {
									p.Tags[i].FriendID = friendID
									c++
								}
							}
							p.Tags = p.Tags[:c]
						}

						if err := db.Create(&p).Error; err != nil {
							sentry.CaptureException(err)
							continue
						}
					}
				case "comments":
					rawComments := &facebook.RawComments{}
					json.Unmarshal(data, &rawComments)
					if err := gormbulk.BulkInsert(db, rawComments.ORM(ts, dataOwner), 1000); err != nil {
						sentry.CaptureException(err)
						continue
					}
				case "reactions":
					rawReactions := &facebook.RawReactions{}
					json.Unmarshal(data, &rawReactions)
					if err := gormbulk.BulkInsert(db, rawReactions.ORM(ts, dataOwner), 1000); err != nil {
						sentry.CaptureException(err)
						continue
					}
				}
			}
		}

		fs.RemoveAll(subDir)
	}

	contextLogger.Info("task finished")
	return nil
}

func main() {
	postgresURI := os.Getenv("POSTGRES_URI")
	s3Bucket := os.Getenv("AWS_S3_BUCKET")
	workingDir := os.Getenv("DATA_PARSER_WORKING_DIR")

	db := storage.NewPostgresORMDB(postgresURI)

	for {
		task, err := storage.GetNextRunningTask(db)
		if err != nil {
			sentry.CaptureException(err)
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
			sentry.CaptureException(err)
		}
	}
}
