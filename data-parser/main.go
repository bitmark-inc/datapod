package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
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

func main() {
	dbURI := os.Args[1]
	rootDir := os.Args[2]
	dataOwner := os.Args[3]

	db, err := gorm.Open("postgres", dbURI)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	workingDir := fmt.Sprintf("%s/%s", rootDir, dataOwner)
	fs := &storage.LocalFileSystem{}

	parseTimestamp := time.Now().UnixNano() / 1000
	postID := int(parseTimestamp)
	postMediaID := int(parseTimestamp)
	placeID := int(parseTimestamp)
	tagID := int(parseTimestamp)

	for _, pattern := range patterns {
		files, err := pattern.SelectFiles(fs, workingDir)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			data, err := fs.ReadFile(file)
			if err != nil {
				panic(err)
			}

			if err := pattern.Validate(data); err != nil {
				errors := strings.Split(err.Error(), "\n")
				fmt.Println(workingDir, files, errors[0])
				continue
			}

			switch pattern.Name {
			case "posts":
				rawPosts := facebook.RawPosts{Items: make([]*facebook.RawPost, 0)}
				json.Unmarshal(data, &rawPosts.Items)
				posts, complexPosts := rawPosts.ORM(dataOwner, &postID, &postMediaID, &placeID, &tagID)
				if err := gormbulk.BulkInsert(db, posts, 1000); err != nil {
					panic(err)
				}
				for _, p := range complexPosts {
					if len(p.Tags) > 0 {
						friends := make([]facebook.FriendORM, 0)
						if err := db.Where("data_owner_id = ?", dataOwner).Find(&friends).Error; err != nil {
							panic(err)
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
						panic(err)
					}
				}
			case "comments":
				rawComments := &facebook.RawComments{}
				json.Unmarshal(data, &rawComments)
				if err := gormbulk.BulkInsert(db, rawComments.ORM(parseTimestamp, dataOwner), 1000); err != nil {
					panic(err)
				}
			case "reactions":
				rawReactions := &facebook.RawReactions{}
				json.Unmarshal(data, &rawReactions)
				if err := gormbulk.BulkInsert(db, rawReactions.ORM(parseTimestamp, dataOwner), 1000); err != nil {
					panic(err)
				}
			case "friends":
				rawFriends := &facebook.RawFriends{}
				json.Unmarshal(data, &rawFriends)
				if err := gormbulk.BulkInsert(db, rawFriends.ORM(parseTimestamp, dataOwner), 1000); err != nil {
					panic(err)
				}
			}
		}
	}
}
