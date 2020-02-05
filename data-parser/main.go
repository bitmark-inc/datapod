package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	"github.com/datapod/data-parser/schema/facebook"
	"github.com/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	{Name: "posts", Location: "posts", Regexp: regexp.MustCompile("your_posts(?P<index>_[0-9]+).json"), Schema: facebook.PostArraySchemaLoader()},
	{Name: "comments", Location: "comments", Regexp: regexp.MustCompile("comments.json"), Schema: facebook.CommentArraySchemaLoader()},
	{Name: "reactions", Location: "likes_and_reactions", Regexp: regexp.MustCompile("posts_and_comments.json"), Schema: facebook.ReactionSchemaLoader()},
	{Name: "friends", Location: "friends", Regexp: regexp.MustCompile("friends.json"), Schema: facebook.FriendSchemaLoader()},
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

	// TODO
	parseTime := 0
	postID := parseTime
	postMediaID := parseTime
	placeID := parseTime

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
				posts := rawPosts.ORM(dataOwner, &postID, &postMediaID, &placeID)
				for _, p := range posts {
					if err := db.Create(&p).Error; err != nil {
						panic(err)
					}
				}
			case "comments":
				rawComments := &facebook.RawComment{}
				err := json.Unmarshal(data, &rawComments)
				if nil != err {
					fmt.Printf("unmarshal comment with error: %s\n", err)
				}
			case "reactions":
				rawReactions := &facebook.RawReaction{}
				err := json.Unmarshal(data, &rawReactions)
				if nil != err {
					fmt.Printf("unmarshal reaction with error: %s", err)
				}
			case "friends":
				rawFriends := &facebook.RawFriend{}
				err := json.Unmarshal(data, &rawFriends)
				if nil != err {
					fmt.Printf("unmarshal friend with error: %s", err)
				}
			}
		}
	}
}
