package main

import (
	"encoding/json"
	"fmt"
	"github.com/datapod/data-parser/schema/facebook"
	"github.com/datapod/data-parser/storage"
	"os"
	"regexp"
)

var patterns = []facebook.Pattern{
	{Name: "posts", Location: "posts", Regexp: regexp.MustCompile("your_posts(?P<index>_[0-9]+).json"), Schema: facebook.PostArraySchemaLoader()},
	{Name: "comments", Location: "comments", Regexp: regexp.MustCompile("comments.json"), Schema: facebook.CommentArraySchemaLoader()},
	{Name: "reactions", Location: "likes_and_reactions", Regexp: regexp.MustCompile("posts_and_comments.json"), Schema: facebook.ReactionSchemaLoader()},
}

func main() {
	workingDir := os.Args[1]

	fs := &storage.LocalFileSystem{}
	for _, pattern := range patterns {
		files := pattern.SelectFiles(fs, workingDir)
		for _, file := range files {
			data, err := fs.ReadFile(file)
			if err != nil {
				panic(err)
			}

			if err := pattern.Validate(data); err != nil {
				panic(err)
			}

			switch pattern.Name {
			case "posts":
				rawPosts := make([]*facebook.RawPost, 0)
				json.Unmarshal(data, &rawPosts)
				fmt.Println(len(rawPosts))
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
			}
		}
	}
}