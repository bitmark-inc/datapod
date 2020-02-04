package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"github.com/datapod/data-parser/schema/facebook"
	"github.com/datapod/data-parser/storage"
)

var patterns = []facebook.Pattern{
	{Name: "posts", Location: "posts", Regexp: regexp.MustCompile("your_posts(?P<index>_[0-9]+).json"), Schema: facebook.PostArraySchemaLoader()},
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
			}
		}
	}
}
