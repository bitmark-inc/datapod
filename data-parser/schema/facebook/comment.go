package facebook

import (
	"fmt"
	"github.com/alecthomas/jsonschema"
	"github.com/jinzhu/gorm"
	"github.com/xeipuuv/gojsonschema"
	"time"
)

type RawComment struct {
	Comments []Comment `json:"comments" jsonschema:"required"`
}

type Comment struct {
	Timestamp int               `json:"timestamp" jsonschema:"required"`
	Title     MojibakeString    `json:"title"`
	Data      []*CommentWrapper `json:"data"`
}

type CommentWrapper struct {
	Comment CommentData `json:"comment"`
}

type CommentData struct {
	Timestamp int            `json:"timestamp" jsonschema:"required"`
	Comment   string         `json:"comment" jsonschema:"required"`
	Author    MojibakeString `json:"author" jsonschema:"required"`
	Group     MojibakeString `json:"group"`
}

func CommentArraySchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	commentSchema := reflector.Reflect(&RawComment{})
	commentsSchema := &jsonschema.Schema{Type: &jsonschema.Type{
		Version: jsonschema.Version,
		Type:    "object",
		Items:   commentSchema.Type,
	}, Definitions: commentSchema.Definitions}

	data, _ := commentsSchema.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}

type CommentORM struct {
	gorm.Model
	CommentsID  int `gorm:"column:comments_id"`
	Timestamp   int
	Author      string
	Comment     string
	Date        string
	Weekday     int
	DataOwnerID string
}

func (c RawComment) Write(db *gorm.DB) {
	for _, comment := range c.Comments {
		tmp := comment.Data[0].Comment
		author, err := tmp.Author.String()
		if nil != err {
			fmt.Printf("convert mojibakestring with error: %s", err)
		}

		t := time.Unix(int64(comment.Timestamp), 0)
		orm := CommentORM{
			CommentsID:  0, // TODO: comments id
			Timestamp:   comment.Timestamp,
			Author:      author,
			Comment:     tmp.Comment,
			Date:        dateOfTime(t),
			Weekday:     weekdayOfTime(t),
			DataOwnerID: "", // TODO: data owner id
		}

		// db.Create(orm) // TODO: batch update
	}
}
