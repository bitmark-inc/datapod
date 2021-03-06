package facebook

import (
	"time"

	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type RawComments struct {
	Comments []Comment `json:"comments" jsonschema:"required"`
}

type Comment struct {
	Timestamp   int               `json:"timestamp" jsonschema:"required"`
	Title       MojibakeString    `json:"title" jsonschema:"required"`
	Data        []*CommentWrapper `json:"data"`
	Attachments []*Attachment     `json:"attachments"`
}

type CommentWrapper struct {
	Comment CommentData `json:"comment" jsonschema:"required"`
}

type CommentData struct {
	Timestamp int            `json:"timestamp" jsonschema:"required"`
	Comment   MojibakeString `json:"comment" jsonschema:"required"`
	Author    MojibakeString `json:"author"`
	Group     MojibakeString `json:"group"`
}

func CommentArraySchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	s := reflector.Reflect(&RawComments{})
	data, _ := s.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}

type CommentORM struct {
	CommentsID  int64
	Timestamp   int
	Author      string
	Comment     string
	Date        string
	Weekday     int
	DataOwnerID string
}

func (CommentORM) TableName() string {
	return "comments_comment"
}

func (c RawComments) ORM(parseTime int64, owner string) []interface{} {
	idx := 0
	result := make([]interface{}, 0)
	for _, c := range c.Comments {
		t := time.Unix(int64(c.Timestamp), 0)
		orm := CommentORM{
			CommentsID:  tableForeignKey(parseTime, idx),
			Timestamp:   c.Timestamp,
			Date:        dateOfTime(t),
			Weekday:     weekdayOfTime(t),
			DataOwnerID: owner,
		}
		if len(c.Data) > 0 {
			orm.Author = string(c.Data[0].Comment.Author)
			orm.Comment = string(c.Data[0].Comment.Comment)
		}

		result = append(result, orm)
		idx++
	}
	return result
}
