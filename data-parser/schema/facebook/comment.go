package facebook

import (
	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
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
