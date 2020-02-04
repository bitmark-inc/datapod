package facebook

import (
	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type RawReaction struct {
	Reactions []*Reaction `json:"reactions" jsonschema:"required"`
}

type Reaction struct {
	Timestamp int            `json:"timestamp" jsonschema:"required"`
	Title     MojibakeString `json:"title" jsonschema:"required"`
	Data      []ReactionData `json:"data"`
}

type ReactionData struct {
	Reaction string         `json:"string" jsonschema:"required"`
	Actor    MojibakeString `json:"actor" jsonschema:"required"`
}

func ReactionSchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	reactionSchema := reflector.Reflect(&RawReaction{})
	reactionsSchema := &jsonschema.Schema{Type: &jsonschema.Type{
		Version: jsonschema.Version,
		Type:    "object",
		Items:   reactionSchema.Type,
	}, Definitions: reactionSchema.Definitions}

	data, _ := reactionsSchema.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}
