package facebook

import (
	"fmt"
	"github.com/alecthomas/jsonschema"
	"github.com/jinzhu/gorm"
	"github.com/xeipuuv/gojsonschema"
	"time"
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

type ReactionORM struct {
	gorm.Model
	ReactionID  int `gorm:"column:reaction_id"`
	Timestamp   int
	Date        string
	Weekday     int
	Title       string
	Actor       string
	Reaction    string
	DataOwnerID string `gorm:"column:data_owner_id"`
}

func (r RawReaction) Write(db *gorm.DB) {
	for _, reaction := range r.Reactions {
		t := time.Unix(int64(reaction.Timestamp), 0)

		title, err := reaction.Title.String()
		if nil != err {
			fmt.Printf("convert reaction title with error: %s", err)
		}

		tmp := reaction.Data[0]
		actor, err := tmp.Actor.String()
		if nil != err {
			fmt.Printf("convert reaction actor with error: %s", err)
		}

		react, err := tmp.Actor.String()
		if nil != err {
			fmt.Printf("convert reaction with error: %s", err)
		}

		orm := ReactionORM{
			ReactionID:  0, // TODO: reaction id
			Timestamp:   reaction.Timestamp,
			Date:        dateOfTime(t),
			Weekday:     weekdayOfTime(t),
			Title:       title,
			Actor:       actor,
			Reaction:    react,
			DataOwnerID: "", // TODO: data owner id
		}

		// db.Create(orm) // TODO: batch update
	}
}
