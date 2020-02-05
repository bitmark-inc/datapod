package facebook

import (
	"time"

	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type RawReactions struct {
	Reactions []*Reaction `json:"reactions" jsonschema:"required"`
}

type Reaction struct {
	Timestamp int               `json:"timestamp" jsonschema:"required"`
	Title     MojibakeString    `json:"title" jsonschema:"required"`
	Data      []ReactionWrapper `json:"data"`
}

type ReactionWrapper struct {
	Reaction ReactionData `json:"reaction" jsonschema:"required"`
}

type ReactionData struct {
	Reaction string         `json:"reaction" jsonschema:"required"`
	Actor    MojibakeString `json:"actor" jsonschema:"required"`
}

func ReactionSchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	s := reflector.Reflect(&RawReactions{})
	data, _ := s.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}

type ReactionORM struct {
	ReactionID  int64
	Timestamp   int
	Date        string
	Weekday     int
	Title       string
	Actor       string
	Reaction    string
	DataOwnerID string
}

func (ReactionORM) TableName() string {
	return "reactions_reaction"
}

func (r RawReactions) ORM(parseTime int64, owner string) []interface{} {
	idx := 0
	result := make([]interface{}, 0)
	for _, r := range r.Reactions {
		t := time.Unix(int64(r.Timestamp), 0)
		orm := ReactionORM{
			ReactionID:  tableForeignKey(parseTime, idx),
			Timestamp:   r.Timestamp,
			Date:        dateOfTime(t),
			Weekday:     weekdayOfTime(t),
			Title:       string(r.Title),
			DataOwnerID: owner,
		}
		if len(r.Data) > 0 {
			orm.Actor = string(r.Data[0].Reaction.Actor)
			orm.Reaction = string(r.Data[0].Reaction.Reaction)
		}

		result = append(result, orm)
		idx++
	}
	return result
}
