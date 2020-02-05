package facebook

import (
	"fmt"
	"github.com/alecthomas/jsonschema"
	"github.com/jinzhu/gorm"
	"github.com/xeipuuv/gojsonschema"
)

type RawFriend struct {
	Friends []*Friend `json:"friends" jsonschema:"required"`
}

type Friend struct {
	Timestamp int            `json:"timestamp" jsonschema:"required"`
	Name      MojibakeString `json:"name" jsonschema:"required"`
}

func FriendSchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	friendSchema := reflector.Reflect(&RawFriend{})
	friendsSchema := &jsonschema.Schema{Type: &jsonschema.Type{
		Version: jsonschema.Version,
		Type:    "object",
		Items:   friendSchema.Type,
	}, Definitions: friendSchema.Definitions}

	data, _ := friendsSchema.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}

type FriendORM struct {
	gorm.Model
	FriendID    int64 `gorm:"column:friend_id"`
	FriendName  string
	Timestamp   int
	DataOwnerID string `gorm:"column:data_owner_id"`
}

func (r RawFriend) ORM(parseTime int) []interface{} {
	idx := 0
	result := make([]interface{}, 0)
	for _, friend := range r.Friends {
		name, err := friend.Name.String()
		if nil != err {
			fmt.Printf("convert friend title with error: %s", err)
		}

		orm := FriendORM{
			FriendID:    tableForeignKey(parseTime, idx),
			FriendName:  name,
			Timestamp:   friend.Timestamp,
			DataOwnerID: "", // TODO: data owner id
		}

		idx++

		result = append(result, orm)
	}

	return result
}
