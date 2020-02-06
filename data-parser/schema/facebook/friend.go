package facebook

import (
	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type RawFriends struct {
	Friends []*Friend `json:"friends" jsonschema:"required"`
}

type Friend struct {
	Timestamp   int            `json:"timestamp" jsonschema:"required"`
	Name        MojibakeString `json:"name" jsonschema:"required"`
	ContactInfo MojibakeString `json:"contact_info"`
}

func FriendSchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	s := reflector.Reflect(&RawFriends{})
	data, _ := s.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}

type FriendORM struct {
	PKID        int   `gorm:"column:pk_id" sql:"PRIMARY_KEY;DEFAULT:nextval('friends_friend_pk_id_seq')"`
	FriendID    int64 `gorm:"column:tags_id"`
	FriendName  string
	Timestamp   int
	DataOwnerID string
}

func (FriendORM) TableName() string {
	return "friends_friend"
}

// FIXME: friends can have the same name
func (r RawFriends) ORM(parseTime int64, owner string) []interface{} {
	idx := 0
	result := make([]interface{}, 0)

	seen := make(map[string]bool)
	for _, f := range r.Friends {
		name := string(f.Name)
		if seen[name] == true {
			continue
		}
		seen[name] = true

		orm := FriendORM{
			FriendID:    tableForeignKey(parseTime, idx),
			FriendName:  name,
			Timestamp:   f.Timestamp,
			DataOwnerID: owner,
		}

		result = append(result, orm)
		idx++
	}
	return result
}
