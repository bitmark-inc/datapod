package facebook

import (
	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type RawPost struct {
	Title       MojibakeString   `json:"title"`
	Data        []*PostData      `json:"data" jsonschema:"maxItems=2"`
	Attachments []*Attachment    `json:"attachments"` // usually the length is 1
	Timestamp   int              `json:"timestamp" jsonschema:"required"`
	Tags        []MojibakeString `json:"tags"`
}

type PostData struct {
	Post               MojibakeString `json:"post"`
	UpdateTimestamp    int            `json:"update_timestamp"`
	BackdatedTimestamp int            `json:"backdated_timestamp"`
}

type Attachment struct {
	Data []*AttachmentData `json:"data" jsonschema:"required"`
}

type AttachmentData struct {
	ExternalContext *ExternalContext `json:"external_context"`
	Event           *Event           `json:"event"`
	Place           *Place           `json:"place"`
	Media           *Media           `json:"media"`
	Name            MojibakeString   `json:"name"`
	Text            MojibakeString   `json:"text"`
	Poll            *Poll            `json:"poll"`
}

type ExternalContext struct {
	Name   MojibakeString `json:"name"`
	Source MojibakeString `json:"source"`
	URL    MojibakeString `json:"url"`
}

type Event struct {
	Name           MojibakeString `json:"name" jsonschema:"required"`
	StartTimestamp int            `json:"start_timestamp" jsonschema:"required"`
	EndTimestamp   int            `json:"end_timestamp" jsonschema:"required"`
}

type Place struct {
	Name       MojibakeString `json:"name" jsonschema:"required"`
	Coordinate *Coordinate    `json:"coordinate"`
	Address    MojibakeString `json:"address" jsonschema:"required"`
	URL        MojibakeString `json:"url"`
}

type Coordinate struct {
	Latitude  float64 `json:"latitude" jsonschema:"required"`
	Longitude float64 `json:"longitude" jsonschema:"required"`
}

type Media struct {
	URI               MojibakeString  `json:"uri" jsonschema:"required"`
	CreationTimestamp int             `json:"creation_timestamp" `
	MediaMetadata     *MediaMetadata  `json:"media_metadata"`
	Thumbnail         *MediaThumbnail `json:"thumbnail"`
	Commens           []*MediaComment `json:"comments"`
	Title             MojibakeString  `json:"title" `
	Description       MojibakeString  `json:"description"`
}

type MediaMetadata struct {
	PhotoMetadata *PhotoMetadata `json:"photo_metadata"`
	VidoMetadata  *VidoMetadata  `json:"video_metadata"`
}

type PhotoMetadata struct {
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	Orientation float64        `json:"orientation"`
	UploadIP    MojibakeString `json:"upload_ip" jsonschema:"required"`
}

type VidoMetadata struct {
	UploadIP        MojibakeString `json:"upload_ip" jsonschema:"required"`
	UploadTimestamp int            `json:"upload_timestamp" jsonschema:"required"`
}

type MediaThumbnail struct {
	URI MojibakeString `json:"uri" jsonschema:"required"`
}

type MediaComment struct {
	Author    MojibakeString `json:"author" jsonschema:"required"`
	Comment   MojibakeString `json:"comment" jsonschema:"required"`
	Timestamp int64          `json:"timestamp" jsonschema:"required"`
}

type Poll struct {
	Question MojibakeString `json:"question" jsonschema:"required"`
	Options  []*PollOption  `json:"options" jsonschema:"required"`
}

type PollOption struct {
	Option MojibakeString `json:"option" jsonschema:"required"`
	Voted  bool           `json:"voted" jsonschema:"required"`
}

func PostArraySchemaLoader() *gojsonschema.Schema {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	postSchema := reflector.Reflect(&RawPost{})
	postsSchema := &jsonschema.Schema{Type: &jsonschema.Type{
		Version: jsonschema.Version,
		Type:    "array",
		Items:   postSchema.Type,
	}, Definitions: postSchema.Definitions}

	data, _ := postsSchema.MarshalJSON()
	schemaLoader := gojsonschema.NewStringLoader(string(data))
	schema, _ := gojsonschema.NewSchema(schemaLoader)
	return schema
}
