package facebook

import (
	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type RawPost struct {
	Timestamp   int              `json:"timestamp" jsonschema:"required"`
	Title       MojibakeString   `json:"title"`
	Data        []*PostData      `json:"data" jsonschema:"maxItems=2"`
	Attachments []*Attachment    `json:"attachments"` // usually the length is 1
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
	ForSaleItem     *ForSaleItem     `json:"for_sale_item"`
	Fundraiser      *Fundraiser      `json:"fundraiser"`
	Media           *Media           `json:"media"`
	Note            *Note            `json:"note"`
	Place           *Place           `json:"place"`
	Poll            *Poll            `json:"poll"`
	Name            MojibakeString   `json:"name"`
	Text            MojibakeString   `json:"text"`
}

type ExternalContext struct {
	Name   MojibakeString `json:"name"`
	Source MojibakeString `json:"source"`
	URL    MojibakeString `json:"url"`
}

type Event struct {
	Name            MojibakeString `json:"name" jsonschema:"required"`
	StartTimestamp  int            `json:"start_timestamp" jsonschema:"required"`
	EndTimestamp    int            `json:"end_timestamp" jsonschema:"required"`
	Place           *Place         `json:"place"`
	Description     MojibakeString `json:"description"`
	CreateTimestamp int            `json:"create_timestamp"`
}

type Place struct {
	Name       MojibakeString `json:"name"`
	Coordinate *Coordinate    `json:"coordinate"`
	Address    MojibakeString `json:"address"`
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
	CameraMake        MojibakeString `json:"camera_make"`
	CameraModel       MojibakeString `json:"camera_model"`
	TakenTimestamp    int            `json:"taken_timestamp"`
	ModifiedTimestamp int            `json:"modified_timestamp"`
	Exposure          MojibakeString `json:"exposure"`
	FocalLength       MojibakeString `json:"focal_length"`
	FStop             MojibakeString `json:"f_stop"`
	ISOSpeed          int            `json:"iso_speed"`
	Latitude          float64        `json:"latitude"`
	Longitude         float64        `json:"longitude"`
	Orientation       float64        `json:"orientation"`
	OriginalWidth     int            `json:"original_width"`
	OriginalHeight    int            `json:"original_height"`
	UploadIP          MojibakeString `json:"upload_ip" jsonschema:"required"`
}

type VidoMetadata struct {
	UploadIP        MojibakeString `json:"upload_ip" jsonschema:"required"`
	UploadTimestamp int            `json:"upload_timestamp" jsonschema:"required"`
}

type MediaThumbnail struct {
	URI MojibakeString `json:"uri" jsonschema:"required"`
}

type MediaComment struct {
	Comment   MojibakeString `json:"comment" jsonschema:"required"`
	Timestamp int64          `json:"timestamp" jsonschema:"required"`
	Author    MojibakeString `json:"author"`
	Group     MojibakeString `json:"group"`
}

type Poll struct {
	Question MojibakeString `json:"question" jsonschema:"required"`
	Options  []*PollOption  `json:"options" jsonschema:"required"`
}

type PollOption struct {
	Option MojibakeString `json:"option" jsonschema:"required"`
	Voted  bool           `json:"voted" jsonschema:"required"`
}

type ForSaleItem struct {
	Title            MojibakeString `json:"title" jsonschema:"required"`
	Price            MojibakeString `json:"price" jsonschema:"required"`
	Seller           MojibakeString `json:"seller" jsonschema:"required"`
	CreatedTimestamp int            `json:"created_timestamp" jsonschema:"required"`
	UpdatedTimestamp int            `json:"updated_timestamp" jsonschema:"required"`
	Marketplace      MojibakeString `json:"marketplace" jsonschema:"required"`
	Location         *Place         `json:"location" jsonschema:"required"`
	Description      MojibakeString `json:"description" jsonschema:"required"`
	Category         MojibakeString `json:"category"`
}

type Note struct {
	Tags             []*NoteTag     `json:"tags" jsonschema:"required"`
	Text             MojibakeString `json:"text" jsonschema:"required"`
	Title            MojibakeString `json:"title" jsonschema:"required"`
	CreatedTimestamp int            `json:"created_timestamp" jsonschema:"required"`
	UpdatedTimestamp int            `json:"updated_timestamp" jsonschema:"required"`
	Media            []*Media       `json:"media"`
	CoverPhoto       Media          `json:"cover_photo"`
}

type NoteTag struct {
	Name MojibakeString `json:"name" jsonschema:"required"`
}

type Fundraiser struct {
	Title         MojibakeString `json:"title" jsonschema:"required"`
	DonatedAmount MojibakeString `json:"donated_amount" jsonschema:"required"`
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
