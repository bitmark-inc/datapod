package facebook

import (
	"path/filepath"
	"time"

	"github.com/alecthomas/jsonschema"
	"github.com/xeipuuv/gojsonschema"
)

type Post struct {
	PKID                  int `gorm:"column:pk_id" sql:"PRIMARY_KEY;DEFAULT:nextval('posts_post_pk_id_seq')"`
	PostID                int
	Timestamp             int
	UpdateTimestamp       int
	Date                  string
	Weekday               int
	Title                 string
	Post                  string
	ExternalContextURL    string
	ExternalContextSource string
	ExternalContextName   string
	EventName             string
	EventStartTimestamp   int
	EventEndTimestamp     int
	MediaAttached         bool
	DataOwnerID           string
	MediaItems            []PostMedia `gorm:"foreignkey:PostID;association_foreignkey:PKID"`
	Places                []Place     `gorm:"foreignkey:PostID;association_foreignkey:PPID"`
}

func (Post) TableName() string {
	return "posts_post"
}

type PostMedia struct {
	PMID              int
	MediaURI          string
	FilenameExtension string
	DataOwnerID       string
	PostID            int `gorm:"column:post_id_id"`
}

func (PostMedia) TableName() string {
	return "post_media_postmedia"
}

type Place struct {
	PPID        int
	Name        string
	Address     string
	Latitude    float64
	Longitude   float64
	DataOwnerID string
	PostID      int `gorm:"column:post_id_id"`
}

func (Place) TableName() string {
	return "places_place"
}

type Tag struct {
	TFID        int
	FriendID    int
	DataOwnerID string
	PostID      int
	TagsID      int
	name        string
}

type RawPosts struct {
	Items []*RawPost
}

func (r *RawPosts) ORM(dataOwner string, postID *int, postMediaID *int, placeID *int) []Post {
	posts := make([]Post, 0)

	for _, rp := range r.Items {
		ts := time.Unix(int64(rp.Timestamp), 0)
		post := Post{
			PostID:      *postID,
			Timestamp:   rp.Timestamp,
			Date:        ts.Format("2006-01-02"),
			Weekday:     int(ts.Weekday()),
			Title:       string(rp.Title),
			DataOwnerID: dataOwner,
		}

		for _, d := range rp.Data {
			if d.Post != "" {
				post.Post = string(d.Post)
			}
			if d.UpdateTimestamp != 0 {
				post.UpdateTimestamp = d.UpdateTimestamp
			}
		}

		for _, a := range rp.Attachments {
			for _, item := range a.Data {
				if item.Media != nil {
					post.MediaAttached = true
					postMedia := PostMedia{
						PMID:              *postMediaID,
						MediaURI:          string(item.Media.URI),
						FilenameExtension: filepath.Ext(string(item.Media.URI)),
						DataOwnerID:       dataOwner,
					}
					post.MediaItems = append(post.MediaItems, postMedia)
					*postMediaID++
				}
				if item.ExternalContext != nil {
					post.ExternalContextName = string(item.ExternalContext.Name)
					post.ExternalContextSource = string(item.ExternalContext.Source)
					post.ExternalContextURL = string(item.ExternalContext.URL)
				}
				if item.Event != nil {
					post.EventName = string(item.Event.Name)
					post.EventStartTimestamp = item.Event.StartTimestamp
					post.EventEndTimestamp = item.Event.EndTimestamp
				}
				if item.Place != nil {
					place := Place{
						PPID:        *placeID,
						Name:        string(item.Place.Name),
						Address:     string(item.Place.Address),
						Latitude:    item.Place.Coordinate.Latitude,
						Longitude:   item.Place.Coordinate.Longitude,
						DataOwnerID: dataOwner,
						PostID:      post.PostID,
					}
					*placeID++
					post.Places = append(post.Places, place)
				}
			}
		}

		*postID++
		posts = append(posts, post)
	}

	return posts
}

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
	Place           *Location        `json:"place"`
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
	Place           *Location      `json:"place"`
	Description     MojibakeString `json:"description"`
	CreateTimestamp int            `json:"create_timestamp"`
}

type Location struct {
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
	Location         *Location      `json:"location" jsonschema:"required"`
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
