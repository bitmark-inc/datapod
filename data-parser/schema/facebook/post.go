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
	Places                []Place     `gorm:"foreignkey:PostID;association_foreignkey:PKID"`
	Tags                  []Tag       `gorm:"foreignkey:PostID;association_foreignkey:PKID"`
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
	TFID        int `gorm:"column:tfid"`
	DataOwnerID string
	PostID      int    `gorm:"column:post_id_id"`
	FriendID    int    `gorm:"column:tags_id"`
	Name        string `gorm:"-"`
}

func (Tag) TableName() string {
	return "tags_tag"
}

type RawPosts struct {
	Items []*RawPost
}

func (r *RawPosts) ORM(dataOwner string, postID *int, postMediaID *int, placeID *int, tagID *int) ([]interface{}, []Post) {
	posts := make([]interface{}, 0)
	complexPosts := make([]Post, 0)

	for _, rp := range r.Items {
		ts := time.Unix(int64(rp.Timestamp), 0)
		post := Post{
			PostID:      *postID,
			Timestamp:   rp.Timestamp,
			Date:        dateOfTime(ts),
			Weekday:     weekdayOfTime(ts),
			Title:       string(rp.Title),
			DataOwnerID: dataOwner,
		}
		*postID++

		for _, d := range rp.Data {
			if d.Post != "" {
				post.Post = string(d.Post)
			}
			if d.UpdateTimestamp != 0 {
				post.UpdateTimestamp = d.UpdateTimestamp
			}
		}

		complex := false
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
					*postMediaID++
					post.MediaItems = append(post.MediaItems, postMedia)
					complex = true
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
						DataOwnerID: dataOwner,
					}
					if item.Place.Coordinate != nil {
						place.Latitude = item.Place.Coordinate.Latitude
						place.Longitude = item.Place.Coordinate.Longitude
					}
					*placeID++
					post.Places = append(post.Places, place)
					complex = true
				}
			}
		}

		if len(rp.Tags) > 0 {
			for _, t := range rp.Tags {
				tag := Tag{
					TFID:        *tagID,
					DataOwnerID: dataOwner,
					Name:        string(t),
				}
				*tagID++
				post.Tags = append(post.Tags, tag)
				complex = true
			}
		}

		if complex {
			complexPosts = append(complexPosts, post)
		} else {
			posts = append(posts, post)
		}
	}

	return posts, complexPosts
}

type RawPost struct {
	Timestamp   int              `json:"timestamp" jsonschema:"required"`
	Title       MojibakeString   `json:"title"`
	Data        []*PostData      `json:"data" jsonschema:"maxItems=2"`
	Attachments []*Attachment    `json:"attachments"`
	Tags        []MojibakeString `json:"tags"`
}

type PostData struct {
	Post               MojibakeString `json:"post"`
	UpdateTimestamp    int            `json:"update_timestamp"`
	BackdatedTimestamp int            `json:"backdated_timestamp"`
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
