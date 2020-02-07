package storage

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

func NewPostgresORMDB(dbURI string) *gorm.DB {
	db, err := gorm.Open("postgres", dbURI)
	if err != nil {
		panic(err)
	}
	return db
}
