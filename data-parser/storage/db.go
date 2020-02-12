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

func GetPendingTasks(db *gorm.DB) ([]*Task, error) {
	tasks := make([]*Task, 0)

	dataOwnerWithRunningTasks := make([]string, 0)
	err := db.
		Table("tasks_task").
		Select("data_owner_id").
		Where("status = ?", TaskStatusRunning).
		Group("data_owner_id").Having("COUNT(1) > ?", 0).
		Pluck("data_owner_id", &dataOwnerWithRunningTasks).Error
	if err != nil {
		return nil, err
	}

	query := db.
		Preload("Archive").
		Table("tasks_task").
		Select("DISTINCT ON (data_owner_id) id, data_owner_id, archive_id").
		Where("status = ?", TaskStatusPending).
		Order("data_owner_id, created_at ASC")
	if len(dataOwnerWithRunningTasks) > 0 {
		query = query.Not("data_owner_id in (?)", dataOwnerWithRunningTasks)
	}
	if err := query.Find(&tasks).Error; err != nil {
		return nil, err
	}

	return tasks, nil
}

func UpdateTaskStatus(db *gorm.DB, task *Task, status TaskStatusType) error {
	return db.Model(task).UpdateColumn("status", status).Error
}
