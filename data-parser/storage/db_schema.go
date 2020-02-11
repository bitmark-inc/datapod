package storage

import (
	"time"
)

type Archive struct {
	ID          string
	File        string
	FileName    string
	FileSize    int
	UploadedAt  string
	DataOwnerID string
}

func (Archive) TableName() string {
	return "archives_archive"
}

type TaskStatusType int

const (
	TaskStatusPending  = 1
	TaskStatusRunning  = 10
	TaskStatusFailed   = 99
	TaskStatusFinished = 100
)

type Task struct {
	ID          string
	DataOwnerID string
	ArchiveID   string
	Archive     Archive `gorm:"foreignkey:ArchiveID;association_foreignkey:ID"`
	Status      int
	CreatedAt   time.Time
}

func (Task) TableName() string {
	return "tasks_task"
}
