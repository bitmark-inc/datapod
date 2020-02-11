package storage

import (
	"time"
)

type Archive struct {
	ID          string
	File        string
	FileName    string
	FileSize    int
	UploadedAt  time.Time
	DataOwnerID string
}

func (Archive) TableName() string {
	return "archives_archive"
}

type TaskStatusType int

const (
	TaskStatusPending  = TaskStatusType(1)
	TaskStatusRunning  = TaskStatusType(10)
	TaskStatusFailed   = TaskStatusType(99)
	TaskStatusFinished = TaskStatusType(100)
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
