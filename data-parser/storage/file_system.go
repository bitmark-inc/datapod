package storage

import (
	"io/ioutil"
)

type FileSystem interface {
	ListFileNames(dirname string) []string
	ReadFile(filename string) ([]byte, error)
}

type LocalFileSystem struct{}

func (l *LocalFileSystem) ListFileNames(dirname string) []string {
	filenames := make([]string, 0)

	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if !f.IsDir() {
			filenames = append(filenames, f.Name())
		}
	}

	return filenames
}

func (l *LocalFileSystem) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}
