package storage

import (
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type FileSystem interface {
	ListFileNames(dirname string) ([]string, error)
	ReadFile(filename string) ([]byte, error)
}

type LocalFileSystem struct{}

func (l *LocalFileSystem) ListFileNames(dirname string) ([]string, error) {
	filenames := make([]string, 0)

	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if !f.IsDir() {
			filenames = append(filenames, f.Name())
		}
	}

	return filenames, nil
}

func (l *LocalFileSystem) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

type S3FileSystem struct {
	svc *s3.S3
}

func NewS3FileSystem(sess *session.Session) *S3FileSystem {
	return &S3FileSystem{s3.New(sess)}
}

func (s *S3FileSystem) ListFileNames(dirname string) ([]string, error) {
	filenames := make([]string, 0)

	parts := strings.Split(dirname, "/")
	input := &s3.ListObjectsInput{
		Bucket:    aws.String(parts[0]),
		Prefix:    aws.String(strings.Join(parts[1:], "/") + "/"),
		Delimiter: aws.String("/"),
	}
	output, err := s.svc.ListObjects(input)
	if err != nil {
		return nil, err
	}
	for _, c := range output.Contents {
		if c.Key != nil {
			filenames = append(filenames, filepath.Base(*c.Key))
		}
	}

	return filenames, nil
}

func (s *S3FileSystem) ReadFile(filename string) ([]byte, error) {
	parts := strings.Split(filename, "/")
	input := &s3.GetObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(strings.Join(parts[1:], "/")),
	}
	output, err := s.svc.GetObject(input)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(output.Body)
}
