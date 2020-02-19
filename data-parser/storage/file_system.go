package storage

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
)

var (
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
)

func init() {
	// TODO: could the regsion set from environment variable?
	sess, err := session.NewSession(&aws.Config{Region: aws.String(endpoints.ApNortheast1RegionID)})
	if err != nil {
		panic(err)
	}
	uploader = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)
}

type DirectoryIterator struct {
	baseDir   string
	filePaths []string
	bucket    string
	keyPrefix string
	next      struct {
		path string
		f    *os.File
	}
	err error
}

// NewDirectoryIterator creates and returns a new BatchUploadIterator
func NewDirectoryIterator(bucket, dir, keyPrefix string) s3manager.BatchUploadIterator {
	paths := []string{}
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// We care only about files, not directories
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})

	return &DirectoryIterator{
		baseDir:   filepath.Base(dir),
		filePaths: paths,
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

// Next opens the next file and stops iteration if it fails to open
// a file.
func (iter *DirectoryIterator) Next() bool {
	if len(iter.filePaths) == 0 {
		iter.next.f = nil
		return false
	}

	f, err := os.Open(iter.filePaths[0])
	iter.err = err

	iter.next.f = f
	iter.next.path = iter.filePaths[0]

	iter.filePaths = iter.filePaths[1:]
	return true && iter.Err() == nil
}

// Err returns an error that was set during opening the file
func (iter *DirectoryIterator) Err() error {
	return iter.err
}

// UploadObject returns a BatchUploadObject and sets the After field to
// close the file.
func (iter *DirectoryIterator) UploadObject() s3manager.BatchUploadObject {
	parts := strings.Split(iter.next.path, iter.baseDir)
	key := filepath.Join(iter.keyPrefix, iter.baseDir, parts[1])
	f := iter.next.f
	return s3manager.BatchUploadObject{
		Object: &s3manager.UploadInput{
			Bucket: &iter.bucket,
			Key:    &key,
			Body:   f,
		},
		// After was introduced in version 1.10.7
		After: func() error {
			return f.Close()
		},
	}
}

func DownloadArchiveFromS3(bucket, key string, file *os.File) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if _, err := downloader.Download(file, input); err != nil {
		return err
	}
	return nil
}

func UploadDirToS3(bucket, keyPrefix, dirpath string) error {
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		return nil
	}

	iter := NewDirectoryIterator(bucket, dirpath, keyPrefix)
	return uploader.UploadWithIterator(aws.BackgroundContext(), iter)
}

func CreateFile(fs afero.Fs, path string) (*os.File, error) {
	if err := fs.MkdirAll(filepath.Dir(path), os.FileMode(0777)); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func ExtractArchive(source, target, destination string) error {
	r, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		if within(target, f.Name) {
			fpath := filepath.Join(destination, f.Name)

			// check for Zip Slip vulnerability: http://bit.ly/2MsjAWE
			if !strings.HasPrefix(fpath, filepath.Clean(destination)+string(os.PathSeparator)) {
				return fmt.Errorf("zip slip detected")
			}

			if f.FileInfo().IsDir() {
				if err := os.MkdirAll(fpath, os.ModePerm); err != nil {
					return err
				}
				continue
			}

			if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
				return err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}

			rc, err := f.Open()
			if err != nil {
				return err
			}

			_, err = io.Copy(outFile, rc)

			outFile.Close()
			rc.Close()

			if err != nil {
				return err
			}
		}
	}
	return nil
}

func within(parent, sub string) bool {
	rel, err := filepath.Rel(parent, sub)
	if err != nil {
		return false
	}
	return !strings.Contains(rel, "..")
}
