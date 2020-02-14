package storage

import (
	"compress/flate"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mholt/archiver"
	"github.com/spf13/afero"
)

var (
	downloader *s3manager.Downloader
)

func init() {
	// TODO: could the regsion set from environment variable?
	sess := session.New(&aws.Config{Region: aws.String(endpoints.ApNortheast1RegionID)})
	downloader = s3manager.NewDownloader(sess)
}

func CreateFile(fs afero.Fs, path string) (*os.File, error) {
	if err := fs.MkdirAll(filepath.Dir(path), os.FileMode(0777)); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func DownloadArchive(bucket, key string, file *os.File) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if _, err := downloader.Download(file, input); err != nil {
		return err
	}
	return nil
}

func ExtractArchive(source, target, destination string) error {
	z := archiver.Zip{
		CompressionLevel:       flate.DefaultCompression,
		MkdirAll:               true,
		SelectiveCompression:   true,
		ContinueOnError:        false,
		OverwriteExisting:      false,
		ImplicitTopLevelFolder: false,
	}
	return z.Extract(source, target, destination)
}
