package facebook

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/datapod/data-parser/storage"
	"github.com/xeipuuv/gojsonschema"
)

type Pattern struct {
	Name     string
	Location string
	Regexp   *regexp.Regexp
	Schema   *gojsonschema.Schema
}

func (p *Pattern) SelectFiles(fs storage.FileSystem, parentDir string) []string {
	targetedFiles := make([]string, 0)

	childDir := filepath.Join(parentDir, p.Location)
	for _, name := range fs.ListFileNames(childDir) {
		if p.Regexp.MatchString(name) {
			filename := filepath.Join(childDir, name)
			targetedFiles = append(targetedFiles, filename)
		}
	}

	return targetedFiles
}

func (p *Pattern) Validate(data []byte) error {
	docLoader := gojsonschema.NewStringLoader(string(data))
	result, err := p.Schema.Validate(docLoader)
	if err != nil {
		return err
	}
	if !result.Valid() {
		reasons := make([]string, 0)
		for _, desc := range result.Errors() {
			reasons = append(reasons, desc.String())
		}
		return errors.New(strings.Join(reasons, "\n"))
	}
	return nil
}
