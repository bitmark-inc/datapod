package facebook

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/xeipuuv/gojsonschema"

	"github.com/datapod/data-parser/storage"
)

type Pattern struct {
	Name     string
	Location string
	Regexp   *regexp.Regexp
	Schema   *gojsonschema.Schema
}

func (p *Pattern) SelectFiles(fs storage.FileSystem, dir string) ([]string, error) {
	targetedFiles := make([]string, 0)

	// exists, err := fs.Exists(context.Background(), dir)
	// if err != nil {
	// 	return nil, fmt.Errorf("unable to list files under directory %s: %s", dir, err)
	// }
	// if !exists {
	// 	return nil, nil
	// }

	names, err := fs.ListFileNames(dir)
	if err != nil {
		return nil, fmt.Errorf("unable to list files under directory %s: %s", dir, err)
	}
	for _, name := range names {
		if p.Regexp.MatchString(name) {
			targetedFiles = append(targetedFiles, filepath.Join(dir, name))
		}
	}

	return targetedFiles, nil
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
