package repomofo

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
)

type AddTagInput struct {
	Name    string
	Tagger  string
	Message string
}

type RemoveTagInput struct {
	Name string
}

func (repo *Repo) addTag(input AddTagInput) (string, error) {
	if !ValidateRefName(input.Name) {
		return "", errors.New("invalid tag name")
	}

	// read HEAD to get the target commit OID
	targetOID, err := repo.ReadHeadRecur()
	if err != nil {
		return "", err
	}

	// write the tag object
	tagOID, err := repo.writeTag(input, targetOID)
	if err != nil {
		return "", err
	}

	// write the tag ref
	refPath := "refs/tags/" + input.Name
	if err := repo.writeRef(refPath, RefOrOid{OID: tagOID}); err != nil {
		return "", err
	}

	return tagOID, nil
}

func (repo *Repo) removeTag(input RemoveTagInput) error {
	tagsDir := filepath.Join(repo.repoDir, "refs", "tags")

	if err := os.Remove(filepath.Join(tagsDir, input.Name)); err != nil {
		return err
	}

	// delete empty parent dirs (for tags with slashes in their name)
	dir := filepath.Dir(filepath.Join(tagsDir, input.Name))
	for dir != tagsDir {
		if err := os.Remove(dir); err != nil {
			break // not empty or doesn't exist
		}
		dir = filepath.Dir(dir)
	}

	return nil
}

func (repo *Repo) listTags() ([]string, error) {
	tagsDir := filepath.Join(repo.repoDir, "refs", "tags")
	return listRefsRecursive(tagsDir, "")
}

func listRefsRecursive(dir, prefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var names []string
	for _, e := range entries {
		name := e.Name()
		if prefix != "" {
			name = prefix + "/" + name
		}
		if e.IsDir() {
			sub, err := listRefsRecursive(filepath.Join(dir, e.Name()), name)
			if err != nil {
				return nil, err
			}
			names = append(names, sub...)
		} else {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}
