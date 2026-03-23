package repomofo

import (
	"fmt"
)

// TreeEntry represents an entry in a git tree object.
type TreeEntry struct {
	OID  Hash
	Mode Mode
}

// TreeChange represents a change between two tree entries.
type TreeChange struct {
	Old *TreeEntry // nil means added
	New *TreeEntry // nil means removed
}

// treeDiff computes changes between two commit OIDs.
// Either oid may be nil to represent an empty tree.
func (repo *Repo) treeDiff(oldOID, newOID Hash) (map[string]TreeChange, error) {
	changes := make(map[string]TreeChange)
	return repo.treeCompare(oldOID, newOID, "", changes)
}

func (repo *Repo) treeCompare(oldTreeOID, newTreeOID Hash, prefix string, changes map[string]TreeChange) (map[string]TreeChange, error) {
	oldEntries, err := repo.loadTree(oldTreeOID)
	if err != nil {
		return nil, err
	}
	newEntries, err := repo.loadTree(newTreeOID)
	if err != nil {
		return nil, err
	}

	// deletions and edits
	for name, oldEntry := range oldEntries {
		path := name
		if prefix != "" {
			path = prefix + "/" + name
		}
		oe := oldEntry
		if newEntry, ok := newEntries[name]; ok {
			ne := newEntry
			if !HashEqual(oe.OID, ne.OID) || oe.Mode != ne.Mode {
				oldIsTree := oe.Mode.ObjType() == ModeObjectTypeTree
				newIsTree := ne.Mode.ObjType() == ModeObjectTypeTree
				var oldSub, newSub Hash
				if oldIsTree {
					oldSub = oe.OID
				}
				if newIsTree {
					newSub = ne.OID
				}
				if oldIsTree || newIsTree {
					if _, err := repo.treeCompare(oldSub, newSub, path, changes); err != nil {
						return nil, err
					}
				}
				if !oldIsTree || !newIsTree {
					var oldP, newP *TreeEntry
					if !oldIsTree {
						oldP = &oe
					}
					if !newIsTree {
						newP = &ne
					}
					changes[path] = TreeChange{Old: oldP, New: newP}
				}
			}
		} else {
			if oe.Mode.ObjType() == ModeObjectTypeTree {
				if _, err := repo.treeCompare(oe.OID, nil, path, changes); err != nil {
					return nil, err
				}
			} else {
				changes[path] = TreeChange{Old: &oe, New: nil}
			}
		}
	}

	// additions
	for name, newEntry := range newEntries {
		if _, ok := oldEntries[name]; ok {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "/" + name
		}
		ne := newEntry
		if ne.Mode.ObjType() == ModeObjectTypeTree {
			if _, err := repo.treeCompare(nil, ne.OID, path, changes); err != nil {
				return nil, err
			}
		} else {
			changes[path] = TreeChange{Old: nil, New: &ne}
		}
	}

	return changes, nil
}

func (repo *Repo) loadTree(oid Hash) (map[string]TreeEntry, error) {
	result := make(map[string]TreeEntry)
	if oid == nil {
		return result, nil
	}
	obj, err := repo.NewObject(oid, true)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	switch obj.Kind {
	case ObjectKindTree:
		if obj.Tree != nil {
			for _, e := range obj.Tree.Entries {
				result[e.Name] = TreeEntry{OID: e.OID, Mode: e.Mode}
			}
		}
		return result, nil
	case ObjectKindCommit:
		if obj.Commit != nil {
			return repo.loadTree(obj.Commit.Tree)
		}
		return result, nil
	case ObjectKindBlob, ObjectKindTag:
		return result, nil
	default:
		return result, nil
	}
}

// pathToTreeEntry walks the HEAD tree to find the entry at the given path.
func (repo *Repo) pathToTreeEntry(pathParts []string) (Hash, Mode, error) {
	headOID, err := repo.ReadHeadRecurMaybe()
	if err != nil || headOID == nil {
		return nil, 0, fmt.Errorf("no HEAD")
	}

	treeOID, err := repo.readCommitTree(headOID)
	if err != nil {
		return nil, 0, err
	}

	currentTreeOID := treeOID
	for i, part := range pathParts {
		obj, err := repo.NewObject(currentTreeOID, true)
		if err != nil {
			return nil, 0, err
		}

		found := false
		if obj.Tree != nil {
			for _, te := range obj.Tree.Entries {
				if te.Name == part {
					obj.Close()
					if i == len(pathParts)-1 {
						return te.OID, te.Mode, nil
					}
					if te.Mode.ObjType() != ModeObjectTypeTree {
						return nil, 0, fmt.Errorf("not a tree: %s", part)
					}
					currentTreeOID = te.OID
					found = true
					break
				}
			}
		}
		if !found {
			obj.Close()
			return nil, 0, fmt.Errorf("not found: %s", part)
		}
	}
	return nil, 0, fmt.Errorf("not found")
}

// headTreeEntry returns the OID and mode of a path in the HEAD tree.
// Returns nil OID if not found.
func (repo *Repo) headTreeEntry(filePath string) (Hash, Mode) {
	parts := splitPath(filePath)
	oid, mode, err := repo.pathToTreeEntry(parts)
	if err != nil {
		return nil, 0
	}
	return oid, mode
}

// flattenHeadTree returns a flat map of all file paths in the HEAD tree.
func (repo *Repo) flattenHeadTree() (map[string]TreeEntry, error) {
	headOID, err := repo.ReadHeadRecurMaybe()
	if err != nil || headOID == nil {
		return nil, fmt.Errorf("no HEAD")
	}

	treeOID, err := repo.readCommitTree(headOID)
	if err != nil {
		return nil, err
	}

	result := make(map[string]TreeEntry)
	if err := repo.flattenTree(treeOID, "", result); err != nil {
		return nil, err
	}
	return result, nil
}

func (repo *Repo) flattenTree(treeOID Hash, prefix string, result map[string]TreeEntry) error {
	obj, err := repo.NewObject(treeOID, true)
	if err != nil {
		return err
	}
	defer obj.Close()

	if obj.Tree == nil {
		return nil
	}

	for _, e := range obj.Tree.Entries {
		var path string
		if prefix == "" {
			path = e.Name
		} else {
			path = prefix + "/" + e.Name
		}

		if e.Mode.ObjType() == ModeObjectTypeTree {
			if err := repo.flattenTree(e.OID, path, result); err != nil {
				return err
			}
		} else {
			result[path] = TreeEntry{OID: e.OID, Mode: e.Mode}
		}
	}
	return nil
}
