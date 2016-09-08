package db

import (
	"fmt"
	"strings"
	"time"

	"github.com/contiv/errored"
)

// SetKey is used to overwrite or add the key used to store this object. It
// is expected that the implementor parse the key and add the data it needs
// to its structs and for generating Path().
func (pr *PolicyRevision) SetKey(key string) error {
	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return errored.Errorf("Could not validate path %q during policy revision key parse", key)
	}

	pr.Policy = NewPolicy(parts[0])
	pr.Revision = parts[1]

	return nil
}

// Prefix is the base path for all keys; used for watchall and list
func (pr *PolicyRevision) Prefix() string {
	return "policyrevisions"
}

func (pr *PolicyRevision) String() string {
	return strings.Join([]string{pr.name, pr.Revision}, "/")
}

// Path to object; used for watch and i/o operations.
func (pr *PolicyRevision) Path() (string, error) {
	if pr.name == "" || pr.Revision == "" {
		return "", errored.Errorf("Invalid revision: name or revision was empty: %q", pr)
	}

	return strings.Join([]string{pr.Prefix(), pr.String()}, "/"), nil
}

// Validate the object and ensure it is safe to write, and to use after read.
func (pr *PolicyRevision) Validate() error {
	return nil
}

// Copy the object and returns Entity. Typically used to work around
// interface polymorphism idioms in go.
func (pr *PolicyRevision) Copy() Entity {
	pr2 := *pr
	return &pr2
}

func (pr *PolicyRevision) createRevision(c Client, obj Entity) error {
	pr.Revision = fmt.Sprintf("%d", time.Now().UnixNano())
	return nil
}

// Hooks returns a db.Hooks which contains several functions for the Entity
// lifecycle, such as pre-set and post-get.
func (pr *PolicyRevision) Hooks() *Hooks {
	return &Hooks{
		PreSet: pr.createRevision,
	}
}
