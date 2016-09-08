package db

import (
	"strings"

	"github.com/contiv/errored"
)

// NewSnapshot creates a new snapshot signal.
func NewSnapshot(v *Volume) *Snapshot {
	return &Snapshot{volume: v}
}

// Prefix returns the bucket prefix for k/v operations.
func (*Snapshot) Prefix() string {
	return rootSnapshots
}

// Path returns the path to this specific snapshot.
func (s *Snapshot) Path() (string, error) {
	return strings.Join([]string{s.Prefix(), s.volume.String()}, "/"), nil
}

// Copy returns a copy of this snapshot record.
func (s *Snapshot) Copy() Entity {
	s2 := *s
	return &s2
}

// Validate validates a snapshot record.
func (s *Snapshot) Validate() error {
	if s.volume == nil {
		return errored.New("Volume was not pressent during snapshot take operations")
	}

	return s.volume.Validate()
}

// SetKey sets the key based on the provided path.
func (s *Snapshot) SetKey(str string) error {
	path := strings.TrimPrefix(str, s.Prefix())
	parts := strings.Split(path, "/")

	if len(parts) != 2 {
		return errored.Errorf("Could not parse volume name during snapshot internal set key operation, invalid path %q", path)
	}

	s.volume = NewVolume(parts[0], parts[1])

	return nil
}

func (s *Snapshot) String() string {
	return s.volume.String()
}

// Hooks returns an empty struct because we have no reason to do anything with them.
func (s *Snapshot) Hooks() *Hooks {
	return &Hooks{}
}
