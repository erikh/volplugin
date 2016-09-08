package db

import (
	"fmt"
	"strings"

	"github.com/contiv/errored"
	"github.com/contiv/volplugin/errors"
)

const (
	typeVolume        = 0
	typeSnapshot      = iota
	typeVolsupervisor = iota
)

// Use conforms to db.Lock and is used to manage volumes in volplugin.
type Use struct {
	Volume    string `json:"volume"`
	UseOwner  string `json:"owner"`
	UseReason string `json:"reason"`
	Type      int    `json:"type"`
}

func returnUse(typ int, reason, owner string, v *Volume) *Use {
	vstr := ""
	if v != nil {
		vstr = v.String()
	}

	return &Use{Type: typ, Volume: vstr, UseOwner: owner, UseReason: reason}
}

// NewEmptyOwner is an empty struct used for passing to methods which expect
// empty values (such as unmarshalling), or when the value itself needs fine
// tuning.
func NewEmptyOwner(owner string, v *Volume) *Use {
	return returnUse(typeSnapshot, "", owner, v)
}

// NewVolsupervisor creates a new volsupervisor use lock
func NewVolsupervisor(owner string) *Use {
	return returnUse(typeVolsupervisor, "", owner, nil)
}

// NewEmptySnapshot is an empty struct used for passing to methods which expect
// empty values (such as unmarshalling), or when the value itself needs fine
// tuning.
func NewEmptySnapshot(v *Volume) *Use {
	return returnUse(typeSnapshot, "", "", v)
}

// NewSnapshotCreate returns a db.Lock suitable for holding while taking snapshots.
func NewSnapshotCreate(v *Volume) *Use {
	return returnUse(typeSnapshot, "Create", "", v)
}

// NewSnapshotMaintenance returns a db.Lock suitable for holding while taking snapshots.
func NewSnapshotMaintenance(v *Volume) *Use {
	return returnUse(typeSnapshot, "Maintenance", "", v)
}

// NewSnapshotRemove returns a db.Lock suitable for holding while removing snapshots.
func NewSnapshotRemove(v *Volume) *Use {
	return returnUse(typeSnapshot, "Remove", "", v)
}

// NewSnapshotCopy returns a db.Lock suitable for holding while copying snapshots.
func NewSnapshotCopy(v *Volume) *Use {
	return returnUse(typeSnapshot, "Copy", "", v)
}

// NewCreateOwner returns a lock for a create operation on a volume.
func NewCreateOwner(owner string, v *Volume) *Use {
	return returnUse(typeVolume, "Create", owner, v)
}

// NewMaintenanceOwner locks a volume for maintenance.
func NewMaintenanceOwner(owner string, v *Volume) *Use {
	return returnUse(typeVolume, "Maintenance", owner, v)
}

// NewRemoveOwner returns a lock for a remove operation on a volume.
func NewRemoveOwner(owner string, v *Volume) *Use {
	return returnUse(typeVolume, "Remove", owner, v)
}

// NewMountOwner returns a properly formatted *Use. owner is typically a hostname.
func NewMountOwner(owner string, v *Volume) *Use {
	return returnUse(typeVolume, "Use", owner, v)
}

// Prefix returns the path under which this data should be stored.
func (m *Use) Prefix() string {
	switch m.Type {
	case typeSnapshot:
		return "users/snapshots"
	case typeVolsupervisor:
		return "users/volsupervisor"
	default:
		return "users/volume"
	}
}

// Path returns the volume name.
func (m *Use) Path() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}

	return strings.Join([]string{m.Prefix(), m.Volume}, "/"), nil
}

// Reason is the reason for taking the lock.
func (m *Use) Reason() string {
	return m.UseReason
}

// Owner is the owner of this lock.
func (m *Use) Owner() string {
	return m.UseOwner
}

// Copy copies a use and returns it.
func (m *Use) Copy() Entity {
	m2 := *m
	return &m2
}

// SetKey sets the volume name from the key, and returns an error if necessary.
func (m *Use) SetKey(key string) error {
	m.Volume = strings.Trim(strings.TrimPrefix(key, m.Prefix()), "/")

	return m.Validate()
}

// Validate the use lock.
func (m *Use) Validate() error {
	if m.Type != typeVolsupervisor {
		parts := strings.Split(m.Volume, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return errors.InvalidVolume.Combine(errored.New(m.Volume))
		}
	}

	if m.Owner() == "" && m.Type == typeVolume {
		return errored.Errorf("Host label empty during lock acquire of %q", m.Volume)
	}

	return nil
}

func (m *Use) String() string {
	return fmt.Sprintf("%q: owner: %q; reason %q", m.Volume, m.Owner(), m.Reason())
}

// Hooks returns an empty struct.
func (m *Use) Hooks() *Hooks {
	return &Hooks{}
}
