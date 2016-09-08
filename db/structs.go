package db

import "time"

/*
 * The reason these next few structs exist is because due to how the db library works, we
 * cannot store primary keys in the API responses that come directly from etcd and
 * consul. They will be overwritten during marshal. Additionally, they are not
 * required throughout the rest of the system, since the DB library is capable of
 * populating the key informnation, but passing between API responses,
 * unfortunately that is not possible to do without having a new struct.
 */

// NamedPolicy is a named policy; used for API responses.
type NamedPolicy struct {
	Name string `json:"name"`
	*Policy
}

// NamedVolume is a named volume; used for API responses.
type NamedVolume struct {
	PolicyName string `json:"policy"`
	VolumeName string `json:"volume"`
	*Volume
}

// Snapshot is a signaling system to take snapshots. The apiserver sends these
// siganls, and volsupervisor watches for these signals.
type Snapshot struct {
	volume *Volume
}

// Policy is the configuration of the policy. It includes default
// information for items such as pool and volume configuration.
type Policy struct {
	name string

	Unlocked       bool              `json:"unlocked,omitempty" merge:"unlocked"`
	CreateOptions  CreateOptions     `json:"create"`
	RuntimeOptions *RuntimeOptions   `json:"runtime"`
	DriverOptions  map[string]string `json:"driver"`
	FileSystems    map[string]string `json:"filesystems"`
	Backends       *BackendDrivers   `json:"backends,omitempty"`
	Backend        string            `json:"backend,omitempty"`
}

// PolicyRevision is a policy with new paths.
type PolicyRevision struct {
	*Policy
	Revision string
}

// BackendDrivers is a struct containing all the drivers used under this policy
type BackendDrivers struct {
	CRUD     string `json:"crud"`
	Mount    string `json:"mount"`
	Snapshot string `json:"snapshot"`
}

// VolumeRequest provides a request structure for communicating volumes to the
// apiserver or internally. it is the basic representation of a volume.
type VolumeRequest struct {
	Name    string
	Policy  string
	Options map[string]string
}

// Global is the global configuration.
type Global struct {
	Debug     bool
	Timeout   time.Duration
	TTL       time.Duration
	MountPath string
}

// Volume is the configuration of the policy. It includes pool and
// snapshot information.
type Volume struct {
	policyName string
	volumeName string

	Unlocked       bool              `json:"unlocked,omitempty" merge:"unlocked"`
	DriverOptions  map[string]string `json:"driver"`
	MountSource    string            `json:"mount" merge:"mount"`
	CreateOptions  CreateOptions     `json:"create"`
	RuntimeOptions *RuntimeOptions   `json:"runtime"`
	Backends       *BackendDrivers   `json:"backends,omitempty"`
}

// CreateOptions are the set of options used by apiserver during the volume
// create operation.
type CreateOptions struct {
	Size       string `json:"size" merge:"size"`
	FileSystem string `json:"filesystem" merge:"filesystem"`
}

// RuntimeOptions are the set of options used by volplugin when mounting the
// volume, and by volsupervisor for calculating periodic work.
type RuntimeOptions struct {
	UseSnapshots bool            `json:"snapshots" merge:"snapshots"`
	Snapshot     SnapshotConfig  `json:"snapshot"`
	RateLimit    RateLimitConfig `json:"rate-limit,omitempty"`

	policyName string
	volumeName string
}

// RateLimitConfig is the configuration for limiting the rate of disk access.
type RateLimitConfig struct {
	WriteBPS uint64 `json:"write-bps" merge:"rate-limit.write.bps"`
	ReadBPS  uint64 `json:"read-bps" merge:"rate-limit.read.bps"`
}

// SnapshotConfig is the configuration for snapshots.
type SnapshotConfig struct {
	Frequency string `json:"frequency" merge:"snapshots.frequency"`
	Keep      uint   `json:"keep" merge:"snapshots.keep"`
}
