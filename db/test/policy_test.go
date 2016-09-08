package test

import (
	"github.com/contiv/volplugin/db"
	. "gopkg.in/check.v1"
)

func init() {
	for name, policy := range testPolicies {
		policy.SetKey(name)
	}
}

var testPolicies = map[string]*db.Policy{
	"basic": {
		Backends: &db.BackendDrivers{
			CRUD:     "ceph",
			Mount:    "ceph",
			Snapshot: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			Size:       "10MB",
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{
			UseSnapshots: true,
			Snapshot: db.SnapshotConfig{
				Keep:      10,
				Frequency: "1m",
			},
		},
		FileSystems: db.DefaultFilesystems,
	},
	"basic2": {
		Backends: &db.BackendDrivers{
			CRUD:     "ceph",
			Mount:    "ceph",
			Snapshot: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			Size:       "20MB",
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{},
		FileSystems:    db.DefaultFilesystems,
	},
	"untouchedwithzerosize": {
		Backends: &db.BackendDrivers{
			CRUD:     "ceph",
			Mount:    "ceph",
			Snapshot: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			Size:       "0",
			FileSystem: db.DefaultFilesystem,
		},
		FileSystems: db.DefaultFilesystems,
	},
	"badsize3": {
		Backends: &db.BackendDrivers{
			CRUD:     "ceph",
			Mount:    "ceph",
			Snapshot: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			Size:       "not a number",
			FileSystem: db.DefaultFilesystem,
		},
		FileSystems: db.DefaultFilesystems,
	},
	"badsnaps": {
		Backends: &db.BackendDrivers{
			CRUD:     "ceph",
			Mount:    "ceph",
			Snapshot: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			Size:       "10MB",
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{
			UseSnapshots: true,
			Snapshot: db.SnapshotConfig{
				Keep:      0,
				Frequency: "",
			},
		},
		FileSystems: db.DefaultFilesystems,
	},
	"blanksize": {
		Backends: &db.BackendDrivers{
			Mount: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{},
		FileSystems:    db.DefaultFilesystems,
	},
	"blanksizewithcrud": {
		Backends: &db.BackendDrivers{
			CRUD:  "ceph",
			Mount: "ceph",
		},
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{},
		FileSystems:    db.DefaultFilesystems,
	},
	"nobackend": {
		DriverOptions: map[string]string{"pool": "rbd"},
		CreateOptions: db.CreateOptions{
			Size:       "10MB",
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{},
		FileSystems:    db.DefaultFilesystems,
	},
	"nfs": {
		Backends: &db.BackendDrivers{
			Mount: "nfs",
		},
		CreateOptions:  db.CreateOptions{},
		RuntimeOptions: &db.RuntimeOptions{},
	},
	"cephbackend": {
		Backend: "ceph",
		CreateOptions: db.CreateOptions{
			Size:       "10MB",
			FileSystem: db.DefaultFilesystem,
		},
	},
	"nfsbackend": {
		Backend: "nfs",
		CreateOptions: db.CreateOptions{
			Size:       "10MB",
			FileSystem: db.DefaultFilesystem,
		},
		RuntimeOptions: &db.RuntimeOptions{},
	},
	"emptybackend": {
		Backend: "",
	},
	"badbackend": {
		Backend: "dummy",
	},
	"backends": { // "Backend" attribute will be ignored
		Backends: &db.BackendDrivers{
			Mount: "nfs",
		},
		Backend: "ceph",
		CreateOptions: db.CreateOptions{
			Size:       "10MB",
			FileSystem: db.DefaultFilesystem,
		},
	},
	"badbackends": {
		Backend: "nfs",
		Backends: &db.BackendDrivers{
			Mount: "", // This should not be empty
		},
	},
}

var (
	passingPolicies = []string{
		"basic",
		"basic2",
		"nfsbackend",
		"nfs",
		"blanksize",
		"backends",
	}

	failingPolicies = []string{
		"untouchedwithzerosize",
		"badsize3",
		"badsnaps",
		"blanksizewithcrud",
		"nobackend",
		"emptybackend",
		"badbackend",
		"badbackends",
	}
)

func (s *testSuite) TestPolicyCRUD(c *C) {
	for _, name := range passingPolicies {
		c.Assert(s.client.Set(testPolicies[name]), IsNil, Commentf("%v", name))
		policy := db.NewPolicy(name)
		c.Assert(s.client.Get(policy), IsNil, Commentf("%v", name))
		c.Assert(policy, DeepEquals, testPolicies[name])
	}

	for _, name := range failingPolicies {
		c.Assert(s.client.Set(testPolicies[name]), NotNil, Commentf("%v", name))
	}
}

func (s *testSuite) TestCopy(c *C) {
	policyCopy := testPolicies["basic"].Copy()
	policyCopy.(*db.Policy).RuntimeOptions.UseSnapshots = false
	c.Assert(testPolicies["basic"].RuntimeOptions.UseSnapshots, Equals, true, Commentf("runtime options pointer was not copied"))
}
