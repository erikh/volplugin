package cgroup

import (
	"fmt"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/storage"
)

const (
	writeBPSFile = "/sys/fs/cgroup/blkio/blkio.throttle.write_bps_device"
	readBPSFile  = "/sys/fs/cgroup/blkio/blkio.throttle.read_bps_device"
)

func makeLimit(mc *storage.Mount, limit uint64) []byte {
	return []byte(fmt.Sprintf("%d:%d %d\n", mc.DevMajor, mc.DevMinor, limit))
}

// ApplyCGroupRateLimit applies cgroups based on the runtime options. Current
// this is restricted to BPS-related functions.
func ApplyCGroupRateLimit(ro *db.RuntimeOptions, mc *storage.Mount) error {
	logrus.Debugf("Apply rate limits: [write: %d] [read: %d] to mount %v", ro.RateLimit.WriteBPS, ro.RateLimit.ReadBPS, mc.Volume)

	opMap := map[string]uint64{
		writeBPSFile: ro.RateLimit.WriteBPS,
		readBPSFile:  ro.RateLimit.ReadBPS,
	}

	for fn, val := range opMap {
		if err := ioutil.WriteFile(fn, makeLimit(mc, val), 0600); err != nil {
			logrus.Errorf("Error writing cgroups: %v", err)
			return err
		}
	}

	return nil
}
