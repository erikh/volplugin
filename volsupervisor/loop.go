package volsupervisor

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/errors"
	"github.com/contiv/volplugin/storage"
	"github.com/contiv/volplugin/storage/backend"
)

var (
	volumes     = map[string]*db.Volume{}
	volumeMutex = &sync.Mutex{}
)

func (dc *DaemonConfig) updateVolumes() {
	myVolumes, err := dc.Client.List(&db.Volume{})
	if err != nil {
		logrus.Error(err)
		return
	}

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	volumes = map[string]*db.Volume{}
	for _, vol := range myVolumes {
		volumes[vol.String()] = vol.(*db.Volume)
	}
}

func (dc *DaemonConfig) pruneSnapshots(val *db.Volume) {
	logrus.Infof("starting snapshot prune for %q", val)
	if val.Backends.Snapshot == "" {
		logrus.Debugf("Snapshot driver for volume %v was empty, not snapshotting.", val)
		return
	}

	uc := db.NewSnapshotRemove(val)

	stopChan, err := dc.Client.AcquireAndRefresh(uc, dc.Global.TTL)
	if err != nil {
		logrus.Error(errors.LockFailed.Combine(err))
		return
	}

	defer close(stopChan)

	driver, err := backend.NewSnapshotDriver(val.Backends.Snapshot)
	if err != nil {
		logrus.Errorf("failed to get driver: %v", err)
		return
	}

	driverOpts := storage.DriverOptions{
		Volume: storage.Volume{
			Name: val.String(),
			Params: storage.Params{
				"pool": val.DriverOptions["pool"],
			},
		},
		Timeout: dc.Global.Timeout,
	}

	list, err := driver.ListSnapshots(driverOpts)
	if err != nil {
		logrus.Errorf("Could not list snapshots for volume %q: %v", val, err)
		return
	}

	logrus.Debugf("Volume %q: keeping %d snapshots", val, val.RuntimeOptions.Snapshot.Keep)

	toDeleteCount := len(list) - int(val.RuntimeOptions.Snapshot.Keep)
	if toDeleteCount < 0 {
		return
	}

	for i := 0; i < toDeleteCount; i++ {
		logrus.Infof("Removing snapshot %q for volume %q", list[i], val)
		if err := driver.RemoveSnapshot(list[i], driverOpts); err != nil {
			logrus.Errorf("Removing snapshot %q for volume %q failed: %v", list[i], val, err)
		}
	}
}

func (dc *DaemonConfig) createSnapshot(val *db.Volume) {
	logrus.Infof("Snapshotting %q.", val)

	uc := db.NewSnapshotCreate(val)

	stopChan, err := dc.Client.AcquireAndRefresh(uc, dc.Global.TTL)
	if err != nil {
		logrus.Error(err)
		return
	}

	defer close(stopChan)

	driver, err := backend.NewSnapshotDriver(val.Backends.Snapshot)
	if err != nil {
		logrus.Errorf("Error establishing driver backend %q; cannot snapshot", val.Backends.Snapshot)
		return
	}

	driverOpts := storage.DriverOptions{
		Volume: storage.Volume{
			Name: val.String(),
			Params: storage.Params{
				"pool": val.DriverOptions["pool"],
			},
		},
		Timeout: dc.Global.Timeout,
	}

	if err := driver.CreateSnapshot(time.Now().String(), driverOpts); err != nil {
		logrus.Errorf("Error creating snapshot for volume %q: %v", val, err)
	}
}

func (dc *DaemonConfig) loop() {
	for {
		time.Sleep(time.Second)

		// XXX this copy is so we can free the mutex quickly for more additions
		volumeCopy := map[string]*db.Volume{}

		volumeMutex.Lock()
		for volume, val := range volumes {
			logrus.Debugf("Adding volume %q for processing", volume)
			val2 := *val
			volumeCopy[volume] = &val2
		}
		volumeMutex.Unlock()

		for volume, val := range volumeCopy {
			if val.RuntimeOptions.UseSnapshots {
				freq, err := time.ParseDuration(val.RuntimeOptions.Snapshot.Frequency)
				if err != nil {
					logrus.Errorf("Volume %q has an invalid frequency. Skipping snapshot.", volume)
				}

				if time.Now().Unix()%int64(freq.Seconds()) == 0 {
					var isUsed bool
					if !val.Unlocked {
						uc := db.NewMountOwner("", val)
						if err := dc.Client.Acquire(uc); err != nil {
							if er, ok := err.(*errored.Error); ok {
								isUsed = er.Contains(errors.Exists)
							} else {
								logrus.Errorf("etcd error: %s", errors.EtcdToErrored(err)) // some issue with "etcd GET"; we should not hit this case
							}
						}
						dc.Client.Free(uc, false)
					}

					go func(val *db.Volume, isUsed bool) {
						// XXX we still want to prune snapshots even if the volume is not in use.
						if isUsed {
							dc.createSnapshot(val)
						}
						dc.pruneSnapshots(val)
					}(val, isUsed)
				}
			}
		}
	}
}
