package api

import (
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/errors"
	"github.com/contiv/volplugin/storage"
	"github.com/contiv/volplugin/storage/cgroup"
	"github.com/contiv/volplugin/storage/control"
)

func (a *API) createVolume(w http.ResponseWriter, volume *db.VolumeRequest) func(ucs []db.Lock) error {
	return func(ucs []db.Lock) error {
		global := *a.Global

		policy := db.NewPolicy(volume.Policy)
		if err := a.Client.Get(policy); err != nil {
			return errors.NotExists.Combine(errored.Errorf("policy %q not found", volume.Policy)).Combine(err)
		}

		volConfig, err := db.CreateVolume(policy, volume.Name, volume.Options)
		if err != nil {
			return err
		}

		logrus.Debugf("Volume Create: %#v", *volConfig)

		do, err := control.CreateVolume(policy, volConfig, global.Timeout)
		if err == errors.NoActionTaken {
			goto publish
		}

		if err != nil {
			return errors.CreateVolume.Combine(err)
		}

		if err := control.FormatVolume(volConfig, do); err != nil {
			if err := control.RemoveVolume(volConfig, global.Timeout); err != nil {
				logrus.Errorf("Error during cleanup of failed format: %v", err)
			}
			return errors.FormatVolume.Combine(err)
		}

	publish:
		if err := a.Client.Set(volConfig); err != nil && err != errors.Exists {
			if _, ok := err.(*errored.Error); !ok {
				return errors.PublishVolume.Combine(err)
			}
			return err
		}

		return a.WriteCreate(volConfig, w)
	}
}

// Create fully creates a volume
func (a *API) Create(w http.ResponseWriter, r *http.Request) {
	volume, err := a.ReadCreate(r)
	if err != nil {
		a.HTTPError(w, err)
		return
	}

	vol := db.NewVolume(volume.Policy, volume.Name)

	if err := a.Client.Get(vol); err == nil {
		a.HTTPError(w, errors.Exists.Combine(err))
		return
	}

	logrus.Infof("Creating volume %s", volume)

	err = db.ExecuteWithMultiUseLock(
		a.Client,
		a.createVolume(w, volume),
		(*a.Global).Timeout,
		db.NewSnapshotCreate(vol),
		db.NewCreateOwner(a.Hostname, vol),
	)

	if err != nil && err != errors.Exists {
		a.HTTPError(w, errors.CreateVolume.Combine(err))
		return
	}
}

func (a *API) get(origName string, r *http.Request) (string, error) {
	policy, name, err := storage.SplitName(origName)
	if err != nil {
		return "", errors.GetVolume.Combine(err)
	}

	driver, volConfig, driverOpts, err := a.GetStorageParameters(&Volume{Policy: policy, Name: name})
	if err != nil {
		return "", errors.GetVolume.Combine(err)
	}

	if err := volConfig.Validate(); err != nil {
		return "", errors.ConfiguringVolume.Combine(err)
	}

	path, err := driver.MountPath(driverOpts)
	if err != nil {
		return "", errors.MountPath.Combine(err)
	}

	return path, nil
}

func (a *API) writePathError(w http.ResponseWriter, err error) {
	if err, ok := err.(*errored.Error); ok && err.Contains(errors.NotExists) {
		w.Write([]byte("{}"))
		return
	}
	a.HTTPError(w, err)
	return
}

func (a *API) getMountPath(driver storage.MountDriver, driverOpts storage.DriverOptions) (string, error) {
	path, err := driver.MountPath(driverOpts)
	return path, err
}

// Path is the handler for both Path and Remove requests. We do not honor
// remove requests; they can be done with volcli.
func (a *API) Path(w http.ResponseWriter, r *http.Request) {
	origName, err := a.ReadPath(r)
	if err != nil {
		a.HTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	path, err := a.get(origName, r)
	if err != nil {
		a.writePathError(w, err)
		return
	}

	if err := a.WritePath(path, w); err != nil {
		a.HTTPError(w, errors.GetVolume.Combine(err))
	}
}

// Get is the request to obtain information about a volume.
func (a *API) Get(w http.ResponseWriter, r *http.Request) {
	origName, err := a.ReadGet(r)
	if err != nil {
		a.HTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	path, err := a.get(origName, r)
	if err != nil {
		a.writePathError(w, err)
		return
	}

	if err := a.WriteGet(origName, path, w); err != nil {
		a.HTTPError(w, errors.GetVolume.Combine(err))
	}
}

// List is the request to obtain a list of the volumes.
func (a *API) List(w http.ResponseWriter, r *http.Request) {
	volList, err := a.Client.List(&db.Volume{})
	if err != nil {
		a.HTTPError(w, errors.ListVolume.Combine(err))
		return
	}

	if err := a.WriteList(volList, w); err != nil {
		a.HTTPError(w, errors.ListVolume.Combine(err))
	}
}

type mountState struct {
	w          http.ResponseWriter
	err        error
	ut         db.Lock
	driver     storage.MountDriver
	driverOpts storage.DriverOptions
	volConfig  *db.Volume
}

// triggered on any failure during call into mount.
func (a *API) clearMount(ms mountState) {
	logrus.Errorf("MOUNT FAILURE: %v", ms.err)

	if err := ms.driver.Unmount(ms.driverOpts); err != nil {
		// literally can't do anything about this situation. Log.
		logrus.Errorf("Failure during unmount after failed mount: %v %v", err, ms.err)
	}

	if err := a.Client.Free(ms.ut, false); err != nil {
		a.HTTPError(ms.w, errors.RefreshMount.Combine(errored.New(ms.volConfig.String())).Combine(err).Combine(ms.err))
		return
	}

	a.HTTPError(ms.w, errors.MountFailed.Combine(ms.err))
	return
}

// Mount is the request to mount a volume.
func (a *API) Mount(w http.ResponseWriter, r *http.Request) {
	request, err := a.ReadMount(r)
	if err != nil {
		a.HTTPError(w, errors.ConfiguringVolume.Combine(err))
		return
	}

	logrus.Infof("Mounting volume %q", request)
	logrus.Debugf("%#v", a.MountCollection)

	driver, volConfig, driverOpts, err := a.GetStorageParameters(request)
	if err != nil {
		a.HTTPError(w, errors.ConfiguringVolume.Combine(err))
		return
	}

	volName := volConfig.String()
	ut := db.NewMountOwner(a.Hostname, volConfig)

	if !volConfig.Unlocked {
		if err := a.startTTLRefresh(volConfig); err != nil {
			a.clearMount(mountState{w, err, ut, driver, driverOpts, volConfig})
			return
		}
	}

	// XXX docker issues unmount request after every mount failure so, this evens out
	//     decreaseMount() in unmount
	if a.MountCounter.Add(volName) > 1 {
		if volConfig.Unlocked {
			logrus.Warnf("Duplicate mount of %q detected: returning existing mount path", volName)
			path, err := a.getMountPath(driver, driverOpts)
			if err != nil {
				a.HTTPError(w, errors.MarshalResponse.Combine(err))
				return
			}
			a.WriteMount(path, w)
			return
		}

		logrus.Warnf("Duplicate mount of %q detected: Lock failed", volName)
		a.HTTPError(w, errors.LockFailed.Combine(errored.Errorf("Duplicate mount of %q", volName)))
		return
	}

	// so. if EBUSY is returned here, the resulting unmount will unmount an
	// existing mount. However, this should never happen because of the above
	// counter check.
	// I'm leaving this in because it will break tons of tests if it double
	// mounts something, after the resulting unmount occurs. This seems like a
	// great way to fix tons of errors in our code before they ever accidentally
	// reach a user.
	mc, err := driver.Mount(driverOpts)
	if err != nil {
		a.clearMount(mountState{w, err, ut, driver, driverOpts, volConfig})
		return
	}

	a.MountCollection.Add(mc)

	if err := cgroup.ApplyCGroupRateLimit(volConfig.RuntimeOptions, mc); err != nil {
		logrus.Errorf("Could not apply cgroups to volume %q", volConfig)
	}

	path, err := driver.MountPath(driverOpts)
	if err != nil {
		a.RemoveStopChan(volName)
		a.clearMount(mountState{w, err, ut, driver, driverOpts, volConfig})
		return
	}

	a.WriteMount(path, w)
}

func (a *API) startTTLRefresh(volConfig *db.Volume) error {
	ut := db.NewMountOwner(a.Hostname, volConfig)
	stopChan, err := a.Client.AcquireAndRefresh(ut, (*a.Global).TTL)
	if err != nil {
		return err
	}

	a.AddStopChan(volConfig.String(), stopChan)

	return nil
}

// Unmount is the request to unmount a volume.
func (a *API) Unmount(w http.ResponseWriter, r *http.Request) {
	request, err := a.ReadMount(r)
	if err != nil {
		a.HTTPError(w, errors.UnmarshalRequest.Combine(err))
		return
	}

	logrus.Infof("Unmounting volume %q", request)

	driver, volConfig, driverOpts, err := a.GetStorageParameters(request)
	if err != nil {
		a.HTTPError(w, errors.GetDriver.Combine(err))
		return
	}

	volName := volConfig.String()

	if a.MountCounter.Sub(volName) > 0 {
		logrus.Warnf("Duplicate unmount of %q detected: ignoring and returning success", volName)
		path, err := a.getMountPath(driver, driverOpts)
		if err != nil {
			a.HTTPError(w, errors.MarshalResponse.Combine(err))
			return
		}

		a.WriteMount(path, w)
		return
	}

	if err := driver.Unmount(driverOpts); err != nil {
		a.HTTPError(w, errors.UnmountFailed.Combine(err))
		return
	}

	a.MountCollection.Remove(volName)

	if !volConfig.Unlocked {
		a.RemoveStopChan(volName)
	}

	path, err := a.getMountPath(driver, driverOpts)
	if err != nil {
		a.HTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	a.WriteMount(path, w)
}
