package volplugin

import (
	"github.com/Sirupsen/logrus"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/errors"
	"github.com/contiv/volplugin/storage/cgroup"
)

func (dc *DaemonConfig) pollRuntime() {
	volumeChan, errChan := dc.Client.WatchPrefix(&db.Volume{})
	for {
		select {
		case err := <-errChan:
			logrus.Errorf("Failed watch for volumes: %v", err)
		case volWatch := <-volumeChan:
			if volWatch == nil {
				continue
			}

			var vol *db.Volume
			var ok bool

			if vol, ok = volWatch.(*db.Volume); !ok {
				logrus.Error(errored.Errorf("Error processing runtime update for volume %q: assertion failed", vol))
				continue
			}

			logrus.Infof("Adjusting runtime parameters for volume %q", vol)
			thisMC, err := dc.API.MountCollection.Get(vol.String())

			if er, ok := err.(*errored.Error); ok && !er.Contains(errors.NotExists) {
				logrus.Errorf("Unknown error processing runtime configuration parameters for volume %q: %v", vol, er)
				continue
			}

			// if we can't look it up, it's possible it was mounted on a different host.
			if err != nil {
				logrus.Errorf("Error retrieving mount information for %q from cache: %v", vol, err)
				continue
			}

			if err := cgroup.ApplyCGroupRateLimit(vol.RuntimeOptions, thisMC); err != nil {
				logrus.Error(errored.Errorf("Error processing runtime update for volume %q", vol).Combine(err))
				continue
			}
		}
	}
}
