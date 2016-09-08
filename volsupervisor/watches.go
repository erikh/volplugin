package volsupervisor

import (
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/volplugin/db"
)

func (dc *DaemonConfig) signalSnapshot() {
	snapshotChan, errChan := dc.Client.WatchPrefix(&db.Snapshot{})

	go func() {
		for {
			select {
			case err := <-errChan:
				logrus.Errorf("Error received reading snapshot signal: %v", err)
				time.Sleep(100 * time.Millisecond) // throttle
			case snapshot := <-snapshotChan:
				parts := strings.Split(snapshot.(*db.Use).Volume, "/")
				if len(parts) != 2 {
					logrus.Errorf("Invalid volume name %q during snapshot signal", snapshot.(*db.Use).Volume)
					continue
				}

				vol := db.NewVolume(parts[0], parts[1])
				if err := dc.Client.Get(vol); err != nil {
					logrus.Errorf("Volume %q missing during snapshot signal: %v", vol, err)
					continue
				}

				go dc.createSnapshot(vol)
				if err := dc.Client.Delete(snapshot); err != nil {
					logrus.Errorf("Error removing snapshot reference: %v", err)
					continue
				}
			}
		}
	}()
}
