package info

import (
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/db"
)

func numFileDescriptors() int {
	fds, err := ioutil.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(fds)
}

func getCephVersion() (string, error) {
	cmd := exec.Command("ceph", "version")
	out, err := cmd.Output()
	if err != nil {
		return "", errored.Errorf("encountered error: %v", err)
	}

	output := strings.TrimLeft(string(out), "ceph version ")
	output = strings.TrimSpace(output)
	return output, nil
}

func logDebugInfo() {
	cephVersion, err := getCephVersion()
	if err != nil {
		cephVersion = "n/a"
	}

	logrus.WithFields(logrus.Fields{
		"file_descriptors": numFileDescriptors(),
		"goroutines":       runtime.NumGoroutine(),
		"architecture":     runtime.GOARCH,
		"os":               runtime.GOOS,
		"cpus":             runtime.NumCPU(),
		"go_version":       runtime.Version(),
		"ceph_version":     cephVersion,
	}).Info("received SIGUSR1; providing debug info")
}

// HandleDebugSignal watches for SIGUSR1 and logs the debug information
// using logrus
func HandleDebugSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	for {
		select {
		case <-signals:
			logDebugInfo()
		}
	}
}

// HandleDumpTarballSignal watches for SIGUSR2 and creates a gzipped tarball
// of the current etcd directories/keys under the "/volplugin" namespace
func HandleDumpTarballSignal(client db.Client) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR2)
	for {
		select {
		case <-signals:
			logrus.Info("received SIGUSR2; dumping etcd namespace to tarball")

			tarballPath, err := client.Dump(os.Getenv("TMPDIR"))
			if err != nil {
				logrus.Info("Failed to dump db namespace: ", err)
			} else {
				logrus.Info("Dumped db namespace to ", tarballPath)
			}
		}
	}
}
