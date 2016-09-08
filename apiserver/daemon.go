package apiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/api"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/errors"
	"github.com/contiv/volplugin/info"
	"github.com/contiv/volplugin/storage"
	"github.com/contiv/volplugin/storage/backend"
	"github.com/contiv/volplugin/storage/control"
	"github.com/gorilla/mux"
)

// DaemonConfig is the configuration struct used by the apiserver to hold globals.
type DaemonConfig struct {
	Client   db.Client
	Global   *db.Global
	Hostname string
}

// volume is the json response of a volume. Taken from
// https://github.com/docker/docker/blob/master/volume/drivers/adapter.go#L75
type volume struct {
	Name       string
	Mountpoint string
}

type routeHandlers map[string]func(http.ResponseWriter, *http.Request)

// Daemon initializes the daemon for use.
func (d *DaemonConfig) Daemon(listen string) {
	global := db.NewGlobal()

	if err := d.Client.Get(global); err != nil {
		logrus.Errorf("Error fetching global configuration: %v", err)
		logrus.Infof("No global configuration. Proceeding with defaults...")
		global = db.NewGlobal()
	}

	d.Global = global
	if d.Global.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	errored.AlwaysDebug = d.Global.Debug
	errored.AlwaysTrace = d.Global.Debug

	go info.HandleDebugSignal()
	go info.HandleDumpTarballSignal(d.Client)

	activity, errChan := d.Client.Watch(&db.Global{})
	go func() {
		for {
			select {
			case err := <-errChan:
				logrus.Errorf("Error during global watch: %v", err)
			case tmp := <-activity:
				d.Global = tmp.(*db.Global)

				errored.AlwaysDebug = d.Global.Debug
				errored.AlwaysTrace = d.Global.Debug
				if d.Global.Debug {
					logrus.SetLevel(logrus.DebugLevel)
				}
			}
		}
	}()

	r := mux.NewRouter()

	postRouter := map[string]func(http.ResponseWriter, *http.Request){
		"/global":                           d.handleGlobalUpload,
		"/volumes/create":                   d.handleCreate,
		"/volumes/copy":                     d.handleCopy,
		"/volumes/request":                  d.handleRequest,
		"/policies/{policy}":                d.handlePolicyUpload,
		"/runtime/{policy}/{volume}":        d.handleRuntimeUpload,
		"/snapshots/take/{policy}/{volume}": d.handleSnapshotTake,
	}

	if err := addRoute(r, postRouter, "POST", d.Global.Debug); err != nil {
		logrus.Fatalf("Error starting apiserver: %v", err)
	}

	deleteRouter := map[string]func(http.ResponseWriter, *http.Request){
		"/volumes/remove":      d.handleRemove,
		"/volumes/removeforce": d.handleRemoveForce,
		"/policies/{policy}":   d.handlePolicyDelete,
	}

	if err := addRoute(r, deleteRouter, "DELETE", d.Global.Debug); err != nil {
		logrus.Fatalf("Error starting apiserver: %v", err)
	}

	getRouter := map[string]func(http.ResponseWriter, *http.Request){
		"/global": d.handleGlobal,
		// "/policy-archives/{policy}":            d.handlePolicyListRevisions,
		// "/policy-archives/{policy}/{revision}": d.handlePolicyGetRevision,
		"/policies":                         d.handlePolicyList,
		"/policies/{policy}":                d.handlePolicy,
		"/uses/mounts/{policy}/{volume}":    d.handleUsesMountsVolume,
		"/uses/snapshots/{policy}/{volume}": d.handleUsesMountsSnapshots,
		"/volumes":                          d.handleListAll,
		"/volumes/{policy}":                 d.handleList,
		"/volumes/{policy}/{volume}":        d.handleGet,
		"/runtime/{policy}/{volume}":        d.handleRuntime,
		"/snapshots/{policy}/{volume}":      d.handleSnapshotList,
	}

	if err := addRoute(r, getRouter, "GET", d.Global.Debug); err != nil {
		logrus.Fatalf("Error starting apiserver: %v", err)
	}

	if d.Global.Debug {
		r.HandleFunc("{action:.*}", d.handleDebug)
	}

	if err := http.ListenAndServe(listen, r); err != nil {
		logrus.Fatalf("Error starting apiserver: %v", err)
	}
}

func addRoute(r *mux.Router, handlers routeHandlers, method string, debug bool) error {
	for path, f := range handlers {
		if strings.HasSuffix(path, "/") {
			return fmt.Errorf("route path %v has trailing slash", path)
		}
		r.HandleFunc(path, logHandler(path, debug, f)).Methods(method)
		pathSlash := fmt.Sprintf("%v/", path)
		r.HandleFunc(pathSlash, logHandler(pathSlash, debug, f)).Methods(method)
	}
	return nil
}

func logHandler(name string, debug bool, actionFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if debug {
			buf := new(bytes.Buffer)
			io.Copy(buf, r.Body)
			logrus.Debugf("Dispatching %s with %v", name, strings.TrimSpace(string(buf.Bytes())))
			var writer *io.PipeWriter
			r.Body, writer = io.Pipe()
			go func() {
				io.Copy(writer, buf)
				writer.Close()
			}()
		}

		actionFunc(w, r)
	}
}

func (d *DaemonConfig) handleDebug(w http.ResponseWriter, r *http.Request) {
	io.Copy(os.Stderr, r.Body)
	w.WriteHeader(404)
}

func (d *DaemonConfig) handleGlobalUpload(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.RESTHTTPError(w, errors.ReadBody.Combine(err))
		return
	}

	global := db.NewGlobal()
	if err := json.Unmarshal(data, global); err != nil {
		api.RESTHTTPError(w, errors.UnmarshalGlobal.Combine(err))
		return
	}

	if err := d.Client.Set(global.Canonical()); err != nil {
		api.RESTHTTPError(w, errors.PublishGlobal.Combine(err))
		return
	}
}

func (d *DaemonConfig) handlePolicyUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policyName := vars["policy"]

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.RESTHTTPError(w, errors.ReadBody.Combine(err))
		return
	}

	policy := db.NewPolicy(policyName)
	if err := json.Unmarshal(data, policy); err != nil {
		api.RESTHTTPError(w, errors.UnmarshalPolicy.Combine(err))
		return
	}

	if err := d.Client.Set(policy); err != nil {
		api.RESTHTTPError(w, errors.PublishPolicy.Combine(err))
		return
	}
}

func (d *DaemonConfig) handlePolicyDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := db.NewPolicy(vars["policy"])

	if err := d.Client.Delete(policy); err != nil {
		api.RESTHTTPError(w, errors.PublishGlobal.Combine(err))
		return
	}
}

/*
func (d *DaemonConfig) handlePolicyListRevisions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := db.NewPolicyRevision(vars["policy"])

	revisions, err := d.Client.ListPrefix(policy)
	if err != nil {
		api.RESTHTTPError(w, errors.ListPolicyRevision.Combine(err))
		return
	}

	content, err := json.Marshal(revisions)
	if err != nil {
		api.RESTHTTPError(w, errors.ListPolicyRevision.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handlePolicyGetRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	revision := vars["revision"]

	policyText, err := d.Config.GetPolicyRevision(policy, revision)
	if err != nil {
		api.RESTHTTPError(w, errors.GetPolicyRevision.Combine(err))
		return
	}

	w.Write([]byte(policyText))
}
*/

func (d *DaemonConfig) handlePolicyList(w http.ResponseWriter, r *http.Request) {
	policies, err := d.Client.List(&db.Policy{})
	if err != nil {
		api.RESTHTTPError(w, errors.ListPolicy.Combine(err))
		return
	}

	names := []*db.NamedPolicy{}
	for _, policy := range policies {
		names = append(names, policy.(*db.Policy).Named())
	}

	content, err := json.Marshal(names)
	if err != nil {
		api.RESTHTTPError(w, errors.ListPolicy.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handlePolicy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := db.NewPolicy(vars["policy"])

	if err := d.Client.Get(policy); err != nil {
		api.RESTHTTPError(w, errors.GetPolicy.Combine(err))
		return
	}

	content, err := json.Marshal(policy)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalPolicy.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) getRequestVolume(r *http.Request) (*db.Volume, error) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	volumeName := vars["volume"]

	vc := db.NewVolume(policy, volumeName)

	if err := d.Client.Get(vc.Named()); err != nil {
		return nil, err
	}
	return vc, nil
}

func (d *DaemonConfig) handleUsesMountsVolume(w http.ResponseWriter, r *http.Request) {
	vc, err := d.getRequestVolume(r)
	if err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	d.handleUserEndpoints(db.NewMountOwner(d.Hostname, vc), w, r)
}

func (d *DaemonConfig) handleUsesMountsSnapshots(w http.ResponseWriter, r *http.Request) {
	vc, err := d.getRequestVolume(r)
	if err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	d.handleUserEndpoints(db.NewSnapshotCreate(vc), w, r)
}

func (d *DaemonConfig) handleUserEndpoints(ul db.Lock, w http.ResponseWriter, r *http.Request) {
	content, err := json.Marshal(ul)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handleRuntime(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	volumeName := vars["volume"]

	runtime := db.NewRuntimeOptions(policy, volumeName)
	if err := d.Client.Get(runtime); err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	content, err := json.Marshal(runtime)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handleRuntimeUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	volumeName := vars["volume"]

	volume := db.NewVolume(policy, volumeName)
	if err := d.Client.Get(volume); err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.RESTHTTPError(w, errors.ReadBody.Combine(err))
		return
	}

	runtime := db.NewRuntimeOptions(policy, volumeName)
	if err := json.Unmarshal(data, &runtime); err != nil {
		api.RESTHTTPError(w, errors.UnmarshalRuntime.Combine(err))
		return
	}

	if err := d.Client.Set(runtime); err != nil {
		api.RESTHTTPError(w, errors.PublishRuntime.Combine(err))
		return
	}
}

func (d *DaemonConfig) handleSnapshotList(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	volumeName := vars["volume"]

	volConfig := db.NewVolume(policy, volumeName)
	if err := d.Client.Get(volConfig); err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	if volConfig.Backends.Snapshot == "" {
		api.RESTHTTPError(w, errors.SnapshotsUnsupported.Combine(errored.Errorf("%q", volConfig)))
		return
	}

	driver, err := backend.NewSnapshotDriver(volConfig.Backends.Snapshot)
	if err != nil {
		api.RESTHTTPError(w, errors.GetDriver.Combine(err))
		return
	}

	do := storage.DriverOptions{
		Volume: storage.Volume{
			Name:   volConfig.String(),
			Params: volConfig.DriverOptions,
		},
		Timeout: d.Global.Timeout,
	}

	results, err := driver.ListSnapshots(do)
	if err != nil {
		api.RESTHTTPError(w, errors.ListSnapshots.Combine(err))
		return
	}

	content, err := json.Marshal(results)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handleSnapshotTake(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	volume := vars["volume"]

	vol := db.NewVolume(policy, volume)
	if err := d.Client.Get(vol); err != nil {
		api.RESTHTTPError(w, errors.InvalidVolume.Combine(err))
		return
	}

	snap := db.NewSnapshot(vol)

	if err := d.Client.Set(snap); err != nil {
		api.RESTHTTPError(w, errors.SnapshotFailed.Combine(err))
		return
	}

	content, err := json.Marshal(snap)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handleCopy(w http.ResponseWriter, r *http.Request) {
	req, err := unmarshalRequest(r)
	if err != nil {
		api.RESTHTTPError(w, errors.UnmarshalRequest.Combine(err))
		return
	}

	if _, ok := req.Options["snapshot"]; !ok {
		api.RESTHTTPError(w, errors.MissingSnapshotOption)
		return
	}

	if _, ok := req.Options["target"]; !ok {
		api.RESTHTTPError(w, errors.MissingTargetOption)
		return
	}

	if strings.Contains(req.Options["target"], "/") {
		api.RESTHTTPError(w, errors.InvalidVolume.Combine(errored.New("/")))
		return
	}

	volConfig := db.NewVolume(req.Policy, req.Name)
	if err := d.Client.Get(volConfig); err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	if volConfig.Backends.Snapshot == "" {
		api.RESTHTTPError(w, errors.SnapshotsUnsupported.Combine(errored.New(volConfig.Backends.Snapshot)))
		return
	}

	driver, err := backend.NewSnapshotDriver(volConfig.Backends.Snapshot)
	if err != nil {
		api.RESTHTTPError(w, errors.GetDriver.Combine(err))
		return
	}

	newVolConfig := db.NewVolume(req.Policy, req.Name)
	if err := d.Client.Get(newVolConfig); err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	newVolConfig.SetName(req.Options["target"])

	do := storage.DriverOptions{
		Volume: storage.Volume{
			Name:   volConfig.String(),
			Params: volConfig.DriverOptions,
		},
		Timeout: d.Global.Timeout,
	}

	if volConfig.String() == newVolConfig.String() {
		api.RESTHTTPError(w, errors.CannotCopyVolume.Combine(errored.Errorf("You cannot copy volume %q onto itself.", volConfig.String())))
		return
	}

	snapUC := db.NewSnapshotCopy(volConfig)
	newUC := db.NewCreateOwner(d.Hostname, newVolConfig)
	newSnapUC := db.NewSnapshotCopy(newVolConfig)

	lockFun := func(lock []db.Lock) error {
		if err := d.Client.Set(newVolConfig); err != nil {
			return err
		}

		if err := driver.CopySnapshot(do, req.Options["snapshot"], newVolConfig.String()); err != nil {
			return err
		}
		return nil
	}

	if err := db.ExecuteWithMultiUseLock(d.Client, lockFun, d.Global.Timeout, snapUC, newUC, newSnapUC); err != nil {
		api.RESTHTTPError(w, errors.PublishVolume.Combine(errored.Errorf(
			"Creating new volume %q from volume %q, snapshot %q",
			req.Options["target"],
			volConfig.String(),
			req.Options["snapshot"],
		)).Combine(err))
		return
	}

	content, err := json.Marshal(newVolConfig)
	if err != nil {
		api.RESTHTTPError(w, errors.PublishVolume.Combine(errored.Errorf(
			"Creating new volume %q from volume %q, snapshot %q",
			req.Options["target"],
			volConfig.String(),
			req.Options["snapshot"],
		)).Combine(err))
	}

	w.Write(content)
}

func (d *DaemonConfig) handleGlobal(w http.ResponseWriter, r *http.Request) {
	content, err := json.Marshal(d.Global.Published())
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalGlobal.Combine(err))
		return
	}

	w.Write(content)
}

func transformToNamed(vols []db.Entity) []*db.NamedVolume {
	named := []*db.NamedVolume{}
	for _, vol := range vols {
		named = append(named, vol.(*db.Volume).Named())
	}

	return named
}

func (d *DaemonConfig) handleList(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if vars["policy"] == "" {
		api.RESTHTTPError(w, errors.ListVolume.Combine(errored.New("Policy was blank")))
		return
	}

	vols, err := d.Client.ListPrefix(vars["policy"], &db.Volume{})
	if err != nil {
		api.RESTHTTPError(w, errors.ListVolume.Combine(err))
		return
	}

	content, err := json.Marshal(transformToNamed(vols))
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handleListAll(w http.ResponseWriter, r *http.Request) {
	vols, err := d.Client.List(&db.Volume{})
	if err != nil {
		api.RESTHTTPError(w, errors.ListVolume.Combine(err))
		return
	}

	content, err := json.Marshal(transformToNamed(vols))
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
	}

	w.Write(content)
}

func (d *DaemonConfig) handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	policy := vars["policy"]
	volumeName := vars["volume"]

	volConfig := db.NewVolume(policy, volumeName)

	err := d.Client.Get(volConfig)

	if erd, ok := err.(*errored.Error); ok && erd.Contains(errors.NotExists) {
		w.WriteHeader(404)
		return
	} else if err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	content, err := json.Marshal(volConfig)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) createRemoveLocks(vc *db.Volume) ([]db.Lock, error) {
	uc := db.NewRemoveOwner(d.Hostname, vc)
	snapUC := db.NewSnapshotRemove(vc)

	return []db.Lock{uc, snapUC}, nil
}

func (d *DaemonConfig) removeVolume(vc *db.Volume) error {
	if err := d.Client.Delete(vc); err != nil {
		return errors.ClearVolume.Combine(errored.New(vc.String())).Combine(err)
	}

	return nil
}

func (d *DaemonConfig) completeRemove(vc *db.Volume) error {
	if err := control.RemoveVolume(vc, d.Global.Timeout); err != nil && err != errors.NoActionTaken {
		logrus.Warn(errors.RemoveImage.Combine(errored.New(vc.String())).Combine(err))
	}

	return d.removeVolume(vc)
}

// this cleans up uses when forcing the removal
func (d *DaemonConfig) removeVolumeUse(lock db.Lock, vc *db.Volume) {
	if err := d.Client.Free(lock, true); err != nil {
		logrus.Warn(errors.RemoveImage.Combine(errored.New(vc.String())).Combine(err))
	}
}

func (d *DaemonConfig) handleForceRemoveLock(vc *db.Volume, locks []db.Lock) error {
	exists, err := control.ExistsVolume(vc, d.Global.Timeout)
	if err != nil && err != errors.NoActionTaken {
		return errors.RemoveVolume.Combine(errored.New(vc.String())).Combine(err)
	}

	if err == errors.NoActionTaken {
		if err := d.completeRemove(vc); err != nil {
			return err
		}

		d.removeVolumeUse(locks[0], vc)
	}

	if err != nil {
		return errors.RemoveVolume.Combine(errored.New(vc.String())).Combine(err)
	}

	if !exists {
		d.removeVolume(vc)
		return errors.RemoveVolume.Combine(errored.New(vc.String())).Combine(errors.NotExists)
	}

	err = d.completeRemove(vc)
	if err != nil {
		return errors.RemoveVolume.Combine(errored.New(vc.String())).Combine(errors.NotExists)
	}

	d.removeVolumeUse(locks[0], vc)
	return nil
}

func (d *DaemonConfig) handleRemove(w http.ResponseWriter, r *http.Request) {
	// set a default timeout if none is specified
	timeout := d.Global.Timeout
	req, err := unmarshalRequest(r)
	if err != nil {
		api.RESTHTTPError(w, errors.UnmarshalRequest.Combine(err))
		return
	}

	if req.Options["timeout"] != "" {
		var t time.Duration
		if t, err = time.ParseDuration(req.Options["timeout"]); err != nil {
			api.RESTHTTPError(w, errors.RemoveVolume.Combine(err))
			return
		}
		timeout = t
	}

	vc := db.NewVolume(req.Policy, req.Name)
	if err := d.Client.Get(vc); err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	locks, err := d.createRemoveLocks(vc)
	if err != nil {
		api.RESTHTTPError(w, err)
		return
	}

	if req.Options["force"] == "true" {
		if err := d.handleForceRemoveLock(vc, locks); err != nil {
			api.RESTHTTPError(w, err)
			return
		}
	}

	removeVolume := func(locks []db.Lock) error {
		exists, err := control.ExistsVolume(vc, timeout)
		if err != nil && err != errors.NoActionTaken {
			return err
		}

		if err == errors.NoActionTaken {
			return d.completeRemove(vc)
		}

		if !exists {
			d.removeVolume(vc)
			return errors.NotExists
		}

		return d.completeRemove(vc)
	}

	err = db.ExecuteWithMultiUseLock(d.Client, removeVolume, timeout, locks...)
	if err == errors.NotExists {
		w.WriteHeader(404)
		return
	}

	if err != nil {
		api.RESTHTTPError(w, errors.RemoveVolume.Combine(errored.New(vc.String())).Combine(err))
		return
	}
}

func (d *DaemonConfig) handleRemoveForce(w http.ResponseWriter, r *http.Request) {
	req, err := unmarshalRequest(r)
	if err != nil {
		api.RESTHTTPError(w, errors.UnmarshalRequest.Combine(err))
		return
	}

	vol := db.NewVolume(req.Policy, req.Name)
	err = d.Client.Delete(vol)
	if err == errors.NotExists {
		w.WriteHeader(404)
		return
	}

	if err != nil {
		api.RESTHTTPError(w, errors.RemoveVolume.Combine(errored.Errorf("%v/%v", req.Policy, req.Name)).Combine(err))
		return
	}
}

func (d *DaemonConfig) handleRequest(w http.ResponseWriter, r *http.Request) {
	req, err := unmarshalRequest(r)
	if err != nil {
		api.RESTHTTPError(w, errors.UnmarshalRequest.Combine(err))
		return
	}

	vol := db.NewVolume(req.Policy, req.Name)
	err = d.Client.Get(vol)
	if erd, ok := err.(*errored.Error); ok && erd.Contains(errors.NotExists) {
		w.WriteHeader(404)
		return
	} else if err != nil {
		api.RESTHTTPError(w, errors.GetVolume.Combine(err))
		return
	}

	content, err := json.Marshal(vol)
	if err != nil {
		api.RESTHTTPError(w, errors.MarshalResponse.Combine(err))
		return
	}

	w.Write(content)
}

func (d *DaemonConfig) handleCreate(w http.ResponseWriter, r *http.Request) {
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.RESTHTTPError(w, errors.ReadBody.Combine(err))
		return
	}

	req := &db.VolumeRequest{}

	if err := json.Unmarshal(content, req); err != nil {
		api.RESTHTTPError(w, errors.UnmarshalRequest.Combine(err))
		return
	}

	if req.Policy == "" {
		api.RESTHTTPError(w, errors.GetPolicy.Combine(errored.Errorf("policy was blank")))
		return
	}

	if req.Name == "" {
		api.RESTHTTPError(w, errors.GetVolume.Combine(errored.Errorf("volume was blank")))
		return
	}

	policy := db.NewPolicy(req.Policy)
	if err := d.Client.Get(policy); err != nil {
		api.RESTHTTPError(w, errors.GetPolicy.Combine(errored.New(req.Policy).Combine(err)))
		return
	}

	vol := db.NewVolume(req.Policy, req.Name)

	uc := db.NewCreateOwner(d.Hostname, vol)
	snapUC := db.NewSnapshotCreate(vol)

	err = db.ExecuteWithMultiUseLock(d.Client, d.createVolume(w, req, policy), d.Global.Timeout, uc, snapUC)
	if err != nil && err != errors.Exists {
		api.RESTHTTPError(w, errors.CreateVolume.Combine(err))
		return
	}
}

func (d *DaemonConfig) createVolume(w http.ResponseWriter, req *db.VolumeRequest, policy *db.Policy) func(locks []db.Lock) error {
	return func(locks []db.Lock) error {
		volConfig, err := db.CreateVolume(policy, req.Name, req.Options)
		if err != nil {
			return err
		}

		logrus.Debugf("Volume Create: %#v", volConfig)

		do, err := control.CreateVolume(policy, volConfig, d.Global.Timeout)
		if err == errors.NoActionTaken {
			goto publish
		}

		if err != nil {
			return errors.CreateVolume.Combine(err)
		}

		if err := control.FormatVolume(volConfig, do); err != nil {
			if err := control.RemoveVolume(volConfig, d.Global.Timeout); err != nil {
				logrus.Errorf("Error during cleanup of failed format: %v", err)
			}
			return errors.FormatVolume.Combine(err)
		}

	publish:
		if err := d.Client.Set(volConfig); err != nil && err != errors.Exists {
			// FIXME this shouldn't leak down to the client.
			if _, ok := err.(*errored.Error); !ok {
				return errors.PublishVolume.Combine(err)
			}
			return err
		}

		content, err := json.Marshal(volConfig)
		if err != nil {
			return errors.MarshalPolicy.Combine(err)
		}

		w.Write(content)
		return nil
	}
}

func unmarshalRequest(r *http.Request) (*db.VolumeRequest, error) {
	cfg := &db.VolumeRequest{}

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return cfg, err
	}

	if err := json.Unmarshal(content, cfg); err != nil {
		return cfg, err
	}

	if cfg.Policy == "" {
		return cfg, errored.New("Policy was blank")
	}

	if cfg.Name == "" {
		return cfg, errored.New("volume was blank")
	}

	return cfg, nil
}
