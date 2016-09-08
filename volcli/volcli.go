package volcli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/codegangsta/cli"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/db/impl/consul"
	"github.com/contiv/volplugin/db/impl/etcd"
	"github.com/contiv/volplugin/errors"
	"github.com/hashicorp/consul/api"
	"github.com/kr/pty"
)

// GetClientByName is provided a prefix and list of hosts to contact, then
// proceeds to connect to the host with the named client. The underlying client
// implementation will be selected based on the name provided, and the
// appropriate structures will be set up, etc.
func GetClientByName(name string, prefix string, hosts []string) (db.Client, error) {
	switch name {
	case "etcd":
		return etcd.NewClient(hosts, prefix)
	case "consul":
		return consul.NewClient(&api.Config{Address: hosts[0]}, prefix)
	}

	return nil, errored.Errorf("Invalid client driver name: %v", name)
}

func errorInvalidVolumeSyntax(rcvd, exptd string) error {
	return errored.Errorf("Invalid syntax: %q must be in the form of %q)", rcvd, exptd)
}

func errorInvalidArgCount(rcvd, exptd int, args []string) error {
	return errored.Errorf("Invalid number of arguments: expected %d but received %d %v", exptd, rcvd, args)
}

func splitVolume(ctx *cli.Context) (string, string, error) {
	volumeparts := strings.SplitN(ctx.Args()[0], "/", 2)

	if len(volumeparts) < 2 || volumeparts[0] == "" || volumeparts[1] == "" {
		return "", "", errorInvalidVolumeSyntax(ctx.Args()[0], `<policyName>/<volumeName>`)
	}

	return volumeparts[0], volumeparts[1], nil
}

func errExit(ctx *cli.Context, err error, help bool) {
	fmt.Fprintf(os.Stderr, "\nError: %v\n\n", err)
	if help {
		cli.ShowAppHelp(ctx)
	}
	os.Exit(1)
}

func execCliAndExit(ctx *cli.Context, f func(ctx *cli.Context) (bool, error)) {
	if showHelp, err := f(ctx); err != nil {
		errExit(ctx, err, showHelp)
	}
}

func ppJSON(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

func deleteRequest(url string, bodyType string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest("DELETE", url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request: %v", err)
	}
	req.Header.Set("Content-Type", bodyType)

	return http.DefaultClient.Do(req)
}

// GlobalGet retrives the global configuration and displays it on standard output.
func GlobalGet(ctx *cli.Context) {
	execCliAndExit(ctx, globalGet)
}

func queryGlobalConfig(ctx *cli.Context) (*db.Global, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/global", ctx.GlobalString("apiserver")))
	if err != nil {
		return nil, err
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errored.Errorf("Status code was %d not 200: %s", resp.StatusCode, string(content))
	}

	// rebuild and divide the contents so they are cast out of their internal
	// representation.
	return db.NewGlobalFromJSON(content)
}

func globalGet(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 0 {
		return true, errorInvalidArgCount(len(ctx.Args()), 0, ctx.Args())
	}

	global, err := queryGlobalConfig(ctx)
	if err != nil {
		return false, err
	}

	content, err := ppJSON(global)
	if err != nil {
		return false, err
	}

	fmt.Println(string(content))
	return false, nil
}

// GlobalUpload uploads the global configuration
func GlobalUpload(ctx *cli.Context) {
	execCliAndExit(ctx, globalUpload)
}

func globalUpload(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 0 {
		return true, errorInvalidArgCount(len(ctx.Args()), 0, ctx.Args())
	}

	content, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return false, err
	}

	global := db.NewGlobal()
	if err := json.Unmarshal(content, global); err != nil {
		return false, err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/global", ctx.GlobalString("apiserver")), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	return false, nil
}

// PolicyUpload uploads a Policy intent from stdin.
func PolicyUpload(ctx *cli.Context) {
	execCliAndExit(ctx, policyUpload)
}

func policyUpload(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	content, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return false, err
	}

	policy := db.NewPolicy(ctx.Args()[0])

	if err := json.Unmarshal(content, policy); err != nil {
		return false, err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/policies/%s", ctx.GlobalString("apiserver"), policy), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	return false, nil
}

// PolicyDelete removes a policy supplied as an argument.
func PolicyDelete(ctx *cli.Context) {
	execCliAndExit(ctx, policyDelete)
}

func policyDelete(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy := ctx.Args()[0]

	resp, err := deleteRequest(fmt.Sprintf("http://%s/policies/%s", ctx.GlobalString("apiserver"), policy), "application/json", nil)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	fmt.Printf("%q removed!\n", policy)

	return false, nil
}

// PolicyGet retrieves policy configuration, the name of which is supplied as
// an argument.
func PolicyGet(ctx *cli.Context) {
	execCliAndExit(ctx, policyGet)
}

func policyGet(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy := ctx.Args()[0]

	resp, err := http.Get(fmt.Sprintf("http://%s/policies/%s", ctx.GlobalString("apiserver"), policy))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	fmt.Println(string(content))

	return false, nil
}

// PolicyList provides a list of the policy names.
func PolicyList(ctx *cli.Context) {
	execCliAndExit(ctx, policyList)
}

func policyList(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 0 {
		return true, errorInvalidArgCount(len(ctx.Args()), 0, ctx.Args())
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/policies", ctx.GlobalString("apiserver")))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	var policies []*db.NamedPolicy
	if err := json.Unmarshal(content, &policies); err != nil {
		return false, err
	}

	for _, policy := range policies {
		fmt.Println(policy)
	}

	return false, nil
}

// PolicyGetRevision retrieves a single revision from a policy's history.
func PolicyGetRevision(ctx *cli.Context) {
	execCliAndExit(ctx, policyGetRevision)
}

func policyGetRevision(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 2 {
		return true, errorInvalidArgCount(len(ctx.Args()), 2, ctx.Args())
	}

	name := ctx.Args()[0]
	revision := ctx.Args()[1]

	resp, err := http.Get(fmt.Sprintf("http://%s/policy-archives/%s/%s",
		ctx.GlobalString("apiserver"),
		name,
		revision,
	))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	fmt.Println(string(content))

	return false, nil
}

// PolicyListRevisions retrieves all the revisions for a given policy.
func PolicyListRevisions(ctx *cli.Context) {
	execCliAndExit(ctx, policyListRevisions)
}

func policyListRevisions(ctx *cli.Context) (bool, error) {
	var revisions []string

	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	name := ctx.Args()[0]

	resp, err := http.Get(fmt.Sprintf("http://%s/policy-archives/%s",
		ctx.GlobalString("apiserver"),
		name,
	))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(content, &revisions); err != nil {
		return false, err
	}

	for _, revision := range revisions {
		fmt.Println(revision)
	}

	return false, nil
}

// PolicyWatch watches etcd for policy changes and prints the name and revision when a policy is uploaded.
func PolicyWatch(ctx *cli.Context) {
	execCliAndExit(ctx, policyWatch)
}

func policyWatch(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 0 {
		return true, errorInvalidArgCount(len(ctx.Args()), 0, ctx.Args())
	}

	cfg, err := GetClientByName(ctx.GlobalString("store"), ctx.GlobalString("prefix"), ctx.GlobalStringSlice("store-url"))
	if err != nil {
		return false, err
	}

	watchChan, errChan := cfg.Watch(&db.PolicyRevision{})

	for {
		select {
		case err := <-errChan:
			return false, err
		case obj := <-watchChan:
			fmt.Println(obj)
		}
	}
}

// VolumeCreate creates a new volume with a JSON specification to store its
// information.
func VolumeCreate(ctx *cli.Context) {
	execCliAndExit(ctx, volumeCreate)
}

func volumeCreate(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	opts := map[string]string{}

	for _, str := range ctx.StringSlice("opt") {
		pair := strings.SplitN(str, "=", 2)
		if len(pair) < 2 {
			return false, errored.Errorf("Mismatched option pair %q", pair)
		}

		opts[pair[0]] = pair[1]
	}

	tc := &db.VolumeRequest{
		Policy:  policy,
		Name:    volume,
		Options: opts,
	}

	content, err := json.Marshal(tc)
	if err != nil {
		return false, errored.Errorf("Could not create request JSON: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/volumes/create", ctx.GlobalString("apiserver")), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, errored.Errorf("Error in request: %v", err)
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	return false, nil
}

// VolumeGet retrieves the metadata for a volume and prints it.
func VolumeGet(ctx *cli.Context) {
	execCliAndExit(ctx, volumeGet)
}

func volumeGet(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/volumes/%s/%s", ctx.GlobalString("apiserver"), policy, volume))
	if err != nil {
		return false, err
	}

	if resp.StatusCode == 404 {
		return false, errored.Errorf("Volume %v/%v no longer exists.", policy, volume)
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	var vol *db.Volume

	if err := json.Unmarshal(content, &vol); err != nil {
		return false, err
	}

	content, err = ppJSON(vol)
	if err != nil {
		return false, err
	}

	fmt.Println(string(content))

	return false, nil
}

// VolumeForceRemove removes a volume forcefully.
func VolumeForceRemove(ctx *cli.Context) {
	execCliAndExit(ctx, volumeForceRemove)
}

func volumeForceRemove(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	request := db.VolumeRequest{
		Policy: policy,
		Name:   volume,
	}

	content, err := json.Marshal(request)
	if err != nil {
		return false, err
	}

	resp, err := deleteRequest(fmt.Sprintf("http://%s/volumes/removeforce", ctx.GlobalString("apiserver")), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, err
	}

	qualifiedVolume := strings.Join([]string{policy, volume}, "/")

	if resp.StatusCode == 404 {
		return false, errored.Errorf("Volume %v no longer exists.", qualifiedVolume)
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	return false, nil
}

// VolumeRemove removes a volume, deleting the image beneath it.
func VolumeRemove(ctx *cli.Context) {
	execCliAndExit(ctx, volumeRemove)
}

func volumeRemove(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	timeout := ctx.String("timeout")
	if _, err = time.ParseDuration(timeout); err != nil && timeout != "" {
		return false, errored.Errorf("%v is not a valid timeout", ctx.String("timeout"))
	}

	request := db.VolumeRequest{
		Policy: policy,
		Name:   volume,
		Options: map[string]string{
			"timeout": timeout,
			"force":   fmt.Sprintf("%v", ctx.Bool("force")),
		},
	}

	content, err := json.Marshal(request)
	if err != nil {
		return false, err
	}

	resp, err := deleteRequest(fmt.Sprintf("http://%s/volumes/remove", ctx.GlobalString("apiserver")), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, err
	}

	qualifiedVolume := strings.Join([]string{policy, volume}, "/")

	if resp.StatusCode == 404 {
		return false, errored.Errorf("Volume %v no longer exists.", qualifiedVolume)
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	return false, nil
}

// VolumeList prints the list of volumes for a pool.
func VolumeList(ctx *cli.Context) {
	execCliAndExit(ctx, volumeList)
}

func volumeList(ctx *cli.Context) (bool, error) {
	var volumes []*db.NamedVolume
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy := ctx.Args()[0]

	resp, err := http.Get(fmt.Sprintf("http://%s/volumes/%s", ctx.GlobalString("apiserver"), policy))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(content, &volumes); err != nil {
		return false, err
	}

	for _, volume := range volumes {
		fmt.Println(volume)
	}

	return false, nil
}

// VolumeSnapshotTake takes a snapshot for a volume immediately.
func VolumeSnapshotTake(ctx *cli.Context) {
	execCliAndExit(ctx, volumeSnapshotTake)
}

func volumeSnapshotTake(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 3, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/snapshots/take/%s/%s", ctx.GlobalString("apiserver"), policy, volume), "application/json", nil)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	return false, nil
}

// VolumeSnapshotCopy lists all snapshots for a given volume.
func VolumeSnapshotCopy(ctx *cli.Context) {
	execCliAndExit(ctx, volumeSnapshotCopy)
}

func volumeSnapshotCopy(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 3 {
		return true, errorInvalidArgCount(len(ctx.Args()), 3, ctx.Args())
	}

	policy, volume1, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	snapName := ctx.Args()[1]
	volume2 := ctx.Args()[2]

	req := &db.VolumeRequest{
		Name:   volume1,
		Policy: policy,
		Options: map[string]string{
			"target":   volume2,
			"snapshot": snapName,
		},
	}

	content, err := json.Marshal(req)
	if err != nil {
		return false, errored.Errorf("Could not create request JSON: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/volumes/copy", ctx.GlobalString("apiserver")), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume1)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	content, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, errored.New("Reading body processing response").Combine(err)
	}

	vol := &db.Volume{}
	if err := json.Unmarshal(content, vol); err != nil {
		return false, errors.UnmarshalVolume.Combine(err)
	}

	fmt.Println(vol)

	return false, nil
}

// VolumeSnapshotList lists all snapshots for a given volume.
func VolumeSnapshotList(ctx *cli.Context) {
	execCliAndExit(ctx, volumeSnapshotList)
}

func volumeSnapshotList(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/snapshots/%s/%s", ctx.GlobalString("apiserver"), policy, volume))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	var results []string

	if err := json.Unmarshal(content, &results); err != nil {
		return false, err
	}

	for _, result := range results {
		fmt.Println(result)
	}

	return false, nil
}

// VolumeListAll returns a list of the pools the apiserver knows about.
func VolumeListAll(ctx *cli.Context) {
	execCliAndExit(ctx, volumeListAll)
}

func volumeListAll(ctx *cli.Context) (bool, error) {
	var volumes []*db.NamedVolume
	if len(ctx.Args()) != 0 {
		return true, errorInvalidArgCount(len(ctx.Args()), 0, ctx.Args())
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/volumes/", ctx.GlobalString("apiserver")))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\nResponse Status Code was %d, not 200", err, resp.StatusCode)
		}
		return false, errored.Errorf("Response Status Code was %d, not 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(content, &volumes); err != nil {
		return false, err
	}

	for _, volume := range volumes {
		fmt.Println(volume)
	}

	return false, nil
}

// UseList returns a list of the mounts the apiserver knows about.
func UseList(ctx *cli.Context) {
	execCliAndExit(ctx, useList)
}

func useList(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 0 {
		return true, errorInvalidArgCount(len(ctx.Args()), 0, ctx.Args())
	}

	cfg, err := GetClientByName(ctx.GlobalString("store"), ctx.GlobalString("prefix"), ctx.GlobalStringSlice("store-url"))
	if err != nil {
		return false, err
	}

	var uses []db.Entity
	// FIXME I really dislike this use of NewSnapshotCreate
	if ctx.Bool("snapshots") {
		uses, err = cfg.List(db.NewSnapshotCreate(nil))
	} else {
		uses, err = cfg.List(&db.Use{})
	}

	if err != nil {
		return false, err
	}

	for _, name := range uses {
		fmt.Println(name)
	}

	return false, nil
}

// UseGet retrieves the JSON information for a mount.
func UseGet(ctx *cli.Context) {
	execCliAndExit(ctx, useGet)
}

func useGet(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	cfg, err := GetClientByName(ctx.GlobalString("store"), ctx.GlobalString("prefix"), ctx.GlobalStringSlice("store-url"))
	if err != nil {
		return false, err
	}

	var ul db.Lock

	if ctx.Bool("snapshot") {
		ul = db.NewSnapshotCreate(db.NewVolume(policy, volume))
	} else {
		ul = db.NewCreateOwner(ctx.GlobalString("hostname"), db.NewVolume(policy, volume))
	}

	if err := cfg.Get(ul); err != nil {
		return false, err
	}

	content, err := ppJSON(ul)
	if err != nil {
		return false, err
	}

	fmt.Println(string(content))

	return false, nil
}

// UseTheForce deletes the use entry from etcd; useful for clearing a
// stale mount.
func UseTheForce(ctx *cli.Context) {
	execCliAndExit(ctx, useTheForce)
}

func useTheForce(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	cfg, err := GetClientByName(ctx.GlobalString("store"), ctx.GlobalString("prefix"), ctx.GlobalStringSlice("store-url"))
	if err != nil {
		return false, err
	}

	vc := db.NewVolume(policy, volume)

	ul := db.NewCreateOwner(ctx.GlobalString("hostname"), vc)
	if err := cfg.Delete(ul); err != nil {
		fmt.Fprintf(os.Stderr, "Trouble removing mount lock (may be harmless) for %q: %v", vc, err)
	}

	ul = db.NewSnapshotCreate(vc)
	if err := cfg.Delete(ul); err != nil {
		fmt.Fprintf(os.Stderr, "Trouble removing snapshot lock (may be harmless) for %q: %v", vc, err)
	}

	return false, nil
}

// UseExec acquires a lock (waiting if necessary) and executes a command when it takes it.
func UseExec(ctx *cli.Context) {
	execCliAndExit(ctx, useExec)
}

func useExec(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) < 2 {
		return true, errorInvalidArgCount(len(ctx.Args()), 2, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	cfg, err := GetClientByName(ctx.GlobalString("store"), ctx.GlobalString("prefix"), ctx.GlobalStringSlice("store-url"))
	if err != nil {
		return false, err
	}

	vc := db.NewVolume(policy, volume)
	um := db.NewMaintenanceOwner(ctx.GlobalString("hostname"), vc)
	us := db.NewSnapshotMaintenance(vc)

	args := ctx.Args()[1:]
	if args[0] == "--" {
		if len(args) < 2 {
			return true, errored.Errorf("You must supply a command to run")
		}
		args = args[1:]
	}

	err = db.ExecuteWithMultiUseLock(cfg, func(locks []db.Lock) error {
		cmd := exec.Command("/bin/sh", "-c", strings.Join(args, " "))

		signals := make(chan os.Signal)

		go signal.Notify(signals, syscall.SIGINT)
		go func() {
			<-signals
			cmd.Process.Signal(syscall.SIGINT)
		}()

		if _, err := pty.Start(cmd); err != nil {
			return err
		}

		return cmd.Wait()
	}, time.Minute, um, us)

	return false, err
}

// VolumeRuntimeGet retrieves the runtime configuration for a volume.
func VolumeRuntimeGet(ctx *cli.Context) {
	execCliAndExit(ctx, volumeRuntimeGet)
}

func volumeRuntimeGet(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/runtime/%s/%s", ctx.GlobalString("apiserver"), policy, volume))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	runtime := &db.RuntimeOptions{}

	if err := json.Unmarshal(content, runtime); err != nil {
		return false, err
	}

	content, err = ppJSON(runtime)
	if err != nil {
		return false, err
	}

	fmt.Println(string(content))

	return false, nil
}

// VolumeRuntimeUpload retrieves the runtime configuration for a volume.
func VolumeRuntimeUpload(ctx *cli.Context) {
	execCliAndExit(ctx, volumeRuntimeUpload)
}

func volumeRuntimeUpload(ctx *cli.Context) (bool, error) {
	if len(ctx.Args()) != 1 {
		return true, errorInvalidArgCount(len(ctx.Args()), 1, ctx.Args())
	}

	policy, volume, err := splitVolume(ctx)
	if err != nil {
		return true, err
	}

	content, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return false, err
	}

	runtime := &db.RuntimeOptions{}

	if err := json.Unmarshal(content, runtime); err != nil {
		return false, err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/runtime/%s/%s", ctx.GlobalString("apiserver"), policy, volume), "application/json", bytes.NewBuffer(content))
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		qualifiedVolume := fmt.Sprintf("%v/%v", policy, volume)
		if _, err := io.Copy(os.Stderr, resp.Body); err != nil {
			return false, errored.Errorf("Error copying body: %v\n Volume %v Response Status Code was %d, not 200", err, qualifiedVolume, resp.StatusCode)
		}
		return false, errored.Errorf("Volume %v Response Status Code was %d, not 200", qualifiedVolume, resp.StatusCode)
	}

	return false, nil
}
