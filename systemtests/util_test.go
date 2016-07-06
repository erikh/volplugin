package systemtests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/contiv/errored"
	utils "github.com/contiv/systemtests-utils"
	"github.com/contiv/vagrantssh"
	"github.com/contiv/volplugin/config"
)

var startedContainers struct {
	sync.Mutex
	names map[string]struct{}
}

// genRandomString returns a pseudo random string.
// It doesn't worry about name collisions much at the moment.
func genRandomString(prefix, suffix string, strlen int) string {
	charSet := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	randStr := make([]byte, 0, strlen)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < strlen; i++ {
		randStr = append(randStr, charSet[rand.Int()%len(charSet)])
	}
	return prefix + string(randStr) + suffix
}

func (s *systemtestSuite) rbd(cmd string) (string, error) {
	return s.mon0cmd(fmt.Sprintf("docker run -i --volumes-from=mon0 ceph/rbd %s", cmd))
}

func (s *systemtestSuite) dockerRun(host string, tty, daemon bool, volume, command string) (string, error) {
	ttystr := ""
	daemonstr := ""

	if tty {
		ttystr = "-t"
	}

	if daemon {
		daemonstr = "-d"
	}

	// generate a container name.
	// The probability of name collisions in a test should be low as there are
	// about 62^10 possible strings. But we simply search for a container name
	// in `startedContainers` with some additional locking overhead to keep tests reliable.
	// Note: we don't remove the container name from the list on a run failure, this
	// allows full cleanup later.
	startedContainers.Lock()
	cName := genRandomString("", "", 10)
	for _, ok := startedContainers.names[cName]; ok; {
		cName = genRandomString("", "", 10)
	}
	startedContainers.names[cName] = struct{}{}
	startedContainers.Unlock()

	dockerCmd := fmt.Sprintf(
		"docker run --name %s -i %s %s -v %v:/mnt:nocopy alpine %s",
		cName,
		ttystr,
		daemonstr,
		volume,
		command,
	)

	log.Infof("Starting docker on %q with: %q", host, dockerCmd)

	str, err := s.vagrant.GetNode(host).RunCommandWithOutput(dockerCmd)
	if err != nil {
		return str, err
	}

	return str, nil
}

func (s *systemtestSuite) mon0cmd(command string) (string, error) {
	return s.vagrant.GetNode("mon0").RunCommandWithOutput(command)
}

func (s *systemtestSuite) volcli(command string) (string, error) {
	return s.mon0cmd("volcli " + command)
}

func (s *systemtestSuite) readIntent(fn string) (*config.Policy, error) {
	content, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	cfg := config.NewPolicy()

	if err := json.Unmarshal(content, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (s *systemtestSuite) purgeVolume(host, policy, name string, purgeCeph bool) error {
	log.Infof("Purging %s/%s. Purging ceph: %v", host, name, purgeCeph)

	// // ignore the error here so we get to the purge if we have to
	if out, err := s.vagrant.GetNode(host).RunCommandWithOutput(fmt.Sprintf("docker volume rm %s/%s", policy, name)); err != nil {
		log.Error(out, err)
	}

	defer func() {
		if purgeCeph && cephDriver() {
			s.rbd(fmt.Sprintf("snap purge rbd/%s.%s", policy, name))
			s.rbd(fmt.Sprintf("rm rbd/%s.%s", policy, name))
		}
	}()

	if out, err := s.volcli(fmt.Sprintf("volume remove %s/%s", policy, name)); err != nil {
		log.Error(out)
		return err
	}

	return nil
}

func (s *systemtestSuite) purgeVolumeHost(policy, host string, purgeCeph bool) {
	s.purgeVolume(host, policy, host, purgeCeph)
}

func (s *systemtestSuite) createVolumeHost(policy, host string, opts map[string]string) error {
	return s.createVolume(host, policy, host, opts)
}

func (s *systemtestSuite) createVolume(host, policy, name string, opts map[string]string) error {
	log.Infof("Creating %s/%s on %q", policy, name, host)

	optsStr := []string{}

	if nfsDriver() {
		log.Infof("Making NFS mount directory /volplugin/%s/%s", policy, name)
		_, err := s.mon0cmd(fmt.Sprintf("sudo mkdir -p /volplugin/%s/%s && sudo chmod 4777 /volplugin/%s/%s", policy, name, policy, name))
		if err != nil {
			return err
		}

		if opts == nil {
			opts = map[string]string{}
		}

		mountstr := fmt.Sprintf("%s:/volplugin/%s/%s", s.mon0ip, policy, name)
		log.Infof("Mapping NFS mount %q", mountstr)
		opts["mount"] = mountstr
	}

	if opts != nil {
		for key, value := range opts {
			optsStr = append(optsStr, "--opt")
			optsStr = append(optsStr, key+"="+value)
		}

		log.Infof("Creating with options: %q", strings.Join(optsStr, " "))
	}

	cmd := fmt.Sprintf("docker volume create -d volplugin --name %s/%s %s", policy, name, strings.Join(optsStr, " "))

	if out, err := s.vagrant.GetNode(host).RunCommandWithOutput(cmd); err != nil {
		log.Info(string(out))
		return err
	}

	if out, err := s.volcli(fmt.Sprintf("volume get %s/%s", policy, name)); err != nil {
		log.Error(out)
		return err
	}

	return nil
}

func (s *systemtestSuite) uploadGlobal(configFile string) error {
	log.Infof("Uploading global configuration %s", configFile)
	out, err := s.volcli(fmt.Sprintf("global upload < /testdata/globals/%s.json", configFile))
	if err != nil {
		log.Error(out)
	}

	return err
}

func (s *systemtestSuite) clearNFS() {
	log.Info("Clearing NFS directories")
	s.mon0cmd("sudo rm -rf /volplugin && sudo mkdir /volplugin")
}

func clearVolumeHost(node vagrantssh.TestbedNode) error {
	log.Infof("Clearing docker volumes")
	out, err := node.RunCommandWithOutput("docker volume ls | grep volplugin | tail -n +2 | awk \"{ print \\$2 }\" | xargs docker volume rm")
	if err != nil {
		log.Error(out)
	}
	return err
}

func (s *systemtestSuite) rebootstrap() error {
	if os.Getenv("NO_TEARDOWN") == "" {
		if cephDriver() {
			s.clearRBD()
		}

		if nfsDriver() {
			s.clearNFS()
		}

		s.vagrant.IterateNodes(stopVolplugin)
		s.vagrant.IterateNodes(stopApiserver)
		s.vagrant.IterateNodes(stopVolsupervisor)

		log.Info("Clearing etcd")
		utils.ClearEtcd(s.vagrant.GetNode("mon0"))
	}

	if err := s.vagrant.IterateNodes(startApiserver); err != nil {
		return err
	}

	if err := s.vagrant.IterateNodes(waitForApiserver); err != nil {
		return err
	}

	if err := s.uploadGlobal("global1"); err != nil {
		return err
	}

	if err := startVolsupervisor(s.vagrant.GetNode("mon0")); err != nil {
		return err
	}

	if err := s.vagrant.IterateNodes(startVolplugin); err != nil {
		return err
	}

	if out, err := s.uploadIntent("policy1", "policy1"); err != nil {
		log.Errorf("Intent upload failed. Error: %v, Output: %s", err, out)
		return err
	}

	return nil
}

func getDriver() string {
	driver := "ceph"
	if strings.TrimSpace(os.Getenv("USE_DRIVER")) != "" {
		driver = strings.TrimSpace(os.Getenv("USE_DRIVER"))
	}
	return driver
}

func cephDriver() bool {
	return getDriver() == "ceph"
}

func nullDriver() bool {
	return getDriver() == "null"
}

func nfsDriver() bool {
	return getDriver() == "nfs"
}

func (s *systemtestSuite) createExports() error {
	out, err := s.mon0cmd("sudo mkdir -p /volplugin")
	if err != nil {
		log.Error(out)
		return errored.Errorf("Creating volplugin root").Combine(err)
	}

	out, err = s.mon0cmd("echo /volplugin \\*\\(rw,no_root_squash\\) | sudo tee /etc/exports.d/basic.exports")
	if err != nil {
		log.Error(out)
		return errored.Errorf("Creating export").Combine(err)
	}

	out, err = s.mon0cmd("sudo exportfs -a")
	if err != nil {
		log.Error(out)
		return errored.Errorf("exportfs").Combine(err)
	}

	return nil
}

func (s *systemtestSuite) uploadIntent(policyName, fileName string) (string, error) {
	log.Infof("Uploading intent %q as policy %q", fileName, policyName)
	return s.volcli(fmt.Sprintf("policy upload %s < /testdata/%s/%s.json", policyName, getDriver(), fileName))
}

func runCommandUntilNoError(node vagrantssh.TestbedNode, cmd string, timeout int) error {
	runCmd := func() (string, bool) {
		if err := node.RunCommand(cmd); err != nil {
			return "", false
		}
		return "", true
	}
	timeoutMessage := fmt.Sprintf("timeout reached trying to run %v on %q", cmd, node.GetName())
	_, err := utils.WaitForDone(runCmd, 10*time.Millisecond, 10*time.Second, timeoutMessage)
	return err
}

func (s *systemtestSuite) pullImage() error {
	log.Infof("Pulling alpine:latest on all boxes")
	return s.vagrant.SSHExecAllNodes("docker pull alpine")
}

func restartNetplugin(node vagrantssh.TestbedNode) error {
	log.Infof("Restarting netplugin on %q", node.GetName())
	err := node.RunCommand("sudo systemctl restart netplugin netmaster")
	if err != nil {
		return err
	}
	time.Sleep(5 * time.Second)
	return nil
}

func waitForApiserver(node vagrantssh.TestbedNode) error {
	var err error

	for i := 0; i < 600; i++ { // 10 seconds
		err = node.RunCommand("docker ps | grep -q apiserver")
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return err
}

func startVolsupervisor(node vagrantssh.TestbedNode) error {
	log.Infof("Starting the volsupervisor on %q", node.GetName())
	return node.RunCommand("docker start volsupervisor")
}

func stopVolsupervisor(node vagrantssh.TestbedNode) error {
	log.Infof("Stopping the volsupervisor on %q", node.GetName())
	return node.RunCommand("docker stop volsupervisor")
}

func startApiserver(node vagrantssh.TestbedNode) error {
	log.Infof("Starting the apiserver on %q", node.GetName())
	return node.RunCommand("docker start apiserver")
}

func stopApiserver(node vagrantssh.TestbedNode) error {
	log.Infof("Stopping the apiserver on %q", node.GetName())
	return node.RunCommand("docker stop apiserver")
}

func startVolplugin(node vagrantssh.TestbedNode) error {
	log.Infof("Starting the volplugin on %q", node.GetName())
	return node.RunCommand("docker start volplugin")
}

func stopVolplugin(node vagrantssh.TestbedNode) error {
	log.Infof("Stopping the volplugin on %q", node.GetName())
	return node.RunCommand("docker stop volplugin")
}

func waitDockerizedServicesHost(node vagrantssh.TestbedNode) error {
	services := map[string]string{
		"etcd": "etcdctl cluster-health",
	}

	for s, cmd := range services {
		log.Infof("Waiting for %s on %q", s, node.GetName())
		out, err := utils.WaitForDone(
			func() (string, bool) {
				out, err := node.RunCommandWithOutput(cmd)
				if err != nil {
					return out, false
				}
				return out, true
			}, 2*time.Second, time.Minute, fmt.Sprintf("service %s is not healthy", s))
		if err != nil {
			log.Infof("a dockerized service failed. Output: %s, Error: %v", out, err)
			return err
		}
	}
	return nil
}

func (s *systemtestSuite) waitDockerizedServices() error {
	return s.vagrant.IterateNodes(waitDockerizedServicesHost)
}

func restartDockerHost(node vagrantssh.TestbedNode) error {
	log.Infof("Restarting docker on %q", node.GetName())
	// note that for all these restart tasks we error out quietly to avoid other
	// hosts being cleaned up
	//node.RunCommand("sudo service docker restart")
	return nil
}

func (s *systemtestSuite) restartDocker() error {
	return s.vagrant.IterateNodes(restartDockerHost)
}

func (s *systemtestSuite) restartNetplugin() error {
	return s.vagrant.IterateNodes(restartNetplugin)
}

func (s *systemtestSuite) clearContainerHost(node vagrantssh.TestbedNode) error {
	startedContainers.Lock()
	names := []string{}
	for name := range startedContainers.names {
		names = append(names, name)
	}
	startedContainers.Unlock()
	log.Infof("Clearing containers %v on %q", names, node.GetName())
	node.RunCommand(fmt.Sprintf("docker rm -f %s", strings.Join(names, " ")))
	return nil
}

func (s *systemtestSuite) clearContainers() error {
	log.Infof("Clearing containers")
	defer func() {
		startedContainers.Lock()
		startedContainers.names = map[string]struct{}{}
		startedContainers.Unlock()
	}()
	return s.vagrant.IterateNodes(s.clearContainerHost)
}

func (s *systemtestSuite) clearRBD() error {
	if !cephDriver() {
		return nil
	}

	log.Info("Clearing rbd images")

	err := s.vagrant.IterateNodes(func(node vagrantssh.TestbedNode) error {
		out, err := s.rbd("showmapped | tail -n +2 | awk \"{ print \\$5 }\"")
		if err != nil {
			log.Error(out, err)
			return err
		}

		lines := strings.Split(strings.TrimSpace(out), "\n")
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			out, err := s.mon0cmd(fmt.Sprintf("sudo umount %s; sudo umount -f %s", line, line))
			if err != nil {
				log.Error(out, err)
				return err
			}
			out, err = s.rbd(fmt.Sprintf("unmap %s", line))
			if err != nil {
				log.Error(out, err)
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	out, err := s.rbd("ls")
	if err != nil {
		return err
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	for _, line := range lines {
		s.rbd(fmt.Sprintf("snap purge %s", line))
		s.rbd(fmt.Sprintf("rm %s", line))
	}

	return err
}
