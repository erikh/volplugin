package systemtests

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	. "gopkg.in/check.v1"

	log "github.com/Sirupsen/logrus"
	"github.com/contiv/vagrantssh"
)

func (s *systemtestSuite) TestBatteryMultiMountSameHost(c *C) {
	s.BatteryMultiMountSameHost(c, "true")  //unlocked mount
	s.BatteryMultiMountSameHost(c, "false") //locked mount
}
func (s *systemtestSuite) BatteryMultiMountSameHost(c *C, isUnlocked string) {
	totalIterations := 5
	threadCount := 15

	for i := 0; i < totalIterations; i++ {
		syncChan := make(chan struct{}, threadCount)
		volumes := genRandomVolumes(count)

		for workerID := 0; workerID < threadCount; workerID++ {
			go func(volume string, workerID int) {
				defer func() { syncChan <- struct{}{} }()
				fqVolName := fqVolume(volume)
				c.Assert(s.createVolume("mon0", fqVolName, map[string]string{"unlocked": isUnlocked}), IsNil)
				containerID, err := s.dockerRun("mon0", false, true, fqVolName, "sleep 10m")
				c.Assert(err, IsNil, Commentf("Output: %s", containerID))
				containerID2, err := s.dockerRun("mon0", false, true, fqVolName, "sleep 10m")
				if unlocked, _ := strconv.ParseBool(isUnlocked); unlocked {
					log.Debug(containerID2, err)
					c.Assert(err, IsNil, Commentf("Output: %s", containerID2))
				} else { // locked volumes
					log.Debugf("Volume %s already mounted in container %s", volName, containerID)
					c.Assert(err, NotNil)
				}

				if cephDriver() {
					_, err = s.mon0cmd(fmt.Sprintf("mount | grep rbd | grep -q %s", strings.Join([]strings{"policy1", volName}, ".")))
					c.Assert(err, IsNil)
					mountedDirContent, err := s.mon0cmd(fmt.Sprintf("docker exec %s ls /mnt", strings.TrimSpace(containerID)))
					c.Assert(err, IsNil)
					c.Assert(strings.TrimSpace(mountedDirContent), Equals, "lost+found")
				}

				dockerRmOut, err := s.mon0cmd(fmt.Sprintf("docker rm -f %s", strings.TrimSpace(containerID)))
				if err != nil {
					log.Error(strings.TrimSpace(dockerRmOut))
				}
				c.Assert(err, IsNil)
			}(volumes[workerID], workerID)
		}

		for x := 0; x < threadCount; x++ {
			<-syncChan
		}

		if cephDriver() {
			out, _ := s.mon0cmd("mount | grep -q rbd")
			c.Assert(out, Equals, "")
		}

		c.Assert(s.clearContainers(), IsNil)

		purgeChan := make(chan error, count)
		for _, volume := range volumes {
			go func(volume string) { purgeChan <- s.purgeVolume("mon0", fqVolume("policy1", volume)) }(volume)
		}

		var errs int

		for x := 0; x < threadCount; x++ {
			err := <-purgeChan
			if err != nil {
				log.Error(err)
				errs++
			}
		}
		c.Assert(errs, Equals, 0)
	}
}

func (s *systemtestSuite) TestBatteryParallelMount(c *C) {
	// unlocked will be set at the end of the routine. We repeat this test for
	// the NFS driver in unlocked mode to ensure it is not taking locks.
	var unlocked bool

repeat:
	if unlocked {
		log.Info("NFS unlocked test proceeding")
		out, err := s.uploadIntent("policy1", "unlocked")
		c.Assert(err, IsNil, Commentf(out))
	}

	type output struct {
		out    string
		err    error
		volume string
	}

	nodes := s.vagrant.GetNodes()
	outerCount := 5
	count := 15

	for outer := 0; outer < outerCount; outer++ {
		c.Assert(s.uploadGlobal("global1"), IsNil)
		if unlocked {
			out, err := s.uploadIntent("policy1", "unlocked")
			c.Assert(err, IsNil, Commentf(out))
		} else {
			out, err := s.uploadIntent("policy1", "policy1")
			c.Assert(err, IsNil, Commentf(out))
		}

		outputChan := make(chan output, len(nodes)*count)
		volumes := genRandomVolumes(count)

		for _, volume := range volumes {
			go func(nodes []vagrantssh.TestbedNode, volName string) {
				fqVolName := fqVolume("policy1", volName)
				for _, node := range nodes {
					c.Assert(s.createVolume(node.GetName(), fqVolName, nil), IsNil)
				}

				for _, node := range nodes {
					go func(node vagrantssh.TestbedNode, fqVolName string) {
						out, err := s.dockerRun(node.GetName(), false, true, fqVolName, "sleep 10m")
						outputChan <- output{out, err, volName}
					}(node, fqVolName)
				}
			}(nodes, volume)
		}

		var errs int

		for i := 0; i < len(nodes)*count; i++ {
			output := <-outputChan
			if output.err != nil {
				log.Debug(output.out)
				errs++
			}

			//log.Infof("%q: %s", output.volume, output.out)
		}

		errCount := count * (len(nodes) - 1)
		if unlocked {
			// if we have no locking to stop us, we will have no errors.
			errCount = 0
		}

		c.Assert(errs, Equals, errCount)
		c.Assert(s.clearContainers(), IsNil)

		purgeChan := make(chan error, count)
		for _, volume := range volumes {
			go func(volume string) { purgeChan <- s.purgeVolume("mon0", fqVolume("policy1", volume)) }(volume)
		}

		errs = 0

		for x := 0; x < count; x++ {
			err := <-purgeChan
			if err != nil {
				log.Error(err)
				errs++
			}
		}

		c.Assert(errs, Equals, 0)
	}

	if nfsDriver() && !unlocked {
		unlocked = true
		goto repeat
	}
}

func (s *systemtestSuite) TestBatteryParallelCreate(c *C) {
	nodes := s.vagrant.GetNodes()
	count := 15
	outcount := 5
	outwg := sync.WaitGroup{}

	for outer := 0; outer < outcount; outer++ {
		volumes := genRandomVolumes(count)
		for _, volume := range volumes {
			outwg.Add(1)
			go func(nodes []vagrantssh.TestbedNode, volume string) {
				defer outwg.Done()
				wg := sync.WaitGroup{}
				errChan := make(chan error, len(nodes))

				for i := range rand.Perm(len(nodes)) {
					wg.Add(1)
					go func(i int, volume string) {
						defer wg.Done()
						fqVolName := fqVolume("policy1", volume)
						node := nodes[i]
						log.Infof("Creating image %s on %q", fqVolName, node.GetName())

						var opt string

						if nfsDriver() {
							opt = fmt.Sprintf("--opt mount=%s:%s", s.mon0ip, fqVolName)
						}

						_, err := node.RunCommandWithOutput(fmt.Sprintf("volcli volume create %s %s", fqVolName, opt))
						errChan <- err
					}(i, volume)
				}

				var errs int

				wg.Wait()

				for i := 0; i < len(nodes); i++ {
					err := <-errChan
					if err != nil {
						errs++
					}
				}

				if nfsDriver() {
					c.Assert(errs, Equals, 0)
				} else {
					c.Assert(errs, Equals, 2)
				}
			}(nodes, volume)
		}

		outwg.Wait()

		errChan := make(chan error, count)
		for _, volume := range volumes {
			go func(volume string) { errChan <- s.purgeVolume("mon0", fqVolume("policy1", volume)) }(volume)
		}

		var realErr error

		for x := 0; x < count; x++ {
			err := <-errChan
			if err != nil {
				realErr = err
			}
		}

		c.Assert(realErr, IsNil)

		if cephDriver() {
			out, err := s.mon0cmd("sudo rbd ls")
			c.Assert(err, IsNil)
			c.Assert(out, Equals, "")
		}
	}
}
