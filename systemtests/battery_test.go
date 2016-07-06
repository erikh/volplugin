package systemtests

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"

	. "gopkg.in/check.v1"

	log "github.com/Sirupsen/logrus"
	"github.com/contiv/vagrantssh"
)

func (s *systemtestSuite) TestBatteryMultiMountSameHost(c *C) {
	outerCount := 5
	count := 15

	mutex := &sync.Mutex{}
	volumeNames := []string{}

	for i := 0; i < outerCount; i++ {
		syncChan := make(chan struct{}, count)

		for x := 0; x < count; x++ {
			go func(x int) {
				defer func() { syncChan <- struct{}{} }()
				volName := genRandomString("test", "", 20)
				fqVolName := "policy1/" + volName
				mutex.Lock()
				volumeNames = append(volumeNames, volName)
				mutex.Unlock()

				c.Assert(s.createVolume("mon0", "policy1", volName, nil), IsNil)
				out, err := s.dockerRun("mon0", false, true, fqVolName, "sleep 10m")
				c.Assert(err, IsNil, Commentf("Output: %s", out))
				second, err := s.dockerRun("mon0", false, true, fqVolName, "sleep 10m")
				log.Debug(second, err)
				c.Assert(err, IsNil, Commentf("Output: %s", second))

				if cephDriver() {
					_, err = s.mon0cmd(fmt.Sprintf("mount | grep rbd | grep -q %s", "policy1."+volName))
					c.Assert(err, IsNil)
					out2, err := s.mon0cmd(fmt.Sprintf("docker exec %s ls /mnt", strings.TrimSpace(out)))
					c.Assert(err, IsNil)
					c.Assert(strings.TrimSpace(out2), Equals, "lost+found")
				}

				out3, err := s.mon0cmd(fmt.Sprintf("docker rm -f %s", strings.TrimSpace(out)))
				if err != nil {
					log.Error(strings.TrimSpace(out3))
				}
				c.Assert(err, IsNil)
			}(x)
		}

		for x := 0; x < count; x++ {
			<-syncChan
		}

		if cephDriver() {
			_, err := s.mon0cmd("mount | grep -q rbd")
			c.Assert(err, IsNil)
		}

		// FIXME netplugin is broken
		c.Assert(s.restartNetplugin(), IsNil)
		c.Assert(s.clearContainers(), IsNil)
		c.Assert(s.restartNetplugin(), IsNil)

		purgeChan := make(chan error, count)
		for _, name := range volumeNames {
			go func(name string) { purgeChan <- s.purgeVolume("mon0", "policy1", name, true) }(name)
		}

		var errs int

		for x := 0; x < count; x++ {
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
	mutex := &sync.Mutex{}
	volumeNames := []string{}

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

		for x := 0; x < count; x++ {
			go func(nodes []vagrantssh.TestbedNode, x int) {
				volName := genRandomString("test", "", 20)
				fqVolName := "policy1/" + volName
				mutex.Lock()
				volumeNames = append(volumeNames, volName)
				mutex.Unlock()

				for _, node := range nodes {
					c.Assert(s.createVolume(node.GetName(), "policy1", volName, nil), IsNil)
				}

				for _, node := range nodes {
					go func(node vagrantssh.TestbedNode, x int) {
						out, err := s.dockerRun(node.GetName(), false, true, fqVolName, "sleep 10m")
						outputChan <- output{out, err, fqVolName}
					}(node, x)
				}
			}(nodes, x)
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
		for _, name := range volumeNames {
			go func(name string) { purgeChan <- s.purgeVolume("mon0", "policy1", name, true) }(name)
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
	mutex := &sync.Mutex{}
	volumeNames := []string{}

	for outer := 0; outer < outcount; outer++ {
		for x := 0; x < count; x++ {
			outwg.Add(1)
			go func(nodes []vagrantssh.TestbedNode, x int) {
				defer outwg.Done()
				wg := sync.WaitGroup{}
				errChan := make(chan error, len(nodes))

				for i := range rand.Perm(len(nodes)) {
					wg.Add(1)
					go func(i, x int) {
						defer wg.Done()
						volName := genRandomString("test", "", 20)
						fqVolName := "policy1/" + volName
						mutex.Lock()
						volumeNames = append(volumeNames, volName)
						mutex.Unlock()

						node := nodes[i]
						log.Infof("Creating image %q on %q", fqVolName, node.GetName())

						var opt string

						if nfsDriver() {
							opt = fmt.Sprintf("--opt mount=%s:%q", s.mon0ip, fqVolName)
						}

						_, err := node.RunCommandWithOutput(fmt.Sprintf("volcli volume create %s %s", fqVolName, opt))
						errChan <- err
					}(i, x)
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
			}(nodes, x)
		}

		outwg.Wait()

		errChan := make(chan error, count)
		for _, name := range volumeNames {
			go func(name string) { errChan <- s.purgeVolume("mon0", "policy1", name, true) }(name)
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
