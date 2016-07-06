package systemtests

import (
	"os"
	"strings"
	. "testing"

	. "gopkg.in/check.v1"

	log "github.com/Sirupsen/logrus"
	"github.com/contiv/vagrantssh"
)

type systemtestSuite struct {
	vagrant vagrantssh.Vagrant
	mon0ip  string
}

var _ = Suite(&systemtestSuite{})

func TestSystem(t *T) {
	if os.Getenv("HOST_TEST") != "" {
		os.Exit(0)
	}

	if os.Getenv("DEBUG_TEST") != "" {
		log.SetLevel(log.DebugLevel)
		log.Debug("Debug logging enabled")
	}

	TestingT(t)
}

func (s *systemtestSuite) SetUpTest(c *C) {
	c.Assert(s.rebootstrap(), IsNil)
}

func (s *systemtestSuite) SetUpSuite(c *C) {
	log.Infof("Bootstrapping system tests")
	s.vagrant = vagrantssh.Vagrant{}
	c.Assert(s.vagrant.Setup(false, "", 3), IsNil)

	if nfsDriver() {
		log.Info("NFS Driver detected: configuring exports.")
		c.Assert(s.createExports(), IsNil)
		ip, err := s.mon0cmd(`ip addr show dev enp0s8 | grep inet | head -1 | awk "{ print \$2 }" | awk -F/ "{ print \$1 }"`)
		log.Infof("mon0's ip is %s", strings.TrimSpace(ip))
		c.Assert(err, IsNil)
		s.mon0ip = strings.TrimSpace(ip)
	}

	c.Assert(s.clearContainers(), IsNil)
	c.Assert(s.restartDocker(), IsNil)
	c.Assert(s.waitDockerizedServices(), IsNil)
	c.Assert(s.pullImage(), IsNil)
	c.Assert(s.rebootstrap(), IsNil)

	out, err := s.uploadIntent("policy1", "policy1")
	c.Assert(err, IsNil, Commentf("output: %s", out))
}

func (s *systemtestSuite) TearDownSuite(c *C) {
	if cephDriver() && os.Getenv("NO_TEARDOWN") == "" {
		s.clearRBD()
	}
}
