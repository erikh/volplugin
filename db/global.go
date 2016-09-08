package db

import "encoding/json"

// NewGlobal constructs a new global object.
func NewGlobal() *Global {
	g := &Global{}
	g.prep()
	return g
}

// NewGlobalFromJSON constructs a global from a JSON payload.
func NewGlobalFromJSON(content []byte) (*Global, error) {
	global := NewGlobal()
	err := json.Unmarshal(content, global)
	return global, err
}

// SetKey is not implmented here because it is not needed
func (g *Global) SetKey(key string) error {
	return nil
}

// Path returns the path to the global configuration
func (g *Global) Path() (string, error) {
	return rootGlobal, nil
}

// Prefix returns the path to the global configuration
func (g *Global) Prefix() string {
	return ""
}

func (g *Global) prep() {
	if g.MountPath == "" {
		g.MountPath = DefaultMountPath
	}

	if g.TTL < TTLFixBase {
		g.TTL = DefaultGlobalTTL
	}

	if g.Timeout < TimeoutFixBase {
		g.Timeout = DefaultTimeout
	}
}

// Validate validates the global configuration.
func (g *Global) Validate() error {
	g.prep()
	return nil
}

func (g *Global) String() string {
	return rootGlobal
}

// Hooks returns an empty hooks set.
func (g *Global) Hooks() *Hooks {
	return &Hooks{}
}

// Copy returns a copy of this Global.
func (g *Global) Copy() Entity {
	g2 := *g
	return &g2
}

// Published returns a copy of the current global with the parameters adjusted
// to fit the published representation. To see the internal/system/canonical
// version, please see Canonical() below.
//
// It is very important that you do not run this function multiple times
// against the same data set. It will adjust the parameters twice.
func (g *Global) Published() *Global {
	newGlobal := *g

	newGlobal.TTL /= TTLFixBase
	newGlobal.Timeout /= TimeoutFixBase

	return &newGlobal
}

// Canonical returns a copy of the current global with the parameters adjusted
// to fit the internal (or canonical) representation. To see the published
// version, see Published() above.
//
// It is very important that you do not run this function multiple times
// against the same data set. It will adjust the parameters twice.
func (g *Global) Canonical() *Global {
	newGlobal := *g

	if g.TTL < TTLFixBase {
		newGlobal.TTL *= TTLFixBase
	}

	if g.Timeout < TimeoutFixBase {
		newGlobal.Timeout *= TimeoutFixBase
	}

	return &newGlobal
}
