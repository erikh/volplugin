package helpers

import (
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/db/jsonio"
	"github.com/contiv/volplugin/errors"
)

// WatchInfo encapsulates arguments to watch functions for WrapWatch.
type WatchInfo struct {
	Path       string
	Object     db.Entity
	StopChan   chan struct{}
	ReturnChan chan db.Entity
	ErrorChan  chan error
	Recursive  bool
}

// WrapSet wraps set calls in validations, hooks etc. It is intended to be used
// by database implementations of the db.Client interface.
func WrapSet(c db.Client, obj db.Entity, fun func(string, []byte) error) error {
	if err := obj.Validate(); err != nil {
		return err
	}

	if obj.Hooks().PreSet != nil {
		if err := obj.Hooks().PreSet(c, obj); err != nil {
			return err
		}
	}

	content, err := jsonio.Write(obj)
	if err != nil {
		return err
	}

	path, err := obj.Path()
	if err != nil {
		return err
	}

	if err := fun(path, content); err != nil {
		return err
	}

	if obj.Hooks().PostSet != nil {
		if err := obj.Hooks().PostSet(c, obj); err != nil {
			return err
		}
	}

	return nil
}

// WrapGet wraps get calls in a similar fashion to WrapSet. The return of the
// passed function must return a string key + []byte value respectively.
func WrapGet(c db.Client, obj db.Entity, fun func(string) (string, []byte, error)) error {
	if obj.Hooks().PreGet != nil {
		if err := obj.Hooks().PreGet(c, obj); err != nil {
			return errors.EtcdToErrored(err)
		}
	}

	path, err := obj.Path()
	if err != nil {
		return err
	}

	key, value, err := fun(path)
	if err != nil {
		return err
	}

	if err := jsonio.Read(obj, []byte(value)); err != nil {
		return err
	}

	if err := obj.SetKey(TrimPath(c, key)); err != nil {
		return err
	}

	if obj.Hooks().PostGet != nil {
		if err := obj.Hooks().PostGet(c, obj); err != nil {
			return err
		}
	}
	stopChan, ok := watchers[path]
	if !ok {
		return errors.InvalidDBPath.Combine(errored.Errorf("missing key %v during watch", path))
	}

	close(stopChan)
	delete(watchers, path)

	return nil
}

// ReadAndSet reads a json value into a copy of obj, sets the keys and runs any
// hooks. Intended to be used post-watch receive.
func ReadAndSet(c db.Client, obj db.Entity, key string, value []byte) (db.Entity, error) {
	copy := obj.Copy()

	if err := jsonio.Read(copy, value); err != nil {
		// This is kept this way so a buggy policy won't break listing all of them
		return nil, errored.Errorf("Received error retrieving value at path %q: %v", key, err)
	}

	if err := copy.SetKey(TrimPath(c, key)); err != nil {
		return nil, err
	}

	// same here. fire hooks to retrieve the full entity. only log but don't append on error.
	if copy.Hooks().PostGet != nil {
		if err := copy.Hooks().PostGet(c, copy); err != nil {
			return nil, errored.Errorf("Error received trying to run fetch hooks during %q list: %v", key, err)
		}
	}

	return copy, nil
}
