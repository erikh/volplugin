package db

import (
	"time"

	"github.com/Sirupsen/logrus"
)

// ExecuteWithMultiUseLock acquires several locks and runs the function, then
// frees the locks. This is typically used to take both a volume and snapshot
// for CRUD operations.
func ExecuteWithMultiUseLock(c Client, fun func(locks []Lock) error, timeout time.Duration, locks ...Lock) error {
	acquired := []Lock{}

	defer func() {
		for _, lock := range acquired {
			if err := c.Free(lock, false); err != nil {
				// unlike below, do not return on errors; try to free the rest of the locks.
				logrus.Errorf("Could not free lock: %v", err)
			}
		}
	}()

	for _, lock := range locks {
		before := time.Now()
	retry:
		if err := c.Acquire(lock); err != nil {
			if time.Since(before) < timeout {
				// this will likely happen a lot; so this should be a debug message.
				logrus.Debugf("Could not acquire lock, retrying: %v", err)
				time.Sleep(500 * time.Millisecond)
				goto retry
			}

			logrus.Debugf("Could not acquire lock: %v", err)

			return err
		}

		acquired = append(acquired, lock)
	}

	return fun(locks)
}
