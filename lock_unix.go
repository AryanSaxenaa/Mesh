//go:build unix

package mesh0

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// platformAcquireLock uses an advisory OS lock. Unlike an O_EXCL sentinel, it
// is released by the kernel on process death and therefore cannot block crash
// recovery merely because the diagnostic LOCK file remains on disk.
func platformAcquireLock(path string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.WriteString(fmt.Sprintf("mesh0 lock pid=%d\n", os.Getpid())); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return func() error {
		unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		closeErr := file.Close()
		if unlockErr != nil {
			return unlockErr
		}
		return closeErr
	}, nil
}
