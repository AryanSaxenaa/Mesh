//go:build windows

package mesh0

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileFailImmediately = 0x00000001
	lockfileExclusiveLock   = 0x00000002
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	lockFileExProc   = kernel32.NewProc("LockFileEx")
	unlockFileExProc = kernel32.NewProc("UnlockFileEx")
)

// platformAcquireLock uses a kernel-held byte-range lock rather than treating
// file existence as ownership. The LOCK file may survive a crash; the kernel
// lock does not, so recovery remains possible after an unclean termination.
func platformAcquireLock(path string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	var overlapped syscall.Overlapped
	result, _, callErr := lockFileExProc.Call(
		file.Fd(),
		lockfileExclusiveLock|lockfileFailImmediately,
		0,
		0xffffffff,
		0xffffffff,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if result == 0 {
		_ = file.Close()
		if callErr != nil && callErr != syscall.Errno(0) {
			return nil, fmt.Errorf("%w: %v", ErrLocked, callErr)
		}
		return nil, ErrLocked
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
		_, _, unlockErr := unlockFileExProc.Call(
			file.Fd(),
			0,
			0xffffffff,
			0xffffffff,
			uintptr(unsafe.Pointer(&overlapped)),
		)
		closeErr := file.Close()
		if unlockErr != nil && unlockErr != syscall.Errno(0) {
			return unlockErr
		}
		return closeErr
	}, nil
}
