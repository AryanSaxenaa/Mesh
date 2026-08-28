//go:build windows

package mesh0

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	movefileReplaceExisting = 0x00000001
	movefileWriteThrough    = 0x00000008
)

var moveFileExProc = kernel32.NewProc("MoveFileExW")

// atomicReplace atomically replaces destination with source. os.Rename cannot
// replace an existing destination on Windows, while MoveFileEx can.
func atomicReplace(source, destination string) error {
	sourceUTF16, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	destinationUTF16, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	result, _, callErr := moveFileExProc.Call(
		uintptr(unsafe.Pointer(sourceUTF16)),
		uintptr(unsafe.Pointer(destinationUTF16)),
		movefileReplaceExisting|movefileWriteThrough,
	)
	if result != 0 {
		return nil
	}
	if callErr != nil && callErr != syscall.Errno(0) {
		return callErr
	}
	return fmt.Errorf("MoveFileExW failed")
}
