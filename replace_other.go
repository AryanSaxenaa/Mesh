//go:build !windows

package mesh0

import "os"

func atomicReplace(source, destination string) error {
	return os.Rename(source, destination)
}
