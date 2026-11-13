//go:build linux

package opens

import "syscall"

// diskSpace is the free (available to an unprivileged user) and total
// bytes of the filesystem holding path.
func diskSpace(path string) (free, total uint64, ok bool) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return 0, 0, false
	}
	return s.Bavail * uint64(s.Bsize), s.Blocks * uint64(s.Bsize), true
}
