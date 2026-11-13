//go:build !linux

package opens

// diskSpace is not implemented off Linux, where the scanner is not
// deployed; the summary then says the free space is unknown.
func diskSpace(string) (free, total uint64, ok bool) { return 0, 0, false }
