package zfs

import (
	"io"
)

var z ZFS = &zfs{exec: NewLocalExecutor(), logger: &defaultLogger{}}

func SetDefault(zfs ZFS) {
	if zfs != nil {
		z = zfs
	}
}

// SetLogger set a log handler to log all commands including arguments before they are executed.
func SetLogger(l Logger) {
	if z, ok := z.(*zfs); ok && l != nil {
		z.logger = l
	}
}

func Datasets(filter string) ([]*Dataset, error) {
	return z.Datasets(filter)
}
func Snapshots(filter string) ([]*Dataset, error) {
	return z.Snapshots(filter)
}
func Filesystems(filter string) ([]*Dataset, error) {
	return z.Filesystems(filter)
}
func Volumes(filter string) ([]*Dataset, error) {
	return z.Volumes(filter)
}
func GetDataset(name string) (*Dataset, error) {
	return z.GetDataset(name)
}
func ReceiveSnapshot(input io.Reader, name string) (*Dataset, error) {
	return z.ReceiveSnapshot(input, name)
}
func CreateVolume(name string, size uint64, properties map[string]string) (*Dataset, error) {
	return z.CreateVolume(name, size, properties)
}
func CreateFilesystem(name string, properties map[string]string) (*Dataset, error) {
	return z.CreateFilesystem(name, properties)
}
func ListZpools() ([]*Zpool, error) {
	return z.ListZpools()
}
func GetZpool(name string) (*Zpool, error) {
	return z.GetZpool(name)
}
func CreateZpool(name string, properties map[string]string, args ...string) (*Zpool, error) {
	return z.CreateZpool(name, properties, args...)
}
