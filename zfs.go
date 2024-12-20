// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ZFS dataset types, which can indicate if a dataset is a filesystem, snapshot, or volume.
const (
	DatasetFilesystem = "filesystem"
	DatasetSnapshot   = "snapshot"
	DatasetVolume     = "volume"
)

// Dataset is a ZFS dataset.  A dataset could be a clone, filesystem, snapshot, or volume.
// The Type struct member can be used to determine a dataset's type.
//
// The field definitions can be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
type Dataset struct {
	z             *zfs
	Name          string
	Origin        string
	Used          uint64
	Avail         uint64
	Mountpoint    string
	Compression   string
	Type          string
	Written       uint64
	Volsize       uint64
	Logicalused   uint64
	Usedbydataset uint64
	Quota         uint64
	Referenced    uint64
}

// InodeType is the type of inode as reported by Diff.
type InodeType int

// Types of Inodes.
const (
	_                     = iota // 0 == unknown type
	BlockDevice InodeType = iota
	CharacterDevice
	Directory
	Door
	NamedPipe
	SymbolicLink
	EventPort
	Socket
	File
)

// ChangeType is the type of inode change as reported by Diff.
type ChangeType int

// Types of Changes.
const (
	_                  = iota // 0 == unknown type
	Removed ChangeType = iota
	Created
	Modified
	Renamed
)

// DestroyFlag is the options flag passed to Destroy.
type DestroyFlag int

// Valid destroy options.
const (
	DestroyDefault         DestroyFlag = 1 << iota
	DestroyRecursive                   = 1 << iota
	DestroyRecursiveClones             = 1 << iota
	DestroyDeferDeletion               = 1 << iota
	DestroyForceUmount                 = 1 << iota
)

// InodeChange represents a change as reported by Diff.
type InodeChange struct {
	Change               ChangeType
	Type                 InodeType
	Path                 string
	NewPath              string
	ReferenceCountChange int
}

// Logger can be used to log commands/actions.
type Logger interface {
	Log(cmd []string)
}

type defaultLogger struct{}

func (*defaultLogger) Log([]string) {}

type ZFS interface {
	Datasets(filter string) ([]*Dataset, error)
	Snapshots(filter string) ([]*Dataset, error)
	Filesystems(filter string) ([]*Dataset, error)
	Volumes(filter string) ([]*Dataset, error)
	GetDataset(name string) (*Dataset, error)
	ReceiveSnapshot(input io.Reader, name string) (*Dataset, error)
	CreateVolume(name string, size uint64, properties map[string]string) (*Dataset, error)
	CreateFilesystem(name string, properties map[string]string) (*Dataset, error)
	ListZpools() ([]*Zpool, error)
	GetZpool(name string) (*Zpool, error)
	CreateZpool(name string, properties map[string]string, args ...string) (*Zpool, error)
}

func New(opts ...Option) (ZFS, error) {
	var z zfs
	for _, opt := range opts {
		opt(&z)
	}
	if z.exec == nil {
		z.exec = NewLocalExecutor()
	}
	if z.logger == nil {
		z.logger = &defaultLogger{}
	}
	return &z, nil
}

type zfs struct {
	exec   Executor
	sudo   bool
	logger Logger
}

// do is a helper function to wrap typical calls to zfs that ignores stdout.
func (z *zfs) do(arg ...string) error {
	_, err := z.doOutput(arg...)
	return err
}

// doOutput is a helper function to wrap typical calls to zfs.
func (z *zfs) doOutput(arg ...string) ([][]string, error) {
	return z.run(nil, nil, "zfs", arg...)
}

// Datasets returns a slice of ZFS datasets, regardless of type.
// A filter argument may be passed to select a dataset with the matching name, or empty string ("") may be used to select all datasets.
func (z *zfs) Datasets(filter string) ([]*Dataset, error) {
	return z.listByType("all", filter)
}

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name, or empty string ("") may be used to select all snapshots.
func (z *zfs) Snapshots(filter string) ([]*Dataset, error) {
	return z.listByType(DatasetSnapshot, filter)
}

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name, or empty string ("") may be used to select all filesystems.
func (z *zfs) Filesystems(filter string) ([]*Dataset, error) {
	return z.listByType(DatasetFilesystem, filter)
}

// Volumes returns a slice of ZFS volumes.
// A filter argument may be passed to select a volume with the matching name, or empty string ("") may be used to select all volumes.
func (z *zfs) Volumes(filter string) ([]*Dataset, error) {
	return z.listByType(DatasetVolume, filter)
}

// GetDataset retrieves a single ZFS dataset by name.
// This dataset could be any valid ZFS dataset type, such as a clone, filesystem, snapshot, or volume.
func (z *zfs) GetDataset(name string) (*Dataset, error) {
	out, err := z.doOutput("list", "-Hp", "-o", dsPropListOptions, name)
	if err != nil {
		return nil, err
	}

	ds := &Dataset{z: z, Name: name}
	for _, line := range out {
		if err := ds.parseLine(line); err != nil {
			return nil, err
		}
	}

	return ds, nil
}

// Clone clones a ZFS snapshot and returns a clone dataset.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Clone(dest string, properties map[string]string) (*Dataset, error) {
	if d.Type != DatasetSnapshot {
		return nil, errors.New("can only clone snapshots")
	}
	args := make([]string, 2, 4)
	args[0] = "clone"
	args[1] = "-p"
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, []string{d.Name, dest}...)
	if err := d.z.do(args...); err != nil {
		return nil, err
	}
	return d.z.GetDataset(dest)
}

// Unmount unmounts currently mounted ZFS file systems.
func (d *Dataset) Unmount(force bool) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot unmount snapshots")
	}
	args := make([]string, 1, 3)
	args[0] = "umount"
	if force {
		args = append(args, "-f")
	}
	args = append(args, d.Name)
	if err := d.z.do(args...); err != nil {
		return nil, err
	}
	return d.z.GetDataset(d.Name)
}

// Mount mounts ZFS file systems.
func (d *Dataset) Mount(overlay bool, options []string) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot mount snapshots")
	}
	args := make([]string, 1, 5)
	args[0] = "mount"
	if overlay {
		args = append(args, "-O")
	}
	if options != nil {
		args = append(args, "-o")
		args = append(args, strings.Join(options, ","))
	}
	args = append(args, d.Name)
	if err := d.z.do(args...); err != nil {
		return nil, err
	}
	return d.z.GetDataset(d.Name)
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader.
// A new snapshot is created with the specified name, and streams the input data into the newly-created snapshot.
func (z *zfs) ReceiveSnapshot(input io.Reader, name string) (*Dataset, error) {
	if _, err := z.run(input, nil, "zfs", "receive", name); err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// SendSnapshot sends a ZFS stream of a snapshot to the input io.Writer.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) SendSnapshot(output io.Writer) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only send snapshots")
	}
	_, err := d.z.run(nil, output, "zfs", "send", d.Name)
	return err
}

// IncrementalSend sends a ZFS stream of a snapshot to the input io.Writer using the baseSnapshot as the starting point.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) IncrementalSend(baseSnapshot *Dataset, output io.Writer) error {
	if d.Type != DatasetSnapshot || baseSnapshot.Type != DatasetSnapshot {
		return errors.New("can only send snapshots")
	}
	_, err := d.z.run(nil, output, "zfs", "send", "-i", baseSnapshot.Name, d.Name)
	return err
}

// CreateVolume creates a new ZFS volume with the specified name, size, and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (z *zfs) CreateVolume(name string, size uint64, properties map[string]string) (*Dataset, error) {
	args := make([]string, 4, 5)
	args[0] = "create"
	args[1] = "-p"
	args[2] = "-V"
	args[3] = strconv.FormatUint(size, 10)
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, name)
	if err := z.do(args...); err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// Destroy destroys a ZFS dataset.
// If the destroy bit flag is set, any descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred deletion.
func (d *Dataset) Destroy(flags DestroyFlag) error {
	args := make([]string, 1, 3)
	args[0] = "destroy"
	if flags&DestroyRecursive != 0 {
		args = append(args, "-r")
	}

	if flags&DestroyRecursiveClones != 0 {
		args = append(args, "-R")
	}

	if flags&DestroyDeferDeletion != 0 {
		args = append(args, "-d")
	}

	if flags&DestroyForceUmount != 0 {
		args = append(args, "-f")
	}

	args = append(args, d.Name)
	err := d.z.do(args...)
	return err
}

// SetProperty sets a ZFS property on the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) SetProperty(key, val string) error {
	prop := strings.Join([]string{key, val}, "=")
	err := d.z.do("set", prop, d.Name)
	return err
}

// SetProperties sets multiple ZFS properties on the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) SetProperties(keyValPairs ...string) error {
	if len(keyValPairs) == 0 {
		return nil
	}
	if len(keyValPairs)%2 != 0 {
		return errors.New("keyValPairs must be an even number of strings")
	}
	args := []string{"set"}
	for i := 0; i < len(keyValPairs); i += 2 {
		args = append(args, strings.Join(keyValPairs[i:i+2], "="))
	}
	args = append(args, d.Name)
	err := d.z.do(args...)
	return err
}

// GetProperty returns the current value of a ZFS property from the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) GetProperty(key string) (string, error) {
	out, err := d.z.doOutput("get", "-H", "-p", key, d.Name)
	if err != nil {
		return "", err
	}

	return out[0][2], nil
}

// GetProperties returns the current values of multiple ZFS properties from the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) GetProperties(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	out, err := d.z.doOutput("get", "-H", "-p", strings.Join(keys, ","), d.Name)
	if err != nil {
		return nil, err
	}
	var props []string
	for _, v := range out {
		props = append(props, v[2])
	}
	return props, nil
}

// GetAllProperties returns all the ZFS properties from the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) GetAllProperties() (map[string]string, error) {
	out, err := d.z.doOutput("get", "-H", "-p", "all", d.Name)
	if err != nil {
		return nil, err
	}
	props := make(map[string]string)
	for _, v := range out {
		props[v[1]] = v[2]
	}
	return props, nil
}

// Rename renames a dataset.
func (d *Dataset) Rename(name string, createParent, recursiveRenameSnapshots bool) (*Dataset, error) {
	args := make([]string, 3, 5)
	args[0] = "rename"
	args[1] = d.Name
	args[2] = name
	if createParent {
		args = append(args, "-p")
	}
	if recursiveRenameSnapshots {
		args = append(args, "-r")
	}
	if err := d.z.do(args...); err != nil {
		return d, err
	}

	return d.z.GetDataset(name)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Dataset) Snapshots() ([]*Dataset, error) {
	return d.z.Snapshots(d.Name)
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (z *zfs) CreateFilesystem(name string, properties map[string]string) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "create"

	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}

	args = append(args, name)
	if err := z.do(args...); err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the specified name.
// Optionally, the snapshot can be taken recursively, creating snapshots of all descendent filesystems in a single, atomic operation.
func (d *Dataset) Snapshot(name string, recursive bool) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "snapshot"
	if recursive {
		args = append(args, "-r")
	}
	snapName := fmt.Sprintf("%s@%s", d.Name, name)
	args = append(args, snapName)
	if err := d.z.do(args...); err != nil {
		return nil, err
	}
	return d.z.GetDataset(snapName)
}

// Rollback rolls back the receiving ZFS dataset to a previous snapshot.
// Optionally, intermediate snapshots can be destroyed.
// A ZFS snapshot rollback cannot be completed without this option, if more recent snapshots exist.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Rollback(destroyMoreRecent bool) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only rollback snapshots")
	}

	args := make([]string, 1, 3)
	args[0] = "rollback"
	if destroyMoreRecent {
		args = append(args, "-r")
	}
	args = append(args, d.Name)

	err := d.z.do(args...)
	return err
}

// Children returns a slice of children of the receiving ZFS dataset.
// A recursion depth may be specified, or a depth of 0 allows unlimited recursion.
func (d *Dataset) Children(depth uint64) ([]*Dataset, error) {
	args := []string{"list"}
	if depth > 0 {
		args = append(args, "-d")
		args = append(args, strconv.FormatUint(depth, 10))
	} else {
		args = append(args, "-r")
	}
	args = append(args, "-t", "all", "-Hp", "-o", dsPropListOptions)
	args = append(args, d.Name)

	out, err := d.z.doOutput(args...)
	if err != nil {
		return nil, err
	}

	var datasets []*Dataset
	name := ""
	var ds *Dataset
	for _, line := range out {
		if name != line[0] {
			name = line[0]
			ds = &Dataset{z: d.z, Name: name}
			datasets = append(datasets, ds)
		}
		if err := ds.parseLine(line); err != nil {
			return nil, err
		}
	}
	return datasets[1:], nil
}

// Diff returns changes between a snapshot and the given ZFS dataset.
// The snapshot name must include the filesystem part as it is possible to compare clones with their origin snapshots.
func (d *Dataset) Diff(snapshot string) ([]*InodeChange, error) {
	args := []string{"diff", "-FH", snapshot, d.Name}
	out, err := d.z.doOutput(args...)
	if err != nil {
		return nil, err
	}
	inodeChanges, err := parseInodeChanges(out)
	if err != nil {
		return nil, err
	}
	return inodeChanges, nil
}
