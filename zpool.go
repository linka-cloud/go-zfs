package zfs

// ZFS zpool states, which can indicate if a pool is online, offline, degraded, etc.
//
// More information regarding zpool states can be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zpoolconcepts.7.html#Device_Failure_and_Recovery
const (
	ZpoolOnline   = "ONLINE"
	ZpoolDegraded = "DEGRADED"
	ZpoolFaulted  = "FAULTED"
	ZpoolOffline  = "OFFLINE"
	ZpoolUnavail  = "UNAVAIL"
	ZpoolRemoved  = "REMOVED"
)

// Zpool is a ZFS zpool.
// A pool is a top-level structure in ZFS, and can contain many descendent datasets.
type Zpool struct {
	z             *zfs
	Name          string
	Health        string
	Allocated     uint64
	Size          uint64
	Free          uint64
	Fragmentation uint64
	ReadOnly      bool
	Freeing       uint64
	Leaked        uint64
	DedupRatio    float64
}

// zpool is a helper function to wrap typical calls to zpool and ignores stdout.
func (z *zfs) zpool(arg ...string) error {
	_, err := z.zpoolOutput(arg...)
	return err
}

// zpool is a helper function to wrap typical calls to zpool.
func (z *zfs) zpoolOutput(arg ...string) ([][]string, error) {
	return z.run(nil, nil, "zpool", arg...)
}

// GetZpool retrieves a single ZFS zpool by name.
func (z *zfs) GetZpool(name string) (*Zpool, error) {
	args := zpoolArgs
	args = append(args, name)
	out, err := z.zpoolOutput(args...)
	if err != nil {
		return nil, err
	}

	pool := &Zpool{z: z, Name: name}
	for _, line := range out {
		if err := pool.parseLine(line); err != nil {
			return nil, err
		}
	}

	return pool, nil
}

// Datasets returns a slice of all ZFS datasets in a zpool.
func (z *Zpool) Datasets() ([]*Dataset, error) {
	return z.z.Datasets(z.Name)
}

// Snapshots returns a slice of all ZFS snapshots in a zpool.
func (z *Zpool) Snapshots() ([]*Dataset, error) {
	return z.z.Snapshots(z.Name)
}

// CreateZpool creates a new ZFS zpool with the specified name, properties, and optional arguments.
//
// A full list of available ZFS properties and command-line arguments may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
// https://openzfs.github.io/openzfs-docs/man/8/zpool-create.8.html
func (z *zfs) CreateZpool(name string, properties map[string]string, args ...string) (*Zpool, error) {
	cli := make([]string, 1, 4)
	cli[0] = "create"
	if properties != nil {
		cli = append(cli, propsSlice(properties)...)
	}
	cli = append(cli, name)
	cli = append(cli, args...)
	if err := z.zpool(cli...); err != nil {
		return nil, err
	}

	return &Zpool{z: z, Name: name}, nil
}

// Destroy destroys a ZFS zpool by name.
func (z *Zpool) Destroy() error {
	err := z.z.zpool("destroy", z.Name)
	return err
}

// ListZpools list all ZFS zpools accessible on the current system.
func (z *zfs) ListZpools() ([]*Zpool, error) {
	args := []string{"list", "-Ho", "name"}
	out, err := z.zpoolOutput(args...)
	if err != nil {
		return nil, err
	}

	var pools []*Zpool

	for _, line := range out {
		z, err := z.GetZpool(line[0])
		if err != nil {
			return nil, err
		}
		pools = append(pools, z)
	}
	return pools, nil
}
