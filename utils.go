package zfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

func (z *zfs) run(in io.Reader, out io.Writer, cmd string, args ...string) ([][]string, error) {
	var stdout, stderr bytes.Buffer

	if z.sudo {
		args = append([]string{cmd}, args...)
		cmd = "sudo"
	}

	cmdOut := out
	if cmdOut == nil {
		cmdOut = &stdout
	}

	id := uuid.New().String()
	joinedArgs := strings.Join(args, " ")

	z.logger.Log([]string{"ID:" + id, "START", joinedArgs})
	if err := z.exec.Run(in, cmdOut, &stderr, cmd, args...); err != nil {
		return nil, &Error{
			Err:    err,
			Debug:  strings.Join([]string{cmd, joinedArgs}, " "),
			Stderr: stderr.String(),
		}
	}
	z.logger.Log([]string{"ID:" + id, "FINISH"})

	// assume if you passed in something for stdout, that you know what to do with it
	if out != nil {
		return nil, nil
	}

	lines := strings.Split(stdout.String(), "\n")

	// last line is always blank
	lines = lines[0 : len(lines)-1]
	output := make([][]string, len(lines))

	for i, l := range lines {
		output[i] = strings.Fields(l)
	}

	return output, nil
}

func setString(field *string, value string) {
	v := ""
	if value != "-" {
		v = value
	}
	*field = v
}

func setUint(field *uint64, value string) error {
	var v uint64
	if value != "-" {
		var err error
		v, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
	}
	*field = v
	return nil
}

func (d *Dataset) parseProps(out [][]string) error {
	var err error

	if len(out) != 2 {
		return errors.New("output does not match what is expected on this platform")
	}
	for i, v := range out[0] {
		val := "-"
		if i < len(out[1]) {
			val = out[1][i]
		}
		d.props[strings.ToLower(v)] = val
	}

	if len(d.props) <= len(dsPropList) {
		return errors.New("output does not match what is expected on this platform")
	}
	setString(&d.Name, d.props["name"])
	setString(&d.Origin, d.props["origin"])

	if err = setUint(&d.Used, d.props["used"]); err != nil {
		return err
	}
	if err = setUint(&d.Avail, d.props["avail"]); err != nil {
		return err
	}

	setString(&d.Mountpoint, d.props["mountpoint"])
	setString(&d.Compression, d.props["compress"])
	setString(&d.Type, d.props["type"])

	if err = setUint(&d.Volsize, d.props["volsize"]); err != nil {
		return err
	}
	if err = setUint(&d.Quota, d.props["quota"]); err != nil {
		return err
	}
	if err = setUint(&d.Referenced, d.props["refer"]); err != nil {
		return err
	}

	if runtime.GOOS == "solaris" {
		return nil
	}

	if err = setUint(&d.Written, d.props["written"]); err != nil {
		return err
	}
	if err = setUint(&d.Logicalused, d.props["lused"]); err != nil {
		return err
	}
	if err = setUint(&d.Usedbydataset, d.props["usedds"]); err != nil {
		return err
	}
	return nil
}

/*
 * from zfs diff`s escape function:
 *
 * Prints a file name out a character at a time.  If the character is
 * not in the range of what we consider "printable" ASCII, display it
 * as an escaped 3-digit octal value.  ASCII values less than a space
 * are all control characters and we declare the upper end as the
 * DELete character.  This also is the last 7-bit ASCII character.
 * We choose to treat all 8-bit ASCII as not printable for this
 * application.
 */
func unescapeFilepath(path string) (string, error) {
	buf := make([]byte, 0, len(path))
	llen := len(path)
	for i := 0; i < llen; {
		if path[i] == '\\' {
			if llen < i+4 {
				return "", fmt.Errorf("invalid octal code: too short")
			}
			octalCode := path[(i + 1):(i + 4)]
			val, err := strconv.ParseUint(octalCode, 8, 8)
			if err != nil {
				return "", fmt.Errorf("invalid octal code: %w", err)
			}
			buf = append(buf, byte(val))
			i += 4
		} else {
			buf = append(buf, path[i])
			i++
		}
	}
	return string(buf), nil
}

var changeTypeMap = map[string]ChangeType{
	"-": Removed,
	"+": Created,
	"M": Modified,
	"R": Renamed,
}

var inodeTypeMap = map[string]InodeType{
	"B": BlockDevice,
	"C": CharacterDevice,
	"/": Directory,
	">": Door,
	"|": NamedPipe,
	"@": SymbolicLink,
	"P": EventPort,
	"=": Socket,
	"F": File,
}

// matches (+1) or (-1).
var referenceCountRegex = regexp.MustCompile(`\(([+-]\d+?)\)`)

func parseReferenceCount(field string) (int, error) {
	matches := referenceCountRegex.FindStringSubmatch(field)
	if matches == nil {
		return 0, fmt.Errorf("regexp does not match")
	}
	return strconv.Atoi(matches[1])
}

func parseInodeChange(line []string) (*InodeChange, error) {
	llen := len(line) // nolint:ifshort // llen *is* actually used
	if llen < 1 {
		return nil, fmt.Errorf("empty line passed")
	}

	changeType := changeTypeMap[line[0]]
	if changeType == 0 {
		return nil, fmt.Errorf("unknown change type '%s'", line[0])
	}

	switch changeType {
	case Renamed:
		if llen != 4 {
			return nil, fmt.Errorf("mismatching number of fields: expect 4, got: %d", llen)
		}
	case Modified:
		if llen != 4 && llen != 3 {
			return nil, fmt.Errorf("mismatching number of fields: expect 3..4, got: %d", llen)
		}
	default:
		if llen != 3 {
			return nil, fmt.Errorf("mismatching number of fields: expect 3, got: %d", llen)
		}
	}

	inodeType := inodeTypeMap[line[1]]
	if inodeType == 0 {
		return nil, fmt.Errorf("unknown inode type '%s'", line[1])
	}

	path, err := unescapeFilepath(line[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse filename: %w", err)
	}

	var newPath string
	var referenceCount int
	switch changeType {
	case Renamed:
		newPath, err = unescapeFilepath(line[3])
		if err != nil {
			return nil, fmt.Errorf("failed to parse filename: %w", err)
		}
	case Modified:
		if llen == 4 {
			referenceCount, err = parseReferenceCount(line[3])
			if err != nil {
				return nil, fmt.Errorf("failed to parse reference count: %w", err)
			}
		}
	default:
		newPath = ""
	}

	return &InodeChange{
		Change:               changeType,
		Type:                 inodeType,
		Path:                 path,
		NewPath:              newPath,
		ReferenceCountChange: referenceCount,
	}, nil
}

// example input for parseInodeChanges
// M       /       /testpool/bar/
// +       F       /testpool/bar/hello.txt
// M       /       /testpool/bar/hello.txt (+1)
// M       /       /testpool/bar/hello-hardlink

func parseInodeChanges(lines [][]string) ([]*InodeChange, error) {
	changes := make([]*InodeChange, len(lines))

	for i, line := range lines {
		c, err := parseInodeChange(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse line %d of zfs diff: %w, got: '%s'", i, err, line)
		}
		changes[i] = c
	}
	return changes, nil
}

func (z *zfs) listByType(t, filter string) ([]*Dataset, error) {
	args := []string{"list", "-rp", "-t", t, "-o", "all"}

	if filter != "" {
		args = append(args, filter)
	}
	out, err := z.doOutput(args...)
	if err != nil {
		return nil, err
	}

	if len(out) == 0 {
		return nil, nil
	}

	var datasets []*Dataset

	name := ""
	var ds *Dataset
	for _, line := range out[1:] {
		if name != line[0] {
			name = line[0]
			ds = &Dataset{z: z, Name: name, props: make(map[string]string)}
			datasets = append(datasets, ds)
		}
		if err := ds.parseProps([][]string{out[0], line}); err != nil {
			return nil, err
		}
	}

	return datasets, nil
}

func propsSlice(properties map[string]string) []string {
	args := make([]string, 0, len(properties)*3)
	for k, v := range properties {
		args = append(args, "-o")
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}
	return args
}

func (z *Zpool) parseLine(line []string) error {
	prop := line[1]
	val := line[2]

	var err error

	switch prop {
	case "name":
		setString(&z.Name, val)
	case "health":
		setString(&z.Health, val)
	case "allocated":
		err = setUint(&z.Allocated, val)
	case "size":
		err = setUint(&z.Size, val)
	case "free":
		err = setUint(&z.Free, val)
	case "fragmentation":
		// Trim trailing "%" before parsing uint
		i := strings.Index(val, "%")
		if i < 0 {
			i = len(val)
		}
		err = setUint(&z.Fragmentation, val[:i])
	case "readonly":
		z.ReadOnly = val == "on"
	case "freeing":
		err = setUint(&z.Freeing, val)
	case "leaked":
		err = setUint(&z.Leaked, val)
	case "dedupratio":
		// Trim trailing "x" before parsing float64
		z.DedupRatio, err = strconv.ParseFloat(val[:len(val)-1], 64)
	}
	return err
}
