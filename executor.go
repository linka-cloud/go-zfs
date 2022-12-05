package zfs

import (
	"io"
)

type Executor interface {
	Run(stdin io.Reader, stdout io.Writer, stderr io.Writer, cmd string, args ...string) error
}
