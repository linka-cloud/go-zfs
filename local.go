package zfs

import (
	"io"
	"os/exec"
)

func NewLocalExecutor() Executor {
	return &localExec{}
}

type localExec struct{}

func (l *localExec) Run(stdin io.Reader, stdout io.Writer, stderr io.Writer, cmd string, args ...string) error {
	c := exec.Command(cmd, args...)
	if stdin != nil {
        c.Stdin = stdin
    }
	if stdout != nil {
        c.Stdout = stdout
    }
	if stderr != nil {
        c.Stderr = stderr
    }
	return c.Run()
}
