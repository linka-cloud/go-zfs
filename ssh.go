package zfs

import (
	"io"
	"strings"

	"golang.org/x/crypto/ssh"
)

func NewSSHExecutor(c *ssh.Client) Executor {
	return &sshExec{c: c}
}

type sshExec struct {
	c *ssh.Client
}

func (s *sshExec) Run(stdin io.Reader, stdout io.Writer, stderr io.Writer, cmd string, args ...string) error {
	sess, err := s.c.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	if stdin != nil {
		sess.Stdin = stdin
	}
	if stdout != nil {
		sess.Stdout = stdout
	}
	if stderr != nil {
		sess.Stderr = stderr
	}
	return sess.Run(strings.Join(append([]string{cmd}, args...), " "))
}
