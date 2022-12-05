package zfs_test

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/mistifyio/go-zfs/v3"
)

func sleep(delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
}

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}

// https://github.com/benbjohnson/testing
// assert fails the test if the condition is false.
func _assert(t *testing.T, condition bool, msg string, v ...interface{}) {
	t.Helper()

	if !condition {
		_, file, line, _ := runtime.Caller(2)
		t.Logf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		t.FailNow()
	}
}

func assert(t *testing.T, condition bool, msg string, v ...interface{}) {
	t.Helper()
	_assert(t, condition, msg, v...)
}

// ok fails the test if an err is not nil.
func ok(t *testing.T, err error) {
	t.Helper()
	_assert(t, err == nil, "unexpected error: %v", err)
}

// nok fails the test if an err is nil.
func nok(t *testing.T, err error) {
	t.Helper()
	_assert(t, err != nil, "expected error, got nil")
}

// equals fails the test if exp is not equal to act.
func equals(t *testing.T, exp, act interface{}) {
	t.Helper()
	_assert(t, reflect.DeepEqual(exp, act), "exp: %#v\n\ngot: %#v", exp, act)
}

type cleanUpFunc func()

func (f cleanUpFunc) cleanUp() {
	f()
}

// do something like Restorer in github.com/packethost/pkg/internal/testenv/clearer.go
func setupZPool(t *testing.T) cleanUpFunc {
	t.Helper()

	var opts []zfs.Option
	var client *ssh.Client
	if os.Getenv("ZFS_TEST_SSH") != "" {
		h, err := os.UserHomeDir()
		ok(t, err)
		f, err := os.ReadFile(filepath.Join(h, ".ssh", "id_rsa"))
		ok(t, err)
		key, err := ssh.ParsePrivateKey(f)
		ok(t, err)
		client, err = ssh.Dial("tcp", "127.0.0.1:22", &ssh.ClientConfig{
			User:            "root",
			Auth:            []ssh.AuthMethod{ssh.PublicKeys(key)},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		})
		ok(t, err)
		opts = append(opts, zfs.WithExecutor(zfs.NewSSHExecutor(client)))
	}
	if _, err := exec.LookPath("sudo"); err == nil && os.Geteuid() != 0 {
		opts = append(opts, zfs.WithSudo())
	}
	z, err := zfs.New(opts...)
	ok(t, err)
	zfs.SetDefault(z)

	d, err := os.MkdirTemp("/tmp/", "zfs-test-*")
	ok(t, err)

	var skipRemoveAll bool
	defer func() {
		if !skipRemoveAll {
			t.Logf("cleaning up")
			os.RemoveAll(d)
		}
	}()

	tempfiles := make([]string, 3)
	for i := range tempfiles {
		f, err := os.CreateTemp(d, fmt.Sprintf("loop%d", i))
		ok(t, err)

		ok(t, f.Truncate(pow2(30)))

		f.Close()
		tempfiles[i] = f.Name()
	}

	pool, err := zfs.CreateZpool("test", nil, tempfiles...)
	ok(t, err)

	skipRemoveAll = true
	return func() {
		ok(t, pool.Destroy())
		os.RemoveAll(d)
		if client != nil {
			client.Close()
		}
	}
}
