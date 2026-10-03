package vm

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestTransientSSH(t *testing.T) {
	connRefused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused (still booting)", connRefused, true},
		{"wrapped connection error", fmt.Errorf("dial: %w", connRefused), true},
		{"EOF from half-started sshd", io.EOF, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"auth failure never heals", errors.New("ssh: handshake failed: ssh: unable to authenticate, attempted methods [none publickey], no supported methods remain"), false},
		{"context cancellation is not transient", errors.New("context canceled"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := transientSSH(tc.err); got != tc.want {
				t.Errorf("transientSSH(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
