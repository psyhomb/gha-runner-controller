// Package vm wraps tart invocations and guest access over SSH. tart commands
// shell out; SSH runs in-process via golang.org/x/crypto/ssh - no system ssh
// binary, no known_hosts (which would also break when tart recycles an IP),
// no PATH or ~/.ssh/config interference, and no JIT credential in any process
// list.
package vm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

const (
	sshConnectTimeout = 5 * time.Second
	sshReadyTimeout   = 120 * time.Second
)

// Process is a started long-running command (e.g. `tart run`).
type Process interface {
	Wait() error
}

// runCmd executes a command to completion and returns its combined output.
func runCmd(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// startCmd launches a command without blocking; the returned Process exits
// when the command does (call Wait to reap it).
func startCmd(ctx context.Context, name string, args ...string) (Process, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return cmd, nil
}

// Manager performs VM operations via tart and guest operations via SSH.
type Manager struct {
	TartBin    string // path to the tart binary
	SSHUser    string // guest SSH user
	SSHKeyPath string // private key for guest access
}

func NewManager(tartBin, sshUser, sshKeyPath string) *Manager {
	return &Manager{TartBin: tartBin, SSHUser: sshUser, SSHKeyPath: sshKeyPath}
}

// Clone creates name from baseImage.
func (m *Manager) Clone(ctx context.Context, baseImage, name string) error {
	_, err := runCmd(ctx, m.TartBin, "clone", baseImage, name)
	return err
}

// Set adjusts CPU/memory of a stopped VM (0 = keep current).
func (m *Manager) Set(ctx context.Context, name string, cpu, memoryMB int) error {
	if cpu <= 0 && memoryMB <= 0 {
		return nil
	}
	args := []string{"set", name}
	if cpu > 0 {
		args = append(args, "--cpu", fmt.Sprint(cpu))
	}
	if memoryMB > 0 {
		args = append(args, "--memory", fmt.Sprint(memoryMB))
	}
	_, err := runCmd(ctx, m.TartBin, args...)
	return err
}

// Start launches `tart run` as a child process; it exits when the VM stops.
func (m *Manager) Start(ctx context.Context, name string) (Process, error) {
	return startCmd(ctx, m.TartBin, "run", name, "--no-graphics")
}

// IP waits for and returns the VM's IP address.
func (m *Manager) IP(ctx context.Context, name string) (string, error) {
	out, err := runCmd(ctx, m.TartBin, "ip", "--wait", "60", name)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// sshClientConfig builds the client config: key auth only, and no host-key
// verification - guests are fresh clones on the host-local tart NAT whose
// identity is unverifiable by design (the same trust posture as the old
// StrictHostKeyChecking=accept-new, without the known_hosts growth and the
// host-key-mismatch failures when tart recycles an IP).
func (m *Manager) sshClientConfig() (*ssh.ClientConfig, error) {
	key, err := os.ReadFile(m.SSHKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read ssh key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("parse ssh key: %w", err)
	}
	return &ssh.ClientConfig{
		User:            m.SSHUser,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         sshConnectTimeout,
	}, nil
}

// transientSSH reports whether err is a connection-level failure (refused,
// timeout, no route, an EOF from a half-started sshd) - "not up yet, keep
// waiting" - as opposed to an auth or handshake failure, which never heals
// and should fail fast.
func transientSSH(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// dial opens an SSH connection (TCP + handshake) honoring ctx.
func (m *Manager) dial(ctx context.Context, ip string) (*ssh.Client, error) {
	cfg, err := m.sshClientConfig()
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: sshConnectTimeout}
	addr := net.JoinHostPort(ip, "22")
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	// Bound the handshake; the session phase gets no deadline (guest commands
	// are short, and ctx cancellation closes the connection).
	_ = conn.SetDeadline(time.Now().Add(2 * sshConnectTimeout))
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return ssh.NewClient(c, chans, reqs), nil
}

// runSSH runs cmd in the guest over a fresh connection and returns its
// combined output. A fresh connection per call keeps long-lived idle VMs
// free of stale-connection handling; ctx cancellation closes the connection,
// unblocking the session.
func (m *Manager) runSSH(ctx context.Context, ip string, stdin []byte, cmd string) (string, error) {
	client, err := m.dial(ctx, ip)
	if err != nil {
		return "", err
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("ssh session: %w", err)
	}
	if stdin != nil {
		session.Stdin = bytes.NewReader(stdin)
	}
	type result struct {
		out string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := session.CombinedOutput(cmd)
		ch <- result{string(out), err}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err() // the deferred client.Close unblocks the session goroutine
	case r := <-ch:
		if r.err != nil {
			return r.out, fmt.Errorf("ssh %q: %w: %s", cmd, r.err, strings.TrimSpace(r.out))
		}
		return r.out, nil
	}
}

// WaitSSH polls until the guest accepts SSH connections. Connection-level
// errors mean "still booting"; auth or handshake failures never heal and
// fail fast instead of burning the whole ready timeout.
func (m *Manager) WaitSSH(ctx context.Context, ip string) error {
	deadline := time.Now().Add(sshReadyTimeout)
	for {
		_, err := m.runSSH(ctx, ip, nil, "true")
		if err == nil {
			return nil
		}
		if !transientSSH(err) {
			return fmt.Errorf("ssh to %s failed permanently (auth or handshake): %w", ip, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ssh to %s not ready after %s: %w", ip, sshReadyTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// WriteRunnerConfig writes the runner's config files (from a decoded JIT
// bundle) into the guest. File contents travel over the SSH session's stdin -
// never in a command line, so the JIT credential never appears in any
// process list. Files land with 0600 permissions (umask 077), exactly as the
// runner itself would write them.
func (m *Manager) WriteRunnerConfig(ctx context.Context, ip string, files map[string][]byte) error {
	for name, content := range files {
		remote := fmt.Sprintf(`cd ~/actions-runner && umask 077 && cat > '%s'`, name)
		if _, err := m.runSSH(ctx, ip, content, remote); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

// StartRunner starts the runner inside the guest, detached. The JIT config
// files were already written by WriteRunnerConfig, so plain ./run.sh picks
// them up from disk - no --jitconfig on the command line. The login profile
// is sourced in the SAME shell that spawns run.sh so the full PATH is
// inherited by the runner process and every job step: ~/.zprofile on macOS
// (Homebrew), ~/.profile on Linux guests. The .path file is written for any
// future svc.sh-based flow (only runsvc.sh reads it back).
func (m *Manager) StartRunner(ctx context.Context, ip string) error {
	remote := `cd ~/actions-runner && if [ -f ~/.zprofile ]; then source ~/.zprofile; elif [ -f ~/.profile ]; then source ~/.profile; fi; echo "$PATH" > .path; nohup ./run.sh > /tmp/runner.log 2>&1 < /dev/null &`
	_, err := m.runSSH(ctx, ip, nil, remote)
	return err
}

// RunnerAlive reports whether the runner process is running in the guest.
func (m *Manager) RunnerAlive(ctx context.Context, ip string) bool {
	_, err := m.runSSH(ctx, ip, nil, "pgrep -f Runner.Listener")
	return err == nil
}

// ReadRunnerLog fetches the guest's runner log (/tmp/runner.log). Call it
// before deleting a VM so the reason a runner exited is never lost.
func (m *Manager) ReadRunnerLog(ctx context.Context, ip string) (string, error) {
	return m.runSSH(ctx, ip, nil, "cat /tmp/runner.log")
}

// Stop gracefully stops the VM (tart force-kills after its timeout).
func (m *Manager) Stop(ctx context.Context, name string) error {
	_, err := runCmd(ctx, m.TartBin, "stop", name)
	return err
}

// Delete removes a stopped VM.
func (m *Manager) Delete(ctx context.Context, name string) error {
	_, err := runCmd(ctx, m.TartBin, "delete", name)
	return err
}

// List returns the names of all local VMs.
func (m *Manager) List(ctx context.Context) ([]string, error) {
	out, err := runCmd(ctx, m.TartBin, "list")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "local" {
			names = append(names, fields[1])
		}
	}
	return names, nil
}
