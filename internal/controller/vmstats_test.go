package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakeSrc struct {
	inFlight int
	busy     map[string]bool
}

func (f fakeSrc) JobsInFlight() int                { return f.inFlight }
func (f fakeSrc) BusyRunnerNames() map[string]bool { return f.busy }

// vmMap builds registered VM states (registeredAt set = runner registered).
func vmMap(names ...string) map[string]*vmState {
	m := make(map[string]*vmState, len(names))
	for _, n := range names {
		m[n] = &vmState{vmName: n, registeredAt: time.Now()}
	}
	return m
}

func TestVMStatsBusy(t *testing.T) {
	c := &Controller{
		vms: vmMap("w1", "w2", "w3"),
		src: fakeSrc{busy: map[string]bool{"w2": true}},
	}
	idle, busy := c.vmStats()
	if len(idle) != 2 || busy != 1 {
		t.Errorf("vmStats = (%v, %d), want ([w1 w3], 1)", idle, busy)
	}
	if c.vms["w2"].busySince.IsZero() {
		t.Error("busySince should be set for the busy VM")
	}
}

func TestVMStatsBootingNotCounted(t *testing.T) {
	c := &Controller{
		vms: vmMap("w1"),
		src: fakeSrc{},
	}
	c.vms["w2"] = &vmState{vmName: "w2"} // still booting: registeredAt zero
	idle, busy := c.vmStats()
	if len(idle) != 1 || busy != 0 {
		t.Errorf("vmStats = (%v, %d), want ([w1], 0) - booting w2 must not count", idle, busy)
	}
}

func TestVMStatsBusySinceClearedWhenNotBusy(t *testing.T) {
	c := &Controller{
		vms: vmMap("w1"),
		src: fakeSrc{busy: map[string]bool{"w1": true}},
	}
	c.vmStats()
	if c.vms["w1"].busySince.IsZero() {
		t.Fatal("busySince should be set while busy")
	}
	c.src = fakeSrc{busy: map[string]bool{}}
	c.vmStats()
	if !c.vms["w1"].busySince.IsZero() {
		t.Error("busySince should be cleared when the VM is no longer busy")
	}
}

func TestReapableRequiresIdleAge(t *testing.T) {
	c := &Controller{vms: vmMap("fresh")}
	c.vms["old"] = &vmState{vmName: "old", registeredAt: time.Now().Add(-2 * reapIdleDelay)}
	got := c.reapable([]string{"fresh", "old"})
	if len(got) != 1 || got[0] != "old" {
		t.Errorf("reapable = %v, want [old] - fresh VM is inside the assignment window", got)
	}
}

func TestTTLExpired(t *testing.T) {
	c := &Controller{vms: vmMap("idle1")}
	c.vms["stuck"] = &vmState{vmName: "stuck", registeredAt: time.Now(), busySince: time.Now().Add(-2 * time.Hour)}
	got := c.ttlExpired()
	if len(got) != 1 || got[0] != "stuck" {
		t.Errorf("ttlExpired = %v, want [stuck]", got)
	}
}

func TestBootFailureBackoff(t *testing.T) {
	c := &Controller{}
	threshold := c.cfg.EffectiveBootFailureThreshold()
	for i := 0; i < threshold; i++ {
		if _, _, paused := c.scaleUpPaused(); paused {
			t.Fatalf("paused after %d failures, want pause only at %d", i, threshold)
		}
		c.noteBootFailure()
	}
	if _, failures, paused := c.scaleUpPaused(); !paused || failures != threshold {
		t.Fatalf("scale-up should pause after %d failures (paused=%v failures=%d)", threshold, paused, failures)
	}
	c.noteBootSuccess()
	if _, _, paused := c.scaleUpPaused(); paused {
		t.Fatal("a successful registration should reset the backoff")
	}
}

func TestBootFailureBackoffCustomThreshold(t *testing.T) {
	c := &Controller{}
	c.cfg.VM.BootFailureThreshold = 2
	c.noteBootFailure()
	if _, _, paused := c.scaleUpPaused(); paused {
		t.Fatal("paused after 1 failure, want pause only at the configured 2")
	}
	c.noteBootFailure()
	until, _, paused := c.scaleUpPaused()
	if !paused {
		t.Fatal("scale-up should pause at the configured threshold of 2")
	}
	// Default cooldown (5 min) when unset
	if d := time.Until(until); d < 4*time.Minute || d > 6*time.Minute {
		t.Errorf("cooldown = %v, want ~5m", d)
	}

	c = &Controller{}
	c.cfg.VM.BootFailureThreshold = 1
	c.cfg.VM.BootFailureCooldownMinutes = 30
	c.noteBootFailure()
	until, _, paused = c.scaleUpPaused()
	if !paused {
		t.Fatal("scale-up should pause at the configured threshold of 1")
	}
	if d := time.Until(until); d < 29*time.Minute || d > 31*time.Minute {
		t.Errorf("cooldown = %v, want ~30m", d)
	}
}

func TestScalingLogFiresOnChangeOnly(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)

	count := func() int { return strings.Count(buf.String(), "msg=scaling") }

	// 1 busy + 1 idle registered VM, one job in flight -> scaleUp=scaleDown=0,
	// so reconcile never touches vmm (nil is fine).
	c := &Controller{
		src: fakeSrc{inFlight: 1, busy: map[string]bool{"vm-1": true}},
		vms: vmMap("vm-1", "vm-2"),
	}
	c.reconcile(context.Background())
	if got := count(); got != 1 {
		t.Fatalf("first tick: %d scaling lines, want 1", got)
	}
	c.reconcile(context.Background())
	if got := count(); got != 1 {
		t.Fatalf("identical state: %d scaling lines, want 1 (no repeat)", got)
	}
	c.src = fakeSrc{inFlight: 2, busy: map[string]bool{"vm-1": true}}
	c.reconcile(context.Background())
	if got := count(); got != 2 {
		t.Fatalf("changed state: %d scaling lines, want 2", got)
	}
	// A quiet tick logs nothing and resets the latch; the next burst logs once.
	c.src = fakeSrc{inFlight: 0}
	c.reconcile(context.Background())
	if got := count(); got != 2 {
		t.Fatalf("quiet tick: %d scaling lines, want 2 (silence)", got)
	}
	c.src = fakeSrc{inFlight: 1, busy: map[string]bool{"vm-1": true}}
	c.reconcile(context.Background())
	if got := count(); got != 3 {
		t.Fatalf("post-quiet burst: %d scaling lines, want 3 (latch reset)", got)
	}
}
