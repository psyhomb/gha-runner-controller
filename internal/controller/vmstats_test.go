package controller

import (
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
	for i := 0; i < maxConsecutiveBootFailures; i++ {
		if _, _, paused := c.scaleUpPaused(); paused {
			t.Fatalf("paused after %d failures, want pause only at %d", i, maxConsecutiveBootFailures)
		}
		c.noteBootFailure()
	}
	if _, failures, paused := c.scaleUpPaused(); !paused || failures != maxConsecutiveBootFailures {
		t.Fatalf("scale-up should pause after maxConsecutiveBootFailures (paused=%v failures=%d)", paused, failures)
	}
	c.noteBootSuccess()
	if _, _, paused := c.scaleUpPaused(); paused {
		t.Fatal("a successful registration should reset the backoff")
	}
}
