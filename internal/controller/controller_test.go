package controller

import "testing"

func TestPlanScale(t *testing.T) {
	cases := []struct {
		name                       string
		inFlight, idle, total      int
		minVMs, maxVMs             int
		wantScaleUp, wantScaleDown int
	}{
		{"idle, pure on-demand", 0, 0, 0, 0, 2, 0, 0},
		{"two jobs in flight, pure on-demand", 2, 0, 0, 0, 2, 2, 0},
		{"baseline met, nothing to do", 0, 1, 1, 1, 2, 0, 0},
		{"baseline refill after VM exit", 0, 0, 0, 1, 2, 1, 0},
		{"job in flight: refill starts immediately", 1, 1, 1, 1, 2, 1, 0},
		{"job running on the only VM: replacement from the min term", 1, 0, 1, 1, 2, 1, 0},
		{"demand beyond capacity, capped at max", 5, 1, 1, 1, 2, 1, 0},
		{"booting VM counts toward total: no double-provision", 2, 0, 2, 1, 2, 0, 0},
		{"job evaporated: surplus idle reaped", 0, 2, 2, 1, 2, 0, 1},
		{"scale-down bounded by idle count", 0, 1, 2, 0, 2, 0, 1},
		{"pure on-demand: stray idle reaped toward zero", 0, 1, 1, 0, 2, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up, down := PlanScale(tc.inFlight, tc.idle, tc.total, tc.minVMs, tc.maxVMs)
			if up != tc.wantScaleUp || down != tc.wantScaleDown {
				t.Errorf("PlanScale(%d, %d, %d, %d, %d) = (%d, %d), want (%d, %d)",
					tc.inFlight, tc.idle, tc.total, tc.minVMs, tc.maxVMs,
					up, down, tc.wantScaleUp, tc.wantScaleDown)
			}
		})
	}
}
