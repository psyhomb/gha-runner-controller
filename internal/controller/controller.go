// Package controller reconciles scale-set demand with ephemeral tart VMs
// using min/max scaling: all VMs are
// fungible - any VM may serve any job routed to the scale set. Every tick the
// controller scales the total VM count toward
// min(minRunners + jobsInFlight, maxRunners), where jobsInFlight comes from
// the broker's statistics snapshot.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"gha-runner-controller/internal/config"
	"gha-runner-controller/internal/github"
	"gha-runner-controller/internal/jobsource"
	"gha-runner-controller/internal/vm"
)

// GitHubAPI is the subset of the GitHub client the Controller needs
// (consumer-side interface, for testing with fakes).
type GitHubAPI interface {
	ListRunners(ctx context.Context, scope, owner, repo string) ([]github.RunnerInfo, error)
	DeleteRunnerByName(ctx context.Context, scope, owner, repo, name string) (bool, error)
}

// JITProvider produces a JIT configuration for a runner with the given name.
// In broker-only mode there is a single implementation: scaleSetJITProvider
// (scale-set registration - scale-set-routed jobs can only be picked up by
// scale-set runners).
type JITProvider interface {
	GenerateJITConfig(ctx context.Context, repo, runnerName string) (string, error)
}

// scaleSetJitter is the subset of the broker client the scaleSetJITProvider needs.
type scaleSetJitter interface {
	GenerateScaleSetJITConfig(ctx context.Context, scaleSetID int, name, workDir string) (string, error)
}

type scaleSetJITProvider struct {
	broker     scaleSetJitter
	scaleSetID int
	workDir    string
}

// NewScaleSetJITProvider returns a JITProvider registering runners into the
// given scale set.
func NewScaleSetJITProvider(b scaleSetJitter, scaleSetID int, workDir string) JITProvider {
	return &scaleSetJITProvider{broker: b, scaleSetID: scaleSetID, workDir: workDir}
}

func (p *scaleSetJITProvider) GenerateJITConfig(ctx context.Context, _ string, name string) (string, error) {
	return p.broker.GenerateScaleSetJITConfig(ctx, p.scaleSetID, name, p.workDir)
}

// Controller reconciles scale-set demand with ephemeral tart VMs.
type Controller struct {
	cfg config.Config
	gh  GitHubAPI
	src jobsource.Source
	vmm *vm.Manager
	jit JITProvider

	mu  sync.Mutex
	vms map[string]*vmState
	wg  sync.WaitGroup

	// Boot-failure backoff: consecutive boot failures pause scale-up, since
	// the demand signal (broker statistics) does not decay on its own.
	bootFails         int
	bootCooldownUntil time.Time

	// Scaling-log latch: reconcile runs on a single goroutine, so a plain
	// field suffices; reset when the state goes quiet so the next burst logs.
	lastScaling   scalingState
	scalingLogged bool
}

// scalingState is the logged scaling tuple; a line is emitted only when it
// changes (or on the first non-quiet tick after a quiet period).
type scalingState struct{ inFlight, busy, idle, total, up, down int }

// vmState tracks one VM's lifecycle. All VMs are fungible: a VM may end up
// serving any job routed to the scale set.
type vmState struct {
	vmName       string
	ip           string
	registeredAt time.Time // zero until the JIT registration exists server-side
	busySince    time.Time // non-zero while continuously busy (TTL backstop)
	cancel       context.CancelFunc
}

func New(cfg config.Config, gh GitHubAPI, src jobsource.Source, vmm *vm.Manager, jit JITProvider) *Controller {
	return &Controller{
		cfg: cfg,
		gh:  gh,
		src: src,
		vmm: vmm,
		jit: jit,
		vms: make(map[string]*vmState),
	}
}

func (c *Controller) vmCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.vms)
}

// trackVM reserves a slot for the VM; false if already tracked or at the cap.
func (c *Controller) trackVM(st *vmState) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.vms[st.vmName]; ok {
		return false
	}
	if len(c.vms) >= c.cfg.EffectiveMaxRunners() {
		return false
	}
	c.vms[st.vmName] = st
	return true
}

func (c *Controller) untrackVM(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.vms, name)
}

// noteBootFailure counts consecutive boot failures; after the configured
// threshold (vm.bootFailureThreshold), scale-up pauses for the configured
// cooldown (vm.bootFailureCooldownMinutes).
func (c *Controller) noteBootFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bootFails++
	if c.bootFails >= c.cfg.EffectiveBootFailureThreshold() {
		c.bootCooldownUntil = time.Now().Add(c.cfg.BootFailureCooldown())
	}
}

// noteBootSuccess resets the failure counter on a successful registration.
func (c *Controller) noteBootSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bootFails = 0
	c.bootCooldownUntil = time.Time{}
}

func (c *Controller) scaleUpPaused() (until time.Time, failures int, paused bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bootCooldownUntil, c.bootFails, time.Now().Before(c.bootCooldownUntil)
}

// PlanScale computes VM scaling for one tick with minRunners/maxRunners
// semantics: keep minVMs idle runners plus one VM per in-flight job, capped
// at maxVMs.
//
//	desired = min(minVMs + inFlight, maxVMs)
//
// A job taken by an idle runner stays inside inFlight (running jobs count as
// assigned) and its replacement comes from the minVMs term; a job with a
// booting VM is covered because booting VMs count in total. Scale-down is
// bounded by idle - busy and booting VMs are never reaped; callers should
// additionally require a minimum idle age (see reapable), so a VM whose job
// assignment is still in flight is not reaped underneath it.
func PlanScale(inFlight, idle, total, minVMs, maxVMs int) (scaleUp, scaleDown int) {
	desired := minVMs + inFlight
	if desired > maxVMs {
		desired = maxVMs
	}
	if scaleUp = desired - total; scaleUp < 0 {
		scaleUp = 0
	}
	if excess := total - desired; excess > 0 {
		scaleDown = min(excess, idle)
	}
	return scaleUp, scaleDown
}

// vmStats reports pool liveness for scaling. A VM counts as idle only when
// its runner is REGISTERED and not busy - a booting VM must not suppress
// scale-up, because it still needs ~60-90 s to be ready. Busy state comes
// from the broker's JobStarted/JobCompleted messages. busySince is maintained
// here: set when a VM is first observed busy, cleared otherwise (a busy VM
// never returns to idle - ephemeral runners exit after their one job - so the
// clear only covers observation glitches).
func (c *Controller) vmStats() (idleNames []string, busy int) {
	busySet := c.src.BusyRunnerNames()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, st := range c.vms {
		if busySet[st.vmName] {
			busy++
			if st.busySince.IsZero() {
				st.busySince = time.Now()
			}
			continue
		}
		st.busySince = time.Time{}
		if !st.registeredAt.IsZero() {
			idleNames = append(idleNames, st.vmName)
		}
	}
	sort.Strings(idleNames)
	return idleNames, busy
}

// reapIdleDelay is how long a VM must be continuously idle before scale-down
// may reap it: a VM whose job assignment is still in flight (assigned but not
// yet started) looks idle, so only VMs idle well beyond the assignment
// window (observed: seconds) are safe to remove.
const reapIdleDelay = 60 * time.Second

// reapable filters idleNames to VMs registered longer than reapIdleDelay.
func (c *Controller) reapable(idleNames []string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, name := range idleNames {
		if st, ok := c.vms[name]; ok && time.Since(st.registeredAt) > reapIdleDelay {
			out = append(out, name)
		}
	}
	return out
}

// ttlExpired returns VMs busy continuously longer than the TTL.
func (c *Controller) ttlExpired() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, st := range c.vms {
		if !st.busySince.IsZero() && time.Since(st.busySince) > c.cfg.TTL() {
			out = append(out, st.vmName)
		}
	}
	return out
}

// stopVM cancels a VM's lifecycle (its deferred cleanup deregisters the
// runner and deletes the VM).
func (c *Controller) stopVM(name string) {
	c.mu.Lock()
	st, ok := c.vms[name]
	c.mu.Unlock()
	if ok && st.cancel != nil {
		st.cancel()
	}
}

// deployRunner decodes the JIT bundle and writes the runner's config files
// into the guest (the JIT credential never appears in a process argument
// list on either side), then starts the runner from disk.
func (c *Controller) deployRunner(ctx context.Context, ip, jit string) error {
	files, err := github.DecodeJITConfig(jit)
	if err != nil {
		return err
	}
	if err := c.vmm.WriteRunnerConfig(ctx, ip, files); err != nil {
		return fmt.Errorf("write runner config: %w", err)
	}
	if err := c.vmm.StartRunner(ctx, ip); err != nil {
		return fmt.Errorf("start runner: %w", err)
	}
	return nil
}

// runnerLogTail fetches the tail of the guest's runner log so the reason a
// runner exited is never deleted with the VM.
func (c *Controller) runnerLogTail(ctx context.Context, ip string) string {
	out, err := c.vmm.ReadRunnerLog(ctx, ip)
	if err != nil || strings.TrimSpace(out) == "" {
		return ""
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) > 30 {
		lines = lines[len(lines)-30:]
	}
	return strings.Join(lines, "\n")
}

// deregisterRunner removes a runner registration (best-effort; failures are
// logged, real removals are logged too).
func (c *Controller) deregisterRunner(ctx context.Context, name string) {
	deleted, err := c.gh.DeleteRunnerByName(ctx, "org", c.cfg.GitHub.Org, "", name)
	if err != nil {
		slog.Warn("deregister runner failed", "runner", name, "error", err)
		return
	}
	if deleted {
		slog.Info("deregistered runner", "runner", name)
	}
}

// startVM starts one VM lifecycle if capacity allows.
func (c *Controller) startVM(ctx context.Context) {
	name := fmt.Sprintf("%s-%d", c.cfg.VMNameBase(), time.Now().UnixNano())
	jobCtx, cancel := context.WithCancel(ctx)
	st := &vmState{vmName: name, cancel: cancel}
	if !c.trackVM(st) {
		cancel()
		return
	}
	slog.Info("VM starting", "vm", name)
	c.wg.Add(1)
	go c.lifecycle(jobCtx, st)
}

// Run starts the reconcile loop until ctx is cancelled (e.g. SIGTERM).
func (c *Controller) Run(ctx context.Context) {
	slog.Info("controller starting",
		"org", c.cfg.GitHub.Org,
		"labels", strings.Join(c.cfg.EffectiveLabels(), ","),
		"baseImage", c.cfg.VM.BaseImage,
		"minRunners", c.cfg.EffectiveMinRunners(),
		"maxRunners", c.cfg.EffectiveMaxRunners(),
		"ttl", c.cfg.TTL(),
		"tickInterval", c.cfg.TickInterval())

	ticker := time.NewTicker(c.cfg.TickInterval())
	defer ticker.Stop()
	c.reconcile(ctx) // first pass immediately
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down, waiting for cleanups", "vms", c.vmCount())
			c.wg.Wait()
			slog.Info("bye")
			return
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) {
	inFlight := c.src.JobsInFlight()
	idleNames, busy := c.vmStats()
	total := c.vmCount()
	scaleUp, scaleDown := PlanScale(inFlight, len(idleNames), total, c.cfg.EffectiveMinRunners(), c.cfg.EffectiveMaxRunners())
	if until, failures, paused := c.scaleUpPaused(); paused && scaleUp > 0 {
		slog.Warn("scale-up paused after repeated boot failures",
			"remaining", time.Until(until).Round(time.Second),
			"until", until.Format(time.RFC3339),
			"failures", failures,
			"suppressed", scaleUp)
		scaleUp = 0
	}
	reap := c.reapable(idleNames)
	scaleDown = min(scaleDown, len(reap))

	// Telemetry: when there is anything to do, the inputs and outputs of
	// PlanScale answer "why is (no) VM starting/stopping" in one line. Logged
	// on state change only - a long-running job would otherwise repeat the
	// identical line every tick.
	cur := scalingState{inFlight, busy, len(idleNames), total, scaleUp, scaleDown}
	if cur.inFlight == 0 && cur.up == 0 && cur.down == 0 {
		c.scalingLogged = false
	} else if !c.scalingLogged || c.lastScaling != cur {
		slog.Info("scaling",
			"inFlight", cur.inFlight,
			"busy", cur.busy,
			"idle", cur.idle,
			"totalVMs", cur.total,
			"scaleUp", cur.up,
			"scaleDown", cur.down)
		c.lastScaling = cur
		c.scalingLogged = true
	}

	for i := 0; i < scaleUp; i++ {
		c.startVM(ctx)
	}
	for i := 0; i < scaleDown; i++ {
		slog.Info("scaling down, reaping idle VM", "vm", reap[i])
		c.stopVM(reap[i])
	}
	for _, name := range c.ttlExpired() {
		slog.Warn("VM busy beyond TTL, force-removing", "vm", name, "ttl", c.cfg.TTL())
		c.stopVM(name)
	}
}

// CleanupOrphans force-removes every VM whose name starts with the VM name
// base and deregisters their runners (used at controller startup, before the
// broker session opens; assumes a single controller instance owns the
// namespace).
func CleanupOrphans(ctx context.Context, vmm *vm.Manager, gh GitHubAPI, org, nameBase string) {
	names, err := vmm.List(ctx)
	if err != nil {
		slog.Warn("cleanup orphans: list VMs failed", "error", err)
		return
	}
	prefix := nameBase + "-"
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		slog.Info("cleanup orphan VM", "vm", name)
		if deleted, err := gh.DeleteRunnerByName(ctx, "org", org, "", name); err != nil {
			slog.Warn("deregister runner failed", "runner", name, "error", err)
		} else if deleted {
			slog.Info("deregistered runner", "runner", name)
		}
		_ = vmm.Stop(ctx, name) // ignore "not running" errors
		if err := vmm.Delete(ctx, name); err != nil {
			slog.Warn("cleanup orphan VM: delete failed", "vm", name, "error", err)
		}
	}
}

// lifecycle runs one VM from clone to delete: boot, register an ephemeral
// runner into the scale set, wait for the runner process to exit (it exits
// after its one job), delete the VM. Completion detection: the runner process
// is authoritative. The runners API list is eventually consistent, so absence
// from it never drives deletion; a runner that never appears in the API
// within 3 min is treated as a registration failure and deleted. There is no
// TTL here: idle-forever is a valid state (minRunners), and the reconcile
// loop force-stops VMs busy beyond the TTL and reaps surplus idle VMs.
func (c *Controller) lifecycle(ctx context.Context, st *vmState) {
	defer c.wg.Done()
	defer c.untrackVM(st.vmName)

	started := false
	defer func() {
		// Deregister the runner before stopping the VM: it is a fast API call.
		// Self-deregistration only happens when the runner exits gracefully
		// after a job; killing the VM skips it.
		if !st.registeredAt.IsZero() {
			dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
			c.deregisterRunner(dctx, st.vmName)
			dcancel()
		}
		// Always leave the host clean: stop + delete the VM if it was created.
		if !started {
			return
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stopCancel()
		// Capture the runner log first: once the VM is gone so is the reason.
		if st.ip != "" {
			if l := c.runnerLogTail(stopCtx, st.ip); l != "" {
				slog.Info("runner log", "vm", st.vmName, "runnerLog", l)
			}
		}
		_ = c.vmm.Stop(stopCtx, st.vmName) // ignore "not running" errors
		if err := c.vmm.Delete(stopCtx, st.vmName); err != nil {
			slog.Error("delete VM failed", "vm", st.vmName, "error", err)
		} else {
			slog.Info("VM deleted", "vm", st.vmName)
		}
	}()

	if err := c.vmm.Clone(ctx, c.cfg.VM.BaseImage, st.vmName); err != nil {
		slog.Error("clone failed", "vm", st.vmName, "error", err)
		c.noteBootFailure()
		return
	}
	started = true
	if err := c.vmm.Set(ctx, st.vmName, c.cfg.VM.CPU, c.cfg.VM.MemoryMB); err != nil {
		slog.Error("tart set failed", "vm", st.vmName, "error", err)
		c.noteBootFailure()
		return
	}
	proc, err := c.vmm.Start(ctx, st.vmName)
	if err != nil {
		slog.Error("start VM failed", "vm", st.vmName, "error", err)
		c.noteBootFailure()
		return
	}
	go proc.Wait() // reap the process when the VM exits

	ip, err := c.vmm.IP(ctx, st.vmName)
	if err != nil {
		slog.Error("get VM IP failed", "vm", st.vmName, "error", err)
		c.noteBootFailure()
		return
	}
	st.ip = ip
	if err := c.vmm.WaitSSH(ctx, ip); err != nil {
		slog.Error("wait SSH failed", "vm", st.vmName, "ip", ip, "error", err)
		c.noteBootFailure()
		return
	}

	jit, err := c.jit.GenerateJITConfig(ctx, "", st.vmName)
	if err != nil {
		slog.Error("generate JIT config failed", "vm", st.vmName, "error", err)
		c.noteBootFailure()
		return
	}
	c.mu.Lock()
	st.registeredAt = time.Now() // the registration exists server-side from this call on
	c.mu.Unlock()
	if err := c.deployRunner(ctx, ip, jit); err != nil {
		slog.Error("deploy runner failed", "vm", st.vmName, "error", err)
		c.noteBootFailure()
		return
	}
	c.noteBootSuccess()
	slog.Info("runner registered idle, waiting for a job", "runner", st.vmName, "ip", ip)

	// A runner that never appears in the runners API within 3 min is treated
	// as a registration failure and deleted.
	seenInList := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
		if !c.vmm.RunnerAlive(ctx, st.ip) {
			slog.Info("runner exited - job finished (or never picked one up)", "runner", st.vmName)
			return
		}
		runners, err := c.gh.ListRunners(ctx, "org", c.cfg.GitHub.Org, "")
		if err != nil {
			slog.Warn("runners check failed", "vm", st.vmName, "error", err)
			continue
		}
		for _, r := range runners {
			if r.Name == st.vmName {
				seenInList = true
				break
			}
		}
		if !seenInList && time.Since(st.registeredAt) > 3*time.Minute {
			slog.Warn("runner never appeared in the runners API - deleting", "runner", st.vmName)
			c.noteBootFailure()
			return
		}
	}
}
