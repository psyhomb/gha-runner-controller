// Package config defines the controller's YAML configuration.
package config

import (
	_ "embed"
	"fmt"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

//go:embed config.template.yaml
var configTemplate string

// Default returns the default config template (every parameter documented
// inline) with __HOME__ substituted for home.
func Default(home string) string {
	return strings.ReplaceAll(configTemplate, "__HOME__", home)
}

// Config is loaded from a YAML file (see config.template.yaml, printed via
// -gen-config, which documents every parameter inline). Unknown keys are
// rejected, so stale or misspelled keys fail at startup instead of being
// silently ignored.
type Config struct {
	LogLevel string       `yaml:"logLevel"`
	GitHub   GitHubConfig `yaml:"github"`
	Runner   RunnerConfig `yaml:"runner"`
	VM       VMConfig     `yaml:"vm"`
	Jobs     JobsConfig   `yaml:"jobs"`
}

// GitHubConfig is the GitHub connection: the account the runners serve plus
// the App credentials used to access it.
type GitHubConfig struct {
	Org            string `yaml:"org"`   // organization that owns the runners
	Scope          string `yaml:"scope"` // "org" (empty = auto-detect); repo scope is not supported - runner scale sets are an org concept
	AppID          int64  `yaml:"appID"`
	InstallationID int64  `yaml:"installationID"`
	KeyPath        string `yaml:"keyPath"`
}

// RunnerConfig is runner registration (what the runner looks like to GitHub).
type RunnerConfig struct {
	Labels  []string `yaml:"labels"`  // the complete, exact label set of the runner (started with --no-default-labels); a job matches when ALL its runs-on labels are among these
	GroupID int      `yaml:"groupID"` // runner group ID (default 1)
	WorkDir string   `yaml:"workDir"` // runner work folder inside the guest
}

// VMConfig is the tart VM fleet: what to boot and how many.
type VMConfig struct {
	BaseImage  string `yaml:"baseImage"`
	CPU        int    `yaml:"cpu"`        // 0 = keep image default
	MemoryMB   int    `yaml:"memoryMB"`   // 0 = keep image default
	NamePrefix string `yaml:"namePrefix"` // ephemeral VM/runner name prefix; "eph-" when empty; a trailing "-" is added if missing
	MinRunners int    `yaml:"minRunners"` // minimum IDLE runners to keep registered (0 = pure on-demand); counts toward maxRunners
	MaxRunners int    `yaml:"maxRunners"` // hard cap on total VMs (busy + idle + booting)
	TTLMinutes int    `yaml:"ttlMinutes"` // force-delete VMs busy longer than this (stuck job)

	// Softnet userspace networking (tart --net-softnet*); enabled only when
	// NetSoftnet is true - the allow/block lists are ignored otherwise.
	NetSoftnet      bool     `yaml:"netSoftnet"`
	NetSoftnetAllow []string `yaml:"netSoftnetAllow"`
	NetSoftnetBlock []string `yaml:"netSoftnetBlock"`

	// SSH access into guests (key auth; key installed in the base image).
	SSH SSHConfig `yaml:"ssh"`
}

type SSHConfig struct {
	User           string `yaml:"user"`
	PrivateKeyPath string `yaml:"privateKeyPath"`
}

// JobsConfig is the scaling loop and the broker (GitHub actions-service
// long-poll, the only job-discovery mode).
type JobsConfig struct {
	TickSeconds int `yaml:"tickSeconds"` // scaling-tick interval

	// Broker settings (the long-poll session on the runner scale set).
	Broker BrokerConfig `yaml:"broker"`
}

type BrokerConfig struct {
	ScaleSetName string `yaml:"scaleSetName"` // runner scale set name (required); also the VM name base when vm.namePrefix is empty
	Capacity     int    `yaml:"capacity"`     // capacity advertised to the broker; default maxRunners+1
}

func (c Config) TickInterval() time.Duration {
	if c.Jobs.TickSeconds <= 0 {
		return 15 * time.Second
	}
	return time.Duration(c.Jobs.TickSeconds) * time.Second
}

func (c Config) TTL() time.Duration {
	if c.VM.TTLMinutes <= 0 {
		return 90 * time.Minute
	}
	return time.Duration(c.VM.TTLMinutes) * time.Minute
}

func (c Config) EffectiveMaxRunners() int {
	if c.VM.MaxRunners <= 0 {
		return 2
	}
	return c.VM.MaxRunners
}

func (c Config) EffectiveMinRunners() int {
	if c.VM.MinRunners < 0 {
		return 0
	}
	return c.VM.MinRunners
}

func (c Config) EffectiveWorkDir() string {
	if c.Runner.WorkDir == "" {
		return "_work"
	}
	return c.Runner.WorkDir
}

func (c Config) EffectiveRunnerGroupID() int {
	if c.Runner.GroupID <= 0 {
		return 1
	}
	return c.Runner.GroupID
}

func (c Config) EffectiveSSHUser() string {
	if c.VM.SSH.User == "" {
		return "admin"
	}
	return c.VM.SSH.User
}

// VMNameBase returns the base for VM/runner names: namePrefix when set,
// otherwise the scale set name. VMs are named <base>-<unixts-ns>; the base
// is also the orphan-cleanup namespace (single controller per base).
func (c Config) VMNameBase() string {
	if c.VM.NamePrefix != "" {
		return strings.TrimSuffix(c.VM.NamePrefix, "-")
	}
	return c.Jobs.Broker.ScaleSetName
}

// EffectiveBrokerCapacity is the capacity advertised to the broker
// (X-ScaleSetMaxCapacity). It gates message delivery and job acquisition only;
// provisioning is still capped by EffectiveMaxRunners.
func (c Config) EffectiveBrokerCapacity() int {
	if c.Jobs.Broker.Capacity > 0 {
		return c.Jobs.Broker.Capacity
	}
	return c.EffectiveMaxRunners() + 1
}

// EffectiveLabels returns the runner label set, which always includes the
// scale set name so that jobs targeting it via runs-on match everywhere
// (scale set creation, runner registration, and GitHub's label routing).
func (c Config) EffectiveLabels() []string {
	return append(append([]string{}, c.Runner.Labels...), c.Jobs.Broker.ScaleSetName)
}

// validateSoftnetRule checks one netSoftnetAllow/netSoftnetBlock entry:
// an optional "in "/"out " direction prefix, then an IPv4 CIDR or "@host"
// (the vmnet gateway). Mirrors the softnet --allow/--block rule syntax.
func validateSoftnetRule(rule string) error {
	s := strings.TrimPrefix(rule, "in ")
	s = strings.TrimPrefix(s, "out ")
	if s == "@host" {
		return nil
	}
	if _, err := netip.ParsePrefix(s); err != nil {
		return fmt.Errorf("config: invalid softnet rule %q (want [in|out] (IPv4 CIDR|@host))", rule)
	}
	return nil
}

// LoadConfig reads and validates the YAML config file at path. Unknown keys
// are rejected (KnownFields) so stale or misspelled keys fail loudly.
func LoadConfig(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, fmt.Errorf("read config: %w", err)
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	switch c.GitHub.Scope {
	case "", "org":
	default:
		return c, fmt.Errorf("config: github.scope must be \"org\" or empty (auto-detect); repo scope is not supported - runner scale sets are an org concept")
	}
	switch {
	case c.GitHub.Org == "":
		return c, fmt.Errorf("config: github.org is required")
	case c.GitHub.AppID == 0:
		return c, fmt.Errorf("config: github.appID is required")
	case c.GitHub.InstallationID == 0:
		return c, fmt.Errorf("config: github.installationID is required")
	case c.GitHub.KeyPath == "":
		return c, fmt.Errorf("config: github.keyPath is required")
	case c.VM.BaseImage == "":
		return c, fmt.Errorf("config: vm.baseImage is required")
	case len(c.Runner.Labels) == 0:
		return c, fmt.Errorf("config: runner.labels is required (at least one)")
	case c.VM.SSH.PrivateKeyPath == "":
		return c, fmt.Errorf("config: vm.ssh.privateKeyPath is required")
	case c.Jobs.Broker.ScaleSetName == "":
		return c, fmt.Errorf("config: jobs.broker.scaleSetName is required")
	case c.EffectiveMinRunners() > c.EffectiveMaxRunners():
		return c, fmt.Errorf("config: vm.minRunners (%d) must be <= vm.maxRunners (%d)", c.EffectiveMinRunners(), c.EffectiveMaxRunners())
	}
	for _, r := range slices.Concat(c.VM.NetSoftnetAllow, c.VM.NetSoftnetBlock) {
		if err := validateSoftnetRule(r); err != nil {
			return c, err
		}
	}
	return c, nil
}
