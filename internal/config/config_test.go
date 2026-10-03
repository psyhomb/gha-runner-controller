package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVMNameBase(t *testing.T) {
	c := Config{}
	c.Jobs.Broker.ScaleSetName = "my-scale-set"
	if got := c.VMNameBase(); got != "my-scale-set" {
		t.Errorf("VMNameBase() = %q, want scale set name when namePrefix is empty", got)
	}
	c.VM.NamePrefix = "mac-mini-1-ephemeral-vm"
	if got := c.VMNameBase(); got != "mac-mini-1-ephemeral-vm" {
		t.Errorf("VMNameBase() = %q, want the namePrefix", got)
	}
	c.VM.NamePrefix = "eph-"
	if got := c.VMNameBase(); got != "eph" {
		t.Errorf("VMNameBase() = %q, want trailing dash trimmed", got)
	}
}

func TestEffectiveLabels(t *testing.T) {
	c := Config{}
	c.Jobs.Broker.ScaleSetName = "my-scale-set"
	c.Runner.Labels = []string{"self-hosted", "macOS"}
	got := c.EffectiveLabels()
	if len(got) != 3 || got[2] != "my-scale-set" {
		t.Errorf("labels = %v, want scale set name appended", got)
	}
	// must not mutate the original slice
	if len(c.Runner.Labels) != 2 {
		t.Errorf("Labels mutated: %v", c.Runner.Labels)
	}
}

const validYAML = `
github:
  org: o
  appID: 1
  installationID: 2
  keyPath: /k.pem
runner:
  labels: [self-hosted]
vm:
  baseImage: img
  minRunners: 1
  maxRunners: 2
  ssh:
    privateKeyPath: /s
jobs:
  broker:
    scaleSetName: my-scale-set
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfigMinMaxValidation(t *testing.T) {
	if _, err := LoadConfig(writeConfig(t, validYAML)); err != nil {
		t.Errorf("valid min/max rejected: %v", err)
	}
	inverted := strings.Replace(validYAML, "minRunners: 1", "minRunners: 3", 1)
	if _, err := LoadConfig(writeConfig(t, inverted)); err == nil || !strings.Contains(err.Error(), "minRunners") {
		t.Errorf("minRunners > maxRunners should be rejected, got %v", err)
	}
}

func TestLoadConfigRejectsUnknownKeys(t *testing.T) {
	withTypo := validYAML + "\nvm:\n  minRunner: 1\n"
	if _, err := LoadConfig(writeConfig(t, withTypo)); err == nil {
		t.Error("unknown key should be rejected (KnownFields), got nil error")
	}
	staleJSONKey := strings.Replace(validYAML, "minRunners: 1", "warmPool: 1", 1)
	if _, err := LoadConfig(writeConfig(t, staleJSONKey)); err == nil {
		t.Error("stale renamed key should be rejected, got nil error")
	}
}

func TestLoadConfigRequiredFields(t *testing.T) {
	if _, err := LoadConfig(writeConfig(t, "github:\n  org: o\n")); err == nil {
		t.Error("missing required fields should be rejected")
	}
	// scaleSetName is mandatory (no default derivation)
	missing := strings.Replace(validYAML, "    scaleSetName: my-scale-set\n", "", 1)
	if _, err := LoadConfig(writeConfig(t, missing)); err == nil || !strings.Contains(err.Error(), "scaleSetName") {
		t.Errorf("missing scaleSetName should be rejected, got %v", err)
	}
}

func TestDefaultTemplate(t *testing.T) {
	out := Default("/home/test")
	if strings.Contains(out, "__HOME__") {
		t.Error("Default() left __HOME__ placeholders unsubstituted")
	}
	if !strings.Contains(out, "/home/test/.config/gha-runner-controller/app.pem") ||
		!strings.Contains(out, "/home/test/.ssh/gha-runner-controller") {
		t.Error("Default() did not substitute home into paths")
	}
	// the template must always stay loadable against the schema
	if _, err := LoadConfig(writeConfig(t, out)); err != nil {
		t.Errorf("generated template should pass LoadConfig, got %v", err)
	}
}
