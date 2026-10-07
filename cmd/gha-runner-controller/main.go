// gha-runner-controller: ephemeral macOS GitHub Actions runners on tart VMs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"gha-runner-controller/internal/config"
	"gha-runner-controller/internal/controller"
	"gha-runner-controller/internal/github"
	"gha-runner-controller/internal/jobsource"
	"gha-runner-controller/internal/vm"
)

var version = "dev" // overridden at build: -ldflags "-X main.version=1.2.3"

// resolveConfigPath picks the config file: an explicit -config wins as-is;
// the default searches ./config.yaml first, then
// ~/.config/<binary name>/config.yaml (the directory follows the binary
// name, not a hardcoded one).
func resolveConfigPath(p string) string {
	if p != "config.yaml" { // explicit -config
		return p
	}
	if _, err := os.Stat(p); err == nil {
		return p
	}
	home, err := os.UserHomeDir()
	if err == nil {
		alt := filepath.Join(home, ".config", filepath.Base(os.Args[0]), "config.yaml")
		if _, err := os.Stat(alt); err == nil {
			slog.Info("using config", "path", alt) // fallback is visible, never silent
			return alt
		}
	}
	return p // LoadConfig reports the original default path in its error
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	genConfig := flag.Bool("gen-config", false, "print default config to stdout and exit")
	deleteScaleSets := flag.String("delete-runner-scale-sets", "", "comma-separated scale set names to delete (in the configured runner group), then exit")
	runnerGroupID := flag.Int("runner-group-id", 0, "runner group ID for -delete-runner-scale-sets (default: runner.groupID from config, else 1)")
	showHelp := flag.Bool("help", false, "print usage and exit")
	flag.Parse()

	if *showHelp {
		flag.Usage()
		return
	}
	if *runnerGroupID != 0 && *deleteScaleSets == "" {
		fmt.Fprintln(os.Stderr, "-runner-group-id only works in combination with -delete-runner-scale-sets")
		os.Exit(2)
	}
	if *showVersion {
		fmt.Println(version)
		return
	}
	if *genConfig {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot determine home directory: %v\n", err)
			os.Exit(1)
		}
		fmt.Print(config.Default(home))
		return
	}

	var levelVar slog.LevelVar
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: &levelVar})))
	slog.Info("starting", "version", version)

	cfg, err := config.LoadConfig(resolveConfigPath(*configPath))
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	levelVar.Set(parseLevel(cfg.LogLevel))

	gh, err := github.NewClient(cfg.GitHub.AppID, cfg.GitHub.InstallationID, cfg.GitHub.KeyPath)
	if err != nil {
		slog.Error("failed to create github client", "error", err)
		os.Exit(1)
	}

	labels := cfg.EffectiveLabels()

	// Scope: explicit config value or auto-detected from the account type.
	// Runner scale sets are an org concept - org scope is required.
	scope := cfg.GitHub.Scope
	if scope == "" {
		if scope, err = gh.DetectScope(context.Background(), cfg.GitHub.Org); err != nil {
			slog.Error("could not detect scope", "org", cfg.GitHub.Org, "error", err)
			os.Exit(1)
		}
		slog.Info("owner scope auto-detected", "owner", cfg.GitHub.Org, "scope", scope)
	}
	if scope != "org" {
		slog.Error("only org scope is supported (runner scale sets are an org concept)")
		os.Exit(1)
	}

	// Demand discovery: the actions-service long-poll session on the runner
	// scale set.
	bc, err := gh.NewBrokerClient(context.Background(), cfg.GitHub.Org)
	if err != nil {
		slog.Error("broker client setup failed", "error", err)
		os.Exit(1)
	}

	// Manual scale set cleanup: delete and exit, before any fleet/scale-set
	// work (a management op - no tart required, no VMs touched).
	if *deleteScaleSets != "" {
		groupID := cfg.EffectiveRunnerGroupID()
		if *runnerGroupID > 0 {
			groupID = *runnerGroupID
		}
		failed := false
		for _, name := range strings.Split(*deleteScaleSets, ",") {
			if name = strings.TrimSpace(name); name == "" {
				continue
			}
			if err := bc.DeleteScaleSetByName(context.Background(), groupID, name); err != nil {
				fmt.Fprintf(os.Stderr, "delete runner scale set %q: %v\n", name, err)
				failed = true
				continue
			}
			slog.Info("scale set deleted", "name", name)
		}
		if failed {
			os.Exit(1)
		}
		return
	}

	tartBin, err := exec.LookPath("tart")
	if err != nil {
		slog.Error("tart not found in PATH", "PATH", os.Getenv("PATH"), "error", err)
		os.Exit(1)
	}

	vmm := vm.NewManager(tartBin, cfg.EffectiveSSHUser(), cfg.VM.SSH.PrivateKeyPath, vm.SoftnetConfig{
		Enabled: cfg.VM.NetSoftnet,
		Allow:   cfg.VM.NetSoftnetAllow,
		Block:   cfg.VM.NetSoftnetBlock,
	})

	// Startup fleet cleanup before the broker session opens (fresh statistics
	// snapshot) and before the scale set is reconciled (a label-drift recreate
	// requires no registered runners).
	controller.CleanupOrphans(context.Background(), vmm, gh, cfg.GitHub.Org, cfg.VMNameBase())

	scaleSetID, err := bc.GetOrCreateScaleSet(context.Background(), cfg.EffectiveRunnerGroupID(), cfg.Jobs.Broker.ScaleSetName, labels)
	if err != nil {
		slog.Error("broker scale set setup failed", "name", cfg.Jobs.Broker.ScaleSetName, "error", err)
		os.Exit(1)
	}
	src := jobsource.NewBrokerSource(bc, scaleSetID, cfg.EffectiveBrokerCapacity())
	jit := controller.NewScaleSetJITProvider(bc, scaleSetID, cfg.EffectiveWorkDir())
	slog.Info("broker mode enabled", "scaleSet", cfg.Jobs.Broker.ScaleSetName, "scaleSetID", scaleSetID, "capacity", cfg.EffectiveBrokerCapacity())

	ctl := controller.New(cfg, gh, src, vmm, jit)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	src.Start(ctx)
	defer src.Close()

	ctl.Run(ctx)
}
