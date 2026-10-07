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
	"strings"
	"syscall"

	"gha-runner-controller/internal/config"
	"gha-runner-controller/internal/controller"
	"gha-runner-controller/internal/github"
	"gha-runner-controller/internal/jobsource"
	"gha-runner-controller/internal/vm"
)

var version = "dev" // overridden at build: -ldflags "-X main.version=1.2.3"

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
	flag.Parse()

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

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	levelVar.Set(parseLevel(cfg.LogLevel))

	tartBin, err := exec.LookPath("tart")
	if err != nil {
		slog.Error("tart not found in PATH", "PATH", os.Getenv("PATH"), "error", err)
		os.Exit(1)
	}

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

	vmm := vm.NewManager(tartBin, cfg.EffectiveSSHUser(), cfg.VM.SSH.PrivateKeyPath, vm.SoftnetConfig{
		Enabled: cfg.VM.NetSoftnet,
		Allow:   cfg.VM.NetSoftnetAllow,
		Block:   cfg.VM.NetSoftnetBlock,
	})

	// Startup fleet cleanup before the broker session opens (fresh statistics
	// snapshot) and before the scale set is reconciled (a label-drift recreate
	// requires no registered runners).
	controller.CleanupOrphans(context.Background(), vmm, gh, cfg.GitHub.Org, cfg.VMNameBase())

	// Demand discovery: the actions-service long-poll session on the runner
	// scale set.
	bc, err := gh.NewBrokerClient(context.Background(), cfg.GitHub.Org)
	if err != nil {
		slog.Error("broker client setup failed", "error", err)
		os.Exit(1)
	}
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
