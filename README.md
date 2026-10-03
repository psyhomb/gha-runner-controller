# gha-runner-controller

Ephemeral **macOS** GitHub Actions runners on [tart](https://github.com/openai/tart) VMs
(Apple Silicon hosts, macOS guests).

Listens to an org's **runner scale set** over GitHub's actions-service
long-poll (the internal API ARC uses), and for each job routed to the set:
clones a fresh macOS VM from a base image, boots it, registers an
**ephemeral** runner into the scale set via GitHub's JIT API, lets the job
run, then stops and deletes the VM. One job = one fresh VM, always.

Two external dependencies (`gopkg.in/yaml.v3`, `golang.org/x/crypto`);
otherwise pure Go stdlib. Shells out to `tart`; guest access runs over
in-process SSH (`x/crypto/ssh`) - no system ssh binary, no known_hosts.

## Architecture

```
GitHub org
   │  broker: HTTPS long-poll session on the runner scale set (the internal
   │  API ARC uses): job events + a statistics snapshot on every batch
   ▼
gha-runner-controller (LaunchDaemon on the tart host)
   │  scales VMs toward min(vm.minRunners + jobs in flight, vm.maxRunners),
   │  driven by the broker statistics (no client-side job bookkeeping):
   │    1. tart clone <baseImage> <nameBase>-<seq>-<ts>   # nameBase = vm.namePrefix or the scale set name
   │    2. tart run <name> --no-graphics
   │    3. tart ip --wait 60 + wait for SSH
   │    4. generate scale-set JIT config
   │    5. decode JIT bundle -> write runner config files into the guest via SSH
   │    6. plain ./run.sh in the guest; job runs; runner exits + self-deregisters
   │    7. tart stop + tart delete <name>
   ▼
tart VMs (baseImage clones)
```

Job demand comes from `BrokerSource` (`internal/jobsource`): a long-poll
session on the runner scale set. Scaling is statistics-driven - every message
batch (and session creation) carries a server-side snapshot of job/runner
counts, so there is no client-side job state to go stale (ARC's model).

Runner registration never exposes the JIT credential in a process argument
list: the controller decodes the JIT bundle (which is just the runner's
config files - `.runner`, `.credentials`, `.credentials_rsaparams`) and writes
them into the guest over an in-process SSH session's stdin with 0600
permissions, then starts plain `./run.sh`. The runner binary itself does
exactly this when given `--jitconfig`; we do it ourselves so the credential
is never visible in `ps aux` - and since SSH runs in-process
(`x/crypto/ssh`), no ssh process or known_hosts entry exists at all.

Safety nets: boot-failure backoff (scale-up pauses after repeated VM boot
failures), TTL force-delete of VMs busy too long (default 90 min), startup
cleanup of leftover `<nameBase>-*` VMs, graceful SIGTERM handling (in-flight
VMs are stopped and deleted). The guest's `/tmp/runner.log` is captured into
the controller log before a VM is deleted, so runner exits always carry their
reason.

## One-time setup

### 1. GitHub App

Settings -> Developer settings -> GitHub Apps -> New. Can be created under a
**personal account** if you lack org permissions - set "Where can this app be
installed?" to **Any account**, then install it on the org (an **org owner
must approve** the installation request).

Form fields (the controller uses installation tokens only, so most user-flow
fields are ignored):

- **Homepage URL**: anything (repo or profile URL)
- **Callback URL / OAuth / Device Flow / Setup URL**: leave blank/unchecked
- **Expire user authorization tokens**: leave selected (ignored)
- **Webhook**: deselect **Active** (we use the actions-service long-poll, not
  webhooks)
- **Permissions**: Organization -> **Self-hosted runners: Read & write**
  (the only permission needed - discovery is message-driven via the scale
  set, so no REST run/job listing permissions are required)

After creation: record **App ID**, generate a **private key** (PEM), install
on the org (all repos, or selected), and note the **Installation ID** from
the installation URL (`.../settings/installations/<id>`).

Also create a dedicated **runner group** (Settings -> Actions -> Runner
groups) restricted to the repos that may use these runners, and note its ID
(API: `GET /orgs/{org}/actions/runner-groups`). **The runner group's repo
visibility is the only repo filter** - there is no controller-side allowlist
(one could only gate provisioning, not GitHub's direct assignment to
registered runners, so it would silently strand jobs until the 24h timeout).

### 2. Base image (with SSH key for the controller)

```bash
# on the tart host
ssh-keygen -t ed25519 -f ~/.ssh/gha-runner-controller -N ''
tart clone macos-tahoe-xcode macos-tahoe-xcode-gha-base
tart run macos-tahoe-xcode-gha-base --no-graphics &
tart ip --wait 60 macos-tahoe-xcode-gha-base
# ssh in with the admin password, then:
#   mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys
#   (paste the .pub contents), chmod 600 ~/.ssh/authorized_keys
# verify: ssh -i ~/.ssh/gha-runner-controller admin@<ip> true
tart stop macos-tahoe-xcode-gha-base
```

### 3. Controller install

Host prerequisites (one-time):

```bash
ssh-copy-id {{REMOTE_USER}}@{{REMOTE_HOST}}   # key auth, no more SSH password prompts
task host-setup                               # passwordless sudo for launchctl (prompts once)
```

From the project directory on your Mac (renders the templates in `config/`
and `deploy/` with the `REMOTE_USER` from `taskfile.yaml`):

```bash
task install    # builds, deploys binary, renders + installs plist and config (if missing);
                # does NOT start the daemon - use 'task start' when ready
```

If `app.pem` (the GitHub App private key) is present in the project
directory, it is deployed automatically with mode 600; otherwise place it
manually on the host. Note: `*.pem` is git-ignored - never commit the key.

Manual equivalent (on the tart host):

```bash
mkdir -p ~/bin ~/.config/gha-runner-controller
cp gha-runner-controller ~/bin/
cp app.pem ~/.config/gha-runner-controller/ && chmod 600 ~/.config/gha-runner-controller/app.pem
# render the config template (it contains __REMOTE_HOME__ placeholders):
sed "s|__REMOTE_HOME__|$HOME|g" deploy/config.example.yaml > ~/.config/gha-runner-controller/config.yaml
chmod 600 ~/.config/gha-runner-controller/config.yaml
# edit config.yaml: github.org, github.appID, github.installationID
```

## Configuration (config.yaml)

`deploy/config.example.yaml` documents **every parameter inline** (detailed
comments, defaults, gotchas) - it is the reference. Unknown keys are
rejected at startup, so stale or misspelled keys fail loudly. Quick map:

| Key | Meaning | Default |
|---|---|---|
| `logLevel` | `debug` (adds broker internals), `info`, `warn`, `error` | `info` |
| `github.org` | organization that owns the runners | (required) |
| `github.scope` | must be `org` (or empty = auto-detect); repo scope is not supported (scale sets are an org concept) | `` (auto) |
| `github.appID` / `github.installationID` / `github.keyPath` | GitHub App credentials (PEM at keyPath, chmod 600) | (required) |
| `runner.labels` | the complete, exact label set of the runner (started with `--no-default-labels`, so nothing is added implicitly); the scale set name is appended automatically | (required) |
| `runner.groupID` | runner group for the scale set; the group's repo visibility is the repo filter | 1 (Default) |
| `runner.workDir` | runner work dir in guest | `_work` |
| `vm.baseImage` | tart image to clone from | (required) |
| `vm.cpu` / `vm.memoryMB` | per-VM resources (0 = image default) | 0 |
| `vm.namePrefix` | VM/runner name base (`<base>-<seq>-<ts>`); also the orphan-cleanup namespace; empty = the scale set name | `` (scale set name) |
| `vm.minRunners` | minimum idle runners kept registered (ARC `minRunners`; 0 = pure on-demand); counts toward maxRunners | 0 |
| `vm.maxRunners` | hard cap on total VMs (busy + idle + booting) | 2 |
| `vm.ttlMinutes` | force-delete VMs busy longer than this (stuck job) | 90 |
| `vm.ssh.user` | guest SSH user | `admin` |
| `vm.ssh.privateKeyPath` | controller's SSH private key | (required) |
| `jobs.tickSeconds` | scaling-tick interval (recompute desired count, reap idle/TTL-expired VMs) | 15 |
| `jobs.broker.scaleSetName` | runner scale set name (unique per runner group); also the VM name base when `vm.namePrefix` is empty | (required) |
| `jobs.broker.capacity` | capacity advertised to the broker (X-ScaleSetMaxCapacity) - gates delivery/acquisition, NOT provisioning (PlanScale enforces the real cap) | maxRunners+1 |

## Run as a service (LaunchDaemon)

The plist in `deploy/` is a template (`__REMOTE_USER__` / `__REMOTE_HOME__`
placeholders) - install it rendered, same pattern as the tart VM daemon:

```bash
task render   # writes rendered/local.gha-runner-controller.plist
sudo cp rendered/local.gha-runner-controller.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/local.gha-runner-controller.plist
sudo chmod 644 /Library/LaunchDaemons/local.gha-runner-controller.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/local.gha-runner-controller.plist
```

(or just `task install`, which does all of the above)

Logs: `~/Library/Logs/gha-runner-controller/gha-runner-controller.{out,err}.log`.

Note: the controller runs `tart` commands, which need the unlocked
`login.keychain` of user admin (see the main README). After a host reboot it
becomes effective once admin logs in via SSH and unlocks the keychain; until
then its retries fail harmlessly.

Manage:

```bash
sudo launchctl kickstart -k system/local.gha-runner-controller   # restart
sudo launchctl bootout system/local.gha-runner-controller        # stop
```

## Using the runners in workflows

```yaml
jobs:
  build:
    runs-on: [self-hosted, tart-eph]
    steps:
      - run: uname -a
```

Each job gets a fresh VM cloned from the base image. Expect ~60-90 s from
queue to job start (clone ~1 s, boot ~30-60 s, registration ~10 s).

Matching semantics mirror GitHub's own: a job is picked up only when **all**
of its `runs-on` labels are among the runner's labels. The runner is started
with `--no-default-labels`, so the config `labels` array is the complete,
exact label contract - nothing is added implicitly. Include
`self-hosted`/`macOS`/`ARM64` explicitly if you want generic jobs like
`runs-on: [self-hosted, macOS]` to match; omit them for strict opt-in (only
jobs carrying your custom label get VMs).

## Scope: organizations only

Runner scale sets are an org concept, so the controller requires **org scope**
(`github.scope: org`, or auto-detected via `GET /users/{owner}`). Runners
register into the scale set; the app needs *Actions: Read* + *Self-hosted
runners: Read & write*. Personal accounts (repo scope) are not supported.

## Project layout

```
cmd/gha-runner-controller/   main - thin entrypoint (flags, wiring, signals)
internal/config/             YAML config, validation, defaults (strict decoding)
internal/github/             GitHub App client (JWT, installation tokens, REST + broker)
internal/jobsource/          BrokerSource - scale-set long-poll, statistics-driven demand
internal/vm/                 tart CLI wrapper + in-process SSH guest access (x/crypto/ssh)
internal/controller/         reconcile loop, scaling, VM lifecycle
deploy/                      deployment assets - plist template + config template
```

Dependencies flow one way, no cycles: github <- jobsource <- controller;
vm -> controller; config is a leaf imported by all.

## Project layout is internal-only

All packages live under `internal/` - nothing here is meant to be imported by
other projects, and Go enforces that.

## Scaling (`vm.minRunners` and `vm.maxRunners`)

The controller follows ARC's minRunners/maxRunners semantics (ADR
2023-11-02): all VMs are fungible - any VM can serve any job routed to the
scale set - and every tick the controller scales the total VM count toward

    desired = min(vm.minRunners + jobsInFlight, vm.maxRunners)

where `jobsInFlight` = available + assigned jobs from the latest broker
statistics snapshot (assigned includes running).

- `vm.minRunners: N` keeps N idle runners registered at all times, so jobs
  start in seconds instead of waiting ~60-90 s for a VM to boot. When a job
  takes an idle runner its replacement boots immediately (the minRunners
  term). `0` (default) = pure on-demand: scale from zero, back to zero.
- `vm.maxRunners` is the hard cap on total VMs (busy + idle + booting).
- Every VM runs one job and self-deletes (the ephemeral runner exits after
  its job). Surplus idle VMs - e.g. a job cancelled while its VM booted - are
  reaped after 60 s of idleness; busy and booting VMs are never reaped.
- A VM counts as idle only when its runner is registered and not busy; busy
  state comes from the broker's JobStarted/JobCompleted messages.
- `ttlMinutes` applies to busy VMs only (a stuck job is force-removed after
  the TTL); idle VMs have no TTL.
- Boot-failure backoff: repeated VM boot failures pause scale-up (the demand
  signal does not decay on its own); a successful registration resets it.

VM names are uniform - `<nameBase>-<seq>-<ts>`, where nameBase is
`vm.namePrefix` or, when empty, the scale set name - there is no
warm/on-demand distinction.

Example with `vm.minRunners: 1, vm.maxRunners: 2`: steady state is one idle
runner plus capacity for one more VM; two concurrent jobs run in parallel
(one on the idle runner, one on a fresh VM), and the idle baseline is
refilled as jobs consume it.

For ARC users: `vm.minRunners` IS ARC's `minRunners` and `vm.maxRunners` IS
`maxRunners` - same formula, same semantics, VM instead of pod granularity.

## How jobs flow (broker long-poll)

The controller opens a long-poll session against GitHub's actions service
(the internal API ARC uses) on a runner scale set and receives
`JobAvailable`/`JobAssigned`/`JobStarted`/`JobCompleted` events in near-real
time (~1-2 s). There is no per-repo REST polling at all, so API rate limits
are a non-issue at any org size.

- **Scaling is statistics-driven** (ARC's model): every message batch carries
  a server-side snapshot (`TotalAvailableJobs`, `TotalAssignedJobs`,
  `TotalRunningJobs`, runner counts) and session creation returns the same
  snapshot - so a (re)started controller converges immediately without event
  replay, and there is no client-side job state to go stale (no sweep, no
  reconciliation loop).
- **Targeting contract:** the Actions Service routes a job to a runner scale
  set when the job's `runs-on` labels are a **subset of the scale set's
  labels** - the scale set's name is simply one more (unique) label in that
  set. So both of these reach the scale set:

  ```yaml
  runs-on: mac-mini-1-ephemeral-vm-scale-set   # precise: the name matches one set only
  runs-on: [self-hosted, macOS, ARM64]          # generic: matches by label subset
  ```

  The name is the **precise targeting** tool: scale set names are unique per
  runner group, so it matches exactly one set. Generic labels match ANY
  target with a superset of them (another scale set, or a standalone
  self-hosted runner), and GitHub distributes matching jobs **arbitrarily**
  (assignment race) - keep that in mind when several scale sets share labels.

- **The full loop** has two shapes, both handled:
  - **Assigned path (the common one):** while the scale set has registered
    runners, GitHub assigns incoming jobs directly - no `JobAvailable`, no
    acquisition. A job assigned to an idle runner starts immediately; a job
    assigned with no free runner counts as in-flight demand (the statistics
    snapshot grows), so a VM scales up for it and the job starts on the next
    registered runner. Observed: a second concurrent job was assigned while
    the only runner was busy and waited for the next registration.
  - **Available path (scale-from-zero):** with no registered runners, the job
    arrives as `JobAvailable` and must be claimed via `AcquireJobs` (an
    unacquired job stays unassigned forever, per the ARC listener contract),
    then a VM is provisioned.
  - Either way: provision VM -> runner registers **into the scale set** via
    the scale-set JIT endpoint -> job runs -> runner self-deregisters -> VM
    deleted.
- On startup the controller creates (or reuses) the runner scale set named
  `jobs.broker.scaleSetName` (required - no default). The scale set name is
  automatically added to the effective label set.
- Every delivered batch is logged at INFO (`broker: message batch` with
  per-type event counts plus the statistics) - the first thing to check when
  jobs are not picked up: batches arriving means delivery works; silence
  means routing (scale set name vs `runs-on`, runner-group repo visibility).
- **Acquisition retry (available path only):** jobs the broker refuses to
  hand out (capacity) are retried via message redelivery - batches containing
  an unacquired job are never acked, so the broker re-delivers them (also
  into fresh sessions after a restart). After 5 failed attempts a message is
  dead-lettered; the statistics keep the job visible to scaling regardless.
- **Repo filtering happens on the GitHub side only:** the runner group's repo
  visibility. There is deliberately no controller-side allowlist - it could
  gate provisioning but not GitHub's direct assignment to registered runners,
  so it would strand jobs (queued until the 24h timeout).
- Caveat: the session/message-queue protocol is internal and undocumented
  (protocol reference: github.com/actions/scaleset); broker errors are loud.
  Note that an active broker session makes the Actions Service hold matching
  jobs for the scale set - they are released when the session ends
  (controller stop) or the token expires.

## REST API usage

Discovery is message-driven; the only remaining REST calls are runner
deregistration, a per-VM registration check (3-min never-registered
backstop), and token refresh - all far below the GitHub App 5,000
requests/hour budget at any org size.

## Troubleshooting

- **Job stays queued**: controller log (`.../gha-runner-controller.out.log`) -
  look for `broker: message batch` lines (silence = routing issue: label
  mismatch, runner group doesn't include the repo); all VMs busy
  (`vm.maxRunners` reached); GitHub fails jobs queued longer than 24h.
- **VM boots but runner never registers**: the controller captures the guest's
  `/tmp/runner.log` into the log before deleting a VM - look for a
  `runner log` line; JIT errors (permissions, runner group) show up there.
- **Leftover `<nameBase>-*` VMs**: deleted automatically at controller start, or
  `tart delete <name>` manually.
- **GitHub API errors**: check App permissions and installation; token is
  cached and auto-refreshed.
