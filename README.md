# solara-scheduler

Lightweight task scheduler written in Rust. Runs as a daemon, executes jobs on configurable intervals via short-lived child processes. Communicates over a Unix socket.

## Install

```bash
cargo install --path .
```

## Usage

Start the daemon:
```bash
sch daemon
```

In another terminal:
```bash
sch job <job_name>      # start a job from jobs.toml
sch joboff <job_name>   # stop a running job
sch jobs                # list active jobs
```

## Configuration

Define jobs in `jobs.toml`:

```toml
[[jobs]]
name = "reconcile_tiers"
command = "python3 manage.py reconcile_tiers"
path = "/home/ubuntu/project"
interval = "2h"

[[jobs]]
name = "backup_docs"
command = "bash backup.sh"
path = "/home/ubuntu/scripts"
interval = "24h"
```

**Fields:**
- `name` — unique job identifier
- `command` — shell command to execute
- `path` — working directory for the command
- `interval` — run frequency (`m` for minutes, `h` for hours)

## Error Handling

Failed jobs log to `/home/ubuntu/solaradocs/tasks.log` with timestamps and exit codes. The job continues running on its interval after a failure.

## Architecture

One binary, two modes. `sch daemon` starts the control plane which listens on a Unix socket. All other `sch` commands act as clients that send messages to the daemon through the socket. Each job execution spawns an isolated child process that dies after completion. The control plane stays alive.