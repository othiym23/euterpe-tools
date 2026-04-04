# Devcontainer

Development container for euterpe-tools, built on [Wolfi](https://wolfi.dev/)
with all project dependencies pre-installed.

## What's included

| Tool           | Source         | Purpose                             |
| -------------- | -------------- | ----------------------------------- |
| Rust 1.94.1    | rustup (Wolfi) | Workspace crates                    |
| cargo-nextest  | cargo install  | Test runner                         |
| Python 3.14    | uv             | pylib + etp porcelain               |
| ruff / pyright | uv tool        | Linting, type checking              |
| ty             | uv tool        | Type checking (experimental)        |
| Docker         | Wolfi          | Docker-in-Docker (build/run images) |
| Node 22 LTS    | Wolfi          | prettier, Claude Code, ccstatusline |
| bun            | Wolfi          | ccstatusline runtime                |
| uv             | Wolfi          | Python toolchain + venv management  |
| just           | Wolfi          | Task runner (justfile)              |
| gh             | Wolfi          | GitHub CLI                          |
| neovim         | Wolfi          | VSCode Neovim extension backend     |
| Claude Code    | npm            | AI assistant (CLI + VS Code ext)    |
| prettier       | npm            | Markdown formatting                 |
| ccstatusline   | npm            | Claude Code status line (container) |

## VS Code extensions

Installed automatically when the container starts:

- **rust-analyzer** — Rust language server
- **Python + Pylance** — Python language server and type checking
- **Ruff** — Python linter (uses the uv-installed binary)
- **VSCode Neovim** — Neovim keybindings (bind-mounts host config)
- **Claude Code** — AI assistant extension
- **KDL** — Syntax highlighting for `.kdl` config files
- **Version Lens** — Shows dependency versions in Cargo.toml/pyproject.toml
- **Dev Containers** — Container management from inside the container

## Host prerequisites

### 1Password SSH agent

The container relies on VS Code's built-in SSH agent forwarding, which forwards
whatever `SSH_AUTH_SOCK` points to on the host. For 1Password:

```sh
# Create the symlink (one-time)
mkdir -p ~/.1password
ln -sf "$HOME/Library/Group Containers/2BUA8C4S2C.com.1password/t/agent.sock" \
  ~/.1password/agent.sock

# Add to ~/.ssh/config (if not already present)
echo -e "\nHost *\n  IdentityAgent ~/.1password/agent.sock" >> ~/.ssh/config

# Set SSH_AUTH_SOCK for all shells and macOS apps
echo 'export SSH_AUTH_SOCK="$HOME/.1password/agent.sock"' >> ~/.zprofile
launchctl setenv SSH_AUTH_SOCK "$HOME/.1password/agent.sock"
```

The `launchctl setenv` is immediate but does not survive reboots. A LaunchAgent
plist at `~/Library/LaunchAgents/com.ogd.ssh-auth-sock.plist` makes it
persistent.

**Important:** VS Code must be quit and relaunched after setting `SSH_AUTH_SOCK`
so it picks up the new value from the login environment.

### GitHub CLI authentication

The `gh` CLI token is extracted from the macOS keychain by `init-host.sh` and
written into the container by `post-create.sh`. This requires `gh auth login` to
have been run on the host at least once.

### NAS shares

The `/Volumes/home`, `/Volumes/docker`, and `/Volumes/video` SMB shares must be
mounted on the host before starting the container. They are bind-mounted
read-only. If a share is not mounted, the container will fail to start — unmount
or remove the corresponding line from `devcontainer.json` if the NAS is
unavailable.

## Initialization scripts

### `init-host.sh` — runs on the host before build

1. Extracts the GitHub PAT from the macOS keychain via `gh auth token`
2. Writes it to `.devcontainer/.gh-token` (gitignored, chmod 600)
3. The token file is cleaned up by `post-create.sh` inside the container

### `post-create.sh` — runs inside the container after creation

1. Reads `.gh-token` and writes `~/.config/gh/hosts.yml`
2. Deletes the token file (via EXIT trap, runs even on error)
3. Configures git identity (`user.name`, `user.email`)
4. Configures SSH commit signing (`gpg.format ssh`, discovers the signing key
   from the forwarded 1Password agent via `ssh-add -L`)
5. Runs `uv sync` to set up the Python virtualenv

### `post-start.sh` — runs inside the container on every start

1. Starts `dockerd` in the background (via sudo, scoped to just this binary)
2. Waits up to 10 seconds for the Docker socket to become available
3. Logs to `/tmp/dockerd.log` for debugging

### `statusline-shim.sh` — Claude Code status line dispatcher

Configured in `.claude/settings.json` as the project-level status line command.
Detects the runtime environment:

- **Inside container** (`/.dockerenv` exists): runs the pre-installed
  `ccstatusline` binary with full widget support
- **On macOS host**: runs the custom `~/.claude/statusline-command.sh`; hook
  invocations (`--hook`) are silent no-ops

This allows ccstatusline to run in the container (where it works) while the host
uses a simpler custom script (ccstatusline causes lockups on macOS).

## Bind mounts

| Host path             | Container path                | Mode       |
| --------------------- | ----------------------------- | ---------- |
| `~/.claude`           | `/home/ogd/.claude`           | read-write |
| `~/.config/nvim`      | `/home/ogd/.config/nvim`      | read-write |
| `~/.local/share/nvim` | `/home/ogd/.local/share/nvim` | read-write |
| `~/.local/state/nvim` | `/home/ogd/.local/state/nvim` | read-write |
| `~/.ssh`              | `/home/ogd/.ssh`              | read-only  |
| `/Volumes/home`       | `/Volumes/home`               | read-only  |
| `/Volumes/docker`     | `/Volumes/docker`             | read-only  |
| `/Volumes/video`      | `/Volumes/video`              | read-only  |

The container user is `ogd` (UID 501, GID 20) to match the host user, ensuring
NAS share files are accessible with correct permissions.

## Dockerfile design notes

- **Wolfi base** rather than Alpine or Debian — minimal, glibc-based, fast
  package installs, Chainguard supply chain security
- **rustup** instead of versioned `rust-N.NN` package — allows adding targets
  (`rustup target add x86_64-unknown-linux-musl`) and updating the toolchain
  without rebuilding
- **`RUSTUP_HOME` and `CARGO_HOME`** set to `/usr/local/` — shared toolchain,
  not per-user
- **`ldconfig`** primes the shared library cache for VS Code Server's
  `check-requirements.sh`
- **`posix-libc-utils`** provides `getconf`, also needed by
  `check-requirements.sh`
- **Python installed via `uv python install`**, not a system package — `uv`
  manages the exact version matching `pyproject.toml`
- **Pre-created `~/.local` and `~/.config`** directories with correct ownership
  prevent permission errors when Docker creates bind mount targets as root
- **Docker-in-Docker** via `privileged: true` — `dockerd` runs inside the
  container, fully isolated from the host Docker daemon
- **Scoped sudo** — `ogd` can only sudo `/usr/bin/dockerd`, not arbitrary
  commands. This is important for running Claude Code with
  `--dangerously-skip-permissions`

## Rebuilding

From the VS Code command palette: **Dev Containers: Rebuild Container** (or
**Rebuild Without Cache** for a clean build).

From the command line (outside the container):

```sh
docker build --no-cache -f .devcontainer/Dockerfile .devcontainer/
```
