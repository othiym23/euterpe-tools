#!/bin/bash
# Runs inside the container after creation.
# Configures gh auth and git signing via the forwarded 1Password SSH agent.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOKEN_FILE="${SCRIPT_DIR}/.gh-token"
GH_CONFIG_DIR="${HOME}/.config/gh"

# Always clean up the token file, even on error.
cleanup() { rm -f "${TOKEN_FILE}"; }
trap cleanup EXIT

if [ -s "${TOKEN_FILE}" ]; then
  TOKEN="$(cat "${TOKEN_FILE}")"
  mkdir -p "${GH_CONFIG_DIR}"
  cat > "${GH_CONFIG_DIR}/hosts.yml" <<YAML
github.com:
    oauth_token: "${TOKEN}"
    user: othiym23
    git_protocol: ssh
YAML
  chmod 600 "${GH_CONFIG_DIR}/hosts.yml"
  echo "gh auth configured."
else
  echo "warning: no GitHub token found. Run: gh auth login"
fi

# The host's ~/.ssh/config sets IdentityAgent to ~/.1password/agent.sock,
# which doesn't exist inside the container.  SSH then ignores the forwarded
# VS Code agent entirely.  Fix: override via git config so all future git
# SSH operations use the forwarded agent via SSH_AUTH_SOCK.
git config --global core.sshCommand 'ssh -o IdentityAgent=$SSH_AUTH_SOCK'

# Configure git identity and SSH commit signing.
# The signing key is discovered from the forwarded 1Password agent.
git config --global user.name "零Rei"
git config --global user.email "othiym23@gmail.com"
git config --global gpg.format ssh
git config --global commit.gpgsign true
git config --global tag.gpgsign true

SIGNING_KEY="$(ssh-add -L 2>/dev/null | grep sign || true)"
if [ -n "${SIGNING_KEY}" ]; then
  git config --global user.signingKey "${SIGNING_KEY}"
  echo "git signing configured."
else
  echo "warning: no signing key found in ssh-agent. Commits won't be signed."
  git config --global commit.gpgsign false
fi

# Sync Python venv
uv sync
