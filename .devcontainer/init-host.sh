#!/bin/bash
# Runs on the HOST before the devcontainer build.
# Writes the GitHub PAT to a file the postCreateCommand can read.
# Cleanup is handled by the EXIT trap in post-create.sh.
set -euo pipefail

TOKEN_FILE="$(cd "$(dirname "$0")" && pwd)/.gh-token"

if command -v gh >/dev/null 2>&1; then
  GH_TOKEN="$(gh auth token 2>/dev/null)" || true
  if [ -n "${GH_TOKEN:-}" ]; then
    echo "${GH_TOKEN}" > "${TOKEN_FILE}"
    chmod 600 "${TOKEN_FILE}"
    exit 0
  fi
fi

echo "warning: could not retrieve GitHub PAT from macOS keychain." >&2
echo "  Run: gh auth login" >&2
: > "${TOKEN_FILE}"
