#!/bin/bash
# Shim that dispatches statusline and hook calls based on environment.
# In a container: runs ccstatusline (with its full widget/hook support).
# On macOS host: runs the custom statusline script; hooks are no-ops.

# Claude Code doesn't pass terminal width to statusline commands
# (see anthropics/claude-code#22115). Try /dev/tty, fall back to 200.
if [ -z "${COLUMNS:-}" ]; then
  COLUMNS=$(stty size </dev/tty 2>/dev/null | awk '{print $2}')
  COLUMNS=${COLUMNS:-200}
  export COLUMNS
fi

IN_CONTAINER=false
if [ -f /.dockerenv ] || [ -f /run/.containerenv ]; then
  IN_CONTAINER=true
fi

if [ "$IN_CONTAINER" = true ]; then
  # Container: delegate to pre-installed ccstatusline
  exec ccstatusline "$@"
else
  # Host: only handle the statusline command (no args).
  # Hook invocations (--hook) are no-ops.
  if [ $# -eq 0 ]; then
    exec bash "$HOME/.claude/statusline-command.sh"
  fi
  # --hook or any other arg: silent no-op
fi
