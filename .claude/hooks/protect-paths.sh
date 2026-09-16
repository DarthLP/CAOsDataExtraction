#!/usr/bin/env bash
# PreToolUse hook (matcher: Bash).
# Enforces the project's hard rule: NEVER modify inputs/, verification_pipeline/inputs/,
# or reference/qa_leave/, and NEVER edit any corrections.csv directly.
# Permission deny rules cover Edit/Write tools; this closes the shell-write gap.

set -uo pipefail

payload="$(cat)"

command -v jq >/dev/null 2>&1 || exit 0

cmd="$(printf '%s' "$payload" | jq -r '.tool_input.command // empty' 2>/dev/null)"
[ -z "$cmd" ] && exit 0

# Protected path fragments (optionally prefixed with ./).
prot='(\./)?((verification_pipeline/)?inputs|(verification_pipeline/)?reference/qa_leave)/'
corr='corrections\.csv'

deny() {
  jq -n --arg r "$1" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $r
    }
  }'
  exit 0
}

reason='Blocked by protect-paths hook: this command writes to a protected path.
Project hard rule: never modify inputs or reference/qa_leave, and never edit
corrections.csv directly (use the aggregator). If this is a legitimate read,
rephrase so the protected path is not a write target.'

# 1) Redirection whose TARGET is a protected path
if printf '%s' "$cmd" | grep -Eq "(>>?|&>|>\|)[[:space:]]*[\"']?${prot}"; then
  deny "$reason"
fi
if printf '%s' "$cmd" | grep -Eq "(>>?|&>|>\|)[[:space:]]*[\"']?[^[:space:]]*${corr}"; then
  deny "$reason"
fi

# 2) In-place / destructive verbs referencing a protected path anywhere.
if printf '%s' "$cmd" | grep -Eq "\b(tee|rm|rmdir|truncate)\b[^|]*${prot}"; then
  deny "$reason"
fi
if printf '%s' "$cmd" | grep -Eq "\bsed\b[^|]*-i[^|]*${prot}"; then
  deny "$reason"
fi
if printf '%s' "$cmd" | grep -Eq "\b(tee|rm|rmdir|truncate)\b[^|]*${corr}"; then
  deny "$reason"
fi
if printf '%s' "$cmd" | grep -Eq "\bsed\b[^|]*-i[^|]*${corr}"; then
  deny "$reason"
fi

# 3) mv/cp/rsync/install whose DESTINATION (last argument) is protected.
if printf '%s' "$cmd" | grep -Eq "\b(mv|cp|rsync|install)\b"; then
  last="$(printf '%s' "$cmd" | awk '{print $NF}' | tr -d '"'"'"'')"
  if printf '%s' "$last" | grep -Eq "^${prot}|${corr}$"; then
    deny "$reason"
  fi
fi

exit 0

