#!/usr/bin/env bash
# PreToolUse hook (matcher: Bash).
# Enforces the project's hard rule: NEVER modify inputs/ or reference/qa_leave/,
# and NEVER edit any corrections.csv directly. Permission deny rules already
# cover the Edit/Write tools; this closes the shell-write gap that permissions
# can't see (redirections, mv/cp/rm/tee/sed -i, etc.).
#
# Reads the tool-call JSON on stdin, inspects the Bash command, and emits a
# PreToolUse "deny" decision when it detects a destructive op targeting a
# protected path. Reads (cat/grep/head on inputs/) are left untouched.
#
# Fail-open by design: any parse error or missing jq allows the command
# through — a guard that crashes must never block legitimate work.

set -uo pipefail

payload="$(cat)"

command -v jq >/dev/null 2>&1 || exit 0

cmd="$(printf '%s' "$payload" | jq -r '.tool_input.command // empty' 2>/dev/null)"
[ -z "$cmd" ] && exit 0

# Protected path fragments (optionally prefixed with ./). Matches inputs/ and
# reference/qa_leave/ anywhere, plus any corrections.csv.
prot='(\./)?(inputs|reference/qa_leave)/'
corr='corrections\.csv'

deny() {
  # PreToolUse deny: block the tool call and tell the model why.
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
Project hard rule: never modify inputs/ or reference/qa_leave/, and never edit
corrections.csv directly (use the aggregator). If this is a legitimate read,
rephrase so the protected path is not a write target.'

# 1) Redirection whose TARGET is a protected path:  > inputs/x   >> reference/qa_leave/y
#    (target immediately follows the redirect operator; a read piped elsewhere,
#     e.g. `grep x inputs/f > /tmp/out`, does NOT match.)
if printf '%s' "$cmd" | grep -Eq "(>>?|&>|>\|)[[:space:]]*[\"']?${prot}"; then
  deny "$reason"
fi
if printf '%s' "$cmd" | grep -Eq "(>>?|&>|>\|)[[:space:]]*[\"']?[^[:space:]]*${corr}"; then
  deny "$reason"
fi

# 2) In-place / destructive verbs referencing a protected path anywhere.
#    tee, sed -i, rm, rmdir, truncate, dd of=, chmod, chown, ln -s/-f target.
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
#    Copying/moving OUT of inputs/ (protected path as a source) stays allowed.
if printf '%s' "$cmd" | grep -Eq "\b(mv|cp|rsync|install)\b"; then
  last="$(printf '%s' "$cmd" | awk '{print $NF}' | tr -d '"'"'"'')"
  if printf '%s' "$last" | grep -Eq "^${prot}|${corr}$"; then
    deny "$reason"
  fi
fi

exit 0
