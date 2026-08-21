# ADR-0002 — A sandboxed tool runs isolated or does not run

- Status: accepted
- Date: 2026-08-21
- Milestone: M4 (`SBX-001`), with the CI half in M2 (`CI-001`)

## Context

`AGENTS.md` states: "Where no backend exists, a sandboxed tool fails rather
than running unsandboxed." The registry does not enforce this.
`tools/registry.rs` guards the isolated path with `if let Some(runner)` and
supplies **no `else`**, so when no isolated executor is configured, control
falls through to the in-process implementation body. The tool's declared
`SandboxMode` is silently discarded.

The blast radius is narrower than "shell runs unsandboxed" (see §4.2 of the
remediation plan): `ShellTool::call` builds its own `Sandbox` and passes it to
`exec::run`, where `harden` is fatal on failure, so the shipped shell tool
self-hardens regardless of the registry. The genuine exposure is (i) any tool
that declares a `SandboxMode` and does its work in-process, where there is no
child to harden, and (ii) MCP tools configured with a sandbox mode, which the
registry routes to `agentos-tool-worker`, a 43-line binary that rejects every
tool but `"shell"` — with no handshake that would let the registry discover
that before dispatching.

Separately, the two backends report availability with unequal rigour. Linux
issues a real `landlock_create_ruleset` version syscall. macOS returns
`Enforced("seatbelt")` from a bare `Path::exists` on `/usr/bin/sandbox-exec`.

## Decision

**A declared `SandboxMode` other than `full_access` is a precondition, not a
preference.** When no compatible isolated executor exists, the call fails with
a typed error before the tool's implementation body is reached. There is no
fallthrough path, and the absence of an `else` branch is itself the bug class
this ADR exists to close.

**Executor compatibility is negotiated, not assumed.** The worker gains a
handshake reporting the tool protocols it can execute and the sandbox
mechanisms it can enforce. The registry checks that at registration where it
can and at invocation always. `ToolRegistryError` gains variants for "no
compatible isolated executor", "executor cannot run this tool protocol", and
"sandbox execution failed" — three distinct conditions that today share the
same silent fallthrough.

**Availability must be probed, not inferred.** A backend reports `Enforced`
only after exercising the mechanism, not after observing that a file exists.

**A skip is not a pass on a supported target.** On Linux and macOS an
unavailable sandbox fails the test. The single named exception is
`AGENTOS_ALLOW_UNSANDBOXED_TESTS=1`, which CI must never set. (Implemented in
M0; recorded here because it is the same decision.)

## Consequences

- A deployment that configures a sandboxed tool without an executor gets a
  startup or first-call failure instead of silent full access. This will
  surface as a regression in environments that were relying on the
  fallthrough without knowing it.
- The worker grows a protocol. It is currently an opaque stdin/stdout JSON
  subprocess, which is why no compatibility check is possible today.
- `full_access` becomes the honest declaration for in-process tools. Declaring
  a narrower mode on a tool that never spawns a child is a claim the kernel is
  not making, and now fails rather than being quietly ignored.

## Verification

- A mock tool declaring `read_only`, registered with no compatible executor:
  its body is never invoked, and the caller receives the typed error naming
  the missing executor.
- A worker that reports it cannot run a tool's protocol produces a distinct
  error from a worker that is absent.
- Real Landlock and Seatbelt CI jobs exercise enforced `read_only` and
  `workspace_write` profiles against a directory the runner can genuinely
  write to.
- The macOS availability probe reports `Unavailable` on a machine where
  `sandbox-exec` exists but cannot apply a profile.
