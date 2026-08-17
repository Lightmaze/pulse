# Security Policy

[Simplified Chinese](SECURITY.zh-CN.md)

Pulse is pre-1.0 Research Alpha software for a single operator on a
trusted host. It has real process, file, HTTP, and optional external-tool
capabilities. Default-off is not the same as absent.

## Reporting a vulnerability

Do not publish a working exploit, credential, private session, or sensitive
path in a public issue. Contact the maintainer privately through the security
contact exposed by the repository host. Include:

- the affected version or commit;
- the deployment Profile and operating system;
- the capability and boundary that were crossed;
- a minimal reproduction with secrets and personal paths removed;
- whether the issue has been disclosed elsewhere.

This is a single-maintainer Alpha and does not promise a response deadline.

## Supported surface

- The public `0.2.0-alpha.1` release and later explicitly supported candidates.
- Windows-first, single-operator, loopback deployment.
- Public code and published artifacts only; private development history is not
  part of the support surface.

## Capability Profiles

| Profile | HTTP mutation | Startup token | Command/process capability |
|---|---|---|---|
| `safe` | rejected | no mutation access | external command and background-process configuration rejected |
| `workspace` | existing write routes may be reached | required | explicitly configured bounded workspace capability |
| `lab` | existing write routes may be reached | required | explicitly configured bounded process capability |

A Profile is a ceiling, not a grant. Selecting `lab` does not enable a tool by
itself. Purpose text, model output, and user-interface state cannot change a
Profile, role lease, file boundary, network boundary, or approval decision.

## HTTP boundary

- The service binds to `127.0.0.1` by default.
- Non-loopback binding requires both `--allow-network-bind` and at least one
  explicit exact Origin.
- CORS rejects wildcard, null, credential-bearing, path-bearing, query-bearing,
  and fragment-bearing Origins.
- State-changing routes under `workspace` or `lab` require the bearer token
  generated for that process start.
- Token comparisons use constant-time comparison.
- The Workbench stores the token only in the current tab's `sessionStorage`.
- Health and Profile endpoints do not return local absolute paths or tokens.

GET routes are unauthenticated by default. Exact CORS limits ordinary browser
access but does not stop another local process from making direct requests.

## Process and workspace boundary

- Workspace writes require an explicit root and checkpoint policy.
- Protected paths, command allowlists, tool allowlists, approvals, and role
  leases are enforced independently.
- Process containment is used to converge child processes during shutdown.
- Process containment is not a complete filesystem, network, kernel, or
  credential sandbox.
- Library callers that construct the runtime directly must apply an equivalent
  or stricter external policy.

## Secrets and sensitive data

Do not commit or publish:

- provider keys, startup tokens, cookies, credentials, or private endpoints;
- raw Pi sessions, prompts, tool results, or databases;
- local absolute paths, personal workspace names, or generated run artifacts;
- private operator-authorized external evidence.

Use a least-privilege operating-system account, a dedicated workspace, and
project-specific provider credentials with spending limits.

## In scope

- Bypass of a Profile, token, exact Origin, non-loopback opt-in, workspace root,
  protected path, checkpoint, allowlist, approval, role lease, task relationship,
  owner lease, or fencing epoch.
- A credential, session, tool result, local path, or private payload entering a
  log, database, response, release artifact, or error where it does not belong.
- A child process that can continue external side effects after runtime shutdown.
- A dependency vulnerability reachable from published code.

## Research risks, not security guarantees

- Model error, bias, and hallucination are not solved by these boundaries.
- Prompt injection that changes text but crosses no tool or authorization
  boundary remains a research risk.
- An operator-approved action may modify files or incur cost within its declared
  permission.
- Mock output is fictional and must not be presented as real-provider evidence.
- `sessionStorage` cannot protect a token from malicious same-origin script.
- Alpha software should not process untrusted content while important personal
  credentials are available to the same process.

See [Architecture](ARCHITECTURE.md), [Capabilities](docs/CAPABILITIES.md), and
[Public release boundary](PUBLIC_RELEASE.md).
