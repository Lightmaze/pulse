# Pulse

[Simplified Chinese](README.zh-CN.md)

> Durable continuity for long-running agent teams.

Pulse is a single-host runtime for persistent agent teams built from Pi Harness
sessions. A `PulseWorld` owns durable scheduling, causal recovery,
cross-Engram content flow, task and life centers, succession, and a local
browser Workbench. Pi owns each Engram's model/tool loop and complete session.

This release is an engineering Research Alpha. It provides installable code,
deterministic contracts, and a reproducible release procedure. It does not claim
consciousness, autonomous life, real-provider validation, or longitudinal
experience-induced behaviour.

## What is included

- One single-host `PulseWorld` with SQLite state, an owner lease, and a fencing
  epoch.
- One persistent Pi Harness session per Engram, with bounded process residency
  and per-Engram failure isolation.
- Durable causal events, turns, generations, and explicit `uncertain` recovery
  for external outcomes that cannot be safely replayed.
- `TaskFront`, `TaskRelationship`, `TaskOffer`, activity-center, role-lease,
  living-concern, living-orientation, portfolio, and Purpose boundaries.
- Separate content, spectrum, and tunnel information flows.
- Reversible factory and field weight layers.
- A loopback REST API, capability Profiles, and a localized browser Workbench.
- Provider-free default tests, pinned toolchains, cold package installation,
  and public-release gates.

See [Capabilities](docs/CAPABILITIES.md) for the current engineering surface.

## Quick start without credentials

The locked release uses Python 3.12.13 and uv 0.11.1.

```bash
uv sync --locked --extra observatory
uv run python demo.py
uv run pulse --mock
```

`--mock` is an explicit offline test Harness. It does not validate Pi RPC or a
real model provider. Production startup fails closed when Pi cannot start; it
does not silently switch to the mock backend.

## Production Harness

A production run also needs the Pi Coding Agent executable:

```bash
npm install -g --ignore-scripts @earendil-works/pi-coding-agent
pi --version
uv sync --locked --extra observatory
uv run pulse
```

Provider, model, executable, Profile, storage, and worker limits are explicit:

```bash
uv run pulse --pi-provider deepseek --pi-model deepseek-v4-flash
uv run pulse --pi-executable /absolute/path/to/pi
uv run pulse --profile workspace
uv run pulse --db .pulse/run.db --port 8100 \
  --with-claustrum --with-router \
  --pulse-workers 4 --pi-resident-sessions 8
```

Windows PowerShell:

```powershell
uv run pulse --pi-executable C:\path\to\pi.cmd
```

The default `safe` Profile rejects HTTP state changes. `workspace` and `lab`
still require a startup bearer token, an exact Origin, and capability-specific
allowlists, approvals, and role leases. A Profile is an application boundary,
not an operating-system sandbox. See [Security](SECURITY.md).

## Workbench

The Web toolchain is pinned to Node.js 24.15.0 and npm 11.12.1.

```bash
cd web
npm ci
npm test
npm run build
cd ..
uv run pulse
```

The Workbench is served by the same runtime. English and Simplified Chinese
use independent locale resources, and the interface renders one locale at a
time. Browser, UI, local TCP, SQLite, and mock regressions remain engineering
contracts; they are not evidence for a real provider or external service.

## Runtime model

```text
one host
└── PulseWorld
    ├── runtime lease, scheduler, and causal ledger
    ├── TaskFront 0..N
    ├── Life Center 0..N
    └── Engram 1..N
        └── one persistent PiSession
```

Pi owns the model/tool loop and transcript of one Engram. `PulseWorld` owns
time, scheduling, propagation, delegation, relationships, durable recovery,
and succession across Engrams. The SQLite message index is not a replacement
for a Pi JSONL transcript.

See [Architecture](ARCHITECTURE.md), [Mechanism invariants](ESSENCE.md), and
[Canonical terms](docs/architecture/TERMS.md).

## Development and verification

```bash
uv sync --locked
uv run pytest -q
uv run python demo.py
uv build
bash scripts/release_check.sh
```

The default suite does not read provider credentials or make paid requests.
Checks that require an external system must remain separate, explicit, and
operator-authorized.

## Claim boundary

- E0: an implementation path exists.
- E1: deterministic provider-free contracts pass.
- E2: an authorized real provider or selected external system is observed.
- E3: a clean checkout can be installed, built, and reproduced.
- E4: long-running counterfactual evidence shows persistent behavioural change.

This public Alpha claims E0/E1 engineering coverage and an E3 release
procedure. It publishes no E2 evidence archive and makes no E4 claim. See
[Capability evidence](docs/evidence/README.md) and
[Public release boundary](PUBLIC_RELEASE.md).

## Known limitations

- Single host, single operator, and pre-1.0.
- No complete filesystem, network, kernel, or credential sandbox.
- No public claim of real-provider, external MCP, interactive PTY, or
  long-duration capacity validation.
- No public claim of consciousness, autonomous life, stable interest formation,
  or longitudinal causality.
- External Pi/provider artifacts and provider costs are outside this repository's
  reproducible supply chain.

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Mechanism invariants](ESSENCE.md)
- [Capabilities](docs/CAPABILITIES.md)
- [Capability evidence](docs/evidence/README.md)
- [Public release boundary](PUBLIC_RELEASE.md)
- [Security](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
