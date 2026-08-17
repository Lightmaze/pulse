# Pulse 0.2 Alpha capabilities

[Simplified Chinese](CAPABILITIES.zh-CN.md)

This document describes the current public engineering surface. It is not a
development diary or a record of how the implementation was reached.

## Shipped

- One fenced, single-host `PulseWorld` with persistent SQLite state.
- One durable Pi Harness session per Engram, with bounded live-process
  residency and per-Engram failure isolation.
- Durable causal events, turns and generations with explicit `uncertain`
  recovery instead of automatic replay.
- TaskFront, ActivityCenter, TaskOffer, TaskRelationship and RoleLease
  boundaries.
- LivingConcern, LivingOrientation, Living Portfolio and settlement-fenced
  purpose amendments.
- Content, spectrum and tunnel flows with separate machine contracts.
- Factory and field weight layers with export/checkpoint/reset/rollback.
- Local REST API, safe/workspace/lab Profiles and browser Workbench.
- Exact build pins, no-key CI and cold-install verification for wheel and sdist.

## Contract evidence

The default suite exercises the published code without provider credentials.
Representative verification paths include:

- `tests/test_pi_harness.py` and `tests/test_runtime_harness_lifecycle.py`;
- `tests/test_causal_ledger.py` and `tests/test_generation_recovery.py`;
- `tests/test_task_relationship_api.py` and `tests/test_harness_role_leases.py`;
- `tests/test_process_containment.py` and `tests/test_api_security.py`;
- `tests/test_storage_migrations.py` and `tests/test_release_contract.py`;
- Web unit tests and the production Web build.

The machine-readable source is
[`evidence/capability-evidence.v1.json`](evidence/capability-evidence.v1.json).

## Not claimed

- Consciousness, sentience or complete autonomous life.
- Longitudinal experience-induced interests or E4 causality.
- Published E2 proof for a real provider, external MCP or full provider-to-UI
  chain.
- Multi-host coordination, distributed consensus or untrusted multi-tenancy.
- A complete operating-system sandbox, interactive PTY or unrestricted access
  to personal files, networks or accounts.
- Reproducibility of an external Pi/provider release artifact or its cost.

Provider-free, mock, browser/UI, local TCP/SQLite and operating-system
regression tests remain E1 contracts.
