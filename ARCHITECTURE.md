# Pulse 0.2 Alpha Architecture

[Simplified Chinese](ARCHITECTURE.zh-CN.md)

Status: current public architecture for `0.2.0-alpha.1`.

This document describes the shipped runtime. It is not a development diary,
experiment report, or roadmap.

## 1. Deployment boundary

The public deployment model is one host, one `PulseWorld`, multiple Engrams,
and one persistent `PiSession` per Engram generation.

```text
host
└── PulseWorld / RuntimeService
    ├── owner lease and fencing epoch
    ├── scheduler and PulseEngine
    ├── causal ledger and generation state
    ├── TaskFront 0..N
    ├── Life Center 0..N
    ├── Engram network and numeric control paths
    └── PiHarnessRuntime
        ├── Engram A ── PiSession A
        ├── Engram B ── PiSession B
        └── Engram N ── PiSession N
```

`RuntimeService` is the implementation of the current `PulseWorld`. A browser
connection, user task, or `TaskFront` is an observation and intervention entry;
it does not create a second world.

## 2. Responsibility layers

| Layer | Responsibility | Owner |
|---|---|---|
| Model/tool exchange | model calls, tool calls, results, stop conditions | Pi |
| Engram turn | keep one Engram running until its model/tool loop settles | Pi session with Pulse boundary checks |
| World activity | time, admission, scheduling, propagation, delegation, recovery, succession | PulseWorld |

A `Pulse` is a world-level allocation that may admit one complete Pi turn. A
turn can contain several model/tool exchanges and must not stop between a tool
call and its result. A scheduler tick is not evidence that an Engram thought.

## 3. Persistent session lifecycle

1. The runtime opens SQLite and acquires the owner lease before recovery or
   process startup.
2. Stored causal, generation, reservation, and Harness bindings are recovered.
3. A Pi process starts lazily when its Engram first needs model computation.
4. Only new natural-language input is submitted to that persistent session.
5. A result is projected or propagated only after terminal and persistence
   checks settle.
6. Turns are serialized per Engram; different Engrams use a bounded worker pool.
7. Idle processes may leave the resident pool without deleting their durable
   session bindings.
8. Shutdown stops admission, joins workers, closes transports, releases the
   matching lease, and then closes SQLite.

Process lifetime and session continuity are therefore separate. Restart can
restore a materialized Pi session; succession creates a new generation and
session while preserving lineage references.

## 4. Information-flow boundaries

| Flow | Carries | Does not carry |
|---|---|---|
| Content | final natural language between Engrams | tool envelopes or full transcripts |
| Spectrum | numeric modulation of timing and propagation | prompts, natural language, or permissions |
| Tunnel | directed natural-language work requests and results | a temporary identity or bypass around the Harness |

Machine contracts validate these boundaries before durable causal insertion.
Structured tool traces remain inside the originating Engram session.

## 5. Durable truth and projections

| Data | Authoritative source |
|---|---|
| Complete model/tool session | Pi JSONL |
| World, relationships, leases, causal states, scheduling | SQLite |
| Browser views | read-only or explicitly authorized projections from the runtime |
| Numeric factory baselines | versioned package data and code defaults |
| Field overrides | operator-managed export/checkpoint state |

The browser is not a second source of truth. It does not reconstruct a complete
causal chain by accumulating whichever events happen to be visible.

## 6. Recovery and external uncertainty

Durable operations use explicit terminal states. If an external action may have
occurred but its result cannot be confirmed, the state is `uncertain`. Restart
does not automatically replay that action. An operator must reconcile it or
choose a new action with a new identity.

The owner lease and fencing epoch prevent an old runtime owner from committing
new state after ownership changes. Role leases, task relationships, capability
Profiles, and approvals remain separate authorization boundaries.

## 7. Task and life surfaces

- `TaskOffer` records proposed terms before a subject accepts work.
- `TaskRelationship` records the subject-owned participation state.
- `TaskFront` is the durable user-facing entry for one task.
- A life center holds non-task activity that may remain quiet.
- `LivingConcern`, `LivingOrientation`, portfolio state, and Purpose revisions
  remain durable without turning all life into a task.

These objects can refer to the same Engram without duplicating its identity or
Pi session.

## 8. Learning state

The release contains three isolated numeric learning paths:

| Path | Signal | Scope |
|---|---|---|
| Connection timing | local temporal co-occurrence | Engram connection weights |
| Delegation routing | relative outcome feedback | delegation router |
| Claustrum modulation | bounded global activation feedback | timing and propagation controls |

Factory weights are shipped baselines. Field weights are local overlays that
can be exported, checkpointed, rolled back, or reset. These paths do not modify
provider model parameters or Pi transcripts.

## 9. API, Profiles, and Workbench

The default service binds to loopback. State-changing HTTP routes require a
non-default Profile and a startup token. Non-loopback binding and browser
origins require explicit configuration.

The Workbench reads runtime state and offers bounded intervention surfaces. Its
English and Simplified Chinese catalogs are separate; the active locale controls
all localized labels. UI regression tests remain engineering contracts, not
external-system evidence.

## 10. Public evidence boundary

The public repository includes the current implementation, deterministic
contracts, and release procedure. It excludes private planning records,
operator-authorized external sessions, raw research data, and generated run
artifacts. The public evidence table makes no E2 or E4 claim.

See [Mechanism invariants](ESSENCE.md), [Canonical terms](docs/architecture/TERMS.md),
[Capabilities](docs/CAPABILITIES.md), and [Public release boundary](PUBLIC_RELEASE.md).
