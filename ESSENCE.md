# Pulse Mechanism Invariants

[Simplified Chinese](ESSENCE.zh-CN.md)

Status: public mechanism boundary for `0.2.0-alpha.1`.

These invariants describe what the implementation must preserve. They are not
claims of consciousness, autonomous life, or longitudinal causality.

## World and identity

1. One host owns one active `PulseWorld` for a given database and fencing epoch.
2. An Engram is not a browser tab, task, process, or model context.
3. Each Engram generation owns one persistent Pi session; session continuity is
   distinct from process lifetime.
4. Succession may create a new generation and session, but must retain lineage
   and causal references to the previous generation.

## Time and computation

5. A scheduler tick advances bounded world machinery; it does not prove that an
   Engram thought.
6. A `Pulse` is a world-level allocation. If model computation is admitted, one
   complete Pi turn settles before its result is published.
7. A model/tool exchange cannot be split between a tool call and its result.
8. Concurrency is bounded across Engrams and serialized within one Engram.

## Information boundaries

9. Content flow carries final natural language, not tool envelopes or full
   transcripts.
10. Spectrum flow changes timing and strength, not content or permissions.
11. Tunnel flow carries directed natural-language requests and results through
    an existing target session; it does not create a temporary identity.
12. Structured provider and tool traces stay within their originating session.

## Persistence and recovery

13. Pi JSONL is authoritative for a model/tool session. SQLite is authoritative
    for world state, relationships, leases, causal terminals, and recovery.
14. External work that may have occurred but cannot be confirmed becomes
    `uncertain` and is not automatically replayed after restart.
15. An old owner or fencing epoch must never commit new world state.
16. Shutdown must stop admission before releasing ownership and storage.

## Authorization

17. Capability Profiles, startup tokens, exact browser Origins, allowlists,
    approvals, role leases, and task relationships are separate boundaries.
18. Purpose text, model output, or a user-interface label cannot grant a
    capability.
19. The default Profile must fail closed for state-changing HTTP routes.

## Learning state

20. Connection, delegation, and claustrum learning paths remain numerically and
    semantically distinct.
21. Factory weights are versioned baselines; field weights are reversible local
    overlays.
22. Learned numeric state must not rewrite provider model parameters or session
    transcripts.

## Evidence

23. Code existence is E0, deterministic contracts are E1, authorized external
    observation is E2, reproducible release procedure is E3, and long-running
    counterfactual causality is E4.
24. UI, local TCP, SQLite, mock, and operating-system regressions remain E1.
25. A lower evidence level must never be presented as a higher one.

See [Architecture](ARCHITECTURE.md), [Capabilities](docs/CAPABILITIES.md), and
[Capability evidence](docs/evidence/README.md).
