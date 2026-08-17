# Capability Evidence

[Simplified Chinese](README.zh-CN.md)

Status: current machine-readable claim boundary.

[`capability-evidence.v1.json`](capability-evidence.v1.json) is the authoritative
public capability table.

## Levels

- E0 — Exists: an implementation path or data structure exists.
- E1 — Contract: deterministic provider-free contracts pass.
- E2 — Live: an authorized real provider or operator-selected external system
  or service is actually observed.
- E3 — Release: clean checkout, pinned build, installation, upgrade, and rollback
  procedures are reproducible.
- E4 — Longitudinal causality: long-running counterfactual observation shows
  that a mechanism persistently changes later behaviour.

Provider-free tests, browser and UI checks, local TCP, SQLite, mock providers,
and operating-system regressions remain E1. They do not become E2 by being
combined or repeated.

## Status values

- `VERIFIED`
- `FAILED`
- `BLOCKED_BY_ENVIRONMENT`
- `NOT_CLAIMED`
- `NOT_APPLICABLE`

The first public release does not distribute provider sessions or research
archives. Every public E2 field therefore remains `NOT_CLAIMED`, and no E4
claim is made.
