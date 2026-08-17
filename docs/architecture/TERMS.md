# Canonical Terms

[Simplified Chinese](TERMS.zh-CN.md)

Status: current for `pulse-terms.v1`.

Stable object and protocol names remain in English when translating them would
change identity. Ordinary interface prose should still be localized.

| Canonical term | Plain meaning | Not equivalent to |
|---|---|---|
| PulseWorld | one persistent world and control plane shared by multiple Engrams | a chat window or one task |
| Engram | one persistent cognitive participant in a PulseWorld | a browser tab, process, or model context |
| EngramLineage | identity references that continue across succession | the current process |
| EngramGeneration | one finite generation within a lineage | the whole lineage |
| PiSession | the provider/model session used by one generation | SQLite world state |
| TaskOffer | proposed task terms awaiting a subject decision | accepted work |
| TaskRelationship | subject-owned participation state after acceptance | permanent authority |
| TaskFront | durable user-facing entry for one task | a new subject |
| Life Center | durable non-task activity surface | a mandatory task queue |
| RoleLease | scoped, expiring, revocable authority | permanent administrator status |
| Purpose | versioned subject-held statement that may remain unset | a system permission or forced objective |
| WorldTick | low-cost advancement of clocks, queues, and resource state | thought or a model call |
| Pulse | one world-level allocation that changes durable state | every scheduler tick |
| PiTurn | one input-to-settle model/tool loop | one API token or tool call |
| Harness | execution substrate for session, tool, process, and recovery boundaries | evidence for a scientific claim |
| Worker | bounded execution role or process in the runtime | a human employee |
| Profile | application-level capability ceiling | an operating-system sandbox |
| uncertain | external work may have occurred but cannot be confirmed | a softer spelling of success or failure |
| cold build | build from a clean directory with locked dependencies | an incremental build using old outputs |
| longitudinal causality | counterfactual long-run evidence that a mechanism changes later behaviour | code existence or one demonstration |

## Data authority

Pi JSONL is authoritative for complete model/tool sessions. SQLite is
authoritative for world state, relationships, authorization, causal terminals,
and recovery.

## Version notation

- Product and Web version: `0.2.0-alpha.1`
- Python package version: `0.2.0a1`

They identify the same release candidate.
