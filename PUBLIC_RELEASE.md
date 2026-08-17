# Pulse 0.2 Alpha public release boundary

[Simplified Chinese](PUBLIC_RELEASE.zh-CN.md)

This repository publishes a product-oriented engineering snapshot of Pulse
Substrate. It contains the installable single-host runtime, Workbench, current
documentation, deterministic contracts, and self-contained acceptance tooling.

It does not publish the private development archive or the project's research
record. Removing a file from the latest tree is insufficient because Git keeps
parent commits reachable; this first public release is therefore created as a
single clean root commit.

## Claim boundary

- **E0 — Exists:** an implementation path exists;
- **E1 — Contract:** deterministic, provider-free contracts pass;
- **E2 — Live:** an authorised real provider or operator-selected external
  system has been observed;
- **E3 — Release:** a clean checkout can be installed, built, and reproduced;
- **E4 — Longitudinal causality:** long-running counterfactual observation
  shows that experience persistently changes later behaviour.

The public 0.2 Alpha claims E0/E1 engineering coverage and an E3 release
procedure. It publishes no E2 evidence archive and makes no E4 claim. Browser,
UI, local TCP/SQLite, mock-provider, and operating-system regression checks
remain E1.

## Included

- `src/`: the Python runtime, Harness boundary, storage, API and weight tools;
- `web/`: the browser Workbench;
- `tests/`: provider-free contracts needed to verify the published code;
- `.github/workflows/ci.yml`: no-key Windows and Ubuntu continuous integration;
- the current README, architecture, capability/evidence, security,
  contribution and licence documents;
- the acceptance and language-boundary scripts needed to verify a clean clone;
- pinned Python, uv, Node and npm metadata.

## Deliberately excluded

- experiment reports, preregistrations, example readings and research notes;
- private planning graphs, execution plans, status logs, completion reports,
  dated external evidence and review archives;
- historical experiment code or configuration tied to private commits;
- raw transcripts, recorded provider output, databases, generated artifacts,
  local paths, internal coordination notes and private release scratchpads;
- credentials, provider configuration and unlicensed assets;
- operator-authorized external execution records.

The authoritative source repository may retain those materials privately for
governance and traceability. They are not copied into the public tree, source
distribution, tags, or reachable public Git history.

## Provenance and clean history

`PUBLIC_SOURCE.json` records a digest and file count for the exported public
snapshot. It does not publish private Git object identifiers. It also records
`public_history_mode=clean_root` and `source_history_included=false`.

The public-history gate requires one root commit, the
`v0.2.0-alpha.1` tag, the project noreply identity, and the intended origin. It
rejects parent commits and earlier tags.

## Verifying a clean clone

Run `uv sync --locked`, `uv run pytest -q`, `uv build`, and
`bash scripts/release_check.sh`. The acceptance script checks the current
repository by default and treats a missing Git tree as a failure.
