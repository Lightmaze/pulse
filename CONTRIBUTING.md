# Contributing

[Simplified Chinese](CONTRIBUTING.zh-CN.md)

Issues and focused pull requests are welcome. Pulse is pre-1.0
Research Alpha software released under Apache-2.0.

## Setup

```bash
uv sync --locked
uv run pytest -q
uv run python demo.py
uv build
```

Python 3.12 or newer is required. The default suite and demo must run without
an API key, network access, or paid request. Web changes additionally require:

```bash
cd web
npm ci
npm test
npm run build
```

Tests that call a live provider must use the `real` marker. Default pytest and
CI deselect them and never inject provider credentials.

## Change discipline

- Behaviour changes need a regression test.
- Changes to STDP or either numeric side path must identify the learning signal
  they implement.
- New learnable state is a specification change, not an ordinary feature.
- Preserve the separation between factory and field weights.
- Report uncertainty as uncertainty; an unavailable observation is not a
  negative result.
- Do not add experiment histories, provider transcripts, Goal/status/completion
  records, generated artifacts or local paths to the public tree.
- Keep pull requests focused and include the commands used for verification.

See [ESSENCE.md](ESSENCE.md), [ARCHITECTURE.md](ARCHITECTURE.md) and
[PUBLIC_RELEASE.md](PUBLIC_RELEASE.md) for the current engineering and release
boundaries.
