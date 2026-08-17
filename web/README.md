# Pulse Workbench

[Simplified Chinese](README.zh-CN.md)

Pulse Workbench is the browser interface shipped with Pulse. It is a
client of the runtime API, not an independent source of world or causal truth.

## Toolchain

- Node.js 24.15.0
- npm 11.12.1

```bash
npm ci
npm test
npm run build
```

For local development:

```bash
npm run dev
```

The development server listens at `http://localhost:5173`. A production build
is written to `web/dist/` and is served by the Pulse runtime.

## Modes

- Replay: open a compatible MetricsRecorder JSONL file.
- Live: read a bounded event stream from a running local runtime.
- Runtime workspace: inspect sessions, task and life centers, causal state,
  scheduling, Harness activity, and capability Profile.

Replay and live views share the same parsing and rendering path. The browser
does not write a second metrics or causal database.

## Localization

English and Simplified Chinese use separate resource modules:

```text
src/locales/en.ts
src/locales/zh-CN.ts
src/workbench/locales/en.ts
src/workbench/locales/zh-CN.ts
```

The active locale controls document semantics, page titles, selectors, and
localized copy. A language selector displays both choices in the active
language rather than mixing autonyms from two languages.

## Security

The Workbench stores a startup bearer token only in the current tab's
`sessionStorage`. It does not put the token in a URL, cookie, SQLite database,
metrics stream, or API response. The browser interface does not turn an
application Profile into an operating-system sandbox.

## Evidence boundary

Web unit tests and the production build are E1 engineering contracts. Replay,
browser, local HTTP, and visual checks do not prove a real provider, external
service, or longitudinal behaviour.
