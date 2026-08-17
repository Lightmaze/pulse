// @ts-nocheck

import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";

globalThis.window = {
  location: {
    origin: "http://pulse.test",
    href: "http://pulse.test/",
    hash: "",
    search: "",
  },
};
globalThis.document = { documentElement: { lang: "en" }, hidden: false };

const vite = await createServer({
  root: fileURLToPath(new URL("../", import.meta.url)),
  logLevel: "silent",
  server: { middlewareMode: true },
  appType: "custom",
});
const {
  fetchRuntimeShutdown,
  parseRuntimeShutdown,
  runtimeShutdownLifecycle,
} = await vite.ssrLoadModule("/src/runtimeShutdown.ts");
await vite.close();

function openBody(overrides: Record<string, unknown> = {}) {
  return {
    protocol_version: "runtime-shutdown.v1",
    shutdown_id: null,
    phase: "open",
    started_at: null,
    finished_at: null,
    timeout_seconds: null,
    elapsed_seconds: 0,
    deadline_exhausted: false,
    admission_frozen: false,
    durable_recovery: "not_attempted",
    publication_fence: "active",
    owner_lease: "not_attempted",
    control_plane_closed: false,
    contract_satisfied: false,
    clean: false,
    physical_exit_proven: false,
    escaped_count: 0,
    storage_state: "open",
    components: [],
    ...overrides,
  };
}

function uncertainComponent(overrides: Record<string, unknown> = {}) {
  return {
    component: "pi_gateway",
    effect: "uncertain",
    owner: "escaped",
    process_tree: "root_exit_only",
    cancel: "signalled",
    started_at: "2026-08-06T12:00:00+00:00",
    finished_at: "2026-08-06T12:00:00.200000+00:00",
    elapsed_seconds: 0.2,
    active_before: 1,
    unresolved: 1,
    escaped: true,
    physical_exit_proven: false,
    clean: false,
    error_code: "shutdown_deadline_exhausted",
    detail: "bounded evidence only",
    ...overrides,
  };
}

function closingBody(overrides: Record<string, unknown> = {}) {
  return {
    protocol_version: "runtime-shutdown.v1",
    shutdown_id: "fedcba9876543210fedcba9876543210",
    phase: "fencing",
    started_at: "2026-08-06T12:00:00+00:00",
    finished_at: null,
    timeout_seconds: 0.2,
    elapsed_seconds: 0.1,
    deadline_exhausted: false,
    admission_frozen: true,
    durable_recovery: "not_attempted",
    publication_fence: "revoked",
    owner_lease: "not_attempted",
    control_plane_closed: false,
    contract_satisfied: false,
    clean: false,
    physical_exit_proven: false,
    escaped_count: 0,
    storage_state: "open",
    components: [],
    ...overrides,
  };
}

function closedBody(overrides: Record<string, unknown> = {}) {
  return {
    protocol_version: "runtime-shutdown.v1",
    shutdown_id: "0123456789abcdef0123456789abcdef",
    phase: "closed",
    started_at: "2026-08-06T12:00:00+00:00",
    finished_at: "2026-08-06T12:00:00.200000+00:00",
    timeout_seconds: 0.2,
    elapsed_seconds: 0.2,
    deadline_exhausted: true,
    admission_frozen: true,
    durable_recovery: "timed_out",
    publication_fence: "revoked",
    owner_lease: "release_pending",
    control_plane_closed: true,
    contract_satisfied: true,
    clean: false,
    physical_exit_proven: false,
    escaped_count: 1,
    storage_state: "retained_for_escaped_workers",
    components: [uncertainComponent()],
    ...overrides,
  };
}

test("parseRuntimeShutdown accepts open, closing, and closed lifecycle evidence", () => {
  const open = parseRuntimeShutdown(openBody());
  const closing = parseRuntimeShutdown(closingBody());
  const closed = parseRuntimeShutdown(closedBody());

  assert.equal(runtimeShutdownLifecycle(open), "open");
  assert.equal(runtimeShutdownLifecycle(closing), "closing");
  assert.equal(runtimeShutdownLifecycle(closed), "closed");
  assert.equal(closed.publication_fence, "revoked");
  assert.equal(closed.contract_satisfied, true);
  assert.equal(closed.clean, false);
  assert.equal(closed.physical_exit_proven, false);
  assert.equal(closed.escaped_count, 1);
  assert.deepEqual(
    {
      effect: closed.components[0].effect,
      owner: closed.components[0].owner,
      process_tree: closed.components[0].process_tree,
      cancel: closed.components[0].cancel,
    },
    {
      effect: "uncertain",
      owner: "escaped",
      process_tree: "root_exit_only",
      cancel: "signalled",
    },
  );
});

test("parser ignores additive and sensitive fields instead of retaining a render path", () => {
  const parsed = parseRuntimeShutdown(closedBody({
    prompt: "SECRET_PROMPT",
    command: "SECRET_COMMAND",
    path: "SECRET_PATH",
    token: "SECRET_TOKEN",
    stdout: "SECRET_STDOUT",
    future_evidence: { kind: "additive" },
    components: [
      uncertainComponent({
        prompt: "SECRET_COMPONENT_PROMPT",
        stdout: "SECRET_COMPONENT_STDOUT",
      }),
    ],
  }));
  const serialized = JSON.stringify(parsed);

  assert.equal(Object.hasOwn(parsed, "prompt"), false);
  assert.equal(Object.hasOwn(parsed.components[0], "detail"), false);
  assert.doesNotMatch(serialized, /SECRET_/);
});

test("parser rejects incompatible versions, enum drift, and inconsistent derived claims", () => {
  assert.throws(
    () => parseRuntimeShutdown(openBody({ protocol_version: "runtime-shutdown.v2" })),
    /protocol_version/,
  );
  assert.throws(
    () => parseRuntimeShutdown(openBody({ admission_frozen: true })),
    /open snapshot/,
  );
  assert.throws(
    () => parseRuntimeShutdown(closedBody({ publication_fence: "cancelled" })),
    /publication_fence/,
  );
  assert.throws(
    () => parseRuntimeShutdown(closedBody({
      components: [uncertainComponent({ clean: true })],
    })),
    /components\[0\]\.clean/,
  );
  assert.throws(
    () => parseRuntimeShutdown(closedBody({ clean: true })),
    /\$\.clean/,
  );
  assert.throws(
    () => parseRuntimeShutdown(closedBody({ physical_exit_proven: true })),
    /physical_exit_proven/,
  );
  assert.throws(
    () => parseRuntimeShutdown(closedBody({ escaped_count: 0 })),
    /escaped_count/,
  );
});

test("fetchRuntimeShutdown performs a read-only GET against the canonical route", async () => {
  const originalFetch = globalThis.fetch;
  const calls: Array<{ input: string; method: string }> = [];
  globalThis.fetch = async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({
      input: String(input),
      method: init?.method ?? "GET",
    });
    return new Response(JSON.stringify(openBody()), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  try {
    const snapshot = await fetchRuntimeShutdown(
      "http://pulse.test/",
      new AbortController().signal,
    );
    assert.equal(snapshot.phase, "open");
    assert.deepEqual(calls, [{
      input: "http://pulse.test/runtime/shutdown",
      method: "GET",
    }]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
