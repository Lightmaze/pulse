// @ts-nocheck

import assert from "node:assert/strict";
import test from "node:test";

import {
  HarnessFault,
  fetchHarnessTerminalSession,
  fetchHarnessTerminalOutput,
  fetchHarnessTerminalSessions,
  stopHarnessTerminalSession,
  subscribeHarnessTurn,
} from "./harness.ts";

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function sessionRow(overrides: Record<string, unknown> = {}) {
  return {
    terminal_session_id: "terminal-session-1",
    turn_id: "turn-1",
    world_id: "world-1",
    engram_id: "engram-1",
    epoch: 7,
    mode: "PIPE_SESSION",
    transport: "pipe",
    session_scope: "runtime_connection",
    state: "RUNNING",
    cwd_relative: "src",
    command_digest: "c".repeat(64),
    started_at: "2026-08-04T12:01:00Z",
    ended_at: null,
    exit_code: null,
    output_bytes: 10,
    output_truncated: false,
    last_output_seq: 3,
    launch_action_digest: "d".repeat(64),
    evidence_class: "FAKE_RPC_CONTRACT",
    sandbox_evidence: "LIVE_GATE_UNVERIFIED",
    tree_containment: "UNVERIFIED",
    error_code: null,
    uncertain_reason: null,
    capabilities: { stdin: true, resize: true, reconnect: true, stop: true },
    command: "must not cross",
    argv: ["private"],
    pid: 23108,
    environment: { TOKEN: "private" },
    ...overrides,
  };
}

test("terminal session client never unlocks v1 stdin/resize or exposes process details", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  let requestedUrl = "";
  globalThis.fetch = async (input) => {
    requestedUrl = String(input);
    return response({
      sessions: [sessionRow()],
      count: 1,
      evidence_class: "FAKE_RPC_CONTRACT",
    });
  };

  const page = await fetchHarnessTerminalSessions("http://pulse.test/", "turn-1", 999);

  assert.match(requestedUrl, /limit=64/);
  assert.deepEqual(page.sessions.map((item) => item.terminal_session_id), [
    "terminal-session-1",
  ]);
  assert.deepEqual(page.sessions[0].capabilities, {
    stdin: false,
    resize: false,
    reconnect: true,
    stop: true,
  });
  assert.equal("command" in page.sessions[0], false);
  assert.equal("pid" in page.sessions[0], false);
});

test("terminal session client fails closed on missing, cross-turn, or duplicate identity", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  const invalidLists = [
    [{ state: "RUNNING", command: "missing id" }],
    [sessionRow({ turn_id: "turn-2" })],
    [sessionRow(), sessionRow()],
  ];

  for (const sessions of invalidLists) {
    globalThis.fetch = async () =>
      response({ sessions, evidence_class: "FAKE_RPC_CONTRACT" });
    await assert.rejects(
      fetchHarnessTerminalSessions("http://pulse.test", "turn-1"),
      (error: unknown) =>
        error instanceof HarnessFault &&
        error.code === "terminal_session_projection_invalid",
    );
  }
});

test("terminal output client preserves structured retention gap and advances only to returned data", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  let requestedUrl = "";
  globalThis.fetch = async (input) => {
    requestedUrl = String(input);
    return response({
      terminal_session_id: "terminal-session-1",
      output: [
        {
          terminal_session_id: "terminal-session-1",
          seq: 3,
          stream: "stdout",
          text: "retained",
          byte_count: 8,
          at: "2026-08-04T12:01:01Z",
        },
      ],
      earliest_seq: 3,
      next_seq: 3,
      has_more: false,
      gap: { missing_from: 1, missing_to: 2, reason: "retention_window" },
      evidence_class: "FAKE_RPC_CONTRACT",
    });
  };

  const page = await fetchHarnessTerminalOutput(
    "http://pulse.test",
    "terminal-session-1",
    "turn-1",
    0,
    900,
  );

  assert.deepEqual(page.output.map((item) => item.seq), [3]);
  assert.match(requestedUrl, /turn_id=turn-1/);
  assert.equal(page.next_seq, 3);
  assert.deepEqual(page.gap, {
    missing_from: 1,
    missing_to: 2,
    reason: "retention_window",
  });
});

test("terminal output client fails closed on undeclared holes, duplicates, and descending chunks", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  const chunk = (seq: number) => ({
    terminal_session_id: "terminal-session-1",
    seq,
    stream: "stdout",
    text: `chunk-${seq}`,
  });
  const invalidPages = [
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(2)],
      earliest_seq: 1,
      gap: null,
    },
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(1), chunk(3)],
      earliest_seq: 1,
      gap: null,
    },
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(1), chunk(1)],
      earliest_seq: 1,
      gap: null,
    },
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(2), chunk(1)],
      earliest_seq: 1,
      gap: null,
    },
  ];

  for (const body of invalidPages) {
    globalThis.fetch = async () => response({ ...body, next_seq: 999 });
    await assert.rejects(
      fetchHarnessTerminalOutput(
        "http://pulse.test",
        "terminal-session-1",
        "turn-1",
      ),
      (error: unknown) =>
        error instanceof HarnessFault &&
        error.code === "terminal_output_projection_invalid",
    );
  }

  globalThis.fetch = async () =>
    response({
      terminal_session_id: "terminal-session-1",
      output: [chunk(1), chunk(3)],
      earliest_seq: 1,
      gap: null,
    });
  await assert.rejects(
    fetchHarnessTerminalOutput(
      "http://pulse.test",
      "terminal-session-1",
      "turn-1",
      0,
      1,
    ),
    (error: unknown) =>
      error instanceof HarnessFault &&
      error.code === "terminal_output_projection_invalid",
  );
});

test("terminal output client accepts declared gaps with an exact advertised cursor", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  const chunk = (seq: number) => ({
    terminal_session_id: "terminal-session-1",
    seq,
    stream: "stdout",
    text: `chunk-${seq}`,
  });
  const pages = [
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(3), chunk(4)],
      earliest_seq: 3,
      next_seq: 4,
      gap: { missing_from: 1, missing_to: 2, reason: "retention_window" },
      expectedNextSeq: 4,
    },
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(1), chunk(3)],
      earliest_seq: 1,
      next_seq: 3,
      gap: { missing_from: 2, missing_to: 2, reason: "retention_window" },
      expectedNextSeq: 3,
    },
    {
      terminal_session_id: "terminal-session-1",
      output: [chunk(1), chunk(2)],
      earliest_seq: 1,
      next_seq: 2,
      gap: null,
      expectedNextSeq: 2,
    },
  ];

  for (const { expectedNextSeq, ...body } of pages) {
    globalThis.fetch = async () => response(body);
    const parsed = await fetchHarnessTerminalOutput(
      "http://pulse.test",
      "terminal-session-1",
      "turn-1",
    );
    assert.equal(parsed.next_seq, expectedNextSeq);
  }
});

test("terminal output client rejects missing, overshooting, or rewinding cursors", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  const chunk = (seq: number) => ({
    terminal_session_id: "terminal-session-1",
    seq,
    stream: "stdout",
    text: `chunk-${seq}`,
  });
  const invalidPages = [
    {},
    { next_seq: 999 },
    { next_seq: 1 },
  ];

  for (const cursor of invalidPages) {
    globalThis.fetch = async () =>
      response({
        terminal_session_id: "terminal-session-1",
        output: [chunk(1), chunk(2)],
        earliest_seq: 1,
        gap: null,
        ...cursor,
      });
    await assert.rejects(
      fetchHarnessTerminalOutput(
        "http://pulse.test",
        "terminal-session-1",
        "turn-1",
      ),
      (error: unknown) =>
        error instanceof HarnessFault &&
        error.code === "terminal_output_projection_invalid",
    );
  }
});

test("terminal output client rejects missing collections and malformed or stale chunks", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  const chunk = (seq: number) => ({
    terminal_session_id: "terminal-session-1",
    seq,
    stream: "stdout",
    text: `chunk-${seq}`,
  });
  const invalidCases = [
    {
      afterSeq: 0,
      body: {
        terminal_session_id: "terminal-session-1",
        next_seq: 0,
      },
    },
    {
      afterSeq: 0,
      body: {
        terminal_session_id: "terminal-session-1",
        output: [chunk(1), { ...chunk(2), stream: "invalid" }],
        earliest_seq: 1,
        next_seq: 1,
        gap: null,
      },
    },
    {
      afterSeq: 1,
      body: {
        terminal_session_id: "terminal-session-1",
        output: [chunk(1), chunk(2)],
        earliest_seq: 1,
        next_seq: 2,
        gap: null,
      },
    },
  ];

  for (const { afterSeq, body } of invalidCases) {
    globalThis.fetch = async () => response(body);
    await assert.rejects(
      fetchHarnessTerminalOutput(
        "http://pulse.test",
        "terminal-session-1",
        "turn-1",
        afterSeq,
      ),
      (error: unknown) =>
        error instanceof HarnessFault &&
        error.code === "terminal_output_projection_invalid",
    );
  }
});

test("terminal output client requires exact page and every chunk session id", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  const validChunk = {
    terminal_session_id: "terminal-session-1",
    seq: 1,
    stream: "stdout",
    text: "safe",
  };
  const invalidPages = [
    { output: [validChunk] },
    { terminal_session_id: "another-session", output: [validChunk] },
    {
      terminal_session_id: "terminal-session-1",
      output: [{ ...validChunk, terminal_session_id: undefined }],
    },
    {
      terminal_session_id: "terminal-session-1",
      output: [{ ...validChunk, terminal_session_id: "another-session" }],
    },
  ];

  for (const body of invalidPages) {
    globalThis.fetch = async () => response(body);
    await assert.rejects(
      fetchHarnessTerminalOutput(
        "http://pulse.test",
        "terminal-session-1",
        "turn-1",
      ),
      (error: unknown) =>
        error instanceof HarnessFault &&
        error.code === "terminal_output_projection_invalid",
    );
  }
});

test("terminal client downgrades unknown evidence labels", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });

  globalThis.fetch = async (input) => {
    const url = String(input);
    if (url.includes("/output")) {
      return response({
        terminal_session_id: "terminal-session-1",
        output: [],
        next_seq: 0,
        evidence_class: "LIVE_CUSTOM_OUTPUT",
      });
    }
    if (url.includes("/turns/")) {
      return response({
        sessions: [
          sessionRow({
            evidence_class: "LIVE_CUSTOM_SESSION",
            sandbox_evidence: "LIVE_CUSTOM_SANDBOX",
          }),
        ],
        evidence_class: "LIVE_CUSTOM_LIST",
      });
    }
    return response(
      sessionRow({
        evidence_class: "LIVE_CUSTOM_SESSION",
        sandbox_evidence: "LIVE_CUSTOM_SANDBOX",
      }),
    );
  };

  const listed = await fetchHarnessTerminalSessions(
    "http://pulse.test",
    "turn-1",
  );
  const inspected = await fetchHarnessTerminalSession(
    "http://pulse.test",
    "terminal-session-1",
    "turn-1",
  );
  const output = await fetchHarnessTerminalOutput(
    "http://pulse.test",
    "terminal-session-1",
    "turn-1",
  );

  assert.equal(listed.evidence_class, "LIVE_GATE_UNVERIFIED");
  assert.equal(listed.sessions[0].evidence_class, "LIVE_GATE_UNVERIFIED");
  assert.equal(inspected.evidence_class, "LIVE_GATE_UNVERIFIED");
  assert.equal(inspected.sandbox_evidence, "LIVE_GATE_UNVERIFIED");
  assert.equal(output.evidence_class, "LIVE_GATE_UNVERIFIED");
});

test("terminal stop client sends the caller's stable request id without hidden retry", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  let posted: Record<string, unknown> | null = null;
  globalThis.fetch = async (_input, init) => {
    posted = JSON.parse(String(init?.body));
    return response(
      sessionRow({
        state: "CANCEL_REQUESTED",
        request_id: "terminal-stop-1",
        accepted: true,
        idempotent: false,
      }),
      202,
    );
  };

  const result = await stopHarnessTerminalSession(
    "http://pulse.test",
    "terminal-session-1",
    {
      request_id: "terminal-stop-1",
      expected_epoch: 7,
      expected_turn_id: "turn-1",
      expected_state: "RUNNING",
    },
  );

  assert.equal(posted?.request_id, "terminal-stop-1");
  assert.equal(result.request_id, "terminal-stop-1");
  assert.equal(result.state, "CANCEL_REQUESTED");
  assert.equal(result.accepted, true);
});

test("terminal stop client preserves explicit natural-exit final and rejects malformed accepted", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });

  globalThis.fetch = async () =>
    response(
      sessionRow({
        state: "EXITED",
        request_id: "terminal-stop-natural-exit",
        accepted: false,
        idempotent: true,
        ended_at: "2026-08-04T12:02:00Z",
        exit_code: 0,
      }),
    );
  const final = await stopHarnessTerminalSession(
    "http://pulse.test",
    "terminal-session-1",
    {
      request_id: "terminal-stop-natural-exit",
      expected_epoch: 7,
      expected_turn_id: "turn-1",
      expected_state: "RUNNING",
    },
  );
  assert.equal(final.state, "EXITED");
  assert.equal(final.accepted, false);
  assert.equal(final.idempotent, true);

  for (const accepted of [undefined, "true", 1, null]) {
    globalThis.fetch = async () =>
      response(
        sessionRow({
          state: "CANCEL_REQUESTED",
          request_id: "terminal-stop-invalid",
          accepted,
        }),
      );
    await assert.rejects(
      stopHarnessTerminalSession(
        "http://pulse.test",
        "terminal-session-1",
        {
          request_id: "terminal-stop-invalid",
          expected_epoch: 7,
          expected_turn_id: "turn-1",
        },
      ),
      (error: unknown) =>
        error instanceof HarnessFault &&
        error.code === "terminal_stop_result_invalid",
    );
  }
});

class FakeEventSource {
  static latest: FakeEventSource | null = null;

  readonly listeners = new Map<string, Set<(event: unknown) => void>>();
  readonly url: string;
  onopen: ((event: unknown) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.latest = this;
  }

  addEventListener(name: string, listener: (event: unknown) => void) {
    const listeners = this.listeners.get(name) ?? new Set();
    listeners.add(listener);
    this.listeners.set(name, listeners);
  }

  removeEventListener(name: string, listener: (event: unknown) => void) {
    this.listeners.get(name)?.delete(listener);
  }

  emit(name: string, body: unknown) {
    const event = { data: JSON.stringify(body) };
    for (const listener of this.listeners.get(name) ?? []) listener(event);
  }

  close() {
    this.closed = true;
  }
}

test("bounded SSE reconnect is transient and only turn_terminal closes the client", (t) => {
  const originalEventSource = globalThis.EventSource;
  t.after(() => {
    globalThis.EventSource = originalEventSource;
  });
  globalThis.EventSource = FakeEventSource;
  const fatal: HarnessFault[] = [];
  const transport: Array<HarnessFault | null> = [];
  let opened = 0;

  subscribeHarnessTurn("http://pulse.test", "turn-1", 0, {
    onEvent: () => undefined,
    onOpen: () => {
      opened += 1;
    },
    onError: (fault) => fatal.push(fault),
    onTransportFault: (fault) => transport.push(fault),
  });
  const source = FakeEventSource.latest;
  assert.ok(source);

  source.onerror?.({});
  assert.equal(fatal.length, 0);
  assert.equal(transport[0]?.code, "harness_stream_disconnected");
  source.onopen?.({});
  assert.equal(opened, 1);
  assert.equal(transport.at(-1), null);

  source.emit("harness_event", {
    event_id: "event-1",
    turn_id: "turn-1",
    seq: 1,
    kind: "tool_completed",
    status: "completed",
  });
  assert.equal(source.closed, false);
  source.emit("harness_event", {
    event_id: "event-2",
    turn_id: "turn-1",
    seq: 2,
    kind: "turn_terminal",
    status: "settled",
  });
  assert.equal(source.closed, true);
});
