// @ts-nocheck

import assert from "node:assert/strict";
import test from "node:test";

import {
  RoleAccountabilityFault,
  fetchRoleAccountability,
  parseRoleAccountability,
} from "./roleAccountability.ts";

function role(overrides: Record<string, unknown> = {}) {
  return {
    role_lease_id: "role-1",
    role_epoch: 3,
    role_class: "subject_role",
    role_label: "primary maker",
    status: "active",
    lineage_id: "lineage-1",
    scope: {
      center_ids: ["center-1"],
      lineage_id: "lineage-1",
      task_front_id: null,
      action_scope: null,
    },
    obligation: {
      kind: "direct_output",
      minimum_direct_outputs: 1,
      max_consecutive_coordination: 2,
      accepted_output_kinds: ["workspace_checkpoint", "habitat_effect"],
    },
    accountability_cycle_id: "cycle-1",
    valid_from: "2026-08-09T11:00:00+00:00",
    renew_after: "2026-08-09T11:30:00+00:00",
    expires_at: "2026-08-09T13:00:00+00:00",
    renewal_count: 1,
    predecessor_lease_id: "role-0",
    contribution_summary: {
      role_lease_id: "role-1",
      accountability_cycle_id: "cycle-1",
      role_epoch: 3,
      direct_output_count: 1,
      coordination_count: 1,
      consecutive_coordination: 0,
      last_direct_output_event_id: "checkpoint-1",
      last_contribution_at: "2026-08-09T11:45:00+00:00",
      renewal_eligible: true,
      reason_code: "role_direct_output_obligation_satisfied",
    },
    renewal_gate: {
      contribution_gate_satisfied: true,
      eligible_now: true,
      reason_code: "role_renewal_window_and_contribution_gate_satisfied",
      authorization_still_required: true,
    },
    evidence: {
      role: "LIVE_GATE_UNVERIFIED",
      contributions: ["CONTROL_ONLY", "LIVE_WORKSPACE_CHECKPOINTED"],
      payload_disclosed: false,
    },
    ...overrides,
  };
}

function payload(roles = [role()], overrides: Record<string, unknown> = {}) {
  return {
    schema_version: "role-accountability.v1",
    world_id: "world-1",
    engram_id: "engram-1",
    projected_at: "2026-08-09T12:00:00+00:00",
    observer_effect: "READ_ONLY_NO_STIMULUS",
    payload_disclosed: false,
    roles,
    role_count: roles.length,
    roles_truncated: false,
    ...overrides,
  };
}

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

test("role accountability parser accepts the exact payload-free contract", () => {
  const parsed = parseRoleAccountability(payload(), "engram-1");

  assert.equal(parsed.observer_effect, "READ_ONLY_NO_STIMULUS");
  assert.equal(parsed.payload_disclosed, false);
  assert.equal(parsed.roles[0].contribution_summary.direct_output_count, 1);
  assert.equal(parsed.roles[0].renewal_gate.eligible_now, true);
  assert.deepEqual(parsed.roles[0].evidence.contributions, [
    "CONTROL_ONLY",
    "LIVE_WORKSPACE_CHECKPOINTED",
  ]);
  assert.equal("prompt" in parsed.roles[0], false);
  assert.equal("output_body" in parsed.roles[0], false);
  assert.equal("runtime_owner_id" in parsed.roles[0], false);
});

test("role accountability parser rejects schema drift, cross-holder data, and duplicates", () => {
  const second = role({
    role_lease_id: "role-2",
    contribution_summary: {
      ...role().contribution_summary,
      role_lease_id: "role-2",
    },
  });
  const duplicate = role();
  const invalid = [
    payload([role({ prompt: "must not cross" })]),
    payload([], { schema_version: "role-accountability.v2" }),
    payload([], { observer_effect: "READ_WRITES_METRIC" }),
    payload([role(), duplicate]),
  ];

  for (const item of invalid) {
    assert.throws(
      () => parseRoleAccountability(item, "engram-1"),
      (error: unknown) =>
        error instanceof RoleAccountabilityFault &&
        error.code === "role_accountability_projection_invalid",
    );
  }
  assert.throws(() => parseRoleAccountability(payload([role(), second]), "engram-2"));
});

test("role accountability parser rejects fabricated progress and renewal conclusions", () => {
  const base = role();
  const invalidRoles = [
    role({
      contribution_summary: {
        ...base.contribution_summary,
        direct_output_count: 0,
      },
    }),
    role({
      contribution_summary: {
        ...base.contribution_summary,
        consecutive_coordination: 2,
        coordination_count: 1,
      },
    }),
    role({
      renewal_gate: {
        ...base.renewal_gate,
        contribution_gate_satisfied: false,
      },
    }),
    role({
      renewal_gate: {
        ...base.renewal_gate,
        eligible_now: false,
        reason_code: "role_direct_output_required",
      },
    }),
    role({
      status: "active",
      expires_at: "2026-08-09T11:59:00+00:00",
    }),
    role({
      evidence: {
        ...base.evidence,
        contributions: ["CONTROL_ONLY"],
      },
    }),
  ];

  for (const item of invalidRoles) {
    assert.throws(() => parseRoleAccountability(payload([item]), "engram-1"));
  }

  const expired = role({
    status: "expired",
    expires_at: "2026-08-09T11:59:00+00:00",
    renewal_gate: {
      contribution_gate_satisfied: true,
      eligible_now: false,
      reason_code: "role_expired",
      authorization_still_required: true,
    },
  });
  assert.equal(
    parseRoleAccountability(payload([expired]), "engram-1")
      .roles[0].renewal_gate.reason_code,
    "role_expired",
  );
});

test("role accountability HTTP client bounds the request and fails closed on API faults", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  let requestedUrl = "";
  globalThis.fetch = async (input) => {
    requestedUrl = String(input);
    return response(payload());
  };

  const parsed = await fetchRoleAccountability(
    "http://pulse.test/",
    "engram-1",
    undefined,
    999,
  );
  assert.equal(parsed.role_count, 1);
  assert.match(requestedUrl, /role-accountability\?limit=64$/);

  globalThis.fetch = async () => response({
    error: "role_accountability_unavailable",
    detail: "durable source unavailable",
    remedy: "repair and retry",
  }, 503);
  await assert.rejects(
    fetchRoleAccountability("http://pulse.test", "engram-1"),
    (error: unknown) =>
      error instanceof RoleAccountabilityFault &&
      error.status === 503 &&
      error.code === "role_accountability_unavailable" &&
      error.remedy === "repair and retry",
  );
});
