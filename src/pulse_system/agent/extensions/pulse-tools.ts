/**
 * Pulse tools for the upstream Pi Harness.
 *
 * This extension owns no world state.  Every operation crosses the local
 * capability Gateway, so the Pi process cannot choose an Engram identity or
 * bypass the causal dispatcher supplied by the host.
 */

import { Type } from "@earendil-works/pi-ai";
import { defineTool, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { scheduleActionTimeout } from "./pulse-timeout.ts";

const REQUEST_TIMEOUT_MS = 15_000;
// The mutable HTTP continuation must cover the 300 second human approval TTL.
// Adapter execution itself remains bounded by the server-side 60..300 second
// envelope.  The extra five seconds is transport grace, not execution
// authority; its timer sends cancellation before aborting this continuation.
const MAX_ACTION_TIMEOUT_SECONDS = 300;
const APPROVAL_TTL_SECONDS = 300;
const ACTION_TRANSPORT_GRACE_MS = 5_000;
const MAX_RESPONSE_BYTES = 1_000_000;
const TOOL_CALL_ID_HEADER = "X-Pulse-Tool-Call-Id";
const EFFECT_TOOL_ONLY_PREFIX = "[[PULSE_EFFECT_TOOL_ONLY_V1]]";
const RELAY_NO_TOOLS_PREFIX = "[[PULSE_RELAY_NO_TOOLS_V1]]";
const SUCCESSION_PROMPT =
	"Please provide a comprehensive summary of everything discussed so far " +
	"in this conversation. Capture the key ideas, conclusions, open questions, " +
	"and any important context. This summary will serve as the foundation " +
	"for continuing this line of thinking in a new session.";

type JsonObject = Record<string, unknown>;

type ToolEnvelope = {
	ok: boolean;
	content?: unknown;
	data?: unknown;
	event_id?: unknown;
	error?: unknown;
};

type AuthorizeEnvelope = {
	allow?: unknown;
	reason?: unknown;
};

function currentPromptWithPrefix(providerInput: string, prefix: string): string | undefined {
	if (providerInput.startsWith(prefix)) return providerInput;
	// Pulse may prepend the fixed member bootstrap before handing the current
	// stimulus to Pi.  Profile only a marker at a prompt boundary; do not let
	// arbitrary substrings in ordinary prose broaden the active tool set.
	const boundary = `\n\n${prefix}`;
	const index = providerInput.indexOf(boundary);
	return index < 0 ? undefined : providerInput.slice(index + 2);
}

function gatewayBase(): { url: string; capability: string } {
	const url = process.env.PULSE_TOOL_GATEWAY_URL;
	const capability = process.env.PULSE_TOOL_CAPABILITY;
	if (!url || !capability) {
		throw new Error("pulse_gateway_unavailable");
	}
	try {
		const parsed = new URL(url);
		if (parsed.protocol !== "http:" || parsed.hostname !== "127.0.0.1") {
			throw new Error("not_loopback");
		}
		return { url: parsed.toString().replace(/\/$/, ""), capability };
	} catch {
		throw new Error("pulse_gateway_unavailable");
	}
}

function safeReasonCode(value: unknown, fallback: string): string {
	if (typeof value === "string" && /^[a-z][a-z0-9_.-]{0,79}$/.test(value)) {
		return value;
	}
	return fallback;
}

function containsIdentityField(value: unknown): boolean {
	if (Array.isArray(value)) return value.some(containsIdentityField);
	if (value === null || typeof value !== "object") return false;
	return Object.entries(value).some(
		([key, child]) => key === "engram_id" || containsIdentityField(child),
	);
}

function cloneInput(value: unknown): JsonObject {
	if (value === null || typeof value !== "object" || Array.isArray(value)) {
		throw new Error("pulse_authorization_failed");
	}
	try {
		const cloned: unknown = JSON.parse(JSON.stringify(value));
		if (cloned === null || typeof cloned !== "object" || Array.isArray(cloned)) {
			throw new Error("not_object");
		}
		if (containsIdentityField(cloned)) {
			throw new Error("identity_field_not_allowed");
		}
		return cloned as JsonObject;
	} catch {
		throw new Error("pulse_authorization_failed");
	}
}

function mutableTransportTimeoutMs(input: JsonObject): number {
	const requested = input.timeout;
	if (requested === undefined) {
		return APPROVAL_TTL_SECONDS * 1_000 + ACTION_TRANSPORT_GRACE_MS;
	}
	if (
		typeof requested !== "number" ||
		!Number.isFinite(requested) ||
		requested <= 0 ||
		requested > MAX_ACTION_TIMEOUT_SECONDS
	) {
		throw new Error("pulse_action_timeout_invalid");
	}
	return (
		Math.max(requested, APPROVAL_TTL_SECONDS) * 1_000 + ACTION_TRANSPORT_GRACE_MS
	);
}

async function postJson(
	path: string,
	body: JsonObject,
	parentSignal: AbortSignal | undefined,
	toolCallId: string,
	timeoutMs = REQUEST_TIMEOUT_MS,
	onTimeout?: () => void,
): Promise<JsonObject> {
	const { url, capability } = gatewayBase();
	if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(toolCallId)) {
		throw new Error("pulse_tool_call_id_invalid");
	}
	const controller = new AbortController();
	const abortParent = () => controller.abort();
	const cancelTimer = scheduleActionTimeout(timeoutMs, () => {
		onTimeout?.();
		controller.abort();
	});
	parentSignal?.addEventListener("abort", abortParent, { once: true });
	try {
		const response = await fetch(`${url}${path}`, {
			method: "POST",
			headers: {
				"Authorization": `Bearer ${capability}`,
				"Content-Type": "application/json",
				[TOOL_CALL_ID_HEADER]: toolCallId,
			},
			body: JSON.stringify(body),
			signal: controller.signal,
		});
		const text = await response.text();
		if (new TextEncoder().encode(text).byteLength > MAX_RESPONSE_BYTES) {
			throw new Error("pulse_gateway_response_too_large");
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(text);
		} catch {
			throw new Error("pulse_gateway_response_invalid");
		}
		if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
			throw new Error("pulse_gateway_response_invalid");
		}
		if (!response.ok) {
			throw new Error("pulse_gateway_request_failed");
		}
		return parsed as JsonObject;
	} catch (error) {
		if (error instanceof Error && error.message.startsWith("pulse_")) {
			throw error;
		}
		throw new Error("pulse_gateway_unavailable");
	} finally {
		cancelTimer();
		parentSignal?.removeEventListener("abort", abortParent);
	}
}

async function dispatchTool(
	name: string,
	params: JsonObject,
	signal: AbortSignal | undefined,
	toolCallId: string,
) {
	const mutable = mutableProxyNames.has(name);
	let cancelOnAbort: (() => void) | undefined;
	let cancellationSent = false;
	const requestCancellation = () => {
		if (cancellationSent) return;
		cancellationSent = true;
		void postJson("/v1/tools/cancel", {}, undefined, toolCallId, 5_000).catch(() => undefined);
	};
	if (mutable && signal) {
		cancelOnAbort = requestCancellation;
		signal.addEventListener("abort", cancelOnAbort, { once: true });
	}
	let response: ToolEnvelope;
	try {
		response = (await postJson(
			`/v1/tools/${encodeURIComponent(name)}`,
			params,
			signal,
			toolCallId,
			mutable ? mutableTransportTimeoutMs(params) : REQUEST_TIMEOUT_MS,
			mutable ? requestCancellation : undefined,
		)) as ToolEnvelope;
	} finally {
		if (cancelOnAbort && signal) signal.removeEventListener("abort", cancelOnAbort);
	}
	if (typeof response.ok !== "boolean") {
		throw new Error("pulse_gateway_response_invalid");
	}
	if (typeof response.content !== "string") {
		throw new Error("pulse_gateway_response_invalid");
	}
	if (response.data !== undefined && (response.data === null || typeof response.data !== "object" || Array.isArray(response.data))) {
		throw new Error("pulse_gateway_response_invalid");
	}
	if (response.event_id !== undefined && response.event_id !== null && typeof response.event_id !== "string") {
		throw new Error("pulse_gateway_response_invalid");
	}
	return {
		content: [{ type: "text", text: response.content }],
		details: {
			data: response.data ?? {},
			event_id: response.event_id ?? null,
			error: typeof response.error === "string" ? response.error : null,
		},
	};
}

async function authorizeTool(
	toolName: string,
	input: JsonObject,
	signal: AbortSignal | undefined,
	toolCallId: string,
): Promise<{ allow: boolean; reason: string }> {
	const response = (await postJson(
		"/v1/authorize-tool",
		{ tool_name: toolName, input },
		signal,
		toolCallId,
	)) as AuthorizeEnvelope;
	if (typeof response.allow !== "boolean") {
		throw new Error("pulse_authorization_failed");
	}
	const reason = safeReasonCode(
		response.reason,
		response.allow ? "allowed" : "pulse_authorization_denied",
	);
	return {
		allow: response.allow,
		reason,
	};
}

const activityKind = Type.Union([
	Type.Literal("hobby"),
	Type.Literal("life_project"),
	Type.Literal("relationship"),
	Type.Literal("exploration"),
	Type.Literal("practice"),
	Type.Literal("expression"),
	Type.Literal("rest"),
	Type.Literal("other"),
]);

const activityStatus = Type.Union([
	Type.Literal("active"),
	Type.Literal("dormant"),
	Type.Literal("paused"),
	Type.Literal("completed"),
	Type.Literal("archived"),
]);

const nonEmptyString = { minLength: 1 } as const;
const concernContent = { minLength: 1, maxLength: 4000 } as const;
const orientationContent = { minLength: 1, maxLength: 4000 } as const;
const arbitraryObject = Type.Record(Type.String(), Type.Unknown());
const livingOrientationState = Type.Union([
	Type.Literal("open"),
	Type.Literal("resting"),
	Type.Literal("closed"),
]);

const pulseTaskOfferRespond = defineTool({
	name: "pulse_task_offer_respond",
	label: "Respond to task offer",
	description:
		"Accept, refuse, or request changes to the task offer bound to the current deliberation turn.",
	promptSnippet: "respond to the current task offer after deliberation",
	parameters: Type.Object(
		{
			decision: Type.Union([
				Type.Literal("accept"),
				Type.Literal("refuse"),
				Type.Literal("request_changes"),
			]),
			expected_revision: Type.Integer({ minimum: 1 }),
			response: Type.Optional(Type.String({ maxLength: 4000 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_task_offer_respond", params, signal, toolCallId);
	},
});

const pulseTaskRelationshipRespond = defineTool({
	name: "pulse_task_relationship_respond",
	label: "Respond to task relationship",
	description:
		"Pause, renegotiate, voluntarily resume, or exit a task relationship as the current subject.",
	promptSnippet: "decide how to continue the current task relationship",
	parameters: Type.Object(
		{
			relationship_id: Type.String({ minLength: 1, maxLength: 128 }),
			expected_revision: Type.Integer({ minimum: 1 }),
			action: Type.Union([
				Type.Literal("pause"),
				Type.Literal("request_changes"),
				Type.Literal("resume"),
				Type.Literal("exit"),
			]),
			response: Type.Optional(Type.String({ maxLength: 4000 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_task_relationship_respond", params, signal, toolCallId);
	},
});

const pulseLifeList = defineTool({
	name: "pulse_life_list",
	label: "List life centers",
	description: "List Activity Centers in the current Engram membership.",
	promptSnippet: "list this Engram's life centers",
	parameters: Type.Object({}, { additionalProperties: false }),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_list", params, signal, toolCallId);
	},
});

const pulseLifePortfolio = defineTool({
	name: "pulse_life_portfolio",
	label: "Read living portfolio",
	description: "Read this persistent Engram's canonical purpose lineage and non-task life centers.",
	promptSnippet: "read my living portfolio",
	parameters: Type.Object(
		{
			history_limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 100 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_portfolio", params, signal, toolCallId);
	},
});

const pulseLifeConcerns = defineTool({
	name: "pulse_life_concerns",
	label: "List living concerns",
	description: "List the current Engram's concerns in its non-task Centers.",
	promptSnippet: "list my living concerns",
	parameters: Type.Object(
		{
			center_id: Type.Optional(Type.String(nonEmptyString)),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_concerns", params, signal, toolCallId);
	},
});

const pulseLifeHold = defineTool({
	name: "pulse_life_hold",
	label: "Hold living concern",
	description: "Keep a natural-language concern alive across future turns.",
	promptSnippet: "hold this as a living concern",
	// Provider tool APIs require a root JSON Schema with type: "object".
	// Cross-field rules (resolved requires concern_id; revisit requires the
	// delay) remain enforced by the server-side Gateway validator, which is
	// the authoritative boundary for all callers, including resumed sessions.
	parameters: Type.Object(
		{
			center_id: Type.String(nonEmptyString),
			content: Type.String(concernContent),
			disposition: Type.Union([
				Type.Literal("quiet"),
				Type.Literal("resolved"),
				Type.Literal("revisit"),
			]),
			concern_id: Type.Optional(Type.String(nonEmptyString)),
			revisit_after_seconds: Type.Optional(
				Type.Number({ minimum: 0, maximum: 31536000 }),
			),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_hold", params, signal, toolCallId);
	},
});

const pulseLifeOrientations = defineTool({
	name: "pulse_life_orientations",
	label: "List living directions",
	description: "List this Engram's current living directions in non-task Centers.",
	promptSnippet: "list my current living directions",
	parameters: Type.Object(
		{
			center_id: Type.Optional(Type.String(nonEmptyString)),
			current_only: Type.Optional(Type.Boolean()),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_orientations", params, signal, toolCallId);
	},
});

const pulseLifeOrient = defineTool({
	name: "pulse_life_orient",
	label: "Maintain living direction",
	description: "Write or maintain the current natural-language direction of one Life Center.",
	promptSnippet: "set or maintain this living direction",
	parameters: Type.Object(
		{
			center_id: Type.String(nonEmptyString),
			content: Type.String(orientationContent),
			state: livingOrientationState,
			orientation_id: Type.Optional(Type.String(nonEmptyString)),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_orient", params, signal, toolCallId);
	},
});

const pulseLifeCreate = defineTool({
	name: "pulse_life_create",
	label: "Create life center",
	description: "Create a non-task Activity Center with the current Engram as focal.",
	promptSnippet: "create a non-task life center",
	parameters: Type.Object(
		{
			kind: activityKind,
			title: Type.String(nonEmptyString),
			description: Type.Optional(Type.String()),
			autonomy: Type.Optional(Type.Number({ minimum: 0, maximum: 1 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_create", params, signal, toolCallId);
	},
});

const pulseLifeUpdate = defineTool({
	name: "pulse_life_update",
	label: "Update life center",
	description: "Update an Activity Center in the current Engram membership.",
	promptSnippet: "update one of this Engram's life centers",
	parameters: Type.Object(
		{
			center_id: Type.String(nonEmptyString),
			title: Type.Optional(Type.String(nonEmptyString)),
			description: Type.Optional(Type.String()),
			status: Type.Optional(activityStatus),
			autonomy: Type.Optional(Type.Number({ minimum: 0, maximum: 1 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_update", params, signal, toolCallId);
	},
});

const pulseLifePurpose = defineTool({
	name: "pulse_life_purpose",
	label: "Read subject purpose",
	description: "Read this continuous subject's current purpose and optional append-only history.",
	promptSnippet: "read my current long-range purpose",
	parameters: Type.Object(
		{
			history: Type.Optional(Type.Boolean()),
			limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 1000 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_purpose", params, signal, toolCallId);
	},
});

const pulseLifeAmendPurpose = defineTool({
	name: "pulse_life_amend_purpose",
	label: "Amend subject purpose",
	description: "Explicitly establish, amend, or withdraw this subject lineage's purpose with revision CAS.",
	promptSnippet: "explicitly amend my long-range purpose",
	parameters: Type.Object(
		{
			amendment_kind: Type.Union([
				Type.Literal("establish"),
				Type.Literal("amend"),
				Type.Literal("withdraw"),
			]),
			expected_revision: Type.Optional(Type.Integer({ minimum: 1 })),
			content: Type.Optional(Type.String({ minLength: 1, maxLength: 4000 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_amend_purpose", params, signal, toolCallId);
	},
});

const pulseLifeRoles = defineTool({
	name: "pulse_life_roles",
	label: "Read subject roles",
	description: "Observe bounded role leases held by this subject lineage.",
	promptSnippet: "read my bounded roles",
	parameters: Type.Object(
		{ active_only: Type.Optional(Type.Boolean()) },
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_roles", params, signal, toolCallId);
	},
});

const pulseLifeAcceptRole = defineTool({
	name: "pulse_life_accept_role",
	label: "Accept bounded role",
	description: "Explicitly accept a time-bounded subject role over named Life Centers.",
	promptSnippet: "accept a bounded role in my life centers",
	parameters: Type.Object(
		{
			role_label: Type.String({ minLength: 1, maxLength: 256 }),
			center_ids: Type.Array(Type.String({ minLength: 1, maxLength: 128 }), {
				minItems: 1,
				maxItems: 16,
				uniqueItems: true,
			}),
			ttl_seconds: Type.Optional(Type.Number({ minimum: 1, maximum: 7776000 })),
			purpose_revision_id: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
			obligation: Type.Optional(
				Type.Object(
					{
						kind: Type.Optional(Type.Literal("direct_output")),
						minimum_direct_outputs: Type.Optional(Type.Integer({ minimum: 1, maximum: 16 })),
						max_consecutive_coordination: Type.Optional(Type.Integer({ minimum: 0, maximum: 64 })),
						accepted_output_kinds: Type.Optional(
							Type.Array(
								Type.Union([
									Type.Literal("workspace_checkpoint"),
									Type.Literal("habitat_effect"),
								]),
								{ minItems: 1, maxItems: 2, uniqueItems: true },
							),
						),
					},
					{ additionalProperties: false },
				),
			),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_accept_role", params, signal, toolCallId);
	},
});

const pulseLifeRenewRole = defineTool({
	name: "pulse_life_renew_role",
	label: "Renew direct-output role",
	description: "Renew an opted-in direct-output role only from its latest trusted production receipt.",
	promptSnippet: "renew a direct-output role after producing its promised output",
	parameters: Type.Object(
		{
			role_lease_id: Type.String({ minLength: 1, maxLength: 128 }),
			expected_role_epoch: Type.Integer({ minimum: 1 }),
			ttl_seconds: Type.Optional(Type.Number({ minimum: 1, maximum: 7776000 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_renew_role", params, signal, toolCallId);
	},
});

const pulseLifeReleaseRole = defineTool({
	name: "pulse_life_release_role",
	label: "Release bounded role",
	description: "Explicitly release one subject role using its current role epoch.",
	promptSnippet: "release one of my bounded roles",
	parameters: Type.Object(
		{
			role_lease_id: Type.String({ minLength: 1, maxLength: 128 }),
			expected_role_epoch: Type.Integer({ minimum: 1 }),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_life_release_role", params, signal, toolCallId);
	},
});

const pulseHabitatObserve = defineTool({
	name: "pulse_habitat_observe",
	label: "Observe Habitat",
	description: "Observe a managed Habitat organ through the PulseWorld adapter.",
	promptSnippet: "observe the Habitat",
	parameters: Type.Object(
		{
			organ: Type.String(nonEmptyString),
			target: Type.Optional(Type.String(nonEmptyString)),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_habitat_observe", params, signal, toolCallId);
	},
});

const pulseHabitatAct = defineTool({
	name: "pulse_habitat_act",
	label: "Act in Habitat",
	description: "Request an action through the managed Habitat adapter.",
	promptSnippet: "act through the Habitat",
	parameters: Type.Object(
		{
			verb: Type.String(nonEmptyString),
			target: Type.Optional(Type.String(nonEmptyString)),
			payload: Type.Optional(arbitraryObject),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_habitat_act", params, signal, toolCallId);
	},
});

const pulseHabitatSubscribe = defineTool({
	name: "pulse_habitat_subscribe",
	label: "Subscribe to Habitat",
	description: "Subscribe the current Engram to a managed Habitat channel.",
	promptSnippet: "subscribe to a Habitat channel",
	parameters: Type.Object(
		{
			channel: Type.Optional(Type.String(nonEmptyString)),
			center_id: Type.Optional(Type.String(nonEmptyString)),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_habitat_subscribe", params, signal, toolCallId);
	},
});

const pulseDelegate = defineTool({
	name: "pulse_delegate",
	label: "Delegate through Pulse",
	description: "Create an asynchronous causal delegation request.",
	promptSnippet: "delegate a task through the PulseWorld",
	parameters: Type.Object(
		{
			task: Type.String(nonEmptyString),
			to: Type.Optional(Type.String(nonEmptyString)),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_delegate", params, signal, toolCallId);
	},
});

const pulseTaskSpawn = defineTool({
	name: "pulse_task_spawn",
	label: "Spawn temporary Pi worker",
	description: "Start one bounded temporary Pi worker below the Engram identity plane.",
	promptSnippet: "spawn a bounded temporary Pi worker",
	parameters: Type.Object(
		{
			task: Type.String({ minLength: 1, maxLength: 8_192 }),
			timeout: Type.Optional(Type.Number({ exclusiveMinimum: 0, maximum: 900 })),
			idle_timeout: Type.Optional(Type.Number({ exclusiveMinimum: 0, maximum: 900 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_task_spawn", params, signal, toolCallId);
	},
});

const pulseTaskWait = defineTool({
	name: "pulse_task_wait",
	label: "Observe temporary worker",
	description: "Observe bounded activity and receive terminal output from a scoped temporary worker.",
	promptSnippet: "observe a temporary Pi worker",
	parameters: Type.Object(
		{
			task_id: Type.String({ minLength: 6, maxLength: 96, pattern: "^task_[A-Za-z0-9_.:-]+$" }),
			after_seq: Type.Optional(Type.Integer({ minimum: 0 })),
			timeout: Type.Optional(Type.Number({ minimum: 0, maximum: 30 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_task_wait", params, signal, toolCallId);
	},
});

const pulseTaskSteer = defineTool({
	name: "pulse_task_steer",
	label: "Steer temporary worker",
	description: "Send bounded natural-language guidance to a running scoped worker.",
	promptSnippet: "steer a temporary Pi worker",
	parameters: Type.Object(
		{
			task_id: Type.String({ minLength: 6, maxLength: 96, pattern: "^task_[A-Za-z0-9_.:-]+$" }),
			message: Type.String({ minLength: 1, maxLength: 8_192 }),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_task_steer", params, signal, toolCallId);
	},
});

const pulseTaskStop = defineTool({
	name: "pulse_task_stop",
	label: "Stop temporary worker",
	description: "Request bounded cancellation of a running scoped temporary worker.",
	promptSnippet: "stop a temporary Pi worker",
	parameters: Type.Object(
		{
			task_id: Type.String({ minLength: 6, maxLength: 96, pattern: "^task_[A-Za-z0-9_.:-]+$" }),
			reason: Type.Optional(Type.String({ minLength: 1, maxLength: 8_192 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_task_stop", params, signal, toolCallId);
	},
});

const pulseMcpCall = defineTool({
	name: "pulse_mcp_call",
	label: "Call approved MCP tool",
	description: "Call one explicitly allowlisted MCP server tool through Pulse approval and recovery fencing.",
	promptSnippet: "call an allowlisted MCP tool through Pulse",
	parameters: Type.Object(
		{
			server_id: Type.String({ minLength: 1, maxLength: 128 }),
			tool_name: Type.String({ minLength: 1, maxLength: 128 }),
			arguments: Type.Record(Type.String(), Type.Unknown()),
			timeout: Type.Optional(Type.Number({ exclusiveMinimum: 0, maximum: MAX_ACTION_TIMEOUT_SECONDS })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("pulse_mcp_call", params, signal, toolCallId);
	},
});

// These names intentionally shadow Pi's mutable built-ins.  PiHarnessRuntime
// uses Pi's --no-builtin-tools mode, which leaves these extension definitions
// active while making the native implementations unreachable.  The server
// decides policy/approval and never falls back to Pi native execution.
const pulseRead = defineTool({
	name: "read",
	label: "Read through Pulse",
	description: "Read a bounded UTF-8 workspace file through the Pulse boundary.",
	promptSnippet: "read a workspace file through Pulse",
	parameters: Type.Object(
		{
			path: Type.String({ minLength: 1, maxLength: 4_096 }),
			offset: Type.Optional(Type.Integer({ minimum: 1, maximum: 10_000_000 })),
			limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 10_000 })),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("read", params, signal, toolCallId);
	},
});
const pulseBash = defineTool({
	name: "bash",
	label: "Run command through Pulse",
	description: "Request a workspace-scoped command through Pulse policy and approval.",
	promptSnippet: "run a command through the Pulse approval boundary",
	parameters: Type.Object(
		{
			command: Type.String({ minLength: 1, maxLength: 32_000 }),
			timeout: Type.Optional(Type.Number({ exclusiveMinimum: 0, maximum: MAX_ACTION_TIMEOUT_SECONDS })),
			background: Type.Optional(Type.Boolean()),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("bash", params, signal, toolCallId);
	},
});

const pulseEdit = defineTool({
	name: "edit",
	label: "Edit through Pulse",
	description: "Request exact workspace edits through Pulse policy and approval.",
	promptSnippet: "edit a workspace file through the Pulse approval boundary",
	parameters: Type.Object(
		{
			path: Type.String({ minLength: 1, maxLength: 4_096 }),
			edits: Type.Array(
				Type.Object(
					{
						oldText: Type.String({ maxLength: 1_000_000 }),
						newText: Type.String({ maxLength: 1_000_000 }),
					},
					{ additionalProperties: false },
				),
				{ minItems: 1, maxItems: 128 },
			),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("edit", params, signal, toolCallId);
	},
});

const pulseWrite = defineTool({
	name: "write",
	label: "Write through Pulse",
	description: "Request a workspace file write through Pulse policy and approval.",
	promptSnippet: "write a workspace file through the Pulse approval boundary",
	parameters: Type.Object(
		{
			path: Type.String({ minLength: 1, maxLength: 4_096 }),
			content: Type.String({ maxLength: 4_000_000 }),
		},
		{ additionalProperties: false },
	),
	executionMode: "sequential",
	async execute(toolCallId, params, signal, _onUpdate, _ctx) {
		return dispatchTool("write", params, signal, toolCallId);
	},
});

const mutableProxyNames = new Set(["bash", "edit", "write", "pulse_mcp_call"]);

export default function pulseTools(pi: ExtensionAPI) {
	pi.registerTool(pulseRead);
	pi.registerTool(pulseBash);
	pi.registerTool(pulseEdit);
	pi.registerTool(pulseWrite);
	pi.registerTool(pulseTaskOfferRespond);
	pi.registerTool(pulseTaskRelationshipRespond);
	pi.registerTool(pulseLifeList);
	pi.registerTool(pulseLifePortfolio);
	pi.registerTool(pulseLifeConcerns);
	pi.registerTool(pulseLifeHold);
	pi.registerTool(pulseLifeOrientations);
	pi.registerTool(pulseLifeOrient);
	pi.registerTool(pulseLifeCreate);
	pi.registerTool(pulseLifeUpdate);
	pi.registerTool(pulseLifePurpose);
	pi.registerTool(pulseLifeAmendPurpose);
	pi.registerTool(pulseLifeRoles);
	pi.registerTool(pulseLifeAcceptRole);
	pi.registerTool(pulseLifeRenewRole);
	pi.registerTool(pulseLifeReleaseRole);
	pi.registerTool(pulseHabitatObserve);
	pi.registerTool(pulseHabitatAct);
	pi.registerTool(pulseHabitatSubscribe);
	pi.registerTool(pulseDelegate);
	pi.registerTool(pulseMcpCall);
	pi.registerTool(pulseTaskSpawn);
	pi.registerTool(pulseTaskWait);
	pi.registerTool(pulseTaskSteer);
	pi.registerTool(pulseTaskStop);

	// Pi binds action methods only after the extension factory has returned.
	// Reading the live registry here therefore aborts RPC startup on Pi 0.80.10.
	// Keep the full-profile inventory registration-derived but load-time safe;
	// setActiveTools itself remains inside before_agent_start, after binding.
	const allToolNames = [
		pulseRead.name,
		pulseBash.name,
		pulseEdit.name,
		pulseWrite.name,
		pulseTaskOfferRespond.name,
		pulseTaskRelationshipRespond.name,
		pulseLifeList.name,
		pulseLifePortfolio.name,
		pulseLifeConcerns.name,
		pulseLifeHold.name,
		pulseLifeOrientations.name,
		pulseLifeOrient.name,
		pulseLifeCreate.name,
		pulseLifeUpdate.name,
		pulseLifePurpose.name,
		pulseLifeAmendPurpose.name,
		pulseLifeRoles.name,
		pulseLifeAcceptRole.name,
		pulseLifeRenewRole.name,
		pulseLifeReleaseRole.name,
		pulseHabitatObserve.name,
		pulseHabitatAct.name,
		pulseHabitatSubscribe.name,
		pulseDelegate.name,
		pulseMcpCall.name,
		pulseTaskSpawn.name,
		pulseTaskWait.name,
		pulseTaskSteer.name,
		pulseTaskStop.name,
	].sort();
	pi.on("before_agent_start", (event) => {
		let activeTools = allToolNames;
		const relayPrompt = currentPromptWithPrefix(event.prompt, RELAY_NO_TOOLS_PREFIX);
		const effectPrompt = currentPromptWithPrefix(event.prompt, EFFECT_TOOL_ONLY_PREFIX);
		if (relayPrompt !== undefined) {
			activeTools = [];
		} else if (effectPrompt !== undefined) {
			activeTools = ["pulse_habitat_act"];
		} else if (event.prompt.endsWith(SUCCESSION_PROMPT)) {
			activeTools = [];
		}
		try {
			pi.setActiveTools(activeTools);
		} catch {
			// Keep compatibility with Pi hosts that do not expose tool profiles.
		}
		return undefined;
	});

	pi.on("tool_call", async (event, ctx) => {
		try {
			const input = cloneInput(event.input);
			if (mutableProxyNames.has(event.toolName)) {
				// The proxy execute path performs the server-side policy and
				// approval transition.  This hook is still before execution and
				// blocks the call if the capability boundary itself is absent.
				gatewayBase();
				return undefined;
			}
			const decision = await authorizeTool(event.toolName, input, ctx.signal, event.toolCallId);
			if (!decision.allow) {
				return { block: true, reason: decision.reason };
			}
			return undefined;
		} catch {
			// Upstream treats tool_call handler errors as blocking failures.  Keep
			// the explicit result safe and deterministic for unattended RPC.
			return { block: true, reason: "pulse_authorization_failed" };
		}
	});
}
