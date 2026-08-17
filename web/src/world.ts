import { getJson, patchJson, postJson, rec, str, num } from "./pulse";

export type ActivityKind =
  | "task"
  | "hobby"
  | "life_project"
  | "relationship"
  | "exploration"
  | "practice"
  | "expression"
  | "rest"
  | "other";

export const LIFE_KINDS: Exclude<ActivityKind, "task">[] = [
  "hobby",
  "life_project",
  "relationship",
  "exploration",
  "practice",
  "expression",
  "rest",
  "other",
];

export interface TaskFrontSummary {
  id: string;
  center_id: string;
  focal_engram_id: string;
  title: string;
  status: "open" | "closed" | "archived" | string;
  created_at: string | null;
  updated_at: string | null;
  last_opened_at: string | null;
}

export interface ActivityCenterSummary {
  id: string;
  kind: ActivityKind;
  title: string;
  description: string;
  status: "active" | "dormant" | "paused" | "completed" | "archived" | string;
  origin: "user" | "self" | "shared" | "system" | string;
  autonomy: number;
  project_id: string | null;
  focal_engram_id: string | null;
  created_at: string | null;
  updated_at: string | null;
  last_active_at: string | null;
}

export interface PulseWorldDirectory {
  worldId: string;
  continuityEngramId: string;
  tick: number;
  harnessKind: string;
  mock: boolean;
  taskFronts: TaskFrontSummary[];
  activityCenters: ActivityCenterSummary[];
}

export interface CreatedTaskFront {
  taskFront: TaskFrontSummary;
  activityCenter: ActivityCenterSummary;
  eventId: string;
}

export type TaskOfferStatus =
  | "pending"
  | "changes_requested"
  | "accepted"
  | "refused"
  | "withdrawn";

export type TaskOfferDecision = "accept" | "refuse" | "request_changes";

export interface TaskOfferRecord {
  id: string;
  world_id: string;
  subject_engram_id: string;
  status: TaskOfferStatus;
  current_revision: number;
  task_front_id: string | null;
  created_at: string;
  updated_at: string;
  decided_at: string | null;
  withdrawn_at: string | null;
}

export interface TaskOfferRevisionView {
  offer_id: string;
  revision: number;
  content: string;
  title: string;
  project_id: string | null;
  latest_offer_event_id: string;
  decision: TaskOfferDecision | null;
  subject_response: string | null;
  decision_event_id: string | null;
  created_at: string;
  decided_at: string | null;
}

export interface TaskOfferSummary {
  taskOffer: TaskOfferRecord;
  currentRevision: TaskOfferRevisionView;
}

export interface TaskOfferDetail extends TaskOfferSummary {
  revisions: TaskOfferRevisionView[];
}

export interface CreatedTaskOffer extends TaskOfferSummary {
  eventId: string;
}

const TASK_OFFER_STATUSES = new Set<TaskOfferStatus>([
  "pending",
  "changes_requested",
  "accepted",
  "refused",
  "withdrawn",
]);
const TASK_OFFER_DECISIONS = new Set<TaskOfferDecision>([
  "accept",
  "refuse",
  "request_changes",
]);
export const MAX_TASK_OFFERS = 50;

export type TaskFrontLifecycleStatus = "open" | "closed" | "archived";

export interface TaskSubjectEngramView {
  id: string;
  project_id: string | null;
  status: string;
  created_at: string;
  last_pulse_at: string | null;
  total_pulses: number;
  name: string | null;
  name_origin: string;
  nickname: string | null;
}

export interface CreatedActivityCenter {
  activityCenter: ActivityCenterSummary;
  focalEngramId: string;
  eventId: string | null;
}

export interface LivingConcernView {
  id: string;
  center_id: string;
  owner_engram_id: string;
  content: string;
  disposition: "quiet" | "revisit" | "resolved" | string;
  revisit_at: string | null;
  causal_id: string;
  source_event_id: string;
  revision: number;
  last_reentry_event_id: string | null;
  created_at: string | null;
  updated_at: string | null;
  resolved_at: string | null;
}

export type LivingOrientationState = "open" | "resting" | "closed";

export interface LivingOrientationView {
  id: string;
  centerId: string;
  ownerEngramId: string;
  content: string;
  state: LivingOrientationState;
  revision: number;
  engagementCount: number;
  nextEligibleAt: string | null;
  lastEngagementEventId: string | null;
  lastEngagedAt: string | null;
  createdAt: string;
  updatedAt: string;
  closedAt: string | null;
}

export interface CenterActivitySummary {
  last_seq: number | null;
  last_event_at: string | null;
  queued: number;
  running: number;
  uncertain: number;
  recent_source: string | null;
  recent_kind: string | null;
}

export interface CenterMessageView {
  seq: number;
  event_id: string;
  causal_id: string;
  parent_event_id: string | null;
  engram_id: string | null;
  center_id: string;
  role: "user" | "assistant" | string;
  kind: string;
  source: string;
  status: string;
  content: string;
  metadata: Record<string, unknown>;
  timestamp: string;
}

export interface TaskCenterMessageView {
  seq: number;
  event_id: string;
  causal_id: string;
  parent_event_id: string | null;
  engram_id: string;
  center_id: string | null;
  role: string;
  kind: string;
  source: string;
  status: string;
  content: string;
  metadata: Record<string, unknown>;
  timestamp: string;
  source_engram_id: string | null;
}

export type TaskRelationshipStatus =
  | "active"
  | "paused"
  | "renegotiation_requested"
  | "exited";

export type TaskRelationshipAction =
  | "accepted"
  | "paused"
  | "renegotiation_requested"
  | "terms_proposed"
  | "resumed"
  | "exited"
  | "succession";

export type TaskRelationshipActorKind = "subject" | "user" | "system";
export type TaskRelationshipMode =
  | "subject_consent_managed"
  | "unmanaged_compatibility";

export const MAX_TASK_RELATIONSHIP_CONTENT = 12_000;

export interface TaskRelationshipView {
  id: string;
  world_id: string;
  accepted_offer_id: string;
  task_front_id: string;
  center_id: string;
  original_subject_engram_id: string;
  current_subject_engram_id: string;
  status: TaskRelationshipStatus;
  revision: number;
  latest_terms_event_id: string | null;
  latest_subject_note: string | null;
  created_at: string;
  updated_at: string;
  exited_at: string | null;
}

export interface TaskRelationshipEventView {
  relationship_id: string;
  seq: number;
  action: TaskRelationshipAction;
  actor_kind: TaskRelationshipActorKind;
  actor_id: string;
  before_status: TaskRelationshipStatus | null;
  after_status: TaskRelationshipStatus;
  content: string | null;
  source_event_id: string | null;
  created_at: string;
}

export interface TaskFrontDetail {
  taskFront: TaskFrontSummary;
  activityCenter: ActivityCenterSummary;
  focalEngram: TaskSubjectEngramView;
  messageScope: "center";
  messages: TaskCenterMessageView[];
  unattributedHistory: TaskCenterMessageView[];
  taskRelationshipMode: TaskRelationshipMode;
  taskRelationship: TaskRelationshipView | null;
  relationshipEvents: TaskRelationshipEventView[];
}

export interface ActivityCenterDetail {
  activityCenter: ActivityCenterSummary;
  livingConcerns: LivingConcernView[];
  livingConcernsTotal: number;
  livingConcernsTruncated: boolean;
  livingOrientations: LivingOrientationView[];
  livingOrientationsTotal: number;
  livingOrientationsTruncated: boolean;
  activitySummary: CenterActivitySummary;
  messages: CenterMessageView[];
  unattributedHistoryCount: number;
}

export type LivingPortfolioState =
  | "active"
  | "quiet"
  | "parked"
  | "completed"
  | "archived";

export type LivingPortfolioRelation = "focal" | "participant" | "shared";

export interface PurposeRevisionView {
  purpose_revision_id: string;
  lineage_id: string;
  author_engram_id: string;
  revision: number;
  predecessor_revision_id: string | null;
  amendment_kind: "establish" | "amend" | "withdraw";
  content: string | null;
  content_digest: string;
  state: "current" | "superseded" | "withdrawn";
  source_event_id: string;
  reflection_event_id: string | null;
  created_at: string;
  superseded_at: string | null;
}

export interface LivingPortfolioSubject {
  requested_engram_id: string;
  lineage_state: "active" | "unestablished";
  lineage_id: string | null;
  root_engram_id: string;
  current_engram_id: string;
  generation: number;
}

export interface LivingPortfolioItem {
  center: ActivityCenterSummary;
  relation: LivingPortfolioRelation;
  portfolio_state: LivingPortfolioState;
}

export interface LivingPortfolio {
  schema_version: "living-portfolio.v1";
  subject: LivingPortfolioSubject;
  purpose: {
    current: PurposeRevisionView | null;
    history: PurposeRevisionView[];
    history_truncated: boolean;
  };
  items: LivingPortfolioItem[];
  item_count: number;
  state_counts: Record<LivingPortfolioState, number>;
}

export type PurposeAmendmentAttemptState =
  | "pending"
  | "committed"
  | "rejected"
  | "uncertain"
  | "conflicted";

export interface PurposeAmendmentAttempt {
  proposal_id: string;
  lineage_id: string;
  author_engram_id: string;
  harness_turn_id: string;
  tool_call_event_id: string;
  tool_call_id: string;
  expected_revision: number | null;
  amendment_kind: "establish" | "amend" | "withdraw";
  content: string | null;
  content_digest: string;
  source_event_id: string;
  source_causal_id: string;
  source_kind: "user" | "self" | "habitat" | "sensory" | "propagation";
  source_domain: "pulse" | "world" | "habitat";
  source_flow: "content" | null;
  source_center_id: string | null;
  source_provenance_digest: string | null;
  state: PurposeAmendmentAttemptState;
  committed_revision_id: string | null;
  result_event_id: string | null;
  resolution_code: string | null;
  created_at: string;
  resolved_at: string | null;
  evidence_class: "CONTRACT_ONLY";
}

export interface PurposeAmendmentsProjection {
  schema_version: "purpose-amendments.v1";
  world_id: string;
  subject: {
    requested_engram_id: string;
    lineage_id: string | null;
    current_engram_id: string;
    generation: number;
  };
  current_purpose_revision_id: string | null;
  attempts: PurposeAmendmentAttempt[];
  attempt_count: number;
  settlement: {
    health: "healthy" | "degraded" | "unavailable";
    last_error_type: string | null;
    startup_recovery: Record<PurposeAmendmentAttemptState, number>;
  };
  evidence_class: "LIVE_GATE_UNVERIFIED";
}

export type ActivityCenterUpdate = Partial<
  Pick<ActivityCenterSummary, "title" | "description" | "status" | "autonomy">
>;

function parseTaskFront(value: unknown): TaskFrontSummary | null {
  const row = rec(value);
  const id = str(row.id);
  const centerId = str(row.center_id);
  const focalEngramId = str(row.focal_engram_id);
  const title = str(row.title);
  if (id === null || centerId === null || focalEngramId === null || title === null) {
    return null;
  }
  return {
    id,
    center_id: centerId,
    focal_engram_id: focalEngramId,
    title,
    status: str(row.status) ?? "open",
    created_at: str(row.created_at),
    updated_at: str(row.updated_at),
    last_opened_at: str(row.last_opened_at),
  };
}

function parseActivityCenter(value: unknown): ActivityCenterSummary | null {
  const row = rec(value);
  const id = str(row.id);
  const kind = str(row.kind) as ActivityKind | null;
  const title = str(row.title);
  if (id === null || kind === null || title === null) return null;
  return {
    id,
    kind,
    title,
    description: typeof row.description === "string" ? row.description : "",
    status: str(row.status) ?? "active",
    origin: str(row.origin) ?? "user",
    autonomy: num(row.autonomy) ?? 1,
    project_id: str(row.project_id),
    focal_engram_id: str(row.focal_engram_id),
    created_at: str(row.created_at),
    updated_at: str(row.updated_at),
    last_active_at: str(row.last_active_at),
  };
}

function taskDetailPayloadError(path: string, expected: string): never {
  throw new Error(`Invalid TaskFront detail: ${path} must be ${expected}`);
}

function taskDetailRecord(
  value: unknown,
  path: string,
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return taskDetailPayloadError(path, "an object");
  }
  return value as Record<string, unknown>;
}

function taskDetailField(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): unknown {
  if (!Object.hasOwn(parent, key)) {
    return taskDetailPayloadError(`${path}.${key}`, "present");
  }
  return parent[key];
}

function taskDetailString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  allowEmpty = false,
): string {
  const value = taskDetailField(parent, key, path);
  if (typeof value !== "string" || (!allowEmpty && value === "")) {
    return taskDetailPayloadError(
      `${path}.${key}`,
      allowEmpty ? "a string" : "a non-empty string",
    );
  }
  return value;
}

function taskDetailNullableString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = taskDetailField(parent, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || value === "") {
    return taskDetailPayloadError(`${path}.${key}`, "a non-empty string or null");
  }
  return value;
}

function taskDetailInteger(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): number {
  const value = taskDetailField(parent, key, path);
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isInteger(value) ||
    value < 0
  ) {
    return taskDetailPayloadError(`${path}.${key}`, "an integer >= 0");
  }
  return value;
}

function taskDetailTimestamp(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string {
  const value = taskDetailString(parent, key, path);
  if (!Number.isFinite(Date.parse(value))) {
    return taskDetailPayloadError(`${path}.${key}`, "an ISO timestamp");
  }
  return value;
}

function taskDetailNullableTimestamp(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = taskDetailNullableString(parent, key, path);
  if (value !== null && !Number.isFinite(Date.parse(value))) {
    return taskDetailPayloadError(`${path}.${key}`, "an ISO timestamp or null");
  }
  return value;
}

const TASK_RELATIONSHIP_STATUSES = new Set<TaskRelationshipStatus>([
  "active",
  "paused",
  "renegotiation_requested",
  "exited",
]);
const TASK_RELATIONSHIP_ACTIONS = new Set<TaskRelationshipAction>([
  "accepted",
  "paused",
  "renegotiation_requested",
  "terms_proposed",
  "resumed",
  "exited",
  "succession",
]);
const TASK_RELATIONSHIP_ACTORS = new Set<TaskRelationshipActorKind>([
  "subject",
  "user",
  "system",
]);

function taskDetailNullableText(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  maxLength: number,
): string | null {
  const value = taskDetailField(parent, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || value.length > maxLength) {
    return taskDetailPayloadError(
      `${path}.${key}`,
      `a string of at most ${maxLength} characters or null`,
    );
  }
  return value;
}

function taskRelationshipStatus(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): TaskRelationshipStatus {
  const value = taskDetailString(parent, key, path) as TaskRelationshipStatus;
  if (!TASK_RELATIONSHIP_STATUSES.has(value)) {
    return taskDetailPayloadError(
      `${path}.${key}`,
      "active, paused, renegotiation_requested, or exited",
    );
  }
  return value;
}

function taskRelationshipNullableStatus(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): TaskRelationshipStatus | null {
  const value = taskDetailField(parent, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || !TASK_RELATIONSHIP_STATUSES.has(value as TaskRelationshipStatus)) {
    return taskDetailPayloadError(
      `${path}.${key}`,
      "a TaskRelationship status or null",
    );
  }
  return value as TaskRelationshipStatus;
}

function parseTaskRelationship(
  value: unknown,
  path: string,
): TaskRelationshipView {
  const row = taskDetailRecord(value, path);
  const status = taskRelationshipStatus(row, "status", path);
  const revision = taskDetailInteger(row, "revision", path);
  const createdAt = taskDetailTimestamp(row, "created_at", path);
  const updatedAt = taskDetailTimestamp(row, "updated_at", path);
  const exitedAt = taskDetailNullableTimestamp(row, "exited_at", path);
  if (revision < 1) {
    return taskDetailPayloadError(`${path}.revision`, "an integer >= 1");
  }
  if (Date.parse(updatedAt) < Date.parse(createdAt)) {
    return taskDetailPayloadError(
      `${path}.updated_at`,
      "an ISO timestamp not preceding created_at",
    );
  }
  if ((status === "exited") !== (exitedAt !== null)) {
    return taskDetailPayloadError(
      `${path}.exited_at`,
      status === "exited"
        ? "an ISO timestamp for an exited relationship"
        : "null for a non-exited relationship",
    );
  }
  return {
    id: taskDetailString(row, "id", path),
    world_id: taskDetailString(row, "world_id", path),
    accepted_offer_id: taskDetailString(row, "accepted_offer_id", path),
    task_front_id: taskDetailString(row, "task_front_id", path),
    center_id: taskDetailString(row, "center_id", path),
    original_subject_engram_id: taskDetailString(
      row,
      "original_subject_engram_id",
      path,
    ),
    current_subject_engram_id: taskDetailString(
      row,
      "current_subject_engram_id",
      path,
    ),
    status,
    revision,
    latest_terms_event_id: taskDetailNullableString(
      row,
      "latest_terms_event_id",
      path,
    ),
    latest_subject_note: taskDetailNullableText(
      row,
      "latest_subject_note",
      path,
      4_000,
    ),
    created_at: createdAt,
    updated_at: updatedAt,
    exited_at: exitedAt,
  };
}

function parseTaskRelationshipEvent(
  value: unknown,
  path: string,
): TaskRelationshipEventView {
  const row = taskDetailRecord(value, path);
  const seq = taskDetailInteger(row, "seq", path);
  const action = taskDetailString(row, "action", path) as TaskRelationshipAction;
  const actorKind = taskDetailString(
    row,
    "actor_kind",
    path,
  ) as TaskRelationshipActorKind;
  if (seq < 1) {
    return taskDetailPayloadError(`${path}.seq`, "an integer >= 1");
  }
  if (!TASK_RELATIONSHIP_ACTIONS.has(action)) {
    return taskDetailPayloadError(`${path}.action`, "a TaskRelationship action");
  }
  if (!TASK_RELATIONSHIP_ACTORS.has(actorKind)) {
    return taskDetailPayloadError(
      `${path}.actor_kind`,
      "subject, user, or system",
    );
  }
  const content = taskDetailNullableText(
    row,
    "content",
    path,
    MAX_TASK_RELATIONSHIP_CONTENT,
  );
  if (
    (action === "renegotiation_requested" || action === "terms_proposed") &&
    (content === null || content.trim() === "")
  ) {
    return taskDetailPayloadError(
      `${path}.content`,
      `non-empty for ${action}`,
    );
  }
  return {
    relationship_id: taskDetailString(row, "relationship_id", path),
    seq,
    action,
    actor_kind: actorKind,
    actor_id: taskDetailString(row, "actor_id", path),
    before_status: taskRelationshipNullableStatus(
      row,
      "before_status",
      path,
    ),
    after_status: taskRelationshipStatus(row, "after_status", path),
    content,
    source_event_id: taskDetailNullableString(row, "source_event_id", path),
    created_at: taskDetailTimestamp(row, "created_at", path),
  };
}

function taskRelationshipEventIsValid(
  event: TaskRelationshipEventView,
  previousStatus: TaskRelationshipStatus | null,
): boolean {
  if (event.before_status !== previousStatus) return false;
  switch (event.action) {
    case "accepted":
      return event.seq === 1 && event.actor_kind === "subject" &&
        previousStatus === null && event.after_status === "active";
    case "paused":
      return event.actor_kind === "subject" && previousStatus === "active" &&
        event.after_status === "paused";
    case "renegotiation_requested":
      return event.actor_kind === "subject" && previousStatus !== null &&
        previousStatus !== "exited" &&
        event.after_status === "renegotiation_requested";
    case "terms_proposed":
      return event.actor_kind === "user" && previousStatus !== null &&
        previousStatus !== "exited" &&
        event.after_status === "renegotiation_requested";
    case "resumed":
      return event.actor_kind === "subject" &&
        (previousStatus === "paused" || previousStatus === "renegotiation_requested") &&
        event.after_status === "active";
    case "exited":
      return event.actor_kind === "subject" && previousStatus !== null &&
        previousStatus !== "exited" && event.after_status === "exited";
    case "succession":
      return event.actor_kind === "system" && previousStatus !== null &&
        event.after_status === previousStatus;
  }
}

function parseTaskSubjectEngram(
  value: unknown,
  path: string,
): TaskSubjectEngramView {
  const row = taskDetailRecord(value, path);
  return {
    id: taskDetailString(row, "id", path),
    project_id: taskDetailNullableString(row, "project_id", path),
    status: taskDetailString(row, "status", path),
    created_at: taskDetailTimestamp(row, "created_at", path),
    last_pulse_at: taskDetailNullableTimestamp(row, "last_pulse_at", path),
    total_pulses: taskDetailInteger(row, "total_pulses", path),
    name: taskDetailNullableString(row, "name", path),
    name_origin: taskDetailString(row, "name_origin", path),
    nickname: taskDetailNullableString(row, "nickname", path),
  };
}

function parseTaskCenterMessage(
  value: unknown,
  path: string,
): TaskCenterMessageView {
  const row = taskDetailRecord(value, path);
  const metadata = taskDetailRecord(
    taskDetailField(row, "metadata", path),
    `${path}.metadata`,
  );
  const rawContent = taskDetailField(row, "content", path);
  if (typeof rawContent !== "string") {
    return taskDetailPayloadError(`${path}.content`, "a string");
  }
  return {
    seq: taskDetailInteger(row, "seq", path),
    event_id: taskDetailString(row, "event_id", path),
    causal_id: taskDetailString(row, "causal_id", path),
    parent_event_id: taskDetailNullableString(row, "parent_event_id", path),
    engram_id: taskDetailString(row, "engram_id", path),
    center_id: taskDetailNullableString(row, "center_id", path),
    role: taskDetailString(row, "role", path),
    kind: taskDetailString(row, "kind", path),
    source: taskDetailString(row, "source", path),
    status: taskDetailString(row, "status", path),
    content: rawContent,
    metadata,
    timestamp: taskDetailTimestamp(row, "timestamp", path),
    source_engram_id: taskDetailNullableString(
      row,
      "source_engram_id",
      path,
    ),
  };
}

export function parseTaskFrontDetail(
  value: unknown,
  expectedFrontId?: string,
): TaskFrontDetail {
  const root = taskDetailRecord(value, "$");
  const frontPath = "$.task_front";
  const front = parseTaskFront(taskDetailField(root, "task_front", "$"));
  if (
    front === null ||
    !(front.status === "open" || front.status === "closed" || front.status === "archived") ||
    front.created_at === null ||
    front.updated_at === null ||
    front.last_opened_at === null
  ) {
    return taskDetailPayloadError(frontPath, "a complete TaskFront view");
  }
  if (expectedFrontId !== undefined && front.id !== expectedFrontId) {
    return taskDetailPayloadError(`${frontPath}.id`, `"${expectedFrontId}"`);
  }

  const centerPath = "$.activity_center";
  const center = parseActivityCenter(taskDetailField(root, "activity_center", "$"));
  if (
    center === null ||
    center.kind !== "task" ||
    center.id !== front.center_id ||
    center.focal_engram_id !== front.focal_engram_id ||
    center.created_at === null ||
    center.updated_at === null
  ) {
    return taskDetailPayloadError(
      centerPath,
      "the complete task Center bound to the TaskFront",
    );
  }

  const focalEngram = parseTaskSubjectEngram(
    taskDetailField(root, "focal_engram", "$"),
    "$.focal_engram",
  );
  if (focalEngram.id !== front.focal_engram_id) {
    return taskDetailPayloadError(
      "$.focal_engram.id",
      `the TaskFront focal Engram "${front.focal_engram_id}"`,
    );
  }

  const messageScope = taskDetailString(root, "message_scope", "$" );
  if (messageScope !== "center") {
    return taskDetailPayloadError("$.message_scope", 'literal "center"');
  }
  const messageRows = taskDetailField(root, "messages", "$" );
  const unattributedRows = taskDetailField(root, "unattributed_history", "$" );
  if (!Array.isArray(messageRows)) {
    return taskDetailPayloadError("$.messages", "an array");
  }
  if (!Array.isArray(unattributedRows) || unattributedRows.length > 50) {
    return taskDetailPayloadError(
      "$.unattributed_history",
      "an array of at most 50 messages",
    );
  }

  const messages = messageRows.map((message, index) =>
    parseTaskCenterMessage(message, `$.messages[${index}]`));
  const unattributedHistory = unattributedRows.map((message, index) =>
    parseTaskCenterMessage(message, `$.unattributed_history[${index}]`));
  if (
    messages.some((message) =>
      message.center_id !== front.center_id) ||
    unattributedHistory.some((message) => message.center_id !== null)
  ) {
    return taskDetailPayloadError(
      "$.messages",
      "history attributed only to this Task Center, with unattributed history kept separate",
    );
  }

  const rawRelationship = taskDetailField(root, "task_relationship", "$" );
  const taskRelationship = rawRelationship === null
    ? null
    : parseTaskRelationship(rawRelationship, "$.task_relationship");
  const rawRelationshipEvents = taskDetailField(
    root,
    "relationship_events",
    "$",
  );
  if (!Array.isArray(rawRelationshipEvents)) {
    return taskDetailPayloadError("$.relationship_events", "an array");
  }
  const relationshipEvents = rawRelationshipEvents.map((event, index) =>
    parseTaskRelationshipEvent(event, `$.relationship_events[${index}]`));
  const taskRelationshipMode = taskDetailString(
    root,
    "task_relationship_mode",
    "$",
  );
  if (
    taskRelationshipMode !== "subject_consent_managed" &&
    taskRelationshipMode !== "unmanaged_compatibility"
  ) {
    return taskDetailPayloadError(
      "$.task_relationship_mode",
      '"subject_consent_managed" or "unmanaged_compatibility"',
    );
  }
  if (
    (taskRelationshipMode === "subject_consent_managed") !==
    (taskRelationship !== null)
  ) {
    return taskDetailPayloadError(
      "$.task_relationship_mode",
      "consistent with the canonical task_relationship presence",
    );
  }
  if (taskRelationship === null) {
    if (relationshipEvents.length !== 0) {
      return taskDetailPayloadError(
        "$.relationship_events",
        "empty when task_relationship is null (unmanaged_compatibility)",
      );
    }
  } else {
    if (
      taskRelationship.task_front_id !== front.id ||
      taskRelationship.center_id !== center.id ||
      taskRelationship.current_subject_engram_id !== focalEngram.id
    ) {
      return taskDetailPayloadError(
        "$.task_relationship",
        "bound to this TaskFront, Task Center, and current focal subject",
      );
    }
    const expectedCenterStatus: Record<TaskRelationshipStatus, string> = {
      active: "active",
      paused: "paused",
      renegotiation_requested: "paused",
      exited: "completed",
    };
    if (center.status !== expectedCenterStatus[taskRelationship.status]) {
      return taskDetailPayloadError(
        "$.activity_center.status",
        `the ${expectedCenterStatus[taskRelationship.status]} projection of the relationship`,
      );
    }
    if (relationshipEvents.length !== taskRelationship.revision) {
      return taskDetailPayloadError(
        "$.relationship_events",
        "the complete event history matching task_relationship.revision",
      );
    }
    let previousStatus: TaskRelationshipStatus | null = null;
    let latestTermsEventId: string | null = null;
    let latestSubjectNote: string | null = null;
    for (let index = 0; index < relationshipEvents.length; index += 1) {
      const event = relationshipEvents[index];
      if (
        event.relationship_id !== taskRelationship.id ||
        event.seq !== index + 1 ||
        !taskRelationshipEventIsValid(event, previousStatus)
      ) {
        return taskDetailPayloadError(
          `$.relationship_events[${index}]`,
          "the next actor-authorized event in the complete relationship history",
        );
      }
      previousStatus = event.after_status;
      if (event.action === "terms_proposed") {
        latestTermsEventId = event.source_event_id;
      }
      if (event.actor_kind === "subject" && event.action !== "succession") {
        latestSubjectNote = event.content;
      }
    }
    if (previousStatus !== taskRelationship.status) {
      return taskDetailPayloadError(
        "$.relationship_events",
        "a final status matching task_relationship.status",
      );
    }
    if (latestTermsEventId !== taskRelationship.latest_terms_event_id) {
      return taskDetailPayloadError(
        "$.task_relationship.latest_terms_event_id",
        "the source event of the latest proposed terms, or null",
      );
    }
    if (latestSubjectNote !== taskRelationship.latest_subject_note) {
      return taskDetailPayloadError(
        "$.task_relationship.latest_subject_note",
        "the content of the latest subject decision, or null",
      );
    }
  }

  return {
    taskFront: front,
    activityCenter: center,
    focalEngram,
    messageScope,
    messages,
    unattributedHistory,
    taskRelationshipMode,
    taskRelationship,
    relationshipEvents,
  };
}

const PORTFOLIO_STATES: LivingPortfolioState[] = [
  "active",
  "quiet",
  "parked",
  "completed",
  "archived",
];

const CENTER_TO_PORTFOLIO_STATE: Record<
  "active" | "dormant" | "paused" | "completed" | "archived",
  LivingPortfolioState
> = {
  active: "active",
  dormant: "quiet",
  paused: "parked",
  completed: "completed",
  archived: "archived",
};

function portfolioPayloadError(path: string, expected: string): never {
  throw new Error(
    `Invalid living-portfolio.v1 payload: ${path} must be ${expected}`,
  );
}

function portfolioRecord(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return portfolioPayloadError(path, "an object");
  }
  return value as Record<string, unknown>;
}

function portfolioField(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): unknown {
  if (!Object.hasOwn(parent, key)) {
    return portfolioPayloadError(`${path}.${key}`, "present");
  }
  return parent[key];
}

function portfolioString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  allowEmpty = false,
): string {
  const value = portfolioField(parent, key, path);
  if (typeof value !== "string" || (!allowEmpty && value === "")) {
    return portfolioPayloadError(
      `${path}.${key}`,
      allowEmpty ? "a string" : "a non-empty string",
    );
  }
  return value;
}

function portfolioNullableString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = portfolioField(parent, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || value === "") {
    return portfolioPayloadError(`${path}.${key}`, "a non-empty string or null");
  }
  return value;
}

function portfolioInteger(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  minimum = 0,
): number {
  const value = portfolioField(parent, key, path);
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isInteger(value) ||
    value < minimum
  ) {
    return portfolioPayloadError(`${path}.${key}`, `an integer >= ${minimum}`);
  }
  return value;
}

function portfolioBoolean(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): boolean {
  const value = portfolioField(parent, key, path);
  if (typeof value !== "boolean") {
    return portfolioPayloadError(`${path}.${key}`, "a boolean");
  }
  return value;
}

function portfolioTimestamp(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string {
  const value = portfolioString(parent, key, path);
  if (!Number.isFinite(Date.parse(value))) {
    return portfolioPayloadError(`${path}.${key}`, "an ISO timestamp");
  }
  return value;
}

function portfolioNullableTimestamp(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = portfolioNullableString(parent, key, path);
  if (value !== null && !Number.isFinite(Date.parse(value))) {
    return portfolioPayloadError(`${path}.${key}`, "an ISO timestamp or null");
  }
  return value;
}

function parsePortfolioCenter(
  value: unknown,
  path: string,
): ActivityCenterSummary {
  const row = portfolioRecord(value, path);
  const kind = portfolioString(row, "kind", path);
  const status = portfolioString(row, "status", path);
  const origin = portfolioString(row, "origin", path);
  const autonomy = portfolioField(row, "autonomy", path);
  if (!LIFE_KINDS.includes(kind as Exclude<ActivityKind, "task">)) {
    return portfolioPayloadError(`${path}.kind`, "a non-task ActivityKind");
  }
  if (!Object.hasOwn(CENTER_TO_PORTFOLIO_STATE, status)) {
    return portfolioPayloadError(`${path}.status`, "an ActivityCenterStatus");
  }
  if (!(["user", "self", "shared", "system"] as const).includes(
    origin as "user" | "self" | "shared" | "system",
  )) {
    return portfolioPayloadError(`${path}.origin`, "an ActivityOrigin");
  }
  if (
    typeof autonomy !== "number" ||
    !Number.isFinite(autonomy) ||
    autonomy < 0 ||
    autonomy > 1
  ) {
    return portfolioPayloadError(`${path}.autonomy`, "a finite number from 0 to 1");
  }
  return {
    id: portfolioString(row, "id", path),
    kind: kind as Exclude<ActivityKind, "task">,
    title: portfolioString(row, "title", path),
    description: portfolioString(row, "description", path, true),
    status,
    origin,
    autonomy,
    project_id: portfolioNullableString(row, "project_id", path),
    focal_engram_id: portfolioNullableString(row, "focal_engram_id", path),
    created_at: portfolioTimestamp(row, "created_at", path),
    updated_at: portfolioTimestamp(row, "updated_at", path),
    last_active_at: portfolioNullableTimestamp(row, "last_active_at", path),
  };
}

function parsePurposeRevision(
  value: unknown,
  path: string,
): PurposeRevisionView {
  const row = portfolioRecord(value, path);
  const amendmentKind = portfolioString(row, "amendment_kind", path);
  const state = portfolioString(row, "state", path);
  const rawContent = portfolioField(row, "content", path);
  const content = rawContent === null
    ? null
    : typeof rawContent === "string" && rawContent.trim() !== ""
      ? rawContent
      : portfolioPayloadError(`${path}.content`, "non-empty text or null");
  if (!(["establish", "amend", "withdraw"] as const).includes(
    amendmentKind as PurposeRevisionView["amendment_kind"],
  )) {
    return portfolioPayloadError(`${path}.amendment_kind`, "a purpose amendment kind");
  }
  if (!(["current", "superseded", "withdrawn"] as const).includes(
    state as PurposeRevisionView["state"],
  )) {
    return portfolioPayloadError(`${path}.state`, "a purpose revision state");
  }
  if ((amendmentKind === "withdraw") !== (content === null)) {
    return portfolioPayloadError(
      `${path}.content`,
      amendmentKind === "withdraw" ? "null for withdrawal" : "present for purpose text",
    );
  }
  const supersededAt = portfolioNullableTimestamp(row, "superseded_at", path);
  if (state === "current" && supersededAt !== null) {
    return portfolioPayloadError(`${path}.superseded_at`, "null for a current revision");
  }
  if (state === "superseded" && supersededAt === null) {
    return portfolioPayloadError(`${path}.superseded_at`, "present for a superseded revision");
  }
  return {
    purpose_revision_id: portfolioString(row, "purpose_revision_id", path),
    lineage_id: portfolioString(row, "lineage_id", path),
    author_engram_id: portfolioString(row, "author_engram_id", path),
    revision: portfolioInteger(row, "revision", path, 1),
    predecessor_revision_id: portfolioNullableString(
      row,
      "predecessor_revision_id",
      path,
    ),
    amendment_kind: amendmentKind as PurposeRevisionView["amendment_kind"],
    content,
    content_digest: portfolioString(row, "content_digest", path),
    state: state as PurposeRevisionView["state"],
    source_event_id: portfolioString(row, "source_event_id", path),
    reflection_event_id: portfolioNullableString(row, "reflection_event_id", path),
    created_at: portfolioTimestamp(row, "created_at", path),
    superseded_at: supersededAt,
  };
}

function comparePortfolioItems(
  left: LivingPortfolioItem,
  right: LivingPortfolioItem,
): number {
  const stateDelta = PORTFOLIO_STATES.indexOf(left.portfolio_state) -
    PORTFOLIO_STATES.indexOf(right.portfolio_state);
  if (stateDelta !== 0) return stateDelta;
  const updatedDelta = Date.parse(right.center.updated_at ?? "") -
    Date.parse(left.center.updated_at ?? "");
  if (updatedDelta !== 0) return updatedDelta;
  return left.center.id < right.center.id
    ? -1
    : left.center.id > right.center.id
      ? 1
      : 0;
}

export function parseLivingPortfolio(value: unknown): LivingPortfolio {
  const root = portfolioRecord(value, "$");
  if (portfolioString(root, "schema_version", "$") !== "living-portfolio.v1") {
    return portfolioPayloadError("$.schema_version", 'literal "living-portfolio.v1"');
  }

  const subjectRow = portfolioRecord(portfolioField(root, "subject", "$"), "$.subject");
  const lineageState = portfolioString(subjectRow, "lineage_state", "$.subject");
  if (lineageState !== "active" && lineageState !== "unestablished") {
    return portfolioPayloadError(
      "$.subject.lineage_state",
      '"active" or "unestablished"',
    );
  }
  const subject: LivingPortfolioSubject = {
    requested_engram_id: portfolioString(
      subjectRow,
      "requested_engram_id",
      "$.subject",
    ),
    lineage_state: lineageState,
    lineage_id: portfolioNullableString(subjectRow, "lineage_id", "$.subject"),
    root_engram_id: portfolioString(subjectRow, "root_engram_id", "$.subject"),
    current_engram_id: portfolioString(
      subjectRow,
      "current_engram_id",
      "$.subject",
    ),
    generation: portfolioInteger(subjectRow, "generation", "$.subject"),
  };
  if (
    subject.lineage_state === "unestablished" &&
    (
      subject.lineage_id !== null ||
      subject.root_engram_id !== subject.requested_engram_id ||
      subject.current_engram_id !== subject.requested_engram_id ||
      subject.generation !== 0
    )
  ) {
    return portfolioPayloadError(
      "$.subject",
      "an unestablished lineage rooted in the requested Engram",
    );
  }
  if (subject.lineage_state === "active" && subject.lineage_id === null) {
    return portfolioPayloadError("$.subject.lineage_id", "present for an active lineage");
  }

  const purposeRow = portfolioRecord(portfolioField(root, "purpose", "$"), "$.purpose");
  const currentValue = portfolioField(purposeRow, "current", "$.purpose");
  const current = currentValue === null
    ? null
    : parsePurposeRevision(currentValue, "$.purpose.current");
  const historyValue = portfolioField(purposeRow, "history", "$.purpose");
  if (!Array.isArray(historyValue)) {
    return portfolioPayloadError("$.purpose.history", "an array");
  }
  const history = historyValue.map((revision, index) =>
    parsePurposeRevision(revision, `$.purpose.history[${index}]`));
  const historyTruncated = portfolioBoolean(
    purposeRow,
    "history_truncated",
    "$.purpose",
  );
  if (
    history.length > 100 ||
    history.some((revision, index) =>
      (index > 0 && revision.revision <= history[index - 1].revision) ||
      (subject.lineage_id !== null && revision.lineage_id !== subject.lineage_id))
  ) {
    return portfolioPayloadError(
      "$.purpose.history",
      "at most 100 ascending revisions from the subject lineage",
    );
  }
  if (
    current !== null &&
    (
      current.state !== "current" ||
      current.lineage_id !== subject.lineage_id ||
      (!historyTruncated &&
        !history.some((revision) =>
          revision.purpose_revision_id === current.purpose_revision_id))
    )
  ) {
    return portfolioPayloadError(
      "$.purpose.current",
      "the current revision from the subject lineage",
    );
  }
  if (
    subject.lineage_state === "unestablished" &&
    (current !== null || history.length !== 0 || historyTruncated)
  ) {
    return portfolioPayloadError(
      "$.purpose",
      "empty for an unestablished lineage",
    );
  }

  const itemRows = portfolioField(root, "items", "$" );
  if (!Array.isArray(itemRows)) {
    return portfolioPayloadError("$.items", "an array");
  }
  const seenCenterIds = new Set<string>();
  const items = itemRows.map((value, index): LivingPortfolioItem => {
    const path = `$.items[${index}]`;
    const row = portfolioRecord(value, path);
    const center = parsePortfolioCenter(portfolioField(row, "center", path), `${path}.center`);
    const relation = portfolioString(row, "relation", path);
    const portfolioState = portfolioString(row, "portfolio_state", path);
    if (!(["focal", "participant", "shared"] as const).includes(
      relation as LivingPortfolioRelation,
    )) {
      return portfolioPayloadError(`${path}.relation`, "a membership relation");
    }
    if (!PORTFOLIO_STATES.includes(portfolioState as LivingPortfolioState)) {
      return portfolioPayloadError(`${path}.portfolio_state`, "a portfolio state");
    }
    const expectedState = CENTER_TO_PORTFOLIO_STATE[
      center.status as keyof typeof CENTER_TO_PORTFOLIO_STATE
    ];
    if (portfolioState !== expectedState) {
      return portfolioPayloadError(
        `${path}.portfolio_state`,
        `"${expectedState}" for Center status "${center.status}"`,
      );
    }
    if (seenCenterIds.has(center.id)) {
      return portfolioPayloadError(`${path}.center.id`, "unique in the Portfolio");
    }
    seenCenterIds.add(center.id);
    return {
      center,
      relation: relation as LivingPortfolioRelation,
      portfolio_state: portfolioState as LivingPortfolioState,
    };
  });
  if (items.some((item, index) => index > 0 && comparePortfolioItems(items[index - 1], item) > 0)) {
    return portfolioPayloadError("$.items", "in canonical portfolio order");
  }

  const itemCount = portfolioInteger(root, "item_count", "$" );
  if (itemCount !== items.length) {
    return portfolioPayloadError("$.item_count", "equal to items.length");
  }
  const stateCountsRow = portfolioRecord(
    portfolioField(root, "state_counts", "$"),
    "$.state_counts",
  );
  const stateCounts = Object.fromEntries(
    PORTFOLIO_STATES.map((state) => [
      state,
      portfolioInteger(stateCountsRow, state, "$.state_counts"),
    ]),
  ) as Record<LivingPortfolioState, number>;
  for (const state of PORTFOLIO_STATES) {
    const actual = items.filter((item) => item.portfolio_state === state).length;
    if (stateCounts[state] !== actual) {
      return portfolioPayloadError(
        `$.state_counts.${state}`,
        `equal to the ${actual} matching items`,
      );
    }
  }

  return {
    schema_version: "living-portfolio.v1",
    subject,
    purpose: {
      current,
      history,
      history_truncated: historyTruncated,
    },
    items,
    item_count: itemCount,
    state_counts: stateCounts,
  };
}

const PURPOSE_ATTEMPT_STATES: PurposeAmendmentAttemptState[] = [
  "pending",
  "committed",
  "rejected",
  "uncertain",
  "conflicted",
];

function purposeAmendmentPayloadError(path: string, expected: string): never {
  throw new Error(
    `Invalid purpose-amendments.v1 payload: ${path} must be ${expected}`,
  );
}

function purposeAmendmentRecord(
  value: unknown,
  path: string,
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return purposeAmendmentPayloadError(path, "an object");
  }
  return value as Record<string, unknown>;
}

function purposeAmendmentExactKeys(
  row: Record<string, unknown>,
  path: string,
  expected: readonly string[],
): void {
  const actual = Object.keys(row).sort();
  const canonical = [...expected].sort();
  if (
    actual.length !== canonical.length ||
    actual.some((key, index) => key !== canonical[index])
  ) {
    purposeAmendmentPayloadError(
      path,
      `exactly the fields ${canonical.join(", ")}`,
    );
  }
}

function purposeAmendmentString(
  row: Record<string, unknown>,
  key: string,
  path: string,
): string {
  const value = row[key];
  if (typeof value !== "string" || value === "") {
    return purposeAmendmentPayloadError(`${path}.${key}`, "a non-empty string");
  }
  return value;
}

function purposeAmendmentNullableString(
  row: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = row[key];
  if (value === null) return null;
  if (typeof value !== "string" || value === "") {
    return purposeAmendmentPayloadError(
      `${path}.${key}`,
      "a non-empty string or null",
    );
  }
  return value;
}

function purposeAmendmentInteger(
  value: unknown,
  path: string,
  minimum: number,
): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isInteger(value) ||
    value < minimum
  ) {
    return purposeAmendmentPayloadError(path, `an integer >= ${minimum}`);
  }
  return value;
}

function purposeAmendmentTimestamp(value: string, path: string): string {
  if (!Number.isFinite(Date.parse(value))) {
    return purposeAmendmentPayloadError(path, "an ISO timestamp");
  }
  return value;
}

function purposeAmendmentDigest(
  value: string | null,
  path: string,
): string | null {
  if (value !== null && !/^[0-9a-f]{64}$/.test(value)) {
    return purposeAmendmentPayloadError(path, "a lowercase SHA-256 digest or null");
  }
  return value;
}

function parsePurposeAmendmentAttempt(
  value: unknown,
  path: string,
): PurposeAmendmentAttempt {
  const row = purposeAmendmentRecord(value, path);
  purposeAmendmentExactKeys(row, path, [
    "proposal_id",
    "lineage_id",
    "author_engram_id",
    "harness_turn_id",
    "tool_call_event_id",
    "tool_call_id",
    "expected_revision",
    "amendment_kind",
    "content",
    "content_digest",
    "source_event_id",
    "source_causal_id",
    "source_kind",
    "source_domain",
    "source_flow",
    "source_center_id",
    "source_provenance_digest",
    "state",
    "committed_revision_id",
    "result_event_id",
    "resolution_code",
    "created_at",
    "resolved_at",
    "evidence_class",
  ]);
  const proposalId = purposeAmendmentString(row, "proposal_id", path);
  const amendmentKind = purposeAmendmentString(row, "amendment_kind", path);
  if (!("establish amend withdraw".split(" ") as string[]).includes(amendmentKind)) {
    return purposeAmendmentPayloadError(
      `${path}.amendment_kind`,
      '"establish", "amend", or "withdraw"',
    );
  }
  const content = purposeAmendmentNullableString(row, "content", path);
  if ((amendmentKind === "withdraw") !== (content === null)) {
    return purposeAmendmentPayloadError(
      `${path}.content`,
      amendmentKind === "withdraw" ? "null for withdrawal" : "purpose text",
    );
  }
  const expectedRevision = row.expected_revision === null
    ? null
    : purposeAmendmentInteger(row.expected_revision, `${path}.expected_revision`, 1);
  const sourceKind = purposeAmendmentString(row, "source_kind", path);
  if (!("user self habitat sensory propagation".split(" ") as string[]).includes(sourceKind)) {
    return purposeAmendmentPayloadError(`${path}.source_kind`, "an eligible life source");
  }
  const sourceDomain = purposeAmendmentString(row, "source_domain", path);
  if (!("pulse world habitat".split(" ") as string[]).includes(sourceDomain)) {
    return purposeAmendmentPayloadError(`${path}.source_domain`, "an eligible life domain");
  }
  const sourceFlow = purposeAmendmentNullableString(row, "source_flow", path);
  if (sourceFlow !== null && sourceFlow !== "content") {
    return purposeAmendmentPayloadError(`${path}.source_flow`, '"content" or null');
  }
  const sourceProvenanceDigest = purposeAmendmentDigest(
    purposeAmendmentNullableString(row, "source_provenance_digest", path),
    `${path}.source_provenance_digest`,
  );
  if (sourceKind !== "self" && sourceProvenanceDigest === null) {
    return purposeAmendmentPayloadError(
      `${path}.source_provenance_digest`,
      "present for a non-self source",
    );
  }
  const state = purposeAmendmentString(row, "state", path);
  if (!PURPOSE_ATTEMPT_STATES.includes(state as PurposeAmendmentAttemptState)) {
    return purposeAmendmentPayloadError(`${path}.state`, "a purpose attempt state");
  }
  const committedRevisionId = purposeAmendmentNullableString(
    row,
    "committed_revision_id",
    path,
  );
  const resultEventId = purposeAmendmentNullableString(row, "result_event_id", path);
  const resolutionCode = purposeAmendmentNullableString(row, "resolution_code", path);
  const resolvedAtRaw = purposeAmendmentNullableString(row, "resolved_at", path);
  const resolvedAt = resolvedAtRaw === null
    ? null
    : purposeAmendmentTimestamp(resolvedAtRaw, `${path}.resolved_at`);
  if (
    (state === "pending" && (
      committedRevisionId !== null ||
      resultEventId !== null ||
      resolutionCode !== null ||
      resolvedAt !== null
    )) ||
    (state === "committed" && (
      committedRevisionId !== proposalId ||
      resultEventId === null ||
      resolutionCode !== "turn_settled" ||
      resolvedAt === null
    )) ||
    (state === "rejected" && (
      committedRevisionId !== null ||
      resultEventId !== null ||
      resolutionCode !== "harness_turn_failed" ||
      resolvedAt === null
    )) ||
    (state === "uncertain" && (
      committedRevisionId !== null ||
      resultEventId !== null ||
      resolutionCode !== "harness_turn_uncertain" ||
      resolvedAt === null
    )) ||
    (state === "conflicted" && (
      committedRevisionId !== null ||
      resultEventId === null ||
      !("lineage_holder_changed purpose_revision_conflict".split(" ") as string[])
        .includes(resolutionCode ?? "") ||
      resolvedAt === null
    ))
  ) {
    return purposeAmendmentPayloadError(
      path,
      `terminal evidence consistent with state "${state}"`,
    );
  }
  if (purposeAmendmentString(row, "evidence_class", path) !== "CONTRACT_ONLY") {
    return purposeAmendmentPayloadError(
      `${path}.evidence_class`,
      'literal "CONTRACT_ONLY"',
    );
  }
  return {
    proposal_id: proposalId,
    lineage_id: purposeAmendmentString(row, "lineage_id", path),
    author_engram_id: purposeAmendmentString(row, "author_engram_id", path),
    harness_turn_id: purposeAmendmentString(row, "harness_turn_id", path),
    tool_call_event_id: purposeAmendmentString(row, "tool_call_event_id", path),
    tool_call_id: purposeAmendmentString(row, "tool_call_id", path),
    expected_revision: expectedRevision,
    amendment_kind: amendmentKind as PurposeAmendmentAttempt["amendment_kind"],
    content,
    content_digest: purposeAmendmentDigest(
      purposeAmendmentString(row, "content_digest", path),
      `${path}.content_digest`,
    ) as string,
    source_event_id: purposeAmendmentString(row, "source_event_id", path),
    source_causal_id: purposeAmendmentString(row, "source_causal_id", path),
    source_kind: sourceKind as PurposeAmendmentAttempt["source_kind"],
    source_domain: sourceDomain as PurposeAmendmentAttempt["source_domain"],
    source_flow: sourceFlow as "content" | null,
    source_center_id: purposeAmendmentNullableString(row, "source_center_id", path),
    source_provenance_digest: sourceProvenanceDigest,
    state: state as PurposeAmendmentAttemptState,
    committed_revision_id: committedRevisionId,
    result_event_id: resultEventId,
    resolution_code: resolutionCode,
    created_at: purposeAmendmentTimestamp(
      purposeAmendmentString(row, "created_at", path),
      `${path}.created_at`,
    ),
    resolved_at: resolvedAt,
    evidence_class: "CONTRACT_ONLY",
  };
}

export function parsePurposeAmendments(
  value: unknown,
): PurposeAmendmentsProjection {
  const root = purposeAmendmentRecord(value, "$");
  purposeAmendmentExactKeys(root, "$", [
    "schema_version",
    "world_id",
    "subject",
    "current_purpose_revision_id",
    "attempts",
    "attempt_count",
    "settlement",
    "evidence_class",
  ]);
  if (purposeAmendmentString(root, "schema_version", "$") !== "purpose-amendments.v1") {
    return purposeAmendmentPayloadError(
      "$.schema_version",
      'literal "purpose-amendments.v1"',
    );
  }
  if (purposeAmendmentString(root, "evidence_class", "$") !== "LIVE_GATE_UNVERIFIED") {
    return purposeAmendmentPayloadError(
      "$.evidence_class",
      'literal "LIVE_GATE_UNVERIFIED"',
    );
  }
  const subjectRow = purposeAmendmentRecord(root.subject, "$.subject");
  purposeAmendmentExactKeys(subjectRow, "$.subject", [
    "requested_engram_id",
    "lineage_id",
    "current_engram_id",
    "generation",
  ]);
  const requestedEngramId = purposeAmendmentString(
    subjectRow,
    "requested_engram_id",
    "$.subject",
  );
  const lineageId = purposeAmendmentNullableString(
    subjectRow,
    "lineage_id",
    "$.subject",
  );
  const currentEngramId = purposeAmendmentString(
    subjectRow,
    "current_engram_id",
    "$.subject",
  );
  const generation = purposeAmendmentInteger(
    subjectRow.generation,
    "$.subject.generation",
    0,
  );
  const currentPurposeRevisionId = purposeAmendmentNullableString(
    root,
    "current_purpose_revision_id",
    "$",
  );
  if (!Array.isArray(root.attempts)) {
    return purposeAmendmentPayloadError("$.attempts", "an array");
  }
  if (root.attempts.length > 100) {
    return purposeAmendmentPayloadError("$.attempts", "at most 100 attempts");
  }
  const attempts = root.attempts.map((attempt, index) =>
    parsePurposeAmendmentAttempt(attempt, `$.attempts[${index}]`));
  const attemptCount = purposeAmendmentInteger(
    root.attempt_count,
    "$.attempt_count",
    0,
  );
  if (attemptCount !== attempts.length) {
    return purposeAmendmentPayloadError("$.attempt_count", "equal to attempts.length");
  }
  const seenProposals = new Set<string>();
  const seenTurns = new Set<string>();
  attempts.forEach((attempt, index) => {
    if (
      attempt.lineage_id !== lineageId ||
      seenProposals.has(attempt.proposal_id) ||
      seenTurns.has(attempt.harness_turn_id)
    ) {
      purposeAmendmentPayloadError(
        `$.attempts[${index}]`,
        "a unique proposal and turn from the projected lineage",
      );
    }
    if (index > 0) {
      const previous = attempts[index - 1];
      const previousTime = Date.parse(previous.created_at);
      const currentTime = Date.parse(attempt.created_at);
      if (
        previousTime < currentTime ||
        (previousTime === currentTime && previous.proposal_id < attempt.proposal_id)
      ) {
        purposeAmendmentPayloadError(
          "$.attempts",
          "in descending creation/proposal order",
        );
      }
    }
    seenProposals.add(attempt.proposal_id);
    seenTurns.add(attempt.harness_turn_id);
  });
  if (
    lineageId === null && (
      currentEngramId !== requestedEngramId ||
      generation !== 0 ||
      currentPurposeRevisionId !== null ||
      attempts.length !== 0
    )
  ) {
    return purposeAmendmentPayloadError(
      "$.subject",
      "an empty unestablished subject projection",
    );
  }

  const settlementRow = purposeAmendmentRecord(root.settlement, "$.settlement");
  purposeAmendmentExactKeys(settlementRow, "$.settlement", [
    "health",
    "last_error_type",
    "startup_recovery",
  ]);
  const health = purposeAmendmentString(settlementRow, "health", "$.settlement");
  if (!("healthy degraded unavailable".split(" ") as string[]).includes(health)) {
    return purposeAmendmentPayloadError("$.settlement.health", "a settlement health");
  }
  const lastErrorType = purposeAmendmentNullableString(
    settlementRow,
    "last_error_type",
    "$.settlement",
  );
  if (health === "degraded" ? lastErrorType === null : lastErrorType !== null) {
    return purposeAmendmentPayloadError(
      "$.settlement.last_error_type",
      "present exactly when settlement health is degraded",
    );
  }
  const recoveryRow = purposeAmendmentRecord(
    settlementRow.startup_recovery,
    "$.settlement.startup_recovery",
  );
  purposeAmendmentExactKeys(
    recoveryRow,
    "$.settlement.startup_recovery",
    PURPOSE_ATTEMPT_STATES,
  );
  const startupRecovery = Object.fromEntries(
    PURPOSE_ATTEMPT_STATES.map((state) => [
      state,
      purposeAmendmentInteger(
        recoveryRow[state],
        `$.settlement.startup_recovery.${state}`,
        0,
      ),
    ]),
  ) as Record<PurposeAmendmentAttemptState, number>;

  return {
    schema_version: "purpose-amendments.v1",
    world_id: purposeAmendmentString(root, "world_id", "$"),
    subject: {
      requested_engram_id: requestedEngramId,
      lineage_id: lineageId,
      current_engram_id: currentEngramId,
      generation,
    },
    current_purpose_revision_id: currentPurposeRevisionId,
    attempts,
    attempt_count: attemptCount,
    settlement: {
      health: health as PurposeAmendmentsProjection["settlement"]["health"],
      last_error_type: lastErrorType,
      startup_recovery: startupRecovery,
    },
    evidence_class: "LIVE_GATE_UNVERIFIED",
  };
}

function parseLivingConcern(value: unknown): LivingConcernView | null {
  const row = rec(value);
  const id = str(row.id);
  const centerId = str(row.center_id);
  const ownerEngramId = str(row.owner_engram_id);
  const content = typeof row.content === "string" ? row.content : null;
  const disposition = str(row.disposition);
  const causalId = str(row.causal_id);
  const sourceEventId = str(row.source_event_id);
  const revision = num(row.revision);
  if (
    id === null ||
    centerId === null ||
    ownerEngramId === null ||
    content === null ||
    disposition === null ||
    causalId === null ||
    sourceEventId === null ||
    revision === null
  ) {
    return null;
  }
  return {
    id,
    center_id: centerId,
    owner_engram_id: ownerEngramId,
    content,
    disposition,
    revisit_at: str(row.revisit_at),
    causal_id: causalId,
    source_event_id: sourceEventId,
    revision,
    last_reentry_event_id: str(row.last_reentry_event_id),
    created_at: str(row.created_at),
    updated_at: str(row.updated_at),
    resolved_at: str(row.resolved_at),
  };
}

function integer(value: unknown, minimum = 0): number | null {
  return typeof value === "number" && Number.isInteger(value) &&
    Number.isFinite(value) && value >= minimum ? value : null;
}

function requiredTimestamp(value: unknown): string | null {
  return typeof value === "string" && value !== "" &&
    Number.isFinite(Date.parse(value)) ? value : null;
}

function nullableTimestamp(value: unknown): string | null | undefined {
  if (value === null) return null;
  return requiredTimestamp(value) ?? undefined;
}

function parseLivingOrientation(value: unknown): LivingOrientationView | null {
  const row = rec(value);
  const id = str(row.id);
  const centerId = str(row.center_id);
  const ownerEngramId = str(row.owner_engram_id);
  const content = typeof row.content === "string" && row.content.trim() !== "" && row.content.length <= 4000
    ? row.content
    : null;
  const state = row.state === "open" || row.state === "resting" || row.state === "closed"
    ? row.state
    : null;
  const revision = integer(row.revision, 1);
  const engagementCount = integer(row.engagement_count);
  const nextEligibleAt = nullableTimestamp(row.next_eligible_at);
  const lastEngagedAt = nullableTimestamp(row.last_engaged_at);
  const createdAt = requiredTimestamp(row.created_at);
  const updatedAt = requiredTimestamp(row.updated_at);
  const closedAt = nullableTimestamp(row.closed_at);
  const lastEngagementEventId = row.last_engagement_event_id === null
    ? null
    : str(row.last_engagement_event_id);
  if (
    id === null ||
    centerId === null ||
    ownerEngramId === null ||
    content === null ||
    state === null ||
    revision === null ||
    engagementCount === null ||
    nextEligibleAt === undefined ||
    lastEngagedAt === undefined ||
    createdAt === null ||
    updatedAt === null ||
    closedAt === undefined ||
    row.last_engagement_event_id === undefined ||
    (row.last_engagement_event_id !== null && lastEngagementEventId === null)
  ) {
    return null;
  }
  const engagementAccountingIsCoherent = engagementCount === 0
    ? lastEngagementEventId === null && lastEngagedAt === null
    : lastEngagementEventId !== null && lastEngagedAt !== null;
  const stateTimingIsCoherent = state === "open"
    ? closedAt === null
    : state === "resting"
      ? nextEligibleAt === null && closedAt === null
      : nextEligibleAt === null && closedAt !== null;
  if (!engagementAccountingIsCoherent || !stateTimingIsCoherent) {
    return null;
  }
  return {
    id,
    centerId,
    ownerEngramId,
    content,
    state,
    revision,
    engagementCount,
    nextEligibleAt,
    lastEngagementEventId,
    lastEngagedAt,
    createdAt,
    updatedAt,
    closedAt,
  };
}

function parseActivitySummary(value: unknown): CenterActivitySummary | null {
  const row = rec(value);
  const queued = num(row.queued);
  const running = num(row.running);
  const uncertain = num(row.uncertain);
  if (queued === null || running === null || uncertain === null) return null;
  return {
    last_seq: num(row.last_seq),
    last_event_at: str(row.last_event_at),
    queued,
    running,
    uncertain,
    recent_source: str(row.recent_source),
    recent_kind: str(row.recent_kind),
  };
}

function parseCenterMessage(value: unknown): CenterMessageView | null {
  const row = rec(value);
  const seq = num(row.seq);
  const eventId = str(row.event_id);
  const causalId = str(row.causal_id);
  const centerId = str(row.center_id);
  const role = str(row.role);
  const kind = str(row.kind);
  const source = str(row.source);
  const status = str(row.status);
  const content = typeof row.content === "string" ? row.content : null;
  const timestamp = str(row.timestamp);
  if (
    seq === null ||
    eventId === null ||
    causalId === null ||
    centerId === null ||
    role === null ||
    kind === null ||
    source === null ||
    status === null ||
    content === null ||
    timestamp === null
  ) {
    return null;
  }
  return {
    seq,
    event_id: eventId,
    causal_id: causalId,
    parent_event_id: str(row.parent_event_id),
    engram_id: str(row.engram_id),
    center_id: centerId,
    role,
    kind,
    source,
    status,
    content,
    metadata: rec(row.metadata),
    timestamp,
  };
}

function parseList<T>(
  value: unknown,
  parser: (item: unknown) => T | null,
): T[] {
  if (!Array.isArray(value)) return [];
  const rows: T[] = [];
  for (const item of value) {
    const parsed = parser(item);
    if (parsed !== null) rows.push(parsed);
  }
  return rows;
}

function taskOfferPayloadError(path: string, expected: string): never {
  throw new Error(`Invalid task-offer response at ${path}; expected ${expected}.`);
}

function taskOfferObject(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return taskOfferPayloadError(path, "an object");
  }
  return value as Record<string, unknown>;
}

function taskOfferField(
  row: Record<string, unknown>,
  key: string,
  path: string,
): unknown {
  if (!Object.prototype.hasOwnProperty.call(row, key)) {
    return taskOfferPayloadError(`${path}.${key}`, "a present field");
  }
  return row[key];
}

function taskOfferString(
  row: Record<string, unknown>,
  key: string,
  path: string,
): string {
  const value = taskOfferField(row, key, path);
  if (typeof value !== "string" || value === "") {
    return taskOfferPayloadError(`${path}.${key}`, "a non-empty string");
  }
  return value;
}

function taskOfferNullableString(
  row: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = taskOfferField(row, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || value === "") {
    return taskOfferPayloadError(`${path}.${key}`, "a non-empty string or null");
  }
  return value;
}

function taskOfferPositiveInteger(
  row: Record<string, unknown>,
  key: string,
  path: string,
): number {
  const value = taskOfferField(row, key, path);
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    return taskOfferPayloadError(`${path}.${key}`, "a positive integer");
  }
  return value;
}

function parseTaskOfferRecord(value: unknown, path: string): TaskOfferRecord {
  const row = taskOfferObject(value, path);
  const statusValue = taskOfferString(row, "status", path);
  if (!TASK_OFFER_STATUSES.has(statusValue as TaskOfferStatus)) {
    return taskOfferPayloadError(
      `${path}.status`,
      "pending, changes_requested, accepted, refused, or withdrawn",
    );
  }
  const status = statusValue as TaskOfferStatus;
  const taskFrontId = taskOfferNullableString(row, "task_front_id", path);
  const withdrawnAt = taskOfferNullableString(row, "withdrawn_at", path);
  if (status !== "accepted" && taskFrontId !== null) {
    return taskOfferPayloadError(
      `${path}.task_front_id`,
      "null before acceptance",
    );
  }
  if ((status === "withdrawn") !== (withdrawnAt !== null)) {
    return taskOfferPayloadError(
      `${path}.withdrawn_at`,
      status === "withdrawn" ? "the withdrawal timestamp" : "null before withdrawal",
    );
  }
  return {
    id: taskOfferString(row, "id", path),
    world_id: taskOfferString(row, "world_id", path),
    subject_engram_id: taskOfferString(row, "subject_engram_id", path),
    status,
    current_revision: taskOfferPositiveInteger(row, "current_revision", path),
    task_front_id: taskFrontId,
    created_at: taskOfferString(row, "created_at", path),
    updated_at: taskOfferString(row, "updated_at", path),
    decided_at: taskOfferNullableString(row, "decided_at", path),
    withdrawn_at: withdrawnAt,
  };
}

function parseTaskOfferRevision(
  value: unknown,
  path: string,
): TaskOfferRevisionView {
  const row = taskOfferObject(value, path);
  const decisionValue = taskOfferField(row, "decision", path);
  let decision: TaskOfferDecision | null = null;
  if (decisionValue !== null) {
    if (
      typeof decisionValue !== "string" ||
      !TASK_OFFER_DECISIONS.has(decisionValue as TaskOfferDecision)
    ) {
      return taskOfferPayloadError(
        `${path}.decision`,
        "accept, refuse, request_changes, or null",
      );
    }
    decision = decisionValue as TaskOfferDecision;
  }
  const subjectResponse = taskOfferNullableString(row, "subject_response", path);
  const decisionEventId = taskOfferNullableString(row, "decision_event_id", path);
  const decidedAt = taskOfferNullableString(row, "decided_at", path);
  if (decision === null && (decisionEventId !== null || decidedAt !== null)) {
    return taskOfferPayloadError(
      path,
      "no decision evidence before a subject decision",
    );
  }
  if (decision !== null && (decisionEventId === null || decidedAt === null)) {
    return taskOfferPayloadError(
      path,
      "decision_event_id and decided_at for a subject decision",
    );
  }
  if (decision === "request_changes" && subjectResponse === null) {
    return taskOfferPayloadError(
      `${path}.subject_response`,
      "a non-empty change request",
    );
  }
  return {
    offer_id: taskOfferString(row, "offer_id", path),
    revision: taskOfferPositiveInteger(row, "revision", path),
    content: taskOfferString(row, "content", path),
    title: taskOfferString(row, "title", path),
    project_id: taskOfferNullableString(row, "project_id", path),
    latest_offer_event_id: taskOfferString(row, "latest_offer_event_id", path),
    decision,
    subject_response: subjectResponse,
    decision_event_id: decisionEventId,
    created_at: taskOfferString(row, "created_at", path),
    decided_at: decidedAt,
  };
}

export function parseTaskOfferSummary(
  value: unknown,
  path = "$",
): TaskOfferSummary {
  const root = taskOfferObject(value, path);
  const taskOffer = parseTaskOfferRecord(
    taskOfferField(root, "task_offer", path),
    `${path}.task_offer`,
  );
  const currentRevision = parseTaskOfferRevision(
    taskOfferField(root, "current_revision", path),
    `${path}.current_revision`,
  );
  if (
    currentRevision.offer_id !== taskOffer.id ||
    currentRevision.revision !== taskOffer.current_revision
  ) {
    return taskOfferPayloadError(
      path,
      "a current revision matching the task offer",
    );
  }
  const expectedDecision = taskOffer.status === "accepted"
    ? "accept"
    : taskOffer.status === "refused"
      ? "refuse"
      : taskOffer.status === "changes_requested"
        ? "request_changes"
        : null;
  if (expectedDecision !== null && currentRevision.decision !== expectedDecision) {
    return taskOfferPayloadError(
      `${path}.current_revision.decision`,
      expectedDecision,
    );
  }
  if (taskOffer.status === "pending" && currentRevision.decision !== null) {
    return taskOfferPayloadError(
      `${path}.current_revision.decision`,
      "null while the offer is pending",
    );
  }
  return { taskOffer, currentRevision };
}

export function parseTaskOfferList(value: unknown): TaskOfferSummary[] {
  const root = taskOfferObject(value, "$" );
  const rows = taskOfferField(root, "task_offers", "$" );
  if (!Array.isArray(rows)) {
    return taskOfferPayloadError("$.task_offers", "an array");
  }
  if (rows.length > MAX_TASK_OFFERS) {
    return taskOfferPayloadError(
      "$.task_offers",
      `at most ${MAX_TASK_OFFERS} bounded records`,
    );
  }
  const offers = rows.map((row, index) =>
    parseTaskOfferSummary(row, `$.task_offers[${index}]`));
  const ids = new Set(offers.map((offer) => offer.taskOffer.id));
  if (ids.size !== offers.length) {
    return taskOfferPayloadError("$.task_offers", "unique offer ids");
  }
  return offers;
}

function parseTaskOfferMutation(
  value: unknown,
  requireEvent: boolean,
): TaskOfferSummary & { eventId: string | null } {
  const root = taskOfferObject(value, "$" );
  const summary = parseTaskOfferSummary(root);
  const eventValue = Object.prototype.hasOwnProperty.call(root, "event_id")
    ? root.event_id
    : null;
  const eventId = eventValue === null
    ? null
    : typeof eventValue === "string" && eventValue !== ""
      ? eventValue
      : taskOfferPayloadError("$.event_id", "a non-empty string or null");
  if (requireEvent && eventId === null) {
    return taskOfferPayloadError("$.event_id", "the queued offer event id");
  }
  return { ...summary, eventId };
}

export async function fetchPulseWorld(
  base: string,
  signal: AbortSignal,
): Promise<PulseWorldDirectory> {
  const body = rec(await getJson(`${base}/world`, signal));
  const worldId = str(body.world_id);
  const continuityEngramId = str(body.continuity_engram_id);
  if (worldId === null || continuityEngramId === null) {
    throw new Error("The runtime returned no PulseWorld identity.");
  }
  return {
    worldId,
    continuityEngramId,
    tick: num(body.tick) ?? 0,
    harnessKind: str(rec(body.harness).kind) ?? (body.mock === true ? "mock" : "unknown"),
    mock: body.mock === true,
    taskFronts: parseList(body.task_fronts, parseTaskFront),
    activityCenters: parseList(body.activity_centers, parseActivityCenter),
  };
}

export async function createTaskFront(
  base: string,
  content: string,
  title?: string,
  subjectEngramId?: string,
): Promise<CreatedTaskFront> {
  const body = rec(await postJson(`${base}/task-fronts`, {
    content,
    ...(title === undefined ? {} : { title }),
    ...(subjectEngramId === undefined
      ? {}
      : { subject_engram_id: subjectEngramId }),
  }));
  const taskFront = parseTaskFront(body.task_front);
  const activityCenter = parseActivityCenter(body.activity_center);
  const eventId = str(body.event_id);
  if (
    taskFront === null ||
    activityCenter === null ||
    eventId === null ||
    activityCenter.kind !== "task" ||
    activityCenter.id !== taskFront.center_id ||
    activityCenter.focal_engram_id !== taskFront.focal_engram_id ||
    (subjectEngramId !== undefined &&
      taskFront.focal_engram_id !== subjectEngramId)
  ) {
    throw new Error("The runtime returned an incomplete TaskFront.");
  }
  return { taskFront, activityCenter, eventId };
}

export async function fetchTaskOffers(
  base: string,
  subjectEngramId: string,
  signal: AbortSignal,
): Promise<TaskOfferSummary[]> {
  const query = new URLSearchParams({ subject_engram_id: subjectEngramId });
  const offers = parseTaskOfferList(
    await getJson(`${base}/task-offers?${query.toString()}`, signal),
  );
  // The current subject scope includes committed predecessors. Terminal
  // offers deliberately retain the Engram that made the decision, while
  // unresolved offers follow the current successor.
  return offers.sort((left, right) =>
    right.taskOffer.updated_at.localeCompare(left.taskOffer.updated_at));
}

export async function fetchTaskOfferDetail(
  base: string,
  offerId: string,
  signal: AbortSignal,
): Promise<TaskOfferDetail> {
  const body = await getJson(
    `${base}/task-offers/${encodeURIComponent(offerId)}`,
    signal,
  );
  const root = taskOfferObject(body, "$" );
  const summary = parseTaskOfferSummary(root);
  if (summary.taskOffer.id !== offerId) {
    throw new Error("The runtime returned another task offer.");
  }
  const revisionRows = taskOfferField(root, "revisions", "$" );
  if (!Array.isArray(revisionRows)) {
    return taskOfferPayloadError("$.revisions", "an array");
  }
  const revisions = revisionRows.map((row, index) =>
    parseTaskOfferRevision(row, `$.revisions[${index}]`));
  const seen = new Set<number>();
  for (const revision of revisions) {
    if (revision.offer_id !== offerId || seen.has(revision.revision)) {
      return taskOfferPayloadError(
        "$.revisions",
        "unique revisions belonging to the requested offer",
      );
    }
    seen.add(revision.revision);
  }
  if (
    revisions.length !== summary.taskOffer.current_revision ||
    !seen.has(summary.taskOffer.current_revision)
  ) {
    return taskOfferPayloadError(
      "$.revisions",
      "the complete contiguous revision history",
    );
  }
  return { ...summary, revisions };
}

export async function createTaskOffer(
  base: string,
  subjectEngramId: string,
  content: string,
  input: { title?: string; projectId?: string | null } = {},
): Promise<CreatedTaskOffer> {
  const body = await postJson(`${base}/task-offers`, {
    subject_engram_id: subjectEngramId,
    content,
    ...(input.title === undefined ? {} : { title: input.title }),
    ...(input.projectId === undefined ? {} : { project_id: input.projectId }),
  });
  const created = parseTaskOfferMutation(body, true);
  if (
    created.taskOffer.subject_engram_id !== subjectEngramId ||
    created.taskOffer.status !== "pending" ||
    created.eventId === null
  ) {
    throw new Error("The runtime returned an inconsistent task offer.");
  }
  return { ...created, eventId: created.eventId };
}

export async function reviseTaskOffer(
  base: string,
  offerId: string,
  input: {
    expectedRevision: number;
    content: string;
    title?: string;
    projectId?: string | null;
  },
): Promise<CreatedTaskOffer> {
  const body = await postJson(
    `${base}/task-offers/${encodeURIComponent(offerId)}/revisions`,
    {
      expected_revision: input.expectedRevision,
      content: input.content,
      ...(input.title === undefined ? {} : { title: input.title }),
      ...(input.projectId === undefined ? {} : { project_id: input.projectId }),
    },
  );
  const revised = parseTaskOfferMutation(body, true);
  if (
    revised.taskOffer.id !== offerId ||
    revised.taskOffer.status !== "pending" ||
    revised.taskOffer.current_revision !== input.expectedRevision + 1 ||
    revised.eventId === null
  ) {
    throw new Error("The runtime returned an inconsistent task-offer revision.");
  }
  return { ...revised, eventId: revised.eventId };
}

export async function remindTaskOffer(
  base: string,
  offerId: string,
  expectedRevision: number,
): Promise<CreatedTaskOffer> {
  const body = await postJson(
    `${base}/task-offers/${encodeURIComponent(offerId)}/remind`,
    { expected_revision: expectedRevision },
  );
  const reminded = parseTaskOfferMutation(body, true);
  if (
    reminded.taskOffer.id !== offerId ||
    reminded.taskOffer.status !== "pending" ||
    reminded.taskOffer.current_revision !== expectedRevision ||
    reminded.eventId === null
  ) {
    throw new Error("The runtime returned an inconsistent task-offer reminder.");
  }
  return { ...reminded, eventId: reminded.eventId };
}

export async function withdrawTaskOffer(
  base: string,
  offerId: string,
  expectedRevision: number,
): Promise<TaskOfferSummary> {
  const body = await postJson(
    `${base}/task-offers/${encodeURIComponent(offerId)}/withdraw`,
    { expected_revision: expectedRevision },
  );
  const withdrawn = parseTaskOfferMutation(body, false);
  if (
    withdrawn.taskOffer.id !== offerId ||
    withdrawn.taskOffer.status !== "withdrawn" ||
    withdrawn.taskOffer.current_revision !== expectedRevision
  ) {
    throw new Error("The runtime returned an inconsistent task-offer withdrawal.");
  }
  return withdrawn;
}

export async function fetchTaskFrontDetail(
  base: string,
  frontId: string,
  signal: AbortSignal,
): Promise<TaskFrontDetail> {
  return parseTaskFrontDetail(
    await getJson(
      `${base}/task-fronts/${encodeURIComponent(frontId)}`,
      signal,
    ),
    frontId,
  );
}

export async function proposeTaskRelationshipTerms(
  base: string,
  relationshipId: string,
  expectedRevision: number,
  content: string,
): Promise<void> {
  if (!Number.isInteger(expectedRevision) || expectedRevision < 1) {
    throw new Error("TaskRelationship expected revision must be an integer >= 1.");
  }
  const terms = content.trim();
  if (terms === "" || terms.length > MAX_TASK_RELATIONSHIP_CONTENT) {
    throw new Error(
      `Changed terms must contain 1-${MAX_TASK_RELATIONSHIP_CONTENT} characters.`,
    );
  }
  await postJson(
    `${base}/task-relationships/${encodeURIComponent(relationshipId)}/terms`,
    { expected_revision: expectedRevision, content: terms },
  );
}

export async function updateTaskFrontStatus(
  base: string,
  frontId: string,
  status: Exclude<TaskFrontLifecycleStatus, "archived">,
): Promise<TaskFrontSummary> {
  const body = rec(await patchJson(
    `${base}/task-fronts/${encodeURIComponent(frontId)}`,
    { status },
  ));
  const taskFront = parseTaskFront(body.task_front);
  if (
    taskFront === null ||
    taskFront.id !== frontId ||
    taskFront.status !== status
  ) {
    throw new Error("The runtime returned an inconsistent TaskFront update.");
  }
  return taskFront;
}

export async function sendTaskFrontMessage(
  base: string,
  frontId: string,
  content: string,
): Promise<string | null> {
  const body = rec(await postJson(
    `${base}/task-fronts/${encodeURIComponent(frontId)}/messages`,
    { content },
  ));
  return str(body.event_id);
}

export async function createActivityCenter(
  base: string,
  input: {
    kind: Exclude<ActivityKind, "task">;
    title: string;
    description?: string;
    autonomy?: number;
    stimulus?: string;
  },
): Promise<CreatedActivityCenter> {
  const body = rec(await postJson(`${base}/activity-centers`, input));
  const activityCenter = parseActivityCenter(body.activity_center);
  const focalEngramId = str(body.focal_engram_id);
  if (activityCenter === null || focalEngramId === null) {
    throw new Error("The runtime returned an incomplete ActivityCenter.");
  }
  return {
    activityCenter,
    focalEngramId,
    eventId: str(body.event_id),
  };
}

export async function sendActivityCenterMessage(
  base: string,
  centerId: string,
  content: string,
): Promise<string | null> {
  const body = rec(await postJson(
    `${base}/activity-centers/${encodeURIComponent(centerId)}/messages`,
    { content },
  ));
  return str(body.event_id);
}

export async function fetchActivityCenterDetail(
  base: string,
  centerId: string,
  signal: AbortSignal,
): Promise<ActivityCenterDetail> {
  const body = rec(await getJson(
    `${base}/activity-centers/${encodeURIComponent(centerId)}`,
    signal,
  ));
  const activityCenter = parseActivityCenter(body.activity_center);
  const activitySummary = parseActivitySummary(body.activity_summary);
  const livingOrientationsTotal = integer(body.living_orientations_total);
  const livingOrientationsTruncated = body.living_orientations_truncated;
  if (
    activityCenter === null ||
    activitySummary === null ||
    !Array.isArray(body.living_concerns) ||
    !Array.isArray(body.living_orientations) ||
    !Array.isArray(body.messages) ||
    !Array.isArray(body.unattributed_history) ||
    livingOrientationsTotal === null ||
    typeof livingOrientationsTruncated !== "boolean"
  ) {
    throw new Error("The runtime returned an incomplete ActivityCenter detail.");
  }
  const livingConcerns = parseList(body.living_concerns, parseLivingConcern);
  const livingOrientations = parseList(
    body.living_orientations,
    parseLivingOrientation,
  );
  const messages = parseList(body.messages, parseCenterMessage).filter(
    (message) => message.center_id === centerId,
  );
  if (
    livingConcerns.length !== body.living_concerns.length ||
    livingOrientations.length !== body.living_orientations.length ||
    livingOrientationsTotal < livingOrientations.length ||
    (livingOrientationsTruncated
      ? livingOrientationsTotal <= livingOrientations.length
      : livingOrientationsTotal !== livingOrientations.length) ||
    livingOrientations.some((orientation) => orientation.centerId !== centerId) ||
    messages.length !== body.messages.length
  ) {
    throw new Error("The runtime returned an invalid Center-attributed history.");
  }
  return {
    activityCenter,
    livingConcerns,
    livingConcernsTotal: num(body.living_concerns_total) ?? livingConcerns.length,
    livingConcernsTruncated: body.living_concerns_truncated === true,
    livingOrientations,
    livingOrientationsTotal,
    livingOrientationsTruncated,
    activitySummary,
    messages,
    unattributedHistoryCount: body.unattributed_history.length,
  };
}

export async function fetchLivingPortfolio(
  base: string,
  engramId: string,
  signal: AbortSignal,
): Promise<LivingPortfolio> {
  return parseLivingPortfolio(await getJson(
    `${base}/engrams/${encodeURIComponent(engramId)}/living-portfolio`,
    signal,
  ));
}

export async function fetchPurposeAmendments(
  base: string,
  engramId: string,
  signal: AbortSignal,
): Promise<PurposeAmendmentsProjection> {
  return parsePurposeAmendments(await getJson(
    `${base}/engrams/${encodeURIComponent(engramId)}/purpose-amendments`,
    signal,
  ));
}

export async function updateActivityCenter(
  base: string,
  centerId: string,
  updates: ActivityCenterUpdate,
): Promise<ActivityCenterSummary> {
  const body = rec(await patchJson(
    `${base}/activity-centers/${encodeURIComponent(centerId)}`,
    updates,
  ));
  const center = parseActivityCenter(body.activity_center);
  if (center === null) {
    throw new Error("The runtime returned an incomplete ActivityCenter update.");
  }
  return center;
}
