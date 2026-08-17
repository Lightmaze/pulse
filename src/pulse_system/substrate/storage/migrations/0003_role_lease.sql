-- Freeze bounded role authority in the shared PulseWorld database.  The
-- standalone RoleLeaseStore retains its idempotent bootstrap during 0.2.
CREATE TABLE IF NOT EXISTS role_scope_counters (
    scope_key TEXT PRIMARY KEY,
    last_epoch INTEGER NOT NULL CHECK(last_epoch >= 1)
);

CREATE TABLE IF NOT EXISTS role_leases (
    role_lease_id TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    lineage_id TEXT,
    holder_kind TEXT NOT NULL CHECK(holder_kind IN ('engram', 'worker', 'user')),
    holder_id TEXT NOT NULL,
    role_class TEXT NOT NULL CHECK(role_class IN ('subject_role', 'task_role')),
    role_label TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    scope_digest TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    purpose_revision_id TEXT,
    issuer_kind TEXT NOT NULL,
    issuer_id TEXT NOT NULL,
    role_epoch INTEGER NOT NULL CHECK(role_epoch >= 1),
    runtime_owner_id TEXT NOT NULL,
    runtime_epoch INTEGER NOT NULL CHECK(runtime_epoch >= 1),
    valid_from TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    renew_after TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'requested', 'active', 'suspended', 'released', 'expired', 'revoked'
    )),
    predecessor_lease_id TEXT,
    renewal_count INTEGER NOT NULL CHECK(renewal_count >= 0),
    last_evidence_event_id TEXT,
    evidence_class TEXT NOT NULL CHECK(evidence_class IN (
        'CONTRACT_ONLY', 'LIVE_GATE_UNVERIFIED', 'LIVE'
    )),
    handoff_suspended INTEGER NOT NULL DEFAULT 0 CHECK(handoff_suspended IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    released_at TEXT,
    UNIQUE(scope_key, role_epoch),
    CHECK(expires_at > valid_from),
    CHECK(renew_after > valid_from AND renew_after < expires_at),
    CHECK(
        role_class <> 'subject_role'
        OR (holder_kind = 'engram' AND lineage_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS role_leases_one_nonterminal_per_scope
    ON role_leases(scope_key)
    WHERE status IN ('requested', 'active', 'suspended');
CREATE INDEX IF NOT EXISTS role_leases_world_status
    ON role_leases(world_id, status, updated_at);
CREATE INDEX IF NOT EXISTS role_leases_lineage
    ON role_leases(world_id, lineage_id, status);
CREATE INDEX IF NOT EXISTS role_leases_holder
    ON role_leases(world_id, holder_kind, holder_id, status);
CREATE INDEX IF NOT EXISTS role_leases_predecessor
    ON role_leases(predecessor_lease_id);

CREATE TRIGGER IF NOT EXISTS role_leases_immutable_fields
BEFORE UPDATE OF role_lease_id, world_id, lineage_id, holder_kind,
    holder_id, role_class, role_label, scope_json, scope_digest,
    scope_key, purpose_revision_id, issuer_kind, issuer_id,
    role_epoch, runtime_owner_id, runtime_epoch, valid_from,
    expires_at, renew_after, predecessor_lease_id, renewal_count,
    created_at, evidence_class
ON role_leases
BEGIN
    SELECT RAISE(ABORT, 'role lease immutable fields cannot change');
END;
