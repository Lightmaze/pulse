-- Separate role authorization versions, accountability cycles and one-time
-- production receipt claims.  No output payload is stored here.
CREATE TABLE IF NOT EXISTS role_accountability_cycles (
    accountability_cycle_id TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    opened_role_lease_id TEXT NOT NULL REFERENCES role_leases(role_lease_id),
    obligation_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS role_accountability_cycles_immutable_update
BEFORE UPDATE ON role_accountability_cycles
BEGIN
    SELECT RAISE(ABORT, 'role accountability cycles are immutable');
END;

CREATE TRIGGER IF NOT EXISTS role_accountability_cycles_immutable_delete
BEFORE DELETE ON role_accountability_cycles
BEGIN
    SELECT RAISE(ABORT, 'role accountability cycles are immutable');
END;

CREATE TABLE IF NOT EXISTS role_obligations (
    role_lease_id TEXT PRIMARY KEY REFERENCES role_leases(role_lease_id),
    accountability_cycle_id TEXT NOT NULL
        REFERENCES role_accountability_cycles(accountability_cycle_id),
    obligation_json TEXT NOT NULL,
    obligation_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS role_obligations_immutable_update
BEFORE UPDATE ON role_obligations
BEGIN
    SELECT RAISE(ABORT, 'role obligations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS role_obligations_immutable_delete
BEFORE DELETE ON role_obligations
BEGIN
    SELECT RAISE(ABORT, 'role obligations are immutable');
END;

CREATE TABLE IF NOT EXISTS role_contributions (
    contribution_id TEXT PRIMARY KEY,
    accountability_cycle_id TEXT NOT NULL
        REFERENCES role_accountability_cycles(accountability_cycle_id),
    role_lease_id TEXT NOT NULL REFERENCES role_leases(role_lease_id),
    role_epoch INTEGER NOT NULL CHECK(role_epoch >= 1),
    cycle_sequence INTEGER NOT NULL CHECK(cycle_sequence >= 1),
    world_id TEXT NOT NULL,
    holder_kind TEXT NOT NULL CHECK(holder_kind IN ('engram', 'worker', 'user')),
    holder_id TEXT NOT NULL,
    contribution_kind TEXT NOT NULL CHECK(contribution_kind IN (
        'direct_output', 'coordination'
    )),
    output_kind TEXT CHECK(output_kind IN (
        'workspace_checkpoint', 'habitat_effect'
    )),
    evidence_event_id TEXT NOT NULL,
    source_turn_id TEXT,
    evidence_class TEXT NOT NULL CHECK(evidence_class IN (
        'CONTROL_ONLY', 'LIVE_WORKSPACE_CHECKPOINTED', 'LIVE_HABITAT_EFFECT'
    )),
    created_at TEXT NOT NULL,
    UNIQUE(accountability_cycle_id, evidence_event_id),
    UNIQUE(accountability_cycle_id, cycle_sequence),
    CHECK(
        (contribution_kind = 'direct_output' AND output_kind IS NOT NULL
            AND evidence_class <> 'CONTROL_ONLY')
        OR (contribution_kind = 'coordination' AND output_kind IS NULL
            AND evidence_class = 'CONTROL_ONLY')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS role_direct_output_receipt_claim_once
    ON role_contributions(world_id, output_kind, evidence_event_id)
    WHERE contribution_kind = 'direct_output';

CREATE INDEX IF NOT EXISTS role_contributions_cycle_created
    ON role_contributions(accountability_cycle_id, cycle_sequence);

CREATE TRIGGER IF NOT EXISTS role_contributions_immutable_update
BEFORE UPDATE ON role_contributions
BEGIN
    SELECT RAISE(ABORT, 'role contributions are append-only');
END;

CREATE TRIGGER IF NOT EXISTS role_contributions_immutable_delete
BEFORE DELETE ON role_contributions
BEGIN
    SELECT RAISE(ABORT, 'role contributions are append-only');
END;
