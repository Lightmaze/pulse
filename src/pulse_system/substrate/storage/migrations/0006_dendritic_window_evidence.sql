-- Durable closed-window identity for Dendritic Convergence v1.
--
-- Migration 0005 shipped only the many-to-one nexus.  This additive migration
-- preserves that immutable evidence while requiring every newly materialized
-- nexus to reference a separately immutable, independently replayable timing
-- cohort.  Existing v5 nexuses are sealed into an explicit read-only legacy
-- evidence class; no opening policy, closed cohort, or watermark is invented.

CREATE TABLE IF NOT EXISTS dendritic_input_policy_snapshots (
    event_id TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    engram_id TEXT NOT NULL,
    policy_version TEXT NOT NULL CHECK(policy_version = 'dendritic-window.v1'),
    base_silence_threshold_seconds REAL NOT NULL CHECK(
        typeof(base_silence_threshold_seconds) IN ('real', 'integer')
        AND base_silence_threshold_seconds >= 0
    ),
    base_max_wait_seconds REAL NOT NULL CHECK(
        typeof(base_max_wait_seconds) IN ('real', 'integer')
        AND base_max_wait_seconds >= 0
    ),
    wait_modifier REAL NOT NULL CHECK(
        typeof(wait_modifier) IN ('real', 'integer') AND wait_modifier > 0
    ),
    silence_threshold_seconds REAL NOT NULL CHECK(
        typeof(silence_threshold_seconds) IN ('real', 'integer')
        AND silence_threshold_seconds >= 0
    ),
    max_wait_seconds REAL NOT NULL CHECK(
        typeof(max_wait_seconds) IN ('real', 'integer')
        AND max_wait_seconds >= 0
    ),
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (event_id) REFERENCES causal_events(id),
    FOREIGN KEY (engram_id) REFERENCES engrams(id)
);

CREATE INDEX IF NOT EXISTS idx_dendritic_input_policy_engram
    ON dendritic_input_policy_snapshots(engram_id, recorded_at);

CREATE TABLE IF NOT EXISTS dendritic_windows (
    id TEXT PRIMARY KEY CHECK(length(id) = 64),
    world_id TEXT NOT NULL,
    formation_engram_id TEXT NOT NULL,
    policy_version TEXT NOT NULL CHECK(policy_version = 'dendritic-window.v1'),
    event_set_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_set_sha256) = 64),
    event_count INTEGER NOT NULL CHECK(event_count BETWEEN 1 AND 500),
    base_silence_threshold_seconds REAL NOT NULL CHECK(
        typeof(base_silence_threshold_seconds) IN ('real', 'integer')
        AND base_silence_threshold_seconds >= 0
    ),
    base_max_wait_seconds REAL NOT NULL CHECK(
        typeof(base_max_wait_seconds) IN ('real', 'integer')
        AND base_max_wait_seconds >= 0
    ),
    wait_modifier REAL NOT NULL CHECK(
        typeof(wait_modifier) IN ('real', 'integer') AND wait_modifier > 0
    ),
    silence_threshold_seconds REAL NOT NULL CHECK(
        typeof(silence_threshold_seconds) IN ('real', 'integer')
        AND silence_threshold_seconds >= 0
    ),
    max_wait_seconds REAL NOT NULL CHECK(
        typeof(max_wait_seconds) IN ('real', 'integer')
        AND max_wait_seconds >= 0
    ),
    window_opened_at TEXT NOT NULL,
    last_input_at TEXT NOT NULL,
    window_closed_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    observed_event_seq INTEGER NOT NULL CHECK(observed_event_seq >= 1),
    created_at TEXT NOT NULL,
    FOREIGN KEY (formation_engram_id) REFERENCES engrams(id),
    CHECK(window_opened_at <= last_input_at),
    CHECK(last_input_at <= window_closed_at),
    CHECK(window_closed_at <= observed_at),
    CHECK(observed_at <= created_at)
);

CREATE INDEX IF NOT EXISTS idx_dendritic_windows_formation_closed
    ON dendritic_windows(formation_engram_id, window_closed_at);

CREATE TABLE IF NOT EXISTS dendritic_window_members (
    window_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 499),
    event_id TEXT NOT NULL UNIQUE,
    event_seq INTEGER NOT NULL CHECK(event_seq >= 1),
    arrived_at TEXT NOT NULL,
    PRIMARY KEY (window_id, ordinal),
    FOREIGN KEY (window_id) REFERENCES dendritic_windows(id),
    FOREIGN KEY (event_id) REFERENCES causal_events(id)
);

CREATE INDEX IF NOT EXISTS idx_dendritic_window_members_seq
    ON dendritic_window_members(window_id, event_seq);

CREATE TABLE IF NOT EXISTS dendritic_integration_windows (
    integration_id TEXT PRIMARY KEY,
    window_id TEXT NOT NULL,
    FOREIGN KEY (integration_id) REFERENCES dendritic_integrations(id),
    FOREIGN KEY (window_id) REFERENCES dendritic_windows(id)
);

CREATE INDEX IF NOT EXISTS idx_dendritic_integration_windows_window
    ON dendritic_integration_windows(window_id, integration_id);

CREATE TABLE IF NOT EXISTS dendritic_legacy_integrations (
    integration_id TEXT PRIMARY KEY,
    source_schema_version INTEGER NOT NULL CHECK(source_schema_version = 5),
    evidence_class TEXT NOT NULL CHECK(
        evidence_class = 'LEGACY_V5_NO_WINDOW'
    ),
    integration_created_at TEXT NOT NULL,
    FOREIGN KEY (integration_id) REFERENCES dendritic_integrations(id)
);

-- Store's transitional bootstrap may already have installed the closed-set
-- trigger before this migration runs.  Reopen only inside this transaction,
-- snapshot the exact pre-v6 integration set, then permanently close inserts.
DROP TRIGGER IF EXISTS dendritic_legacy_integrations_closed_insert;

INSERT INTO dendritic_legacy_integrations (
    integration_id, source_schema_version, evidence_class,
    integration_created_at
)
SELECT id, 5, 'LEGACY_V5_NO_WINDOW', created_at
FROM dendritic_integrations;

CREATE TRIGGER dendritic_legacy_integrations_closed_insert
BEFORE INSERT ON dendritic_legacy_integrations
BEGIN
    SELECT RAISE(ABORT, 'legacy dendritic integration set is migration-sealed');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_legacy_integrations_immutable_update
BEFORE UPDATE ON dendritic_legacy_integrations
BEGIN
    SELECT RAISE(ABORT, 'legacy dendritic integration evidence is immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_legacy_integrations_immutable_delete
BEFORE DELETE ON dendritic_legacy_integrations
BEGIN
    SELECT RAISE(ABORT, 'legacy dendritic integration evidence is immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_input_policy_immutable_update
BEFORE UPDATE ON dendritic_input_policy_snapshots
BEGIN
    SELECT RAISE(ABORT, 'dendritic input policies are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_input_policy_immutable_delete
BEFORE DELETE ON dendritic_input_policy_snapshots
BEGIN
    SELECT RAISE(ABORT, 'dendritic input policies are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_windows_immutable_update
BEFORE UPDATE ON dendritic_windows
BEGIN
    SELECT RAISE(ABORT, 'dendritic windows are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_windows_immutable_delete
BEFORE DELETE ON dendritic_windows
BEGIN
    SELECT RAISE(ABORT, 'dendritic windows are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_window_members_immutable_update
BEFORE UPDATE ON dendritic_window_members
BEGIN
    SELECT RAISE(ABORT, 'dendritic window members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_window_members_immutable_delete
BEFORE DELETE ON dendritic_window_members
BEGIN
    SELECT RAISE(ABORT, 'dendritic window members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_integration_windows_immutable_update
BEFORE UPDATE ON dendritic_integration_windows
BEGIN
    SELECT RAISE(ABORT, 'dendritic integration-window bindings are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_integration_windows_immutable_delete
BEFORE DELETE ON dendritic_integration_windows
BEGIN
    SELECT RAISE(ABORT, 'dendritic integration-window bindings are immutable');
END;
