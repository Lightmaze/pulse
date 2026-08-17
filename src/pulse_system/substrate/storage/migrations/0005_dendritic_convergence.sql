-- Durable many-to-one causal provenance for dendritic CONTENT integration.
-- The aggregate causal event keeps the existing single-root Harness turn
-- lifecycle; these immutable rows prove which queued inputs formed it.
CREATE TABLE IF NOT EXISTS dendritic_integrations (
    id TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    formation_engram_id TEXT NOT NULL,
    center_id TEXT,
    aggregate_event_id TEXT NOT NULL UNIQUE,
    delivery_class TEXT NOT NULL CHECK(delivery_class IN (
        'external', 'propagation'
    )),
    member_set_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(member_set_sha256) = 64),
    content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
    member_count INTEGER NOT NULL CHECK(member_count BETWEEN 2 AND 64),
    window_opened_at TEXT NOT NULL,
    window_closed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (formation_engram_id) REFERENCES engrams(id),
    FOREIGN KEY (center_id) REFERENCES activity_centers(id),
    FOREIGN KEY (aggregate_event_id) REFERENCES causal_events(id)
);

CREATE INDEX IF NOT EXISTS idx_dendritic_integrations_formation_created
    ON dendritic_integrations(formation_engram_id, created_at);

CREATE TABLE IF NOT EXISTS dendritic_integration_members (
    integration_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 63),
    event_id TEXT NOT NULL UNIQUE,
    event_seq INTEGER NOT NULL CHECK(event_seq >= 1),
    causal_id TEXT NOT NULL,
    source_identity TEXT NOT NULL CHECK(
        length(source_identity) BETWEEN 1 AND 256
    ),
    content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
    arrived_at TEXT NOT NULL,
    PRIMARY KEY (integration_id, ordinal),
    FOREIGN KEY (integration_id) REFERENCES dendritic_integrations(id),
    FOREIGN KEY (event_id) REFERENCES causal_events(id)
);

CREATE INDEX IF NOT EXISTS idx_dendritic_members_integration_event
    ON dendritic_integration_members(integration_id, event_seq);

CREATE TRIGGER IF NOT EXISTS dendritic_integrations_immutable_update
BEFORE UPDATE ON dendritic_integrations
BEGIN
    SELECT RAISE(ABORT, 'dendritic integrations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_integrations_immutable_delete
BEFORE DELETE ON dendritic_integrations
BEGIN
    SELECT RAISE(ABORT, 'dendritic integrations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_members_immutable_update
BEFORE UPDATE ON dendritic_integration_members
BEGIN
    SELECT RAISE(ABORT, 'dendritic integration members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS dendritic_members_immutable_delete
BEFORE DELETE ON dendritic_integration_members
BEGIN
    SELECT RAISE(ABORT, 'dendritic integration members are immutable');
END;
