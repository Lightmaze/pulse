-- Freeze the accepted TaskOffer -> TaskRelationship substrate.  The legacy
-- bootstrap has already supplied dependency tables in this transaction.
CREATE TABLE IF NOT EXISTS task_offers (
    id TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    subject_engram_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
        'pending', 'changes_requested', 'accepted', 'refused', 'withdrawn'
    )),
    current_revision INTEGER NOT NULL DEFAULT 1 CHECK(
        typeof(current_revision) = 'integer' AND current_revision >= 1
    ),
    task_front_id TEXT UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    decided_at TEXT,
    withdrawn_at TEXT,
    FOREIGN KEY (subject_engram_id) REFERENCES engrams(id),
    FOREIGN KEY (task_front_id) REFERENCES task_fronts(id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK(
        (status = 'accepted' AND task_front_id IS NOT NULL
            AND decided_at IS NOT NULL AND withdrawn_at IS NULL)
        OR (status = 'refused' AND task_front_id IS NULL
            AND decided_at IS NOT NULL AND withdrawn_at IS NULL)
        OR (status = 'withdrawn' AND task_front_id IS NULL
            AND decided_at IS NULL AND withdrawn_at IS NOT NULL)
        OR (status IN ('pending', 'changes_requested')
            AND task_front_id IS NULL AND decided_at IS NULL
            AND withdrawn_at IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_task_offers_world_status
    ON task_offers(world_id, status, updated_at, id);
CREATE INDEX IF NOT EXISTS idx_task_offers_subject_status
    ON task_offers(world_id, subject_engram_id, status, updated_at, id);

CREATE TABLE IF NOT EXISTS task_offer_revisions (
    offer_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(
        typeof(revision) = 'integer' AND revision >= 1
    ),
    content TEXT NOT NULL CHECK(
        length(trim(content)) >= 1 AND length(content) <= 12000
    ),
    title TEXT NOT NULL CHECK(length(trim(title)) BETWEEN 1 AND 120),
    project_id TEXT,
    latest_offer_event_id TEXT NOT NULL UNIQUE,
    decision TEXT CHECK(decision IS NULL OR decision IN (
        'accept', 'refuse', 'request_changes'
    )),
    subject_response TEXT CHECK(
        subject_response IS NULL OR length(subject_response) <= 4000
    ),
    decision_event_id TEXT UNIQUE,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    PRIMARY KEY (offer_id, revision),
    FOREIGN KEY (offer_id) REFERENCES task_offers(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (latest_offer_event_id) REFERENCES causal_events(id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (decision_event_id) REFERENCES causal_events(id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK(
        (decision IS NULL AND subject_response IS NULL
            AND decision_event_id IS NULL AND decided_at IS NULL)
        OR (decision IS NOT NULL AND decision_event_id IS NOT NULL
            AND decided_at IS NOT NULL)
    ),
    CHECK(
        decision IS NULL OR decision <> 'request_changes'
        OR (subject_response IS NOT NULL AND length(trim(subject_response)) >= 1)
    )
);
CREATE INDEX IF NOT EXISTS idx_task_offer_revisions_offer
    ON task_offer_revisions(offer_id, revision);
CREATE INDEX IF NOT EXISTS idx_task_offer_revisions_project
    ON task_offer_revisions(project_id, offer_id, revision);

CREATE TABLE IF NOT EXISTS task_relationships (
    id TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    accepted_offer_id TEXT NOT NULL UNIQUE,
    task_front_id TEXT NOT NULL UNIQUE,
    center_id TEXT NOT NULL UNIQUE,
    original_subject_engram_id TEXT NOT NULL,
    current_subject_engram_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'active', 'paused', 'renegotiation_requested', 'exited'
    )),
    revision INTEGER NOT NULL CHECK(
        typeof(revision) = 'integer' AND revision >= 1
    ),
    latest_terms_event_id TEXT,
    latest_subject_note TEXT CHECK(
        latest_subject_note IS NULL OR length(latest_subject_note) <= 4000
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    exited_at TEXT,
    FOREIGN KEY (accepted_offer_id) REFERENCES task_offers(id) ON DELETE RESTRICT,
    FOREIGN KEY (task_front_id) REFERENCES task_fronts(id) ON DELETE RESTRICT,
    FOREIGN KEY (center_id) REFERENCES activity_centers(id) ON DELETE RESTRICT,
    FOREIGN KEY (original_subject_engram_id) REFERENCES engrams(id),
    FOREIGN KEY (current_subject_engram_id) REFERENCES engrams(id),
    FOREIGN KEY (latest_terms_event_id) REFERENCES causal_events(id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK(
        (status = 'exited' AND exited_at IS NOT NULL)
        OR (status <> 'exited' AND exited_at IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_task_relationships_world_status
    ON task_relationships(world_id, status, updated_at, id);
CREATE INDEX IF NOT EXISTS idx_task_relationships_subject_status
    ON task_relationships(
        world_id, current_subject_engram_id, status, updated_at, id
    );

CREATE TABLE IF NOT EXISTS task_relationship_events (
    relationship_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(typeof(seq) = 'integer' AND seq >= 1),
    action TEXT NOT NULL CHECK(action IN (
        'accepted', 'paused', 'renegotiation_requested', 'terms_proposed',
        'resumed', 'exited', 'succession'
    )),
    actor_kind TEXT NOT NULL CHECK(actor_kind IN ('subject', 'user', 'system')),
    actor_id TEXT NOT NULL,
    before_status TEXT CHECK(
        before_status IS NULL OR before_status IN (
            'active', 'paused', 'renegotiation_requested', 'exited'
        )
    ),
    after_status TEXT NOT NULL CHECK(after_status IN (
        'active', 'paused', 'renegotiation_requested', 'exited'
    )),
    content TEXT CHECK(content IS NULL OR length(content) <= 12000),
    source_event_id TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (relationship_id, seq),
    UNIQUE (relationship_id, source_event_id),
    FOREIGN KEY (relationship_id) REFERENCES task_relationships(id)
        ON DELETE RESTRICT,
    FOREIGN KEY (source_event_id) REFERENCES causal_events(id)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS idx_task_relationship_events_source
    ON task_relationship_events(source_event_id);
