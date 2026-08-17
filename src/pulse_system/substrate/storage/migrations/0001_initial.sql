-- Transitional baseline: Store._init_schema creates the historical substrate
-- in the same transaction.  This first formal migration creates the immutable
-- provenance ledger; it does not pretend to reconstruct every pre-0.2 DDL era.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY CHECK(version >= 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    sha256 TEXT NOT NULL CHECK(
        length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    applied_at_utc TEXT NOT NULL CHECK(length(trim(applied_at_utc)) >= 1)
);
