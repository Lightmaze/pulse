"""Schema v6 migration, backup, provenance, and fail-closed contracts."""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from pulse_system.core.causality import CausalLedger
from pulse_system.core.dendrite import (
    DendriticReadyWindow,
    DendriticWindowPolicySnapshot,
)
from pulse_system.core.types import (
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
)
from pulse_system.substrate.storage import (
    SchemaMigrationError,
    SchemaMigrator,
    Storage,
)
from pulse_system.substrate.storage.migrator import TARGET_SCHEMA_VERSION


def _user_version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA user_version").fetchone()[0])


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }


def _records(connection: sqlite3.Connection) -> list[tuple[int, str, str]]:
    return [
        (int(row[0]), str(row[1]), str(row[2]))
        for row in connection.execute(
            "SELECT version, name, sha256 FROM schema_migrations ORDER BY version"
        )
    ]


def _legacy_database(path: Path, *, wal: bool = False) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    if wal:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute(
        "CREATE TABLE legacy_payload (id INTEGER PRIMARY KEY, content TEXT NOT NULL)"
    )
    connection.execute("INSERT INTO legacy_payload(content) VALUES ('before migration')")
    connection.commit()
    return connection


def _closed_window(
    ledger: CausalLedger,
    engram_id: str,
    *events,
) -> DendriticReadyWindow:
    ordered = sorted(events, key=lambda event: (event.created_at, event.seq, event.id))
    policy = DendriticWindowPolicySnapshot(
        policy_version="dendritic-window.v1",
        base_silence_threshold_seconds=60.0,
        base_max_wait_seconds=300.0,
        wait_modifier=1.0,
        silence_threshold_seconds=60.0,
        max_wait_seconds=300.0,
    )
    for event in ordered:
        ledger.ensure_dendritic_input_policy(event.id, policy)
    closed_at = min(
        ordered[0].created_at + timedelta(seconds=300),
        ordered[-1].created_at + timedelta(seconds=60),
    )
    return DendriticReadyWindow(
        engram_id=engram_id,
        event_ids=tuple(event.id for event in ordered),
        event_seqs=tuple(event.seq for event in ordered),
        policy_version="dendritic-window.v1",
        base_silence_threshold_seconds=60.0,
        base_max_wait_seconds=300.0,
        wait_modifier=1.0,
        silence_threshold_seconds=60.0,
        max_wait_seconds=300.0,
        opened_at=ordered[0].created_at,
        last_input_at=ordered[-1].created_at,
        closed_at=closed_at,
        observed_at=closed_at,
    )


def _create_nonempty_v5_dendritic_database(path: Path) -> tuple[str, str, str]:
    """Create real nexus rows, then remove facts that schema v5 never recorded."""

    store = Storage(path)
    ledger = CausalLedger(store)
    target = store.create_engram(auto_name=False)
    first = ledger.enqueue(
        world_id="world-v5-legacy",
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.USER,
        engram_id=target.id,
        content="legacy user input",
    )
    second = ledger.enqueue(
        world_id="world-v5-legacy",
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.HABITAT,
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.HABITAT,
        engram_id=target.id,
        content="legacy habitat input",
    )
    integration, aggregate = ledger.materialize_dendritic_integration(
        [first.id, second.id],
        window=_closed_window(ledger, target.id, first, second),
    )
    with store._lock:
        [raw_metadata] = store._conn.execute(
            "SELECT metadata FROM causal_events WHERE id = ?",
            (aggregate.id,),
        ).fetchone()
        metadata = json.loads(raw_metadata)
        assert metadata.pop("dendritic_window_id") == integration.window.id
        store._conn.execute("DROP TRIGGER dendritic_integrations_immutable_update")
        store._conn.execute(
            "UPDATE dendritic_integrations SET window_closed_at = created_at "
            "WHERE id = ?",
            (integration.id,),
        )
        store._conn.execute(
            "UPDATE causal_events SET metadata = ? WHERE id = ?",
            (
                json.dumps(
                    metadata,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                aggregate.id,
            ),
        )
        store._conn.commit()
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("DROP TABLE dendritic_legacy_integrations")
    connection.execute("DROP TABLE dendritic_integration_windows")
    connection.execute("DROP TABLE dendritic_window_members")
    connection.execute("DROP TABLE dendritic_windows")
    connection.execute("DROP TABLE dendritic_input_policy_snapshots")
    connection.execute("DELETE FROM schema_migrations WHERE version = 6")
    connection.execute("PRAGMA user_version = 5")
    connection.commit()
    connection.close()
    return integration.id, first.id, second.id


def test_new_database_reaches_v6_with_contiguous_checksum_ledger(tmp_path):
    database = tmp_path / "new.db"
    store = Storage(database)
    try:
        assert store.schema_migration_result.from_version == 0
        assert store.schema_migration_result.to_version == TARGET_SCHEMA_VERSION
        assert store.schema_migration_result.applied_versions == (1, 2, 3, 4, 5, 6)
        assert store.schema_migration_result.backup_path is None
        assert _user_version(store._conn) == 6
        records = _records(store._conn)
        assert [row[:2] for row in records] == [
            (1, "initial"),
            (2, "task_relationship"),
            (3, "role_lease"),
            (4, "role_direct_output"),
            (5, "dendritic_convergence"),
            (6, "dendritic_window_evidence"),
        ]
        assert all(len(row[2]) == 64 for row in records)
        assert {
            "schema_migrations",
            "task_relationships",
            "role_scope_counters",
            "role_leases",
            "role_accountability_cycles",
            "role_obligations",
            "role_contributions",
            "dendritic_integrations",
            "dendritic_integration_members",
            "dendritic_input_policy_snapshots",
            "dendritic_windows",
            "dendritic_window_members",
            "dendritic_integration_windows",
            "dendritic_legacy_integrations",
        } <= _tables(store._conn)
    finally:
        store.close()


def test_legacy_v0_is_backed_up_before_upgrade_and_repeat_open_is_quiet(tmp_path):
    database = tmp_path / "legacy.db"
    _legacy_database(database).close()

    first = Storage(database)
    try:
        result = first.schema_migration_result
        assert result.applied_versions == (1, 2, 3, 4, 5, 6)
        assert result.backup_path is not None
        assert result.backup_path.exists()
        assert first._conn.execute(
            "SELECT content FROM legacy_payload"
        ).fetchone() == ("before migration",)
        schema_version = int(first._conn.execute("PRAGMA schema_version").fetchone()[0])
    finally:
        first.close()

    backup = sqlite3.connect(result.backup_path)
    try:
        assert backup.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert _user_version(backup) == 0
        assert backup.execute(
            "SELECT content FROM legacy_payload"
        ).fetchone() == ("before migration",)
        assert "schema_migrations" not in _tables(backup)
    finally:
        backup.close()

    backups_before = tuple(tmp_path.glob("legacy.db.schema-v0-to-v6.*.bak"))
    second = Storage(database)
    try:
        assert second.schema_migration_result.applied_versions == ()
        assert second.schema_migration_result.backup_path is None
        assert int(second._conn.execute("PRAGMA schema_version").fetchone()[0]) == schema_version
    finally:
        second.close()
    assert tuple(tmp_path.glob("legacy.db.schema-v0-to-v6.*.bak")) == backups_before


def test_online_backup_includes_committed_wal_rows(tmp_path):
    database = tmp_path / "wal.db"
    writer = _legacy_database(database, wal=True)
    writer.execute("INSERT INTO legacy_payload(content) VALUES ('committed in WAL')")
    writer.commit()
    assert database.with_name(database.name + "-wal").exists()
    try:
        store = Storage(database)
        backup_path = store.schema_migration_result.backup_path
        store.close()
    finally:
        writer.close()
    assert backup_path is not None
    backup = sqlite3.connect(backup_path)
    try:
        assert backup.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert backup.execute(
            "SELECT content FROM legacy_payload ORDER BY id"
        ).fetchall() == [("before migration",), ("committed in WAL",)]
    finally:
        backup.close()


def test_valid_partial_v2_history_applies_role_migrations(tmp_path):
    database = tmp_path / "partial.db"
    initial = Storage(database)
    initial.close()
    connection = sqlite3.connect(database)
    connection.execute("DROP TABLE role_contributions")
    connection.execute("DROP TABLE role_obligations")
    connection.execute("DROP TABLE role_accountability_cycles")
    connection.execute("DROP TRIGGER role_leases_immutable_fields")
    connection.execute("DROP TABLE role_leases")
    connection.execute("DROP TABLE role_scope_counters")
    connection.execute("DELETE FROM schema_migrations WHERE version >= 3")
    connection.execute("PRAGMA user_version = 2")
    connection.commit()
    connection.close()

    upgraded = Storage(database)
    try:
        assert upgraded.schema_migration_result.from_version == 2
        assert upgraded.schema_migration_result.applied_versions == (3, 4, 5, 6)
        assert upgraded.schema_migration_result.backup_path is not None
        assert _user_version(upgraded._conn) == 6
        assert [row[0] for row in _records(upgraded._conn)] == [1, 2, 3, 4, 5, 6]
        assert {
            "role_scope_counters",
            "role_leases",
            "role_accountability_cycles",
            "role_obligations",
            "role_contributions",
            "dendritic_integrations",
            "dendritic_integration_members",
            "dendritic_input_policy_snapshots",
            "dendritic_windows",
            "dendritic_window_members",
            "dendritic_integration_windows",
            "dendritic_legacy_integrations",
        } <= _tables(upgraded._conn)
    finally:
        upgraded.close()


def test_recorded_v3_applies_only_replayable_role_direct_output_migration(tmp_path):
    database = tmp_path / "v3.db"
    Storage(database).close()
    connection = sqlite3.connect(database)
    connection.execute("DROP TABLE role_contributions")
    connection.execute("DROP TABLE role_obligations")
    connection.execute("DROP TABLE role_accountability_cycles")
    connection.execute("DELETE FROM schema_migrations WHERE version >= 4")
    connection.execute("PRAGMA user_version = 3")
    connection.commit()
    connection.close()

    upgraded = Storage(database)
    try:
        assert upgraded.schema_migration_result.from_version == 3
        assert upgraded.schema_migration_result.applied_versions == (4, 5, 6)
        assert upgraded.schema_migration_result.backup_path is not None
        assert _user_version(upgraded._conn) == 6
        assert {
            "role_accountability_cycles",
            "role_obligations",
            "role_contributions",
            "dendritic_integrations",
            "dendritic_integration_members",
            "dendritic_input_policy_snapshots",
            "dendritic_windows",
            "dendritic_window_members",
            "dendritic_integration_windows",
            "dendritic_legacy_integrations",
        } <= _tables(upgraded._conn)
    finally:
        upgraded.close()


def test_recorded_v4_applies_only_dendritic_convergence_migration(tmp_path):
    database = tmp_path / "v4.db"
    Storage(database).close()
    connection = sqlite3.connect(database)
    connection.execute("DROP TABLE dendritic_legacy_integrations")
    connection.execute("DROP TABLE dendritic_integration_windows")
    connection.execute("DROP TABLE dendritic_window_members")
    connection.execute("DROP TABLE dendritic_windows")
    connection.execute("DROP TABLE dendritic_input_policy_snapshots")
    connection.execute("DROP TABLE dendritic_integration_members")
    connection.execute("DROP TABLE dendritic_integrations")
    connection.execute("DELETE FROM schema_migrations WHERE version >= 5")
    connection.execute("PRAGMA user_version = 4")
    connection.commit()
    connection.close()

    upgraded = Storage(database)
    try:
        assert upgraded.schema_migration_result.from_version == 4
        assert upgraded.schema_migration_result.applied_versions == (5, 6)
        assert upgraded.schema_migration_result.backup_path is not None
        assert _user_version(upgraded._conn) == 6
        assert {
            "dendritic_integrations",
            "dendritic_integration_members",
            "dendritic_input_policy_snapshots",
            "dendritic_windows",
            "dendritic_window_members",
            "dendritic_integration_windows",
            "dendritic_legacy_integrations",
        } <= _tables(upgraded._conn)
    finally:
        upgraded.close()


def test_recorded_v5_applies_only_dendritic_window_evidence_migration(tmp_path):
    database = tmp_path / "v5.db"
    Storage(database).close()
    connection = sqlite3.connect(database)
    connection.execute("DROP TABLE dendritic_legacy_integrations")
    connection.execute("DROP TABLE dendritic_integration_windows")
    connection.execute("DROP TABLE dendritic_window_members")
    connection.execute("DROP TABLE dendritic_windows")
    connection.execute("DROP TABLE dendritic_input_policy_snapshots")
    connection.execute("DELETE FROM schema_migrations WHERE version = 6")
    connection.execute("PRAGMA user_version = 5")
    connection.commit()
    connection.close()

    upgraded = Storage(database)
    try:
        assert upgraded.schema_migration_result.from_version == 5
        assert upgraded.schema_migration_result.applied_versions == (6,)
        assert upgraded.schema_migration_result.backup_path is not None
        assert _user_version(upgraded._conn) == 6
        assert {
            "dendritic_windows",
            "dendritic_input_policy_snapshots",
            "dendritic_window_members",
            "dendritic_integration_windows",
            "dendritic_legacy_integrations",
        } <= _tables(upgraded._conn)
    finally:
        upgraded.close()


def test_nonempty_v5_nexus_migrates_as_explicit_read_only_legacy_evidence(
    tmp_path,
):
    database = tmp_path / "v5-nonempty.db"
    integration_id, first_event_id, second_event_id = (
        _create_nonempty_v5_dendritic_database(database)
    )

    upgraded = Storage(database)
    try:
        assert upgraded.schema_migration_result.from_version == 5
        assert upgraded.schema_migration_result.applied_versions == (6,)
        assert upgraded.schema_migration_result.backup_path is not None
        integration = CausalLedger(upgraded).get_dendritic_integration(
            integration_id
        )
        assert integration is not None
        assert integration.window_evidence_class == "LEGACY_V5_NO_WINDOW"
        assert integration.window is None
        assert [member.event_id for member in integration.members] == [
            first_event_id,
            second_event_id,
        ]
        assert upgraded._conn.execute(
            "SELECT integration_id, source_schema_version, evidence_class, "
            "integration_created_at FROM dendritic_legacy_integrations"
        ).fetchall() == [
            (
                integration.id,
                5,
                "LEGACY_V5_NO_WINDOW",
                integration.created_at.isoformat(),
            )
        ]
        for table in (
            "dendritic_input_policy_snapshots",
            "dendritic_windows",
            "dendritic_window_members",
            "dendritic_integration_windows",
        ):
            assert upgraded._conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone() == (0,)
        with pytest.raises(sqlite3.IntegrityError, match="migration-sealed"):
            upgraded._conn.execute(
                "INSERT INTO dendritic_legacy_integrations "
                "(integration_id, source_schema_version, evidence_class, "
                "integration_created_at) VALUES (?, 5, 'LEGACY_V5_NO_WINDOW', ?)",
                ("post-v6-forgery", integration.created_at.isoformat()),
            )
    finally:
        upgraded.close()

    backup = sqlite3.connect(upgraded.schema_migration_result.backup_path)
    try:
        assert _user_version(backup) == 5
        assert backup.execute(
            "SELECT id FROM dendritic_integrations"
        ).fetchall() == [(integration_id,)]
        assert "dendritic_legacy_integrations" not in _tables(backup)
    finally:
        backup.close()


def test_recorded_v6_drift_is_backed_up_then_repaired_without_ledger_rewrite(tmp_path):
    database = tmp_path / "drift.db"
    initial = Storage(database)
    records_before = _records(initial._conn)
    initial._conn.execute("DROP TABLE living_orientations")
    initial._conn.commit()
    initial.close()

    repaired = Storage(database)
    try:
        result = repaired.schema_migration_result
        assert result.from_version == 6
        assert result.applied_versions == ()
        assert result.repaired_tables == ("living_orientations",)
        assert result.backup_path is not None and result.backup_path.exists()
        assert "living_orientations" in _tables(repaired._conn)
        assert _records(repaired._conn) == records_before
        assert _user_version(repaired._conn) == 6
    finally:
        repaired.close()
    backup = sqlite3.connect(result.backup_path)
    try:
        assert backup.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert "living_orientations" not in _tables(backup)
        assert _user_version(backup) == 6
    finally:
        backup.close()


def test_future_schema_fails_before_backup_or_mutation(tmp_path):
    database = tmp_path / "future.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE future_payload (value TEXT)")
    connection.execute("INSERT INTO future_payload VALUES ('keep')")
    connection.execute("PRAGMA user_version = 7")
    connection.commit()
    schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
    connection.close()

    with pytest.raises(SchemaMigrationError) as caught:
        Storage(database)
    assert caught.value.code == "future_schema_version"
    assert list(tmp_path.glob("future.db.schema-*.bak")) == []
    check = sqlite3.connect(database)
    try:
        assert _user_version(check) == 7
        assert int(check.execute("PRAGMA schema_version").fetchone()[0]) == schema_version
        assert check.execute("SELECT value FROM future_payload").fetchone() == ("keep",)
    finally:
        check.close()


def test_packaged_migration_checksum_drift_fails_closed(tmp_path):
    database = tmp_path / "checksum.db"
    Storage(database).close()
    source = Path(__file__).parents[1] / "src" / "pulse_system" / "substrate" / "storage" / "migrations"
    copied = tmp_path / "migrations"
    shutil.copytree(source, copied)
    changed = copied / "0002_task_relationship.sql"
    changed.write_text(changed.read_text(encoding="utf-8") + "\n-- altered\n", encoding="utf-8")

    connection = sqlite3.connect(database)
    try:
        with pytest.raises(SchemaMigrationError) as caught:
            SchemaMigrator(database, migrations_dir=copied).inspect(connection)
        assert caught.value.code == "migration_checksum_mismatch"
    finally:
        connection.close()


def test_migration_discovery_rejects_a_version_gap(tmp_path):
    migrations = tmp_path / "gap"
    migrations.mkdir()
    (migrations / "0001_initial.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (migrations / "0003_role_lease.sql").write_text("SELECT 3;\n", encoding="utf-8")
    with pytest.raises(SchemaMigrationError) as caught:
        SchemaMigrator(":memory:", migrations_dir=migrations)
    assert caught.value.code == "migration_sequence_invalid"


def test_fault_rolls_back_bootstrap_ledger_version_and_partial_schema(tmp_path):
    database = tmp_path / "fault.db"
    _legacy_database(database).close()
    migrations = tmp_path / "fault-migrations"
    migrations.mkdir()
    (migrations / "0001_initial.sql").write_text(
        "CREATE TABLE schema_migrations ("
        "version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, "
        "applied_at_utc TEXT NOT NULL);\n",
        encoding="utf-8",
    )
    (migrations / "0002_task_relationship.sql").write_text(
        "CREATE TABLE partial_v2 (value TEXT);\nTHIS IS NOT SQL;\n",
        encoding="utf-8",
    )
    (migrations / "0003_role_lease.sql").write_text(
        "CREATE TABLE never_reached (value TEXT);\n",
        encoding="utf-8",
    )
    (migrations / "0004_role_direct_output.sql").write_text(
        "CREATE TABLE also_never_reached (value TEXT);\n",
        encoding="utf-8",
    )
    (migrations / "0005_dendritic_convergence.sql").write_text(
        "CREATE TABLE dendritic_never_reached (value TEXT);\n",
        encoding="utf-8",
    )
    (migrations / "0006_dendritic_window_evidence.sql").write_text(
        "CREATE TABLE dendritic_window_never_reached (value TEXT);\n",
        encoding="utf-8",
    )
    migrator = SchemaMigrator(database, migrations_dir=migrations)
    connection = sqlite3.connect(database)
    plan = migrator.inspect(connection)
    backup_path = migrator.backup_if_needed(connection, plan)
    assert backup_path is not None and backup_path.exists()
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("CREATE TABLE bootstrap_sentinel (value TEXT)")
        with pytest.raises(SchemaMigrationError) as caught:
            migrator.apply_in_transaction(connection, plan)
        assert caught.value.code == "migration_apply_failed"
        connection.rollback()
        assert _user_version(connection) == 0
        assert "schema_migrations" not in _tables(connection)
        assert "partial_v2" not in _tables(connection)
        assert "bootstrap_sentinel" not in _tables(connection)
        assert connection.execute(
            "SELECT content FROM legacy_payload"
        ).fetchone() == ("before migration",)
    finally:
        connection.close()
    backup = sqlite3.connect(backup_path)
    try:
        assert backup.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        backup.close()
