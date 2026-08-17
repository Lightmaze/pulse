"""Auditable SQLite schema migration boundary for the Pulse substrate.

The existing Store DDL remains a transitional bootstrap for legacy databases.
This module supplies the missing provenance boundary: ordered files, immutable
checksums, ``PRAGMA user_version``, a durable ledger, and a consistent backup
before any legacy schema is changed.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator

_logger = logging.getLogger("pulse_system.storage.migrations")

TARGET_SCHEMA_VERSION = 6
_MIGRATION_NAME = re.compile(r"(?P<version>[0-9]{4})_(?P<name>[a-z][a-z0-9_]*)\.sql\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


class SchemaMigrationError(RuntimeError):
    """Typed, fail-closed schema migration error."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class MigrationFile:
    version: int
    name: str
    path: Path
    sql: str
    sha256: str


@dataclass(frozen=True, slots=True)
class MigrationRecord:
    version: int
    name: str
    sha256: str
    applied_at_utc: str


@dataclass(frozen=True, slots=True)
class SchemaMigrationPlan:
    current_version: int
    migrations: tuple[MigrationFile, ...]
    records: tuple[MigrationRecord, ...]
    has_existing_schema: bool
    missing_required_tables: tuple[str, ...]

    @property
    def needs_migration(self) -> bool:
        return self.current_version < TARGET_SCHEMA_VERSION

    @property
    def needs_bootstrap(self) -> bool:
        return self.needs_migration or bool(self.missing_required_tables)

    @property
    def needs_work(self) -> bool:
        return self.needs_bootstrap


@dataclass(frozen=True, slots=True)
class SchemaMigrationResult:
    from_version: int
    to_version: int
    applied_versions: tuple[int, ...]
    backup_path: Path | None
    repaired_tables: tuple[str, ...] = ()

    @property
    def migrated(self) -> bool:
        return bool(self.applied_versions)


def iter_sql_statements(sql: str) -> Iterator[str]:
    """Split a trusted migration/bootstrap script using SQLite's parser.

    ``str.split(';')`` breaks triggers. ``sqlite3.complete_statement`` waits
    through trigger bodies and yields only complete statements, while regular
    ``Connection.execute`` keeps every statement inside the caller's explicit
    transaction.
    """

    if not isinstance(sql, str):
        raise SchemaMigrationError("migration_sql_invalid", "migration SQL must be text")
    buffer: list[str] = []
    for line in sql.splitlines(keepends=True):
        buffer.append(line)
        candidate = "".join(buffer)
        if sqlite3.complete_statement(candidate):
            if candidate.strip():
                yield candidate
            buffer.clear()
    remainder = "".join(buffer)
    if remainder.strip():
        raise SchemaMigrationError(
            "migration_sql_incomplete",
            "migration SQL ended with an incomplete statement",
        )


def execute_sql_script(connection: sqlite3.Connection, sql: str) -> None:
    for statement in iter_sql_statements(sql):
        connection.execute(statement)


class SchemaMigrator:
    """Inspect first, back up second, mutate once in one transaction."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        migrations_dir: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
        required_tables: Iterable[str] = (),
    ) -> None:
        self.db_path = str(db_path)
        self.migrations_dir = (
            Path(migrations_dir)
            if migrations_dir is not None
            else Path(__file__).with_name("migrations")
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        required = {
            "schema_migrations",
            "task_relationships",
            "role_leases",
            "role_accountability_cycles",
            "role_obligations",
            "role_contributions",
            *required_tables,
        }
        if any(re.fullmatch(r"[a-z][a-z0-9_]*", name) is None for name in required):
            raise SchemaMigrationError(
                "migration_required_table_invalid",
                "required schema table names must be safe identifiers",
            )
        self.required_tables = tuple(sorted(required))
        self.migrations = self._discover()

    def _discover(self) -> tuple[MigrationFile, ...]:
        try:
            paths = sorted(self.migrations_dir.glob("*.sql"))
        except OSError as exc:
            raise SchemaMigrationError(
                "migration_files_unreadable",
                "migration directory could not be read",
            ) from exc
        if not paths:
            raise SchemaMigrationError(
                "migration_files_missing",
                "no packaged schema migrations were found",
            )
        migrations: list[MigrationFile] = []
        for path in paths:
            match = _MIGRATION_NAME.fullmatch(path.name)
            if match is None:
                raise SchemaMigrationError(
                    "migration_filename_invalid",
                    f"invalid migration filename: {path.name}",
                )
            version = int(match.group("version"))
            try:
                raw = path.read_bytes()
                sql = raw.decode("utf-8")
            except (OSError, UnicodeError) as exc:
                raise SchemaMigrationError(
                    "migration_file_invalid",
                    f"migration {path.name} is not readable UTF-8",
                ) from exc
            migrations.append(
                MigrationFile(
                    version=version,
                    name=match.group("name"),
                    path=path,
                    sql=sql,
                    sha256=hashlib.sha256(raw).hexdigest(),
                )
            )
        versions = [migration.version for migration in migrations]
        expected = list(range(1, TARGET_SCHEMA_VERSION + 1))
        if versions != expected:
            raise SchemaMigrationError(
                "migration_sequence_invalid",
                f"migration versions must be contiguous {expected}, got {versions}",
            )
        return tuple(migrations)

    @staticmethod
    def _user_version(connection: sqlite3.Connection) -> int:
        try:
            row = connection.execute("PRAGMA user_version").fetchone()
        except sqlite3.Error as exc:
            raise SchemaMigrationError(
                "schema_version_unreadable",
                "PRAGMA user_version could not be read",
            ) from exc
        if row is None or type(row[0]) is not int or row[0] < 0:
            raise SchemaMigrationError(
                "schema_version_invalid",
                "PRAGMA user_version is not a non-negative integer",
            )
        return int(row[0])

    @staticmethod
    def _has_table(connection: sqlite3.Connection, name: str) -> bool:
        try:
            return connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (name,),
            ).fetchone() is not None
        except sqlite3.Error as exc:
            raise SchemaMigrationError(
                "schema_catalog_unreadable",
                "SQLite schema catalog could not be read",
            ) from exc

    @classmethod
    def _records(cls, connection: sqlite3.Connection) -> tuple[MigrationRecord, ...]:
        if not cls._has_table(connection, "schema_migrations"):
            return ()
        try:
            rows = connection.execute(
                "SELECT version, name, sha256, applied_at_utc "
                "FROM schema_migrations ORDER BY version"
            ).fetchall()
        except sqlite3.Error as exc:
            raise SchemaMigrationError(
                "migration_ledger_invalid",
                "schema_migrations does not match the required ledger shape",
            ) from exc
        records: list[MigrationRecord] = []
        for row in rows:
            version, name, sha256, applied_at = row
            if (
                type(version) is not int
                or version < 1
                or not isinstance(name, str)
                or not isinstance(sha256, str)
                or _HEX64.fullmatch(sha256) is None
                or not isinstance(applied_at, str)
                or not applied_at
            ):
                raise SchemaMigrationError(
                    "migration_ledger_invalid",
                    "schema_migrations contains an invalid record",
                )
            records.append(MigrationRecord(version, name, sha256, applied_at))
        return tuple(records)

    def _verify_history(
        self,
        current_version: int,
        records: tuple[MigrationRecord, ...],
    ) -> None:
        if current_version > TARGET_SCHEMA_VERSION:
            raise SchemaMigrationError(
                "future_schema_version",
                f"database schema v{current_version} is newer than supported v{TARGET_SCHEMA_VERSION}",
            )
        if current_version == 0 and not records:
            return
        expected_versions = tuple(range(1, current_version + 1))
        actual_versions = tuple(record.version for record in records)
        if actual_versions != expected_versions:
            raise SchemaMigrationError(
                "migration_history_gap",
                "migration ledger must exactly match PRAGMA user_version without gaps",
            )
        by_version = {migration.version: migration for migration in self.migrations}
        for record in records:
            migration = by_version.get(record.version)
            if migration is None:
                raise SchemaMigrationError(
                    "migration_history_unknown",
                    f"migration ledger contains unknown version {record.version}",
                )
            if record.name != migration.name:
                raise SchemaMigrationError(
                    "migration_name_mismatch",
                    f"migration v{record.version} name differs from the packaged file",
                )
            if not secrets_equal(record.sha256, migration.sha256):
                raise SchemaMigrationError(
                    "migration_checksum_mismatch",
                    f"migration v{record.version} checksum differs from the packaged file",
                )

    @staticmethod
    def _has_existing_schema(connection: sqlite3.Connection) -> bool:
        try:
            return connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
            ).fetchone() is not None
        except sqlite3.Error as exc:
            raise SchemaMigrationError(
                "schema_catalog_unreadable",
                "SQLite schema catalog could not be inspected",
            ) from exc

    def inspect(self, connection: sqlite3.Connection) -> SchemaMigrationPlan:
        if connection.in_transaction:
            raise SchemaMigrationError(
                "migration_inspection_in_transaction",
                "schema inspection must run before a transaction begins",
            )
        current_version = self._user_version(connection)
        records = self._records(connection)
        self._verify_history(current_version, records)
        missing = tuple(
            name for name in self.required_tables if not self._has_table(connection, name)
        )
        return SchemaMigrationPlan(
            current_version=current_version,
            migrations=self.migrations,
            records=records,
            has_existing_schema=self._has_existing_schema(connection),
            missing_required_tables=missing,
        )

    def _backup_paths(self, from_version: int) -> tuple[Path, Path]:
        database = Path(self.db_path).expanduser().resolve()
        timestamp = self._clock().astimezone(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
        stem = (
            f"{database.name}.schema-v{from_version}-to-v{TARGET_SCHEMA_VERSION}."
            f"{timestamp}"
        )
        candidate = database.with_name(stem + ".bak")
        counter = 1
        while candidate.exists() or candidate.with_suffix(candidate.suffix + ".incomplete").exists():
            candidate = database.with_name(f"{stem}.{counter}.bak")
            counter += 1
        return candidate, candidate.with_suffix(candidate.suffix + ".incomplete")

    def backup_if_needed(
        self,
        connection: sqlite3.Connection,
        plan: SchemaMigrationPlan,
    ) -> Path | None:
        if not plan.needs_work or not plan.has_existing_schema:
            return None
        if self.db_path == ":memory:":
            return None
        if connection.in_transaction:
            raise SchemaMigrationError(
                "schema_backup_in_transaction",
                "schema backup must run before the migration transaction",
            )
        final_path, incomplete_path = self._backup_paths(plan.current_version)
        destination: sqlite3.Connection | None = None
        try:
            destination = sqlite3.connect(str(incomplete_path))
            connection.backup(destination)
            destination.commit()
            check = destination.execute("PRAGMA integrity_check").fetchone()
            if check is None or check[0] != "ok":
                raise SchemaMigrationError(
                    "schema_backup_integrity_failed",
                    "SQLite backup did not pass integrity_check",
                )
            destination.close()
            destination = None
            incomplete_path.replace(final_path)
        except (OSError, sqlite3.Error) as exc:
            raise SchemaMigrationError(
                "schema_backup_failed",
                "SQLite online backup could not be completed",
            ) from exc
        finally:
            if destination is not None:
                destination.close()
            if incomplete_path.exists():
                incomplete_path.unlink(missing_ok=True)
        name_digest = hashlib.sha256(final_path.name.encode("utf-8")).hexdigest()
        _logger.info(
            "schema_backup_created from_version=%d to_version=%d backup_name_sha256=%s",
            plan.current_version,
            TARGET_SCHEMA_VERSION,
            name_digest,
        )
        return final_path

    def _verify_required_tables(self, connection: sqlite3.Connection) -> None:
        missing = [
            name for name in self.required_tables if not self._has_table(connection, name)
        ]
        if missing:
            raise SchemaMigrationError(
                "migration_schema_drift",
                f"schema v{TARGET_SCHEMA_VERSION} is missing required tables: "
                + ", ".join(missing),
            )

    def repair_in_transaction(
        self,
        connection: sqlite3.Connection,
        plan: SchemaMigrationPlan,
    ) -> tuple[str, ...]:
        """Replay idempotent files after transitional bootstrap repairs drift."""

        if not connection.in_transaction:
            raise SchemaMigrationError(
                "migration_transaction_required",
                "schema repair requires one caller-owned transaction",
            )
        if plan.current_version != TARGET_SCHEMA_VERSION:
            raise SchemaMigrationError(
                "migration_repair_version_invalid",
                "schema repair is only valid for an already recorded "
                f"v{TARGET_SCHEMA_VERSION} database",
            )
        for migration in plan.migrations:
            try:
                execute_sql_script(connection, migration.sql)
            except SchemaMigrationError:
                raise
            except sqlite3.Error as exc:
                raise SchemaMigrationError(
                    "migration_repair_failed",
                    f"idempotent schema repair failed at v{migration.version}",
                ) from exc
        self._verify_required_tables(connection)
        self._verify_history(self._user_version(connection), self._records(connection))
        _logger.info(
            "schema_bootstrap_repaired missing_table_count=%d",
            len(plan.missing_required_tables),
        )
        return plan.missing_required_tables

    def apply_in_transaction(
        self,
        connection: sqlite3.Connection,
        plan: SchemaMigrationPlan,
    ) -> tuple[int, ...]:
        if not connection.in_transaction:
            raise SchemaMigrationError(
                "migration_transaction_required",
                "schema migrations require one caller-owned transaction",
            )
        applied: list[int] = []
        for migration in plan.migrations:
            if migration.version <= plan.current_version:
                continue
            try:
                execute_sql_script(connection, migration.sql)
                if not self._has_table(connection, "schema_migrations"):
                    raise SchemaMigrationError(
                        "migration_ledger_missing",
                        "migration v1 did not create schema_migrations",
                    )
                applied_at = self._clock().astimezone(timezone.utc).isoformat()
                connection.execute(
                    "INSERT INTO schema_migrations "
                    "(version, name, sha256, applied_at_utc) VALUES (?, ?, ?, ?)",
                    (
                        migration.version,
                        migration.name,
                        migration.sha256,
                        applied_at,
                    ),
                )
                connection.execute(f"PRAGMA user_version = {migration.version}")
            except SchemaMigrationError:
                raise
            except sqlite3.Error as exc:
                raise SchemaMigrationError(
                    "migration_apply_failed",
                    f"migration v{migration.version} could not be applied",
                ) from exc
            applied.append(migration.version)
            _logger.info(
                "schema_migration_applied version=%d name=%s sha256=%s",
                migration.version,
                migration.name,
                migration.sha256,
            )
        final_version = self._user_version(connection)
        final_records = self._records(connection)
        self._verify_history(final_version, final_records)
        if final_version != TARGET_SCHEMA_VERSION:
            raise SchemaMigrationError(
                "migration_target_not_reached",
                f"migration ended at v{final_version}, expected v{TARGET_SCHEMA_VERSION}",
            )
        self._verify_required_tables(connection)
        return tuple(applied)


def secrets_equal(left: str, right: str) -> bool:
    """Constant-shape digest comparison without importing the API boundary."""

    import secrets

    return secrets.compare_digest(left, right)
