"""Library and skills (education/library/) — procedural memory layer.

Three document spaces, all plain files, no registry (the non-cognitive infrastructure boundary — skill
retrieval is the engram's own act, not a central dispatcher's):

    <root>/
    ├── engrams/<engram_id>/
    │   ├── wiki/        # dream consolidation output, refined increments (sleep engine)
    │   ├── diary/       # snapshot-delegation diffs, clone observation diaries
    │   └── skills/      # private skills
    └── projects/<project_id>/
        └── skills/      # shared skills, visible to the whole cluster

Skills are markdown documents with a one-line description marker:

    # <name>
    > <description>
    <body...>

discover_skills() returns metadata only (name, description, path) — the
engram reads the full text itself when it judges a skill relevant, and the
content is *appended* to its session (E1: never inserted into the prefix).

On succession the whole engram directory is carried over to the successor:
episodic detail is forgotten with the old session, procedural memory
survives (spec: 换代式遗忘保留程序性记忆).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pulse_system.core.habitat.managed import (
    ExternalEffectPublicationError,
    ExternalEffectTransaction,
    ExternalEffectUncertainError,
    OrdinaryExternalEffectAuthority,
    bind_external_effect_authority,
)
from pulse_system.core.runtime.publication import (
    RuntimePublicationPermit,
)

_logger = logging.getLogger("pulse_system.library")

_DESCRIPTION_RE = re.compile(r"^>\s*(.+)$", re.MULTILINE)
_SLUG_RE = re.compile(r"[^a-zA-Z0-9一-鿿]+")
_EFFECT_JOURNAL = ".library-effect-journal.jsonl"
_QUARANTINE_DIR = ".library-effect-quarantine"


class LibraryEffectUncertain(ExternalEffectUncertainError):
    """A Library mutation crossed, or may have crossed, a file boundary."""


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _slug(text: str, max_len: int = 40) -> str:
    s = _SLUG_RE.sub("-", text).strip("-")
    return (s[:max_len] or "entry").lower()


@dataclass(frozen=True)
class SkillInfo:
    name: str
    description: str
    path: str
    scope: str  # "engram" | "project"


class Library:
    """Filesystem-backed document spaces for engrams and projects."""

    def __init__(
        self,
        root: str | Path,
        *,
        publication_authority: (
            OrdinaryExternalEffectAuthority
            | RuntimePublicationPermit
            | None
        ) = None,
    ):
        self._root = Path(root).resolve()
        self._publication = bind_external_effect_authority(
            publication_authority,
            unbound_scope="unbound:library",
        )
        if not self._root.exists():
            self._publication.publish(
                "library:root-create",
                lambda: self._root.mkdir(parents=True, exist_ok=False),
            )
        if not self._root.is_dir():
            raise ValueError("Library root must be a directory")
        self._lock = threading.RLock()
        self._effect_journal_path = self._root / _EFFECT_JOURNAL
        self._quarantine_root = self._root / _QUARANTINE_DIR

    @property
    def root(self) -> Path:
        return self._root

    @property
    def publication_origin(self) -> str:
        return self._publication.origin

    def recovery_snapshot(self) -> dict[str, int]:
        """Return payload-free durable recovery counts without modifying files."""

        with self._lock:
            latest: dict[str, str] = {}
            journal_records = 0
            malformed_records = 0
            journal_readable = 1
            if self._effect_journal_path.is_file():
                try:
                    lines = self._effect_journal_path.read_text(
                        encoding="utf-8"
                    ).splitlines()
                except (OSError, UnicodeError):
                    lines = []
                    journal_readable = 0
                for line in lines:
                    journal_records += 1
                    try:
                        record = json.loads(line)
                    except (TypeError, ValueError):
                        malformed_records += 1
                        continue
                    if not isinstance(record, dict):
                        malformed_records += 1
                        continue
                    effect_id = record.get("effect_id")
                    state = record.get("state")
                    if isinstance(effect_id, str) and isinstance(state, str):
                        latest[effect_id] = state
                    else:
                        malformed_records += 1
            staging_readable = 1
            try:
                staged_ids = (
                    {
                        path.stem
                        for path in self._quarantine_root.glob("*.staged")
                        if path.is_file()
                    }
                    if self._quarantine_root.is_dir()
                    else set()
                )
            except OSError:
                staged_ids = set()
                staging_readable = 0
            orphaned_staged = sum(effect_id not in latest for effect_id in staged_ids)
            prepared = sum(state == "prepared" for state in latest.values())
            uncertain = sum(state == "uncertain" for state in latest.values())
            committed = sum(state == "committed" for state in latest.values())
            recovered = sum(state == "recovered" for state in latest.values())
            aborted = sum(state == "aborted" for state in latest.values())
            scan_unresolved = (
                malformed_records
                + (1 - journal_readable)
                + (1 - staging_readable)
            )
            return {
                "attempted": len(latest) + orphaned_staged,
                "committed": committed,
                "recovered": recovered,
                "unresolved": (
                    prepared + uncertain + orphaned_staged + scan_unresolved
                ),
                "uncertain": uncertain,
                "prepared": prepared,
                "aborted": aborted,
                "staged": len(staged_ids),
                "orphaned_staged": orphaned_staged,
                "journal_records": journal_records,
                "malformed_records": malformed_records,
                "journal_readable": journal_readable,
                "staging_readable": staging_readable,
                "evidence_scan_unresolved": scan_unresolved,
            }

    # ── Paths ────────────────────────────────────────────────────

    def engram_dir(self, engram_id: str) -> Path:
        return self._root / "engrams" / engram_id

    def project_skills_dir(self, project_id: str) -> Path:
        return self._root / "projects" / project_id / "skills"

    # ── Diary (episodic side-records: diffs, observations) ──────

    def append_diary(self, engram_id: str, entry: str, *, source: str = "") -> Path:
        with self._lock:
            path = self.engram_dir(engram_id) / "diary" / "diary.md"
            header = f"\n## {_now_stamp()}" + (f" — {source}" if source else "") + "\n\n"
            self._atomic_append_text(
                path,
                header + entry.strip() + "\n",
                kind="library_diary_append",
            )
            return path

    def read_diary(self, engram_id: str) -> str:
        path = self.engram_dir(engram_id) / "diary" / "diary.md"
        return path.read_text(encoding="utf-8") if path.exists() else ""

    # NREM consolidation watermark (sleep engine): the sleep engine consumes only
    # diary content appended since the last consolidation.

    def diary_delta(self, engram_id: str) -> str:
        """Diary text appended since the last consolidation watermark."""
        path = self.engram_dir(engram_id) / "diary" / "diary.md"
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        return text[self._diary_offset(engram_id):]

    def mark_diary_consolidated(self, engram_id: str) -> None:
        with self._lock:
            path = self.engram_dir(engram_id) / "diary" / "diary.md"
            size = len(path.read_text(encoding="utf-8")) if path.exists() else 0
            marker = self.engram_dir(engram_id) / "diary" / ".consolidated"
            self._atomic_replace_text(
                marker,
                str(size),
                kind="library_diary_watermark",
            )

    def _diary_offset(self, engram_id: str) -> int:
        marker = self.engram_dir(engram_id) / "diary" / ".consolidated"
        if not marker.exists():
            return 0
        try:
            return int(marker.read_text(encoding="utf-8").strip() or 0)
        except ValueError:
            return 0

    def engrams_with_new_diary(self) -> list[str]:
        """Engram ids whose diary grew past their consolidation watermark."""
        base = self._root / "engrams"
        if not base.exists():
            return []
        return [
            d.name for d in sorted(base.iterdir())
            if d.is_dir() and self.diary_delta(d.name).strip()
        ]

    # ── Wiki (consolidated knowledge; one file per entry, sleep engine) ───

    def append_wiki(self, engram_id: str, title: str, content: str) -> Path:
        with self._lock:
            return self._append_wiki_locked(engram_id, title, content)

    def supersede_wiki(
        self,
        engram_id: str,
        target: Path | str,
        title: str,
        content: str,
        *,
        evidence: str,
    ) -> Path:
        """Write an entry that explicitly REPLACES an earlier one, naming the
        evidence that forced the change.

        An engram that can only append can never be wrong out loud: it accretes
        claims and nothing ever retracts them. Being corrected is not a failure
        mode to tolerate, it is the shape contact with reality takes (the natural-text result rule),
        so revision needs a first-class way to be expressed.

        Deliberately NOT an in-place overwrite. The library stays append-only,
        so the fact that the engram changed its mind — and what it changed from
        — survives as evidence rather than being erased. The superseded entry
        gets a pointer forward; the new entry records what it replaces and why.

        `evidence` must name what in the world forced the revision (a file, a
        symbol, an observation). A revision that cannot say what corrected it is
        not a correction, it is a mood.
        """
        if not evidence.strip():
            raise ValueError(
                "supersede_wiki requires evidence naming what forced the "
                "revision"
            )
        wiki_dir = self.engram_dir(engram_id) / "wiki"
        old = Path(target) if isinstance(target, Path) else wiki_dir / target
        if not old.exists():
            raise FileNotFoundError(f"no such wiki entry: {old}")

        with self._lock:
            operation_id = uuid.uuid4().hex
            prepared = False
            with self._publication.transaction(
                "library:wiki-supersede-prepare"
            ) as transaction:
                self._record_effect(
                    operation_id,
                    state="prepared",
                    kind="library_wiki_supersede",
                    path=self._relative(old),
                    transaction=transaction,
                )
            prepared = True
            path: Path | None = None
            try:
                path = self._append_wiki_locked(
                    engram_id,
                    title,
                    f"_supersedes: {old.name}_\n_evidence: {evidence.strip()}_\n\n"
                    f"{content.strip()}",
                )
                # Forward pointer on the old entry, appended — the original
                # text remains intact for audit in both directions.
                self._atomic_append_text(
                    old,
                    f"\n\n_superseded by: {path.name} ({_now_stamp()})_\n",
                    kind="library_wiki_forward_pointer",
                    terminal_records=(
                        {
                            "effect_id": operation_id,
                            "state": "committed",
                            "kind": "library_wiki_supersede",
                            "path": self._relative(old),
                        },
                    ),
                )
            except ExternalEffectPublicationError as exc:
                if prepared or path is not None or exc.crossed_boundary:
                    raise LibraryEffectUncertain(
                        "library_supersede_uncertain",
                        effect_name="library:wiki-supersede",
                    ) from exc
                raise
            assert path is not None
            return path

    def wiki_entries(self, engram_id: str) -> list[Path]:
        wiki_dir = self.engram_dir(engram_id) / "wiki"
        return sorted(wiki_dir.glob("*.md")) if wiki_dir.exists() else []

    # ── Skills (procedural memory) ───────────────────────────────

    def write_skill(
        self,
        name: str,
        description: str,
        content: str,
        *,
        engram_id: str | None = None,
        project_id: str | None = None,
    ) -> Path:
        """Write a skill into the engram's private space or a project's
        shared space (exactly one scope must be given)."""
        if (engram_id is None) == (project_id is None):
            raise ValueError("give exactly one of engram_id / project_id")
        base = (
            self.engram_dir(engram_id) / "skills"
            if engram_id is not None
            else self.project_skills_dir(project_id)
        )
        with self._lock:
            path = base / f"{_slug(name)}.md"
            self._atomic_replace_text(
                path,
                f"# {name}\n\n> {description.strip()}\n\n{content.strip()}\n",
                kind="library_skill_write",
            )
            return path

    def discover_skills(
        self, engram_id: str, project_id: str | None = None
    ) -> list[SkillInfo]:
        """Metadata of skills visible to an engram: its private skills plus
        its Project's shared skills. Never returns full text."""
        found: list[SkillInfo] = []
        private = self.engram_dir(engram_id) / "skills"
        if private.exists():
            found.extend(self._scan(private, "engram"))
        if project_id is not None:
            shared = self.project_skills_dir(project_id)
            if shared.exists():
                found.extend(self._scan(shared, "project"))
        return found

    def read_skill(self, path: str | Path) -> str:
        resolved = Path(path).resolve()
        if not resolved.is_relative_to(self._root):
            raise PermissionError(f"Path escapes library root: {path}")
        if (
            resolved == self._effect_journal_path
            or resolved.is_relative_to(self._quarantine_root)
        ):
            raise PermissionError("Library recovery evidence is private")
        return resolved.read_text(encoding="utf-8")

    def _scan(self, directory: Path, scope: str) -> list[SkillInfo]:
        infos = []
        for p in sorted(directory.glob("*.md")):
            try:
                text = p.read_text(encoding="utf-8")
            except OSError as e:
                _logger.warning("unreadable skill %s: %s", p, e)
                continue
            name = p.stem
            for line in text.splitlines():
                if line.startswith("# "):
                    name = line[2:].strip()
                    break
            m = _DESCRIPTION_RE.search(text)
            description = m.group(1).strip() if m else ""
            infos.append(SkillInfo(name=name, description=description,
                                   path=str(p), scope=scope))
        return infos

    # ── Succession ───────────────────────────────────────────────

    def transfer(self, old_id: str, new_id: str) -> None:
        """Carry the library over to a successor engram.

        The directory is renamed: the old (archived) engram no longer acts,
        and its procedural memory belongs to the successor. Merging into an
        existing successor directory moves entries file-by-file.
        """
        with self._lock:
            old_dir = self.engram_dir(old_id)
            if not old_dir.exists():
                return
            new_dir = self.engram_dir(new_id)
            operation_id = uuid.uuid4().hex
            prepared = False
            with self._publication.transaction(
                "library:succession-transfer-prepare"
            ) as transaction:
                self._record_effect(
                    operation_id,
                    state="prepared",
                    kind="library_succession_transfer",
                    path=f"engrams/{old_id}",
                    transaction=transaction,
                )
            prepared = True
            try:
                self._failpoint("before_library_transfer_commit")
                with self._publication.transaction(
                    "library:succession-transfer-commit"
                ) as transaction:
                    if not new_dir.exists():
                        self._ensure_directory_in_transaction(
                            new_dir.parent,
                            transaction=transaction,
                        )
                        self._rename_path(
                            old_dir,
                            new_dir,
                            transaction=transaction,
                        )
                    else:
                        for sub in old_dir.iterdir():
                            target = new_dir / sub.name
                            if not target.exists():
                                self._rename_path(
                                    sub,
                                    target,
                                    transaction=transaction,
                                )
                            else:
                                for item in sub.rglob("*"):
                                    if item.is_file():
                                        dest = target / item.relative_to(sub)
                                        if not dest.exists():
                                            self._ensure_directory_in_transaction(
                                                dest.parent,
                                                transaction=transaction,
                                            )
                                            self._rename_path(
                                                item,
                                                dest,
                                                transaction=transaction,
                                            )
                    self._record_effect(
                        operation_id,
                        state="committed",
                        kind="library_succession_transfer",
                        path=f"engrams/{old_id}",
                        transaction=transaction,
                    )
            except ExternalEffectPublicationError as exc:
                if prepared or exc.crossed_boundary:
                    raise LibraryEffectUncertain(
                        "library_transfer_uncertain",
                        effect_name="library:succession-transfer",
                    ) from exc
                raise
            _logger.info("library transferred: %s -> %s", old_id, new_id)

    # ── Typed external-effect publication ───────────────────────

    def _append_wiki_locked(self, engram_id: str, title: str, content: str) -> Path:
        wiki_dir = self.engram_dir(engram_id) / "wiki"
        self._ensure_directory(wiki_dir)
        seq = len(list(wiki_dir.glob("*.md"))) + 1
        path = wiki_dir / f"{seq:04d}-{_slug(title)}.md"
        self._atomic_replace_text(
            path,
            f"# {title}\n\n_{_now_stamp()}_\n\n{content.strip()}\n",
            kind="library_wiki_append",
        )
        return path

    def _atomic_append_text(
        self,
        path: Path,
        content: str,
        *,
        kind: str,
        terminal_records: tuple[dict[str, str], ...] = (),
    ) -> None:
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        self._atomic_replace_text(
            path,
            current + content,
            kind=kind,
            terminal_records=terminal_records,
        )

    def _atomic_replace_text(
        self,
        path: Path,
        content: str,
        *,
        kind: str,
        terminal_records: tuple[dict[str, str], ...] = (),
    ) -> None:
        effect_id = uuid.uuid4().hex
        relative = self._relative(path)
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        staged_path = self._quarantine_root / f"{effect_id}.staged"
        prepared = False
        directory_created = False
        quarantine_created = False
        try:
            self._publication.assert_active()
            directory_created = self._ensure_directory(path.parent)
            quarantine_created = self._ensure_directory(self._quarantine_root)
            with self._publication.transaction(
                "library:file-prepare"
            ) as transaction:
                self._record_effect(
                    effect_id,
                    state="prepared",
                    kind=kind,
                    path=relative,
                    digest=digest,
                    quarantine=self._relative(staged_path),
                    transaction=transaction,
                )
                with staged_path.open(
                    "x",
                    encoding="utf-8",
                    newline="",
                ) as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
            prepared = True
            self._failpoint("before_external_effect_commit")
            with self._publication.transaction(
                "library:file-commit"
            ) as transaction:
                transaction.mark_mutation()
                os.replace(staged_path, path)
                self._failpoint(
                    "after_library_replace_before_terminal_evidence"
                )
                self._record_effect(
                    effect_id,
                    state="committed",
                    kind=kind,
                    path=relative,
                    digest=digest,
                    transaction=transaction,
                )
                for terminal in terminal_records:
                    self._record_effect(
                        terminal["effect_id"],
                        state=terminal["state"],
                        kind=terminal["kind"],
                        path=terminal["path"],
                        transaction=transaction,
                    )
        except ExternalEffectPublicationError as exc:
            if (
                prepared
                or directory_created
                or quarantine_created
                or exc.crossed_boundary
            ):
                raise LibraryEffectUncertain(
                    "library_file_publication_uncertain",
                    effect_name=exc.effect_name or "library:file-commit",
                ) from exc
            raise

    def _ensure_directory(self, path: Path) -> bool:
        if path.exists():
            return False
        effect_id = uuid.uuid4().hex
        relative = self._relative(path)
        prepared = False
        try:
            with self._publication.transaction(
                "library:directory-prepare"
            ) as transaction:
                self._record_effect(
                    effect_id,
                    state="prepared",
                    kind="library_directory_create",
                    path=relative,
                    transaction=transaction,
                )
            prepared = True
            self._failpoint("before_library_directory_commit")
            with self._publication.transaction(
                "library:directory-commit"
            ) as transaction:
                self._ensure_directory_in_transaction(
                    path,
                    transaction=transaction,
                )
                self._record_effect(
                    effect_id,
                    state="committed",
                    kind="library_directory_create",
                    path=relative,
                    transaction=transaction,
                )
        except ExternalEffectPublicationError as exc:
            if prepared or exc.crossed_boundary:
                raise LibraryEffectUncertain(
                    "library_directory_publication_uncertain",
                    effect_name=exc.effect_name or "library:directory-commit",
                ) from exc
            raise
        return True

    def _ensure_directory_in_transaction(
        self,
        path: Path,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        transaction.mark_mutation()
        path.mkdir(parents=True, exist_ok=True)

    def _rename_path(
        self,
        source: Path,
        destination: Path,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        self._failpoint("inside_library_transfer_commit")
        transaction.mark_mutation()
        os.replace(source, destination)

    def _record_effect(
        self,
        effect_id: str,
        *,
        state: str,
        kind: str,
        path: str,
        digest: str | None = None,
        error: str | None = None,
        quarantine: str | None = None,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        transaction.mark_mutation()
        record = {
            "version": 1,
            "effect_id": effect_id,
            "state": state,
            "kind": kind,
            "path": path,
            "digest": digest,
            "error": error,
            "quarantine": quarantine,
            "recorded_at": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            ),
        }
        try:
            with self._effect_journal_path.open(
                "a",
                encoding="utf-8",
                newline="\n",
            ) as stream:
                stream.write(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as exc:
            raise ExternalEffectPublicationError(
                "external_effect_journal_unavailable"
            ) from exc

    def _relative(self, path: Path) -> str:
        return path.resolve().relative_to(self._root).as_posix()

    def _failpoint(self, name: str) -> None:
        """No-op production hook for deterministic commit-boundary tests."""

        del name

    # ── Tool integration (tool registry) ───────────────────────────────────

    def discover_tool(self, engram_id: str, project_id: str | None = None):
        """Build a discover_skills tool function bound to one engram."""
        from pulse_system.agent.tools.registry import ToolResult

        def discover_skills(query: str = "") -> ToolResult:
            infos = self.discover_skills(engram_id, project_id)
            if not infos:
                return ToolResult(
                    success=True, content="No skills available yet."
                )
            lines = ["Available skills (read one with file_read on its path):"]
            for info in infos:
                lines.append(
                    f"- {info.name} [{info.scope}] — {info.description}\n"
                    f"  path: {info.path}"
                )
            return ToolResult(success=True, content="\n".join(lines))

        return discover_skills
