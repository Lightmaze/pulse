"""Canonical read-only projection for a subject's living portfolio.

The portfolio is deliberately derived from the existing world registry and
purpose governance stores.  It owns no persistence and performs no repair:
inconsistent source state fails closed so callers never receive a plausible
but partial account of a subject's life.
"""

from __future__ import annotations

from typing import Any

from pulse_system.agent.harness.purpose_governance import (
    LineageState,
    PurposeGovernance,
    PurposeRevision,
    SubjectLineage,
)
from pulse_system.core.types import (
    ActivityCenter,
    ActivityCenterStatus,
    ActivityKind,
    CenterMembership,
)
from pulse_system.core.world import WorldRegistry


SCHEMA_VERSION = "living-portfolio.v1"
MIN_HISTORY_LIMIT = 1
MAX_HISTORY_LIMIT = 100

_PORTFOLIO_STATES = (
    "active",
    "quiet",
    "parked",
    "completed",
    "archived",
)
_PORTFOLIO_STATE_BY_CENTER_STATUS = {
    ActivityCenterStatus.ACTIVE: "active",
    ActivityCenterStatus.DORMANT: "quiet",
    ActivityCenterStatus.PAUSED: "parked",
    ActivityCenterStatus.COMPLETED: "completed",
    ActivityCenterStatus.ARCHIVED: "archived",
}
_PORTFOLIO_STATE_RANK = {
    state: index for index, state in enumerate(_PORTFOLIO_STATES)
}


class LivingPortfolioValidationError(ValueError):
    """A projector input is outside the frozen ``living-portfolio.v1`` contract."""


class LivingPortfolioRecoveryError(RuntimeError):
    """Durable source state cannot produce one trustworthy portfolio."""


class LivingPortfolioProjector:
    """Build the frozen ``living-portfolio.v1`` payload without durable writes."""

    def __init__(
        self,
        world: WorldRegistry,
        governance: PurposeGovernance,
        world_id: str,
    ) -> None:
        self._world = world
        self._governance = governance
        self._world_id = world_id

    def project(
        self,
        engram_id: str,
        history_limit: int = 20,
    ) -> dict[str, Any]:
        """Return a complete portfolio or fail without returning partial state."""

        if (
            type(history_limit) is not int
            or not MIN_HISTORY_LIMIT <= history_limit <= MAX_HISTORY_LIMIT
        ):
            raise LivingPortfolioValidationError(
                "history_limit must be an integer in [1, 100]"
            )

        try:
            lineage = self._governance.find_lineage_for_engram(engram_id)
            self._validate_lineage(lineage, engram_id)

            holder_id = (
                engram_id if lineage is None else lineage.current_engram_id
            )
            purpose = self._purpose_view(lineage, history_limit)
            items = self._portfolio_items(holder_id)

            # Purpose governance uses a separate SQLite connection in the
            # Runtime.  Re-resolve after all reads so succession or amendment
            # cannot yield a mixed-generation projection.
            final_lineage = self._governance.find_lineage_for_engram(engram_id)
            if final_lineage != lineage:
                raise LivingPortfolioRecoveryError(
                    "subject lineage changed while projecting the portfolio"
                )

            subject = self._subject_view(engram_id, lineage)
            state_counts = {state: 0 for state in _PORTFOLIO_STATES}
            for item in items:
                state_counts[item["portfolio_state"]] += 1

            return {
                "schema_version": SCHEMA_VERSION,
                "subject": subject,
                "purpose": purpose,
                "items": items,
                "item_count": len(items),
                "state_counts": state_counts,
            }
        except LivingPortfolioRecoveryError:
            raise
        except Exception as exc:
            raise LivingPortfolioRecoveryError(
                "living portfolio source state is unavailable or inconsistent"
            ) from exc

    def _validate_lineage(
        self,
        lineage: SubjectLineage | None,
        requested_engram_id: str,
    ) -> None:
        if lineage is None:
            return
        if lineage.world_id != self._world_id:
            raise LivingPortfolioRecoveryError(
                "subject lineage belongs to a different PulseWorld"
            )
        if lineage.current_engram_id != requested_engram_id:
            raise LivingPortfolioRecoveryError(
                "requested Engram is not the current subject holder"
            )
        if lineage.state is not LineageState.ACTIVE:
            raise LivingPortfolioRecoveryError(
                "current subject lineage is not active"
            )
        if type(lineage.generation) is not int or lineage.generation < 0:
            raise LivingPortfolioRecoveryError(
                "subject lineage generation is invalid"
            )

    @staticmethod
    def _subject_view(
        requested_engram_id: str,
        lineage: SubjectLineage | None,
    ) -> dict[str, Any]:
        if lineage is None:
            return {
                "requested_engram_id": requested_engram_id,
                "lineage_state": "unestablished",
                "lineage_id": None,
                "root_engram_id": requested_engram_id,
                "current_engram_id": requested_engram_id,
                "generation": 0,
            }
        return {
            "requested_engram_id": requested_engram_id,
            "lineage_state": "active",
            "lineage_id": lineage.lineage_id,
            "root_engram_id": lineage.root_engram_id,
            "current_engram_id": lineage.current_engram_id,
            "generation": lineage.generation,
        }

    def _purpose_view(
        self,
        lineage: SubjectLineage | None,
        history_limit: int,
    ) -> dict[str, Any]:
        if lineage is None:
            return {
                "current": None,
                "history": [],
                "history_truncated": False,
            }

        current = self._governance.current_revision(lineage.lineage_id)
        revisions = self._governance.list_revisions(
            lineage.lineage_id,
            limit=history_limit + 1,
        )
        self._validate_purpose(lineage, current, revisions, history_limit)
        visible = revisions[:history_limit]
        return {
            "current": None if current is None else self._purpose_revision_view(current),
            "history": [self._purpose_revision_view(item) for item in visible],
            "history_truncated": len(revisions) > history_limit,
        }

    @staticmethod
    def _validate_purpose(
        lineage: SubjectLineage,
        current: PurposeRevision | None,
        revisions: list[PurposeRevision],
        history_limit: int,
    ) -> None:
        if len(revisions) > history_limit + 1:
            raise LivingPortfolioRecoveryError(
                "purpose history source ignored the bounded read"
            )
        if lineage.current_purpose_revision_id is None:
            if current is not None:
                raise LivingPortfolioRecoveryError(
                    "lineage has no current pointer but returned a current purpose"
                )
        elif (
            current is None
            or current.purpose_revision_id
            != lineage.current_purpose_revision_id
        ):
            raise LivingPortfolioRecoveryError(
                "lineage current purpose pointer is inconsistent"
            )

        if current is not None and current.lineage_id != lineage.lineage_id:
            raise LivingPortfolioRecoveryError(
                "current purpose belongs to a different lineage"
            )
        for expected_revision, revision in enumerate(revisions, start=1):
            if (
                revision.lineage_id != lineage.lineage_id
                or revision.revision != expected_revision
            ):
                raise LivingPortfolioRecoveryError(
                    "purpose history is not a contiguous lineage prefix"
                )

    def _portfolio_items(self, holder_id: str) -> list[dict[str, Any]]:
        memberships = self._world.list_memberships(engram_id=holder_id)
        rows: list[tuple[ActivityCenter, CenterMembership, dict[str, Any]]] = []
        seen_centers: set[str] = set()

        for membership in memberships:
            if membership.engram_id != holder_id:
                raise LivingPortfolioRecoveryError(
                    "membership owner does not match the current subject holder"
                )
            if membership.center_id in seen_centers:
                raise LivingPortfolioRecoveryError(
                    "subject has duplicate memberships for one ActivityCenter"
                )
            seen_centers.add(membership.center_id)

            center = self._world.get_activity_center(membership.center_id)
            if center is None:
                raise LivingPortfolioRecoveryError(
                    "membership points to a missing ActivityCenter"
                )
            if center.kind is ActivityKind.TASK:
                continue
            try:
                portfolio_state = _PORTFOLIO_STATE_BY_CENTER_STATUS[center.status]
                relation = membership.relation.value
            except (AttributeError, KeyError) as exc:
                raise LivingPortfolioRecoveryError(
                    "ActivityCenter membership has an unsupported state"
                ) from exc
            rows.append(
                (
                    center,
                    membership,
                    {
                        "center": self._activity_center_view(center),
                        "relation": relation,
                        "portfolio_state": portfolio_state,
                    },
                )
            )

        # Python's sort is stable.  Applying the tertiary, secondary, then
        # primary keys preserves id ascending within updated_at descending
        # within the frozen state order without timestamp arithmetic.
        rows.sort(key=lambda row: row[0].id)
        rows.sort(key=lambda row: row[0].updated_at, reverse=True)
        rows.sort(key=lambda row: _PORTFOLIO_STATE_RANK[row[2]["portfolio_state"]])
        return [row[2] for row in rows]

    @staticmethod
    def _activity_center_view(center: ActivityCenter) -> dict[str, Any]:
        return {
            "id": center.id,
            "kind": center.kind.value,
            "title": center.title,
            "description": center.description,
            "status": center.status.value,
            "origin": center.origin.value,
            "autonomy": center.autonomy,
            "project_id": center.project_id,
            "focal_engram_id": center.focal_engram_id,
            "created_at": center.created_at.isoformat(),
            "updated_at": center.updated_at.isoformat(),
            "last_active_at": (
                None
                if center.last_active_at is None
                else center.last_active_at.isoformat()
            ),
        }

    @staticmethod
    def _purpose_revision_view(revision: PurposeRevision) -> dict[str, Any]:
        # Keep the existing subject-facing PurposeRevision wire shape.  The
        # domain evidence label is intentionally not another Portfolio field.
        return {
            "purpose_revision_id": revision.purpose_revision_id,
            "lineage_id": revision.lineage_id,
            "author_engram_id": revision.author_engram_id,
            "revision": revision.revision,
            "predecessor_revision_id": revision.predecessor_revision_id,
            "amendment_kind": revision.amendment_kind.value,
            "content": revision.content,
            "content_digest": revision.content_digest,
            "state": revision.state.value,
            "source_event_id": revision.source_event_id,
            "reflection_event_id": revision.reflection_event_id,
            "created_at": revision.created_at.isoformat(),
            "superseded_at": (
                None
                if revision.superseded_at is None
                else revision.superseded_at.isoformat()
            ),
        }


__all__ = [
    "LivingPortfolioProjector",
    "LivingPortfolioRecoveryError",
    "LivingPortfolioValidationError",
]
