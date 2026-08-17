"""Payload-free, zero-stimulus projection of durable role accountability."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pulse_system.agent.harness.role_leases import (
    HolderKind,
    RoleAccountabilityObservation,
    RoleAccountabilitySnapshot,
    RoleLeaseStatus,
    RoleLeaseStore,
)


SCHEMA_VERSION = "role-accountability.v1"
OBSERVER_EFFECT = "READ_ONLY_NO_STIMULUS"
DEFAULT_ROLE_LIMIT = 32
MIN_ROLE_LIMIT = 1
MAX_ROLE_LIMIT = 64


class RoleAccountabilityValidationError(ValueError):
    """A caller asked for a projection outside the frozen v1 bounds."""


class RoleAccountabilityRecoveryError(RuntimeError):
    """Durable role state could not form one trustworthy projection."""


class RoleAccountabilityProjector:
    """Build ``role-accountability.v1`` without mutating any life domain."""

    def __init__(self, store: RoleLeaseStore, world_id: str) -> None:
        self._store = store
        self._world_id = world_id

    def project(
        self,
        engram_id: str,
        *,
        limit: int = DEFAULT_ROLE_LIMIT,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if (
            type(limit) is not int
            or not MIN_ROLE_LIMIT <= limit <= MAX_ROLE_LIMIT
        ):
            raise RoleAccountabilityValidationError(
                f"limit must be an integer in [{MIN_ROLE_LIMIT}, {MAX_ROLE_LIMIT}]"
            )
        try:
            snapshot = self._store.observe_holder(
                world_id=self._world_id,
                holder_kind=HolderKind.ENGRAM,
                holder_id=engram_id,
                limit=limit + 1,
                now=now,
            )
            self._validate_snapshot(snapshot, engram_id)
            visible = snapshot.roles[:limit]
            return {
                "schema_version": SCHEMA_VERSION,
                "world_id": self._world_id,
                "engram_id": engram_id,
                "projected_at": snapshot.observed_at.isoformat(),
                "observer_effect": OBSERVER_EFFECT,
                "payload_disclosed": False,
                "roles": [
                    self._role_view(item, snapshot.observed_at)
                    for item in visible
                ],
                "role_count": len(visible),
                "roles_truncated": len(snapshot.roles) > limit,
            }
        except RoleAccountabilityValidationError:
            raise
        except RoleAccountabilityRecoveryError:
            raise
        except Exception as exc:
            raise RoleAccountabilityRecoveryError(
                "role accountability source state is unavailable or inconsistent"
            ) from exc

    def _validate_snapshot(
        self,
        snapshot: RoleAccountabilitySnapshot,
        engram_id: str,
    ) -> None:
        if (
            snapshot.world_id != self._world_id
            or snapshot.holder_kind is not HolderKind.ENGRAM
            or snapshot.holder_id != engram_id
        ):
            raise RoleAccountabilityRecoveryError(
                "role accountability snapshot belongs to another holder"
            )
        seen: set[str] = set()
        for item in snapshot.roles:
            role = item.role
            summary = item.contribution_summary
            if role.role_lease_id in seen:
                raise RoleAccountabilityRecoveryError(
                    "role accountability snapshot contains a duplicate role"
                )
            seen.add(role.role_lease_id)
            if (
                role.world_id != self._world_id
                or role.holder_kind is not HolderKind.ENGRAM
                or role.holder_id != engram_id
                or summary.role_lease_id != role.role_lease_id
                or summary.role_epoch != role.role_epoch
                or summary.accountability_cycle_id
                != role.accountability_cycle_id
            ):
                raise RoleAccountabilityRecoveryError(
                    "role accountability snapshot has crossed identity or cycle bounds"
                )
            if (
                not role.valid_from < role.renew_after < role.expires_at
                or snapshot.observed_at < role.valid_from
            ):
                raise RoleAccountabilityRecoveryError(
                    "role accountability snapshot has an invalid lifetime"
                )
            if (
                item.effective_status
                in {
                    RoleLeaseStatus.REQUESTED,
                    RoleLeaseStatus.ACTIVE,
                    RoleLeaseStatus.SUSPENDED,
                }
                and role.expires_at <= snapshot.observed_at
            ):
                raise RoleAccountabilityRecoveryError(
                    "role accountability snapshot did not apply effective expiry"
                )
            if (role.obligation is None) != (
                role.accountability_cycle_id is None
            ):
                raise RoleAccountabilityRecoveryError(
                    "role obligation and accountability cycle disagree"
                )
            if (
                summary.direct_output_count < 0
                or summary.coordination_count < 0
                or summary.consecutive_coordination < 0
                or summary.consecutive_coordination
                > summary.coordination_count
            ):
                raise RoleAccountabilityRecoveryError(
                    "role contribution counts are inconsistent"
                )
            evidence = set(item.contribution_evidence_classes)
            has_direct_evidence = any(
                value.value != "CONTROL_ONLY" for value in evidence
            )
            has_coordination_evidence = any(
                value.value == "CONTROL_ONLY" for value in evidence
            )
            if (
                (summary.direct_output_count > 0) != has_direct_evidence
                or (summary.coordination_count > 0)
                != has_coordination_evidence
            ):
                raise RoleAccountabilityRecoveryError(
                    "role contribution evidence and counts disagree"
                )

    @staticmethod
    def _renewal_view(
        item: RoleAccountabilityObservation,
        observed_at: datetime,
    ) -> dict[str, Any]:
        role = item.role
        summary = item.contribution_summary
        contribution_gate = summary.renewal_eligible
        if observed_at >= role.expires_at:
            eligible_now = False
            reason = "role_expired"
        elif item.effective_status is not RoleLeaseStatus.ACTIVE:
            eligible_now = False
            reason = "role_not_active"
        elif observed_at < role.renew_after:
            eligible_now = False
            reason = "role_renewal_window_not_open"
        elif not contribution_gate:
            eligible_now = False
            reason = summary.reason_code
        else:
            eligible_now = True
            reason = "role_renewal_window_and_contribution_gate_satisfied"
        return {
            "contribution_gate_satisfied": contribution_gate,
            "eligible_now": eligible_now,
            "reason_code": reason,
            "authorization_still_required": True,
        }

    @classmethod
    def _role_view(
        cls,
        item: RoleAccountabilityObservation,
        observed_at: datetime,
    ) -> dict[str, Any]:
        role = item.role
        summary = item.contribution_summary
        return {
            "role_lease_id": role.role_lease_id,
            "role_epoch": role.role_epoch,
            "role_class": role.role_class.value,
            "role_label": role.role_label,
            "status": item.effective_status.value,
            "lineage_id": role.lineage_id,
            "scope": role.scope.to_dict(),
            "obligation": (
                None if role.obligation is None else role.obligation.to_dict()
            ),
            "accountability_cycle_id": role.accountability_cycle_id,
            "valid_from": role.valid_from.isoformat(),
            "renew_after": role.renew_after.isoformat(),
            "expires_at": role.expires_at.isoformat(),
            "renewal_count": role.renewal_count,
            "predecessor_lease_id": role.predecessor_lease_id,
            "contribution_summary": summary.to_dict(),
            "renewal_gate": cls._renewal_view(item, observed_at),
            "evidence": {
                "role": role.evidence_class.value,
                "contributions": [
                    value.value for value in item.contribution_evidence_classes
                ],
                "payload_disclosed": False,
            },
        }


__all__ = [
    "DEFAULT_ROLE_LIMIT",
    "MAX_ROLE_LIMIT",
    "MIN_ROLE_LIMIT",
    "OBSERVER_EFFECT",
    "RoleAccountabilityProjector",
    "RoleAccountabilityRecoveryError",
    "RoleAccountabilityValidationError",
    "SCHEMA_VERSION",
]
