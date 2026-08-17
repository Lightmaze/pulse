"""Domain facade for TaskFront and ActivityCenter persistence.

The registry deliberately owns no runtime, clock, HarnessSession, or prompt
state.  It is one view over the process-wide Storage instance, so creating a
front or a life center cannot accidentally create another world.
"""

from __future__ import annotations

from datetime import datetime

from pulse_system.core.types import (
    ActivityCenter,
    ActivityCenterBundle,
    ActivityCenterStatus,
    ActivityKind,
    ActivityOrigin,
    CenterMembership,
    LivingConcern,
    LivingConcernDisposition,
    LivingOrientation,
    LivingOrientationState,
    MembershipRelation,
    TaskFront,
    TaskFrontBundle,
    TaskFrontStatus,
)
from pulse_system.substrate.storage import Storage


class WorldRegistry:
    """Read and mutate the durable fronts and centers of one PulseWorld."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def create_task_bundle(
        self,
        title: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        center_id: str | None = None,
        front_id: str | None = None,
        engram_id: str | None = None,
        focal_engram_id: str | None = None,
    ) -> TaskFrontBundle:
        return self._storage.create_task_bundle(
            title,
            description,
            project_id,
            origin=origin,
            autonomy=autonomy,
            center_id=center_id,
            front_id=front_id,
            engram_id=engram_id,
            focal_engram_id=focal_engram_id,
        )

    def create_task_front(self, *args, **kwargs) -> TaskFrontBundle:
        """Product-level alias: creating a new Front creates its bundle."""
        return self.create_task_bundle(*args, **kwargs)

    def create_task_bundle_for_existing_engram(
        self,
        title: str,
        focal_engram_id: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        center_id: str | None = None,
        front_id: str | None = None,
    ) -> TaskFrontBundle:
        return self._storage.create_task_bundle_for_existing_engram(
            title,
            focal_engram_id,
            description,
            project_id,
            origin=origin,
            autonomy=autonomy,
            center_id=center_id,
            front_id=front_id,
        )

    def create_non_task_center_bundle(
        self,
        kind: ActivityKind | str,
        title: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        center_id: str | None = None,
        engram_id: str | None = None,
        focal_engram_id: str | None = None,
    ) -> ActivityCenterBundle:
        return self._storage.create_non_task_center_bundle(
            kind,
            title,
            description,
            project_id,
            origin=origin,
            autonomy=autonomy,
            center_id=center_id,
            engram_id=engram_id,
            focal_engram_id=focal_engram_id,
        )

    def create_activity_center(self, *args, **kwargs) -> ActivityCenterBundle:
        """Product-level alias for creating a non-task life center."""
        return self.create_non_task_center_bundle(*args, **kwargs)

    def create_center_for_existing_engram(
        self,
        kind: ActivityKind | str,
        title: str,
        focal_engram_id: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.SYSTEM,
        autonomy: float = 1.0,
        center_id: str | None = None,
    ) -> ActivityCenterBundle:
        return self._storage.create_center_for_existing_engram(
            kind,
            title,
            focal_engram_id,
            description,
            project_id,
            origin=origin,
            autonomy=autonomy,
            center_id=center_id,
        )

    def create_task_front_for_center(
        self,
        center_id: str,
        focal_engram_id: str,
        title: str,
        *,
        status: TaskFrontStatus | str = TaskFrontStatus.OPEN,
        front_id: str | None = None,
    ) -> TaskFront:
        return self._storage.create_task_front_for_center(
            center_id,
            focal_engram_id,
            title,
            status=status,
            front_id=front_id,
        )

    def get_activity_center(self, center_id: str) -> ActivityCenter | None:
        return self._storage.get_activity_center(center_id)

    def list_activity_centers(
        self,
        *,
        kind: ActivityKind | str | None = None,
        status: ActivityCenterStatus | str | None = None,
        origin: ActivityOrigin | str | None = None,
        project_id: str | None = None,
        engram_id: str | None = None,
    ) -> list[ActivityCenter]:
        return self._storage.list_activity_centers(
            kind=kind,
            status=status,
            origin=origin,
            project_id=project_id,
            engram_id=engram_id,
        )

    def update_activity_center(
        self,
        center_id: str,
        *,
        title: str | None = None,
        description: str | None = None,
        status: ActivityCenterStatus | str | None = None,
        autonomy: float | None = None,
    ) -> ActivityCenter | None:
        return self._storage.update_activity_center(
            center_id,
            title=title,
            description=description,
            status=status,
            autonomy=autonomy,
        )

    def touch_activity_center(self, center_id: str) -> ActivityCenter | None:
        """Record explicit activity; a dormant Center wakes, paused does not."""
        return self._storage.touch_activity_center(center_id)

    def get_task_front(self, front_id: str) -> TaskFront | None:
        return self._storage.get_task_front(front_id)

    def list_task_fronts(
        self,
        *,
        status: TaskFrontStatus | str | None = None,
        center_id: str | None = None,
        focal_engram_id: str | None = None,
    ) -> list[TaskFront]:
        return self._storage.list_task_fronts(
            status=status,
            center_id=center_id,
            focal_engram_id=focal_engram_id,
        )

    def update_task_front(
        self,
        front_id: str,
        *,
        title: str | None = None,
        status: TaskFrontStatus | str | None = None,
    ) -> TaskFront | None:
        return self._storage.update_task_front(
            front_id,
            title=title,
            status=status,
        )

    def touch_task_front(self, front_id: str) -> TaskFront | None:
        return self._storage.touch_task_front(front_id)

    def add_membership(
        self,
        center_id: str,
        engram_id: str,
        relation: MembershipRelation | str = MembershipRelation.PARTICIPANT,
    ) -> CenterMembership:
        return self._storage.add_center_membership(
            center_id, engram_id, relation
        )

    def list_memberships(
        self,
        *,
        center_id: str | None = None,
        engram_id: str | None = None,
        relation: MembershipRelation | str | None = None,
    ) -> list[CenterMembership]:
        return self._storage.list_center_memberships(
            center_id=center_id,
            engram_id=engram_id,
            relation=relation,
        )

    def update_focal_succession(
        self, old_engram_id: str, new_engram_id: str
    ) -> int:
        return self._storage.update_focal_succession(
            old_engram_id, new_engram_id
        )

    def handle_succession(
        self, old_engram_id: str, new_engram_id: str
    ) -> int:
        """Succession-listener shaped alias."""
        return self.update_focal_succession(old_engram_id, new_engram_id)

    def spontaneous_factor(self, engram_id: str) -> float:
        """Return the life-center sideband multiplier for spontaneous pulses."""
        return self._storage.activity_autonomy_for_engram(engram_id)

    def create_living_concern(
        self,
        center_id: str,
        owner_engram_id: str,
        content: str,
        causal_id: str,
        source_event_id: str,
        *,
        disposition: LivingConcernDisposition | str = (
            LivingConcernDisposition.QUIET
        ),
        revisit_at: datetime | None = None,
        concern_id: str | None = None,
    ) -> LivingConcern:
        return self._storage.create_living_concern(
            center_id,
            owner_engram_id,
            content,
            causal_id,
            source_event_id,
            disposition=disposition,
            revisit_at=revisit_at,
            concern_id=concern_id,
        )

    def get_living_concern(self, concern_id: str) -> LivingConcern | None:
        return self._storage.get_living_concern(concern_id)

    def update_living_concern(
        self,
        concern_id: str,
        *,
        expected_owner_engram_id: str,
        expected_revision: int,
        content: str,
        disposition: LivingConcernDisposition | str,
        revisit_at: datetime | None,
        causal_id: str,
        source_event_id: str,
    ) -> LivingConcern:
        return self._storage.update_living_concern(
            concern_id,
            expected_owner_engram_id=expected_owner_engram_id,
            expected_revision=expected_revision,
            content=content,
            disposition=disposition,
            revisit_at=revisit_at,
            causal_id=causal_id,
            source_event_id=source_event_id,
        )

    def list_living_concerns(
        self,
        *,
        center_id: str | None = None,
        owner_engram_id: str | None = None,
        disposition: LivingConcernDisposition | str | None = None,
    ) -> list[LivingConcern]:
        return self._storage.list_living_concerns(
            center_id=center_id,
            owner_engram_id=owner_engram_id,
            disposition=disposition,
        )

    def list_due_living_concerns(
        self, now: datetime, limit: int
    ) -> list[LivingConcern]:
        return self._storage.list_due_living_concerns(now, limit)

    def mark_living_concern_reentered(
        self,
        concern_id: str,
        expected_revision: int,
        event_id: str,
    ) -> LivingConcern:
        return self._storage.mark_living_concern_reentered(
            concern_id, expected_revision, event_id
        )

    def create_living_orientation(
        self,
        center_id: str,
        owner_engram_id: str,
        content: str,
        causal_id: str,
        source_event_id: str,
        *,
        state: LivingOrientationState | str = LivingOrientationState.OPEN,
        orientation_id: str | None = None,
    ) -> LivingOrientation:
        return self._storage.create_living_orientation(
            center_id,
            owner_engram_id,
            content,
            causal_id,
            source_event_id,
            state=state,
            orientation_id=orientation_id,
        )

    def get_living_orientation(
        self,
        orientation_id: str,
    ) -> LivingOrientation | None:
        return self._storage.get_living_orientation(orientation_id)

    def update_living_orientation(
        self,
        orientation_id: str,
        *,
        expected_owner_engram_id: str,
        expected_revision: int,
        content: str,
        state: LivingOrientationState | str,
        causal_id: str,
        source_event_id: str,
    ) -> LivingOrientation:
        return self._storage.update_living_orientation(
            orientation_id,
            expected_owner_engram_id=expected_owner_engram_id,
            expected_revision=expected_revision,
            content=content,
            state=state,
            causal_id=causal_id,
            source_event_id=source_event_id,
        )

    def list_living_orientations(
        self,
        *,
        center_id: str | None = None,
        owner_engram_id: str | None = None,
        state: LivingOrientationState | str | None = None,
        current_only: bool = False,
    ) -> list[LivingOrientation]:
        return self._storage.list_living_orientations(
            center_id=center_id,
            owner_engram_id=owner_engram_id,
            state=state,
            current_only=current_only,
        )

    def select_living_orientation(
        self,
        engram_id: str,
        now: datetime,
    ) -> LivingOrientation | None:
        return self._storage.select_living_orientation(engram_id, now)

    def mark_living_orientation_engaged(
        self,
        orientation_id: str,
        expected_revision: int,
        expected_engagement_count: int,
        event_id: str,
        next_eligible_at: datetime | None,
    ) -> LivingOrientation:
        return self._storage.mark_living_orientation_engaged(
            orientation_id,
            expected_revision,
            expected_engagement_count,
            event_id,
            next_eligible_at,
        )
