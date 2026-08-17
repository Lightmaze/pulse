"""Project cluster management.

A Project groups related engrams and provides workspace isolation
and intra-cluster connection boosting.
"""

from __future__ import annotations

import os
from pathlib import Path

from pulse_system.core.types import Engram, EngramStatus, Project
from pulse_system.substrate.storage.store import Storage


class ProjectManager:
    """Manages project clusters and their engram membership."""

    def __init__(self, storage: Storage):
        self._storage = storage

    def create(
        self,
        name: str,
        description: str = "",
        workspace_path: str | None = None,
    ) -> Project:
        if workspace_path is None:
            workspace_path = os.path.join(os.getcwd(), ".pulse_projects", name)
        os.makedirs(workspace_path, exist_ok=True)

        return self._storage.create_project(
            name=name,
            description=description,
            workspace_path=workspace_path,
        )

    def add_engram(self, project_id: str, engram_id: str) -> None:
        project = self._storage.get_project(project_id)
        if project is None:
            raise ValueError(f"Project {project_id} not found")
        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ValueError(f"Engram {engram_id} not found")
        self._storage.update_engram_project(engram_id, project_id)

    def remove_engram(self, project_id: str, engram_id: str) -> None:
        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ValueError(f"Engram {engram_id} not found")
        if engram.project_id != project_id:
            raise ValueError(f"Engram {engram_id} is not in project {project_id}")
        self._storage.update_engram_project(engram_id, None)

    def get_engrams(self, project_id: str) -> list[Engram]:
        return self._storage.list_engrams(project_id=project_id)

    def get_project(self, project_id: str) -> Project | None:
        return self._storage.get_project(project_id)

    def list_projects(self) -> list[Project]:
        return self._storage.list_projects()

    def get_workspace(self, project_id: str) -> str | None:
        project = self._storage.get_project(project_id)
        if project is None:
            return None
        return project.workspace_path

    def boost_intra_connections(self, project_id: str, boost_factor: float) -> int:
        """Multiply connection weights between all engram pairs within a project."""
        engrams = self._storage.list_engrams(project_id=project_id)
        ids = {e.id for e in engrams}
        if len(ids) < 2:
            return 0

        boosted = 0
        for eid in ids:
            outgoing = self._storage.get_outgoing(eid)
            for conn in outgoing:
                if conn.to_id in ids:
                    new_weight = min(1.0, conn.weight * boost_factor)
                    self._storage.update_weight(
                        conn.from_id,
                        conn.to_id,
                        new_weight,
                        layer="factory",
                    )
                    boosted += 1
        return boosted
