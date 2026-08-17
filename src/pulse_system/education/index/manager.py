"""Index management.

An Index is a special engram that holds a project's goals, structure,
and progress as free-form natural text in its session history.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pulse_system.core.types import Engram, Message, MessageRole

if TYPE_CHECKING:
    from pulse_system.core.engram.manager import EngramManager
    from pulse_system.substrate.storage.store import Storage


class IndexManager:
    """Convenience layer for creating and maintaining Index engrams."""

    def __init__(self, engram_manager: EngramManager, storage: Storage):
        self._mgr = engram_manager
        self._storage = storage

    def create_index(
        self,
        project_id: str,
        title: str,
        structure: str,
        commitment: str,
    ) -> str:
        """Create an Index engram and attach it to a project.

        The initial session contains the title, structure, and commitment
        as natural-language conversation content — not structured fields.
        """
        initial_content = (
            f"This is the Index for project \"{title}\".\n\n"
            f"Structure:\n{structure}\n\n"
            f"Commitment:\n{commitment}"
        )

        engram = self._mgr.create(
            project_id=project_id,
            initial_messages=[
                Message(role=MessageRole.USER, content=initial_content),
            ],
        )

        self._storage.update_project(project_id, index_engram_id=engram.id)

        return engram.id

    def update_progress(
        self,
        index_id: str,
        node_description: str,
        status: str,
    ) -> None:
        """Append a progress record to the Index engram's session."""
        progress_text = f"Progress update — {node_description}: {status}"
        self._mgr.append_injection(
            index_id,
            progress_text,
            source_id="index_progress",
        )

    def reaffirm(self, index_id: str) -> str:
        """Pulse the Index engram to restate goals in light of current progress."""
        return self._mgr.pulse(index_id).content

    def get_index(self, project_id: str) -> Engram | None:
        """Get the Index engram for a project."""
        project = self._storage.get_project(project_id)
        if project is None or project.index_engram_id is None:
            return None
        return self._mgr.get(project.index_engram_id)
