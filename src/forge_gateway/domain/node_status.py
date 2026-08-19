"""Runtime node health model."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


NodeHealth = Literal["unknown", "ready", "stale", "error"]


@dataclass
class NodeStatus:
    node_id: str
    health: NodeHealth
    updated_at: float
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "health": self.health,
            "updated_at": self.updated_at,
            "details": dict(self.details),
        }
