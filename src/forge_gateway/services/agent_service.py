"""Agent service boundary.

The public session methods are currently preserved on ``GatewayRuntime`` for
compatibility. This module names the service boundary for follow-up extraction
without changing the API surface.
"""

from __future__ import annotations

from typing import Any


class AgentService:
    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def create_session(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        return self._runtime.create_agent_session(body)

    def get_session(self, session_id: str) -> tuple[int, dict[str, Any]]:
        return self._runtime.get_agent_session(session_id)

    def cancel_session(self, session_id: str) -> tuple[int, dict[str, Any]]:
        return self._runtime.cancel_agent_session(session_id)
