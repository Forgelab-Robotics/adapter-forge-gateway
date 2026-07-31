"""Runtime state and orchestration service."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.adapters.state_files import StateFileStore
from forge_gateway.config import GatewayConfig
from forge_gateway.domain.commands import Command, CommandMailboxUnavailable, CommandState
from forge_gateway.domain.node_status import NodeStatus
from forge_gateway.domain.sessions import SessionState
from forge_gateway.services.action_registry import ActionRegistry
from forge_gateway.services.agent_service import (
    AgentEvent,
    AgentService,
    PolicyCommandStatusUpdate,
    PreparedSession,
)
from forge_gateway.services.capability_service import CapabilityService
from forge_gateway.services.command_service import CommandService
from forge_gateway.services.image_service import ImageEncodeWorker

logger = get_logger(__name__)

class GatewayRuntime:
    """Thread-safe shared runtime state for FastAPI and Dora loops."""

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.action_registry = ActionRegistry.from_actions(config.agent.actions)
        self.capability_service = CapabilityService(
            policy_id=config.policy_id,
            action_manifests=config.agent.action_manifests,
            action_registry=self.action_registry,
        )
        self.lock = threading.Lock()
        self.command_service = CommandService(
            default_policy_id=config.policy_id,
            capacity=config.command_queue_capacity,
        )
        self.agent_service = AgentService(
            config=config.agent,
            action_registry=self.action_registry,
            command_sink=self.command_service,
        )
        # Keep these aliases while callers migrate to the command service boundary.
        self.command_queue = self.command_service.command_queue
        self.safety_command_queue = self.command_service.safety_command_queue
        self.record_root: Path | None = None
        self.last_error: str | None = None
        self.start_time = time.time()
        self.current_frame_count = 0
        self.proprio_state: dict[str, float] = {}
        self.action: dict[str, float] = {}
        self.latest_proprio_time: float | None = None
        self.latest_action_time: float | None = None
        self.images: dict[str, dict[str, Any]] = {}
        self.next_image_seq = 1
        self.sim_status: dict[str, Any] = {"status_name": "UNINITIALIZED", "status_code": 0}
        self.record_status: dict[str, Any] = {
            "record_state": "IDLE",
            "episode_count": 0,
            "current_frame_count": 0,
        }
        self.playback_status: dict[str, Any] = {
            "playback_state": "IDLE",
            "current_time_ns": 0,
            "total_messages": 0,
            "mcap_path": None,
            "error": None,
        }
        self.state_ws_clients = 0
        self.image_ws_clients = 0
        self.nodes: dict[str, NodeStatus] = {}
        self.state_store = StateFileStore(
            config.agent.state_dir,
            write_context_snapshot=config.agent.write_context_snapshot,
        )
        self.state_dir: Path | None = self.state_store.state_dir
        self.event_log_path: Path | None = self.state_store.event_log_path
        self.snapshot_path: Path | None = self.state_store.snapshot_path
        self.image_encoder = ImageEncodeWorker(self)

    @property
    def dispatch_blocked_reason(self) -> str | None:
        return self.command_service.dispatch_blocked_reason

    @property
    def sessions(self) -> dict[str, SessionState]:
        return self.agent_service.sessions

    @sessions.setter
    def sessions(self, value: dict[str, SessionState]) -> None:
        self.agent_service.sessions = value

    @property
    def commands(self) -> dict[str, CommandState]:
        return self.agent_service.commands

    @commands.setter
    def commands(self, value: dict[str, CommandState]) -> None:
        self.agent_service.commands = value

    @property
    def active_session_id(self) -> str | None:
        return self.agent_service.active_session_id

    @active_session_id.setter
    def active_session_id(self, value: str | None) -> None:
        self.agent_service.active_session_id = value

    @property
    def last_result(self) -> dict[str, Any] | None:
        return self.agent_service.last_result

    @last_result.setter
    def last_result(self, value: dict[str, Any] | None) -> None:
        self.agent_service.last_result = value



    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any] | None = None,
        *,
        request_id: str = "",
        policy_id: str | None = None,
        tracked_command_id: str | None = None,
        retry_on_failure: bool = False,
        attempt: int = 0,
        safety: bool = False,
    ) -> None:
        self.command_service.enqueue_policy_command(
            command,
            inputs,
            request_id=request_id,
            policy_id=policy_id,
            tracked_command_id=tracked_command_id,
            retry_on_failure=retry_on_failure,
            attempt=attempt,
            safety=safety,
        )

    def set_record_root(self, root: str | None) -> None:
        self.command_service.set_record_root(root)

    def take_next_command(self) -> Command | None:
        return self.command_service.take_next_command()

    def command_dispatch_allowed(self) -> bool:
        return self.command_service.command_dispatch_allowed()

    def block_command_dispatch(self, error: Exception) -> None:
        now = time.time()
        reason = f"safety command dispatch exhausted retries: {error}"
        with self.lock:
            if not self.command_service.block_command_dispatch(reason):
                return
            self.last_error = reason
            self._append_event_locked(
                "command_dispatch_blocked",
                {"error": str(error)},
                now=now,
            )
            self._write_snapshot_locked(now)

    def state_snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "running_time": round(time.time() - self.start_time, 3),
                "current_frame_count": self.current_frame_count,
                "sensors": {
                    "joints": dict(self.proprio_state),
                    "command": dict(self.action),
                },
                "runtime": {
                    "sim_status": dict(self.sim_status),
                    "record_status": dict(self.record_status),
                    "playback_status": dict(self.playback_status),
                    "readiness": self._readiness_locked(time.time()),
                    "last_error": self.last_error,
                },
            }

    def agent_capabilities(self) -> dict[str, Any]:
        return self.capability_service.payload()

    def agent_runtime_status(self) -> dict[str, Any]:
        with self.lock:
            return {
                "readiness": self._readiness_locked(time.time()),
                "active_session_id": self.active_session_id,
                "sessions": {sid: session.status for sid, session in self.sessions.items()},
                "commands": {cid: command.status for cid, command in self.commands.items()},
                "nodes": {node_id: node.to_dict() for node_id, node in self.nodes.items()},
                "last_result": dict(self.last_result or {}),
                "last_error": self.last_error,
            }

    def agent_runtime_context(self) -> dict[str, Any]:
        with self.lock:
            return self._runtime_context_locked(time.time())

    def agent_runtime_reset(self, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
        body = body or {}
        inputs = body.get("inputs") or {}
        if not isinstance(inputs, dict):
            return 400, {"ok": False, "msg": "inputs must be an object"}
        # Agent-facing reset reuses the legacy runtime reset_scene command so
        # PAOS can stay on /agent/* while existing sim dataflows keep working.
        try:
            self.enqueue_policy_command("reset_scene", inputs)
        except CommandMailboxUnavailable as error:
            return 503, {"ok": False, "msg": str(error)}
        return 202, {"ok": True, "data": {"command": "reset_scene", "inputs": inputs}}

    def create_agent_session(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        prepared = self.agent_service.prepare_create(body, now=time.time())
        if not isinstance(prepared, PreparedSession):
            return prepared
        with self.lock:
            result, event = self.agent_service.create_session_locked(prepared)
            if event is not None:
                self._persist_agent_event_locked(event)
        return result

    def get_agent_session(self, session_id: str) -> tuple[int, dict[str, Any]]:
        with self.lock:
            return self.agent_service.get_session_locked(session_id)

    def cancel_agent_session(self, session_id: str) -> tuple[int, dict[str, Any]]:
        now = time.time()
        with self.lock:
            result, event = self.agent_service.cancel_session_locked(
                session_id,
                now=now,
            )
            if event is not None:
                self._persist_agent_event_locked(event)
        return result

    def claim_command_dispatch(self, command_id: str | None) -> bool:
        if not command_id:
            return True
        with self.lock:
            return self.agent_service.claim_command_dispatch_locked(command_id)

    def mark_command_sent(self, command_id: str) -> None:
        if not command_id:
            return
        now = time.time()
        with self.lock:
            event = self.agent_service.mark_command_sent_locked(
                command_id,
                now=now,
            )
            if event is not None:
                self._persist_agent_event_locked(event)

    def mark_command_dispatch_failed(self, command_id: str, error: Exception) -> None:
        now = time.time()
        message = f"policy command dispatch failed: {error}"
        with self.lock:
            self.last_error = message
            event = self.agent_service.mark_command_dispatch_failed_locked(
                command_id,
                message=message,
                error_text=str(error),
                now=now,
            )
            if event is not None:
                self._persist_agent_event_locked(event)

    def apply_policy_command_status(self, value: object) -> None:
        from forge_msgs import PolicyCommandStatus

        status_msg = PolicyCommandStatus.from_arrow(value)  # type: ignore[arg-type]
        update = PolicyCommandStatusUpdate(
            policy_id=status_msg.policy_id,
            command=status_msg.command,
            request_id=status_msg.request_id,
            status=status_msg.status,
            message=status_msg.message,
            outputs=dict(status_msg.outputs()),
        )
        now = time.time()
        with self.lock:
            event = self.agent_service.apply_policy_command_status_locked(
                update,
                now=now,
            )
            self._persist_agent_event_locked(event)

    def latest_image_updates_since(self, cursors: dict[str, int]) -> list[dict[str, Any]]:
        """Return only the newest image payload per stream since each client cursor."""
        with self.lock:
            updates: list[dict[str, Any]] = []
            for image_id in self.config.image_input_ids:
                payload = self.images.get(image_id)
                if not payload:
                    continue
                seq = int(payload["seq"])
                if seq <= cursors.get(image_id, 0):
                    continue
                cursors[image_id] = seq
                updates.append(dict(payload))
            return updates

    def readiness(self) -> dict[str, Any]:
        with self.lock:
            return self._readiness_locked(time.time())

    def close(self) -> None:
        self.image_encoder.close()
        with self.lock:
            self._write_snapshot_locked(time.time())

    def _readiness_locked(self, now: float) -> dict[str, Any]:
        cfg = self.config.readiness
        missing: list[str] = []

        proprio_age = (
            None
            if self.latest_proprio_time is None
            else now - self.latest_proprio_time
        )
        proprio_ready = (
            bool(self.proprio_state)
            and proprio_age is not None
            and (
                cfg.proprio_stale_after_sec is None
                or 0.0 <= proprio_age <= cfg.proprio_stale_after_sec
            )
        )
        if cfg.require_proprio_state and not proprio_ready:
            missing.append("proprio_state")

        image_status: dict[str, bool] = {}
        for image_id in self.config.image_input_ids:
            payload = self.images.get(image_id)
            image_age = (
                None
                if not payload
                else now - float(payload["timestamp"])
            )
            ready = (
                image_age is not None
                and 0.0 <= image_age <= cfg.image_stale_after_sec
            )
            image_status[image_id] = ready
        images_ready = (
            all(image_status.values())
            if self.config.image_input_ids
            else not cfg.require_images
        )
        if cfg.require_images and not self.config.image_input_ids:
            missing.append("image_input_ids")
        elif cfg.require_images and not images_ready:
            missing.extend(
                f"image:{image_id}"
                for image_id, ready in image_status.items()
                if not ready
            )

        state_client_ready = self.state_ws_clients > 0
        if cfg.require_state_client and not state_client_ready:
            missing.append("ws:state")

        image_client_ready = self.image_ws_clients > 0
        if cfg.require_image_client and not image_client_ready:
            missing.append("ws:images")

        return {
            "ready": not missing,
            "missing": missing,
            "proprio_state_ready": proprio_ready,
            "required_images_ready": images_ready,
            "images": image_status,
            "state_clients": self.state_ws_clients,
            "image_clients": self.image_ws_clients,
            "state_client_ready": state_client_ready,
            "image_client_ready": image_client_ready,
        }

    def _runtime_context_locked(self, now: float) -> dict[str, Any]:
        return {
            "updated_at": now,
            "capabilities": self.agent_capabilities(),
            "readiness": self._readiness_locked(now),
            "active_session_id": self.active_session_id,
            "sessions": {sid: session.to_dict() for sid, session in self.sessions.items()},
            "commands": {cid: command.to_dict() for cid, command in self.commands.items()},
            "nodes": {node_id: node.to_dict() for node_id, node in self.nodes.items()},
            "last_result": dict(self.last_result or {}),
            "last_error": self.last_error,
            "runtime": {
                "current_frame_count": self.current_frame_count,
                "sim_status": dict(self.sim_status),
                "record_status": dict(self.record_status),
                "playback_status": dict(self.playback_status),
            },
        }

    def _persist_agent_event_locked(self, event: AgentEvent) -> None:
        self._append_event_locked(event.event_type, event.data, now=event.now)
        self._write_snapshot_locked(event.now)

    def _append_event_locked(self, event_type: str, data: dict[str, Any], *, now: float) -> None:
        self.state_store.append_event(event_type, data, now=now)

    def _write_snapshot_locked(self, now: float) -> None:
        self.state_store.write_snapshot(self._runtime_context_locked(now))

