"""Runtime state and orchestration service."""

from __future__ import annotations

import queue
import threading
import time
import uuid
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
from forge_gateway.domain.commands import Command, CommandState, map_policy_status
from forge_gateway.domain.node_status import NodeStatus
from forge_gateway.domain.sessions import SessionState, session_status_from_command
from forge_gateway.services.action_registry import ActionRegistry
from forge_gateway.services.capability_service import CapabilityService
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
        self.command_queue: queue.Queue[Command] = queue.Queue()
        self.record_root: Path | None = None
        self.last_error: str | None = None
        self.dispatch_blocked_reason: str | None = None
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
        self.sessions: dict[str, SessionState] = {}
        self.commands: dict[str, CommandState] = {}
        self.nodes: dict[str, NodeStatus] = {}
        self.active_session_id: str | None = None
        self.last_result: dict[str, Any] | None = None
        self.state_store = StateFileStore(
            config.agent.state_dir,
            write_context_snapshot=config.agent.write_context_snapshot,
        )
        self.state_dir: Path | None = self.state_store.state_dir
        self.event_log_path: Path | None = self.state_store.event_log_path
        self.snapshot_path: Path | None = self.state_store.snapshot_path
        self.image_encoder = ImageEncodeWorker(self)

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
    ) -> None:
        self.command_queue.put(
            Command(
                kind="POLICY_COMMAND",
                payload={
                    "command": command,
                    "inputs": dict(inputs or {}),
                    "request_id": request_id,
                    "policy_id": policy_id or self.config.policy_id,
                },
                tracked_command_id=tracked_command_id,
                retry_on_failure=retry_on_failure,
                attempt=attempt,
            )
        )

    def set_record_root(self, root: str | None) -> None:
        self.command_queue.put(Command(kind="SET_ROOT", payload={"root": root}))

    def command_dispatch_allowed(self) -> bool:
        with self.lock:
            return self.dispatch_blocked_reason is None

    def block_command_dispatch(self, error: Exception) -> None:
        now = time.time()
        reason = f"safety command dispatch exhausted retries: {error}"
        with self.lock:
            if self.dispatch_blocked_reason is not None:
                return
            self.dispatch_blocked_reason = reason
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
        self.enqueue_policy_command("reset_scene", inputs)
        return 202, {"ok": True, "data": {"command": "reset_scene", "inputs": inputs}}

    def create_agent_session(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if not self.config.agent.enabled:
            return 404, {"ok": False, "msg": "agent API is disabled"}

        now = time.time()
        action_type = body.get("action_type") or body.get("action") or body.get("type") or body.get("command")
        if not isinstance(action_type, str) or not action_type:
            return 400, {"ok": False, "msg": "action_type must be non-empty string"}

        inputs_raw = body.get("inputs", body.get("parameters", {}))
        if inputs_raw is None:
            inputs_raw = {}
        if not isinstance(inputs_raw, dict):
            return 400, {"ok": False, "msg": "inputs must be an object"}

        source_inputs = dict(inputs_raw)
        for key in ("target", "target_name", "instruction"):
            if key in body and key not in source_inputs:
                source_inputs[key] = body[key]

        action_cfg = self.action_registry.get(action_type)
        if action_cfg is None:
            return 400, {
                "ok": False,
                "msg": f"unknown action_type: {action_type}",
                "data": {"supported_actions": self.action_registry.supported_action_names()},
            }
        command = action_cfg.command
        policy_id = action_cfg.policy_id
        policy_inputs = dict(source_inputs)
        for src_key, dst_key in action_cfg.input_mapping.items():
            if src_key in source_inputs and dst_key not in policy_inputs:
                policy_inputs[dst_key] = source_inputs[src_key]
        missing = [
            key
            for key in action_cfg.required_parameters
            if key not in policy_inputs or policy_inputs[key] in (None, "")
        ]
        if missing:
            return 400, {"ok": False, "msg": f"missing required inputs: {', '.join(missing)}"}

        session_id = str(body.get("session_id") or f"session_{uuid.uuid4().hex[:12]}")
        command_id = str(body.get("command_id") or f"command_{uuid.uuid4().hex[:12]}")
        instruction = str(body.get("instruction") or source_inputs.get("instruction") or "")
        source = str(body.get("source") or "paos-agent")
        target = policy_inputs.get("target_name") or policy_inputs.get("target")
        target_value = str(target) if target is not None else None
        policy_inputs.update(
            {
                "session_id": session_id,
                "command_id": command_id,
                "action_type": action_type,
                "source": source,
            }
        )
        policy_inputs["policy_id"] = policy_id
        if instruction:
            policy_inputs["instruction"] = instruction

        with self.lock:
            if session_id in self.sessions:
                return 409, {"ok": False, "msg": f"session already exists: {session_id}"}
            if command_id in self.commands:
                return 409, {"ok": False, "msg": f"command already exists: {command_id}"}
            active_count = sum(
                1 for session in self.sessions.values() if session.status in ("queued", "running")
            )
            if active_count >= self.config.agent.max_active_sessions:
                return 409, {"ok": False, "msg": "another agent session is active", "data": {"active_session_id": self.active_session_id}}

            session = SessionState(
                session_id=session_id,
                status="queued",
                action_type=action_type,
                instruction=instruction,
                source=source,
                target=target_value,
                command_ids=[command_id],
                created_at=now,
                updated_at=now,
            )
            command_state = CommandState(
                command_id=command_id,
                session_id=session_id,
                policy_id=policy_id,
                command=command,
                action_type=action_type,
                inputs=dict(policy_inputs),
                status="queued",
                request_id=command_id,
                created_at=now,
                updated_at=now,
            )
            self.sessions[session_id] = session
            self.commands[command_id] = command_state
            self.active_session_id = session_id
            self.enqueue_policy_command(
                command,
                policy_inputs,
                request_id=command_id,
                policy_id=policy_id,
                tracked_command_id=command_id,
            )
            self._append_event_locked(
                "session_created",
                {"session": session.to_dict(), "command": command_state.to_dict()},
                now=now,
            )
            self._write_snapshot_locked(now)

        return 202, {
            "ok": True,
            "data": {
                "session": session.to_dict(),
                "command": command_state.to_dict(),
                "status": session.status,
            },
        }

    def get_agent_session(self, session_id: str) -> tuple[int, dict[str, Any]]:
        with self.lock:
            session = self.sessions.get(session_id)
            if session is None:
                return 404, {"ok": False, "msg": f"session not found: {session_id}"}
            return 200, {
                "ok": True,
                "data": {
                    "session": session.to_dict(),
                    "commands": [
                        self.commands[command_id].to_dict()
                        for command_id in session.command_ids
                        if command_id in self.commands
                    ],
                },
            }

    def cancel_agent_session(self, session_id: str) -> tuple[int, dict[str, Any]]:
        now = time.time()
        cancellation_requested = False
        with self.lock:
            session = self.sessions.get(session_id)
            if session is None:
                return 404, {"ok": False, "msg": f"session not found: {session_id}"}
            for command_id in session.command_ids:
                command = self.commands.get(command_id)
                if command is None or command.status not in ("queued", "sent", "running"):
                    continue
                previous_status = command.status
                cancellation_requested = True
                command.status = "cancelled"
                command.updated_at = now
                command.message = "cancel requested by agent"
                if previous_status in ("sent", "running") or command.dispatching:
                    self.enqueue_policy_command(
                        "stop",
                        {
                            "session_id": session_id,
                            "command_id": f"cancel_{command.command_id}",
                            "cancelled_command_id": command.command_id,
                            "action_type": command.action_type,
                            "source": "paos-agent",
                            "reason": "agent_cancel",
                        },
                        request_id=f"cancel_{command.command_id}",
                        policy_id=command.policy_id,
                        retry_on_failure=True,
                    )
            if cancellation_requested:
                session.status = "cancelled"
                session.updated_at = now
                session.message = "cancel requested by agent"
                if self.active_session_id == session_id:
                    self.active_session_id = None
                self._append_event_locked(
                    "session_cancelled",
                    {"session_id": session_id},
                    now=now,
                )
                self._write_snapshot_locked(now)
            response_status = session.status

        return 200, {
            "ok": True,
            "data": {"session_id": session_id, "status": response_status},
        }

    def claim_command_dispatch(self, command_id: str | None) -> bool:
        if not command_id:
            return True
        with self.lock:
            command = self.commands.get(command_id)
            if command is None or command.status != "queued" or command.dispatching:
                return False
            command.dispatching = True
            return True

    def mark_command_sent(self, command_id: str) -> None:
        if not command_id:
            return
        now = time.time()
        with self.lock:
            command = self.commands.get(command_id)
            if command is None:
                return
            command.dispatching = False
            if command.status != "queued":
                return
            command.status = "sent"
            command.sent_at = now
            command.updated_at = now
            session = self.sessions.get(command.session_id)
            if session is not None and session.status == "queued":
                session.status = "running"
                session.updated_at = now
            self._append_event_locked("command_sent", {"command_id": command_id}, now=now)
            self._write_snapshot_locked(now)

    def mark_command_dispatch_failed(self, command_id: str, error: Exception) -> None:
        now = time.time()
        message = f"policy command dispatch failed: {error}"
        with self.lock:
            self.last_error = message
            command = self.commands.get(command_id)
            if command is None:
                self._append_event_locked(
                    "command_dispatch_failed",
                    {"command_id": command_id, "error": str(error)},
                    now=now,
                )
                self._write_snapshot_locked(now)
                return
            command.dispatching = False
            if command.status in ("succeeded", "failed", "cancelled"):
                return
            command.status = "failed"
            command.updated_at = now
            command.message = message
            session = self.sessions.get(command.session_id)
            if session is not None and session.status not in ("succeeded", "failed", "cancelled"):
                session.status = "failed"
                session.updated_at = now
                session.message = message
                if self.active_session_id == session.session_id:
                    self.active_session_id = None
            self._append_event_locked(
                "command_dispatch_failed",
                {"command_id": command_id, "error": str(error)},
                now=now,
            )
            self._write_snapshot_locked(now)

    def apply_policy_command_status(self, value: object) -> None:
        from forge_msgs import PolicyCommandStatus

        status_msg = PolicyCommandStatus.from_arrow(value)  # type: ignore[arg-type]
        outputs = status_msg.outputs()
        command_id = status_msg.request_id
        now = time.time()
        with self.lock:
            command = self.commands.get(command_id)
            mapped_status = map_policy_status(status_msg.status)
            if command is not None:
                identity_matches = (
                    status_msg.policy_id == command.policy_id
                    and status_msg.command == command.command
                )
                mutable_status = command.status in ("sent", "running")
                if identity_matches and mutable_status:
                    command.status = mapped_status
                    command.updated_at = now
                    command.message = status_msg.message
                    command.outputs = outputs
                    session = self.sessions.get(command.session_id)
                    if session is not None and session.status in ("queued", "running"):
                        session.status = session_status_from_command(mapped_status)
                        session.updated_at = now
                        session.message = status_msg.message
                        if (
                            session.status in ("succeeded", "failed", "cancelled")
                            and self.active_session_id == session.session_id
                        ):
                            self.active_session_id = None
                elif identity_matches and command.status == "cancelled":
                    command.updated_at = now
                    command.outputs = outputs
            self.last_result = {
                "policy_id": status_msg.policy_id,
                "command": status_msg.command,
                "request_id": status_msg.request_id,
                "status": status_msg.status,
                "message": status_msg.message,
                "outputs": outputs,
            }
            self._append_event_locked("policy_command_status", dict(self.last_result), now=now)
            self._write_snapshot_locked(now)

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
            proprio_age is not None
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
        images_ready = all(image_status.values()) if self.config.image_input_ids else True
        if cfg.require_images and not images_ready:
            missing.extend([f"image:{k}" for k, ready in image_status.items() if not ready])

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

    def _append_event_locked(self, event_type: str, data: dict[str, Any], *, now: float) -> None:
        self.state_store.append_event(event_type, data, now=now)

    def _write_snapshot_locked(self, now: float) -> None:
        self.state_store.write_snapshot(self._runtime_context_locked(now))

