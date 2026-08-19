"""Dora node adapter."""

from __future__ import annotations

import json
import threading
from dataclasses import replace
import time
from collections import deque
from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any, Never

if TYPE_CHECKING:
    from forge_gateway.services.runtime_service import GatewayRuntime

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.domain.commands import Command
from forge_gateway.domain.node_status import NodeStatus

logger = get_logger(__name__)

_DEFAULT_FIFO_CAPACITY = 256


class DoraEventBufferOverflow(BufferError):
    """A lossless Dora input FIFO reached its configured finite capacity."""


class DoraEventBuffer:
    """Coalesce normal inputs while preserving configured inputs in a bounded FIFO."""

    def __init__(
        self,
        *,
        fifo_input_ids: Collection[str] = (),
        fifo_capacity: int = _DEFAULT_FIFO_CAPACITY,
    ) -> None:
        if (
            isinstance(fifo_capacity, bool)
            or not isinstance(fifo_capacity, int)
            or fifo_capacity < 1
        ):
            raise ValueError("fifo_capacity must be a positive integer")
        if any(
            not isinstance(input_id, str) or not input_id for input_id in fifo_input_ids
        ):
            raise ValueError("fifo_input_ids must contain non-empty strings")

        self._condition = threading.Condition()
        self._events: deque[Any] = deque()
        self._input_order: deque[str] = deque()
        self._latest_inputs: dict[str, Any] = {}
        self._fifo_input_ids = frozenset(fifo_input_ids)
        self._fifo_input_count = 0
        self._fifo_capacity = fifo_capacity
        self._pending_ticks = 0

    def put(self, event: Any) -> None:
        with self._condition:
            if isinstance(event, dict) and event.get("type") == "INPUT":
                input_id = str(event.get("id"))
                if input_id == "tick":
                    self._pending_ticks += 1
                elif input_id in self._fifo_input_ids:
                    if self._fifo_input_count >= self._fifo_capacity:
                        raise DoraEventBufferOverflow(
                            "Dora input FIFO capacity "
                            f"{self._fifo_capacity} exceeded by {input_id!r}"
                        )
                    self._events.append(event)
                    self._fifo_input_count += 1
                elif input_id not in self._latest_inputs:
                    self._input_order.append(input_id)
                    self._latest_inputs[input_id] = event
                else:
                    self._latest_inputs[input_id] = event
            else:
                self._events.append(event)
            self._condition.notify()

    def get(self, timeout: float) -> Any | None:
        deadline = time.monotonic() + timeout
        with self._condition:
            while (
                not self._events
                and not self._input_order
                and self._pending_ticks == 0
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(timeout=remaining)

            if self._events:
                return self._pop_priority_event()

            if self._pending_ticks > 0:
                self._pending_ticks -= 1
                return {"type": "INPUT", "id": "tick", "value": None}

            input_id = self._input_order.popleft()
            return self._latest_inputs.pop(input_id)

    def get_priority(self) -> Any | None:
        """Return the next lossless/control event without consuming coalesced inputs."""
        with self._condition:
            if not self._events:
                return None
            return self._pop_priority_event()

    def _pop_priority_event(self) -> Any:
        event = self._events.popleft()
        if (
            isinstance(event, dict)
            and event.get("type") == "INPUT"
            and str(event.get("id")) in self._fifo_input_ids
        ):
            self._fifo_input_count -= 1
        return event

def _joint_values_by_name(msg: Any) -> dict[str, float]:
    for field in ("position", "velocity", "effort"):
        values = getattr(msg, field)
        if values:
            return dict(zip(msg.name, values, strict=True))
    return {name: 0.0 for name in msg.name}


def _ordered(values: dict[str, float], joint_order: list[str]) -> dict[str, float]:
    if not joint_order:
        return values
    return {name: values[name] for name in joint_order if name in values}


def handle_dora_input(runtime: GatewayRuntime, input_id: str, value: object) -> None:
    from forge_msgs import JointCommand, JointState

    now = time.time()
    if input_id == "tick":
        with runtime.lock:
            runtime.current_frame_count += 1
        return

    if input_id == "proprio_state":
        joint_state = JointState.from_arrow(value)  # type: ignore[arg-type]
        if not any((joint_state.position, joint_state.velocity, joint_state.effort)):
            _reject_proprio_state(
                runtime,
                joint_state.name,
                now=now,
                reason="proprio_state has no position, velocity, or effort values",
            )
        joints = _ordered(_joint_values_by_name(joint_state), runtime.config.joint_order)
        if not joints:
            _reject_proprio_state(
                runtime,
                joint_state.name,
                now=now,
                reason="proprio_state does not contain any configured joints",
            )
        with runtime.lock:
            runtime.proprio_state = joints
            runtime.latest_proprio_time = now
            runtime.nodes["proprio_state"] = NodeStatus(
                "proprio_state",
                "ready",
                now,
                {"joint_count": len(joints)},
            )
        return

    if input_id == "action":
        command = JointCommand.from_arrow(value)  # type: ignore[arg-type]
        action = _ordered(_joint_values_by_name(command), runtime.config.joint_order)
        with runtime.lock:
            runtime.action = action
            runtime.latest_action_time = now
            runtime.nodes["action"] = NodeStatus("action", "ready", now, {"joint_count": len(action)})
        return

    if input_id == "runtime_status":
        payload = _json_bytes(value)
        if isinstance(payload, dict):
            with runtime.lock:
                runtime.sim_status.update(payload)
                runtime.nodes["runtime_status"] = NodeStatus("runtime_status", "ready", now, payload)
        return

    if input_id == "record_status":
        payload = _json_bytes(value)
        if isinstance(payload, dict):
            with runtime.lock:
                runtime.record_status.update(payload)
                runtime.nodes["record_status"] = NodeStatus("record_status", "ready", now, payload)
        return

    if input_id == "playback_status":
        payload = _json_bytes(value)
        if isinstance(payload, dict):
            with runtime.lock:
                runtime.playback_status.update(payload)
                runtime.nodes["playback_status"] = NodeStatus("playback_status", "ready", now, payload)
        return

    if input_id == "policy_command_status":
        runtime.apply_policy_command_status(value)
        return

    if input_id in runtime.config.image_input_ids:
        runtime.image_encoder.submit(input_id, value, now)


def _reject_proprio_state(
    runtime: GatewayRuntime,
    received_joints: list[str],
    *,
    now: float,
    reason: str,
) -> Never:
    with runtime.lock:
        runtime.nodes["proprio_state"] = NodeStatus(
            "proprio_state",
            "error",
            now,
            {
                "error": reason,
                "received_joints": list(received_joints),
            },
        )
    raise ValueError(reason)


def _json_bytes(value: object) -> Any:
    try:
        return json.loads(bytes(value).decode("utf-8"))  # type: ignore[arg-type]
    except Exception:
        return None


def handle_command(runtime: GatewayRuntime, node: Any, cmd: Command) -> None:
    tracked_command_id = cmd.tracked_command_id
    if not runtime.claim_command_dispatch(tracked_command_id):
        logger.info(
            "gateway: skip inactive command kind=%s tracked_command_id=%s",
            cmd.kind,
            tracked_command_id,
        )
        return

    try:
        if cmd.kind == "SET_ROOT":
            raw_root = cmd.payload.get("root")
            with runtime.lock:
                runtime.record_root = (
                    Path(raw_root)
                    if isinstance(raw_root, str) and raw_root
                    else None
                )
            return

        try:
            command = str(cmd.payload["command"])
            inputs = dict(cmd.payload.get("inputs") or {})
            policy_id = str(cmd.payload.get("policy_id") or runtime.config.policy_id)
            if command == "start_recording" and not inputs.get("output_path"):
                with runtime.lock:
                    root = runtime.record_root
                inputs["output_path"] = (
                    str(root / "recording.mcap")
                    if root is not None
                    else "recording.mcap"
                )

            from forge_msgs import PolicyCommand

            request_id = str(cmd.payload.get("request_id") or "")
            logger.info(
                "gateway: send policy_command policy_id=%s command=%s inputs=%s",
                policy_id,
                command,
                inputs,
            )
            msg = PolicyCommand.from_inputs(
                policy_id=policy_id,
                command=command,
                inputs=inputs,
                request_id=request_id,
            )
            node.send_output("policy_command", msg.to_arrow())
        except Exception as error:
            runtime.mark_command_dispatch_failed(tracked_command_id or "", error)
            raise
        runtime.mark_command_sent(tracked_command_id or "")
    finally:
        runtime.release_command_dispatch()


def drain_commands(runtime: GatewayRuntime, node: Any) -> None:
    while runtime.command_dispatch_allowed():
        cmd = runtime.take_next_command()
        if cmd is None:
            return
        current = cmd
        while True:
            try:
                handle_command(runtime, node, current)
                break
            except Exception as error:
                logger.error("gateway: dispatch failed: %s", error)
                if not current.retry_on_failure:
                    break
                if current.attempt >= 2:
                    runtime.block_command_dispatch(error)
                    return
                current = replace(current, attempt=current.attempt + 1)

