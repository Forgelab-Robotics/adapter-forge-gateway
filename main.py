#!/usr/bin/env python3
"""Unified Dora gateway node entrypoint.

The implementation lives under ``app/``. This module keeps the historical
imports stable for tests and scripts that still import ``main`` directly.
"""

from __future__ import annotations

from app.adapters.dora_adapter import (
    DoraEventBuffer,
    _joint_values_by_name,
    _json_bytes,
    _ordered,
    drain_commands,
    handle_command,
    handle_dora_input,
)
from app.adapters.http_app import create_app
from app.adapters.websocket import (
    StalledWebSocketClient,
    send_json_with_timeout as _send_json_with_timeout,
    sleep as _sleep,
)
from app.cli import main
from app.domain.commands import (
    Command,
    CommandKind,
    CommandState,
    CommandStatus,
    map_policy_status as _map_policy_status,
)
from app.domain.node_status import NodeHealth, NodeStatus
from app.domain.sessions import (
    SessionState,
    SessionStatus,
    session_status_from_command as _session_status_from_command,
)
from app.services.image_service import ImageEncodeWorker, _image_payload
from app.services.runtime_service import GatewayRuntime

__all__ = [
    "Command",
    "CommandKind",
    "CommandState",
    "CommandStatus",
    "DoraEventBuffer",
    "GatewayRuntime",
    "ImageEncodeWorker",
    "NodeHealth",
    "NodeStatus",
    "SessionState",
    "SessionStatus",
    "StalledWebSocketClient",
    "_image_payload",
    "_joint_values_by_name",
    "_json_bytes",
    "_map_policy_status",
    "_ordered",
    "_send_json_with_timeout",
    "_session_status_from_command",
    "_sleep",
    "create_app",
    "drain_commands",
    "handle_command",
    "handle_dora_input",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())