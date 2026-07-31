"""Gateway node configuration."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from forge_gateway.adapters.manifest_loader import (
    default_action_manifests as _default_action_manifests,
    load_action_manifest,
    load_action_manifests,
    read_frontmatter as _read_frontmatter,
    resolve_path as _resolve_path,
)
from forge_gateway.domain.action_manifest import ActionDefinition as AgentActionConfig

__all__ = [
    "AgentActionConfig",
    "AgentConfig",
    "GatewayConfig",
    "ReadinessConfig",
    "load_config",
]

_GATEWAY_CONFIG_KEYS = frozenset(
    {
        "joint_order",
        "image_input_ids",
        "host",
        "port",
        "state_broadcast_hz",
        "broadcast_hz",
        "image_broadcast_hz",
        "ws_send_timeout_sec",
        "jpeg_quality",
        "policy_id",
        "command_queue_capacity",
        "readiness",
        "agent",
    }
)
_READINESS_CONFIG_KEYS = frozenset(
    {
        "require_proprio_state",
        "require_images",
        "require_state_client",
        "require_image_client",
        "proprio_stale_after_sec",
        "image_stale_after_sec",
    }
)
_AGENT_CONFIG_KEYS = frozenset(
    {
        "enabled",
        "state_dir",
        "command_timeout_sec",
        "max_active_sessions",
        "write_context_snapshot",
        "action_manifests",
    }
)


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _reject_unknown_keys(
    data: Mapping[str, Any], allowed_keys: frozenset[str], context: str
) -> None:
    unknown_keys = [key for key in data if key not in allowed_keys]
    if unknown_keys:
        rendered_keys = ", ".join(sorted(repr(key) for key in unknown_keys))
        raise ValueError(f"unknown {context} config key(s): {rendered_keys}")


def _require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_finite_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be a finite number")
    return number


def _require_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_unique_non_empty_strings(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list of strings")

    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{field_name} entries must be non-empty strings")
        if item in seen:
            raise ValueError(f"{field_name} entries must be unique")
        seen.add(item)
        result.append(item)
    return result


@dataclass(frozen=True)
class ReadinessConfig:
    """Conditions required before /runtime/start may emit start."""

    require_proprio_state: bool = True
    require_images: bool = True
    require_state_client: bool = False
    require_image_client: bool = False
    proprio_stale_after_sec: float | None = 2.0
    image_stale_after_sec: float = 2.0

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ReadinessConfig":
        if data is None:
            return cls()
        if not isinstance(data, Mapping):
            raise ValueError("readiness config must be a YAML mapping or null")
        _reject_unknown_keys(data, _READINESS_CONFIG_KEYS, "readiness")

        proprio_stale_raw = data.get("proprio_stale_after_sec", 2.0)
        proprio_stale_after_sec = (
            None
            if proprio_stale_raw is None
            else max(
                0.1,
                _require_finite_float(
                    proprio_stale_raw, "readiness.proprio_stale_after_sec"
                ),
            )
        )
        return cls(
            require_proprio_state=_require_bool(
                data.get("require_proprio_state", True),
                "readiness.require_proprio_state",
            ),
            require_images=_require_bool(
                data.get("require_images", True), "readiness.require_images"
            ),
            require_state_client=_require_bool(
                data.get("require_state_client", False),
                "readiness.require_state_client",
            ),
            require_image_client=_require_bool(
                data.get("require_image_client", False),
                "readiness.require_image_client",
            ),
            proprio_stale_after_sec=proprio_stale_after_sec,
            image_stale_after_sec=max(
                0.1,
                _require_finite_float(
                    data.get("image_stale_after_sec", 2.0),
                    "readiness.image_stale_after_sec",
                ),
            ),
        )


@dataclass(frozen=True)
class AgentConfig:
    """Agent-facing gateway API and lightweight state configuration."""

    enabled: bool = True
    state_dir: str | None = None
    command_timeout_sec: float = 120.0
    max_active_sessions: int = 1
    write_context_snapshot: bool = True
    action_manifests: list[str] = field(default_factory=list)
    actions: dict[str, AgentActionConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any] | None,
        *,
        base_dir: Path | None = None,
    ) -> "AgentConfig":
        if data is None:
            manifests = _default_action_manifests()
            return cls(
                action_manifests=[str(path) for path in manifests],
                actions=_load_action_manifests(manifests),
            )
        if not isinstance(data, Mapping):
            raise ValueError("agent config must be a YAML mapping or null")
        _reject_unknown_keys(data, _AGENT_CONFIG_KEYS, "agent")

        enabled = _require_bool(data.get("enabled", True), "agent.enabled")
        command_timeout_sec = max(
            1.0,
            _require_finite_float(
                data.get("command_timeout_sec", 120.0),
                "agent.command_timeout_sec",
            ),
        )
        write_context_snapshot = _require_bool(
            data.get("write_context_snapshot", True),
            "agent.write_context_snapshot",
        )
        state_dir_raw = data.get("state_dir")
        state_dir = (
            None
            if state_dir_raw is None
            else _require_non_empty_string(state_dir_raw, "agent.state_dir")
        )
        max_active_sessions = data.get("max_active_sessions", 1)
        if (
            isinstance(max_active_sessions, bool)
            or not isinstance(max_active_sessions, int)
            or max_active_sessions != 1
        ):
            raise ValueError("agent.max_active_sessions must be the integer 1")

        manifest_values = data.get("action_manifests")
        if manifest_values is None:
            manifests = _default_action_manifests()
        else:
            manifest_strings = _require_unique_non_empty_strings(
                manifest_values, "agent.action_manifests"
            )
            manifests = [
                _resolve_path(Path(value), base_dir) for value in manifest_strings
            ]
        actions = _load_action_manifests(manifests)

        return cls(
            enabled=enabled,
            state_dir=state_dir,
            command_timeout_sec=command_timeout_sec,
            max_active_sessions=max_active_sessions,
            write_context_snapshot=write_context_snapshot,
            action_manifests=[str(path) for path in manifests],
            actions=actions,
        )


@dataclass(frozen=True)
class GatewayConfig:
    """Unified HTTP/WebSocket gateway configuration."""

    joint_order: list[str]
    image_input_ids: list[str] = field(default_factory=list)
    host: str = "127.0.0.1"
    port: int = 9001
    state_broadcast_hz: float = 50.0
    image_broadcast_hz: float = 24.0
    ws_send_timeout_sec: float = 1.0
    jpeg_quality: int = 85
    policy_id: str = "default"
    command_queue_capacity: int = 256
    readiness: ReadinessConfig = field(default_factory=ReadinessConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, base_dir: Path | None = None
    ) -> "GatewayConfig":
        if not isinstance(data, Mapping):
            raise ValueError("gateway config must be a YAML mapping")
        _reject_unknown_keys(data, _GATEWAY_CONFIG_KEYS, "gateway")
        if "state_broadcast_hz" in data and "broadcast_hz" in data:
            raise ValueError(
                "state_broadcast_hz and legacy broadcast_hz cannot both be set"
            )

        joint_order = _require_unique_non_empty_strings(
            data.get("joint_order", []), "joint_order"
        )
        image_input_ids = _require_unique_non_empty_strings(
            data.get("image_input_ids", []), "image_input_ids"
        )
        host = _require_non_empty_string(data.get("host", "127.0.0.1"), "host")
        policy_id = _require_non_empty_string(
            data.get("policy_id", "default"), "policy_id"
        )

        port = _require_int(data.get("port", 9001), "port")
        if not 1 <= port <= 65535:
            raise ValueError("port must be in [1, 65535]")

        jpeg_quality = _require_int(data.get("jpeg_quality", 85), "jpeg_quality")
        if not 1 <= jpeg_quality <= 100:
            raise ValueError("jpeg_quality must be in [1, 100]")

        command_queue_capacity = _require_int(
            data.get("command_queue_capacity", 256), "command_queue_capacity"
        )
        if command_queue_capacity < 1:
            raise ValueError("command_queue_capacity must be at least 1")

        state_broadcast_raw = (
            data["state_broadcast_hz"]
            if "state_broadcast_hz" in data
            else data.get("broadcast_hz", 50.0)
        )
        return cls(
            joint_order=joint_order,
            image_input_ids=image_input_ids,
            host=host,
            port=port,
            state_broadcast_hz=_clamp_hz(
                _require_finite_float(state_broadcast_raw, "state_broadcast_hz")
            ),
            image_broadcast_hz=_clamp_hz(
                _require_finite_float(
                    data.get("image_broadcast_hz", 24.0), "image_broadcast_hz"
                )
            ),
            ws_send_timeout_sec=max(
                0.1,
                _require_finite_float(
                    data.get("ws_send_timeout_sec", 1.0), "ws_send_timeout_sec"
                ),
            ),
            jpeg_quality=jpeg_quality,
            policy_id=policy_id,
            command_queue_capacity=command_queue_capacity,
            readiness=ReadinessConfig.from_dict(data.get("readiness")),
            agent=AgentConfig.from_dict(data.get("agent"), base_dir=base_dir),
        )

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "GatewayConfig":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"配置文件不存在: {p}")
        with p.open(encoding="utf-8") as f:
            data = yaml.load(f, Loader=_UniqueKeySafeLoader)
        if not data:
            raise ValueError(f"配置文件为空: {p}")
        if not isinstance(data, dict):
            raise ValueError(f"配置文件必须是 YAML mapping: {p}")
        return cls.from_dict(data, base_dir=p.parent)


def _clamp_hz(value: float) -> float:
    return max(0.1, min(120.0, value))


def _load_action_manifests(paths: list[Path]) -> dict[str, AgentActionConfig]:
    return {
        action_name: action
        for manifest in load_action_manifests(paths)
        for action_name, action in manifest.actions.items()
    }


def _load_action_manifest(path: Path) -> dict[str, AgentActionConfig]:
    return load_action_manifest(path).actions


def load_config(config_path: str | Path | None = None) -> GatewayConfig:
    """Load config from explicit path or GATEWAY_CONFIG."""

    path = config_path or os.environ.get("GATEWAY_CONFIG")
    if path:
        return GatewayConfig.from_yaml_path(path)
    return GatewayConfig.from_dict({"joint_order": [], "image_input_ids": []})
