"""Gateway node configuration."""

from __future__ import annotations

import os
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


@dataclass(frozen=True)
class ReadinessConfig:
    """Conditions required before /runtime/start may emit start."""

    require_proprio_state: bool = True
    require_images: bool = True
    require_state_client: bool = False
    require_image_client: bool = False
    image_stale_after_sec: float = 2.0

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ReadinessConfig":
        if not data:
            return cls()
        return cls(
            require_proprio_state=bool(data.get("require_proprio_state", True)),
            require_images=bool(data.get("require_images", True)),
            require_state_client=bool(data.get("require_state_client", False)),
            require_image_client=bool(data.get("require_image_client", False)),
            image_stale_after_sec=max(0.1, float(data.get("image_stale_after_sec", 2.0))),
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
    def from_dict(cls, data: dict[str, Any] | None, *, base_dir: Path | None = None) -> "AgentConfig":
        if data is None:
            manifests = _default_action_manifests()
            return cls(action_manifests=[str(path) for path in manifests], actions=_load_action_manifests(manifests))
        if not isinstance(data, dict):
            raise ValueError("agent config must be a YAML mapping")
        manifest_values = data.get("action_manifests")
        if manifest_values is None:
            manifests = _default_action_manifests()
        else:
            if not isinstance(manifest_values, list) or not all(isinstance(x, str) for x in manifest_values):
                raise ValueError("agent.action_manifests must be a list of strings")
            manifests = [_resolve_path(Path(value), base_dir) for value in manifest_values]
        actions = _load_action_manifests(manifests)
        state_dir = data.get("state_dir")
        return cls(
            enabled=bool(data.get("enabled", True)),
            state_dir=str(state_dir) if state_dir else None,
            command_timeout_sec=max(1.0, float(data.get("command_timeout_sec", 120.0))),
            max_active_sessions=1,
            write_context_snapshot=bool(data.get("write_context_snapshot", True)),
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
    readiness: ReadinessConfig = field(default_factory=ReadinessConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, base_dir: Path | None = None) -> "GatewayConfig":
        joint_order = data.get("joint_order", [])
        image_input_ids = data.get("image_input_ids", [])
        if not isinstance(joint_order, list) or not all(isinstance(x, str) for x in joint_order):
            raise ValueError("joint_order must be a list of strings")
        if not isinstance(image_input_ids, list) or not all(isinstance(x, str) for x in image_input_ids):
            raise ValueError("image_input_ids must be a list of strings")

        jpeg_quality = int(data.get("jpeg_quality", 85))
        if jpeg_quality < 1 or jpeg_quality > 100:
            raise ValueError("jpeg_quality must be in [1, 100]")

        return cls(
            joint_order=joint_order,
            image_input_ids=image_input_ids,
            host=str(data.get("host", "127.0.0.1")),
            port=int(data.get("port", 9001)),
            state_broadcast_hz=_clamp_hz(float(data.get("state_broadcast_hz", data.get("broadcast_hz", 50.0)))),
            image_broadcast_hz=_clamp_hz(float(data.get("image_broadcast_hz", 24.0))),
            ws_send_timeout_sec=max(0.1, float(data.get("ws_send_timeout_sec", 1.0))),
            jpeg_quality=jpeg_quality,
            policy_id=str(data.get("policy_id", "default")),
            readiness=ReadinessConfig.from_dict(data.get("readiness")),
            agent=AgentConfig.from_dict(data.get("agent"), base_dir=base_dir),
        )

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "GatewayConfig":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"配置文件不存在: {p}")
        with p.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
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
