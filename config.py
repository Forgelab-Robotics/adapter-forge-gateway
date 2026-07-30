"""Compatibility shim for source checkouts importing top-level ``config``."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_SRC_DIR = str((Path(__file__).resolve().parent / "src").resolve())
if not sys.path or sys.path[0] != _SRC_DIR:
    sys.path.insert(0, _SRC_DIR)

from forge_gateway import config as _implementation

AgentActionConfig = _implementation.AgentActionConfig
AgentConfig = _implementation.AgentConfig
GatewayConfig = _implementation.GatewayConfig
ReadinessConfig = _implementation.ReadinessConfig
_clamp_hz = _implementation._clamp_hz
_default_action_manifests = _implementation._default_action_manifests
_load_action_manifest = _implementation._load_action_manifest
_load_action_manifests = _implementation._load_action_manifests
_read_frontmatter = _implementation._read_frontmatter
_resolve_path = _implementation._resolve_path
load_action_manifest = _implementation.load_action_manifest
load_action_manifests = _implementation.load_action_manifests
load_config = _implementation.load_config

__all__ = [name for name in vars(_implementation) if not name.startswith("_")]


def __getattr__(name: str) -> Any:
    return getattr(_implementation, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_implementation)))
