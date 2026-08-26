"""Compatibility shim for source checkouts importing top-level ``config``."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_SRC_DIR = str((Path(__file__).resolve().parent / "src").resolve())
if not sys.path or sys.path[0] != _SRC_DIR:
    sys.path.insert(0, _SRC_DIR)

from forge_gateway import config as _implementation

GatewayConfig = _implementation.GatewayConfig
ReadinessConfig = _implementation.ReadinessConfig
ToolGatewayConfig = _implementation.ToolGatewayConfig
ToolProviderRouteConfig = _implementation.ToolProviderRouteConfig
os = _implementation.os
dataclass = _implementation.dataclass
field = _implementation.field
yaml = _implementation.yaml
_clamp_hz = _implementation._clamp_hz
load_config = _implementation.load_config

__all__ = [
    "GatewayConfig",
    "ReadinessConfig",
    "ToolGatewayConfig",
    "ToolProviderRouteConfig",
    "load_config",
    "os",
    "dataclass",
    "field",
    "Path",
    "Any",
    "yaml",
]


def __getattr__(name: str) -> Any:
    return getattr(_implementation, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_implementation)))
