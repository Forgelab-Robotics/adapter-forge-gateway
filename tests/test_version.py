from __future__ import annotations

import tomllib
from pathlib import Path

from forge_gateway import __version__
from forge_gateway.adapters.http_app import create_app


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeRuntime:
    pass


def test_public_versions_match_project_metadata() -> None:
    metadata = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert metadata["project"]["version"] == __version__ == "1.0.1"
    assert create_app(FakeRuntime()).version == __version__


def test_forge_dependencies_use_stable_v1_tags() -> None:
    metadata = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "forge-msgs>=1.0.0,<2" in metadata["project"]["dependencies"]
    assert "forge-common>=1.0.0,<2" in metadata["project"]["dependencies"]
    assert metadata["tool"]["uv"]["sources"]["forge-msgs"]["tag"] == "forge-msgs-v1.0.0"
    assert metadata["tool"]["uv"]["sources"]["forge-common"]["tag"] == "forge-common-v1.0.0"
