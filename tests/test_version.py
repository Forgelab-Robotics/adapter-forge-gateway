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


def test_forge_dependencies_use_immutable_compatible_sources() -> None:
    metadata = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = metadata["project"]["dependencies"]
    sources = metadata["tool"]["uv"]["sources"]

    assert "forge-msgs>=1.1.0,<2" in dependencies
    assert "forge-common>=1.0.0,<2" in dependencies
    assert "forge-tool[dora]>=0.1.0,<0.2" in dependencies
    assert sources["forge-common"]["tag"] == "forge-common-v1.0.0"
    expected_revision = "446d12d12f7760540369919582f78efb6c6f5ce7"
    assert sources["forge-msgs"]["rev"] == expected_revision
    assert sources["forge-tool"]["rev"] == expected_revision
