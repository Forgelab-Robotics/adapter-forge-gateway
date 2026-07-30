from __future__ import annotations

import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path

import pytest

from forge_gateway import cli
from forge_gateway.adapters.http_app import STATIC_DIR
from forge_gateway.adapters.manifest_loader import default_action_manifests
from forge_gateway.config import load_config


def test_package_resources_are_discoverable_without_project_cwd(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GATEWAY_CONFIG", raising=False)

    manifests = default_action_manifests()
    config = load_config()

    assert (STATIC_DIR / "index.html").is_file()
    assert (STATIC_DIR / "app.js").is_file()
    assert (STATIC_DIR.parent / "actions" / "piper" / "sam3_policy_core.md").is_file()
    assert len(manifests) == 1
    assert manifests[0].is_file()
    assert config.agent.action_manifests == [str(manifests[0])]
    assert "grasp" in config.agent.actions


def test_python_module_entrypoint_help_works_outside_project(
    tmp_path: Path,
) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "forge_gateway", "--help"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Forge unified gateway node" in result.stdout


def test_console_script_points_to_package_cli(tmp_path: Path) -> None:
    entry_points = [
        entry_point
        for entry_point in distribution("forge-gateway").entry_points
        if entry_point.group == "console_scripts" and entry_point.name == "gateway"
    ]

    assert [entry_point.value for entry_point in entry_points] == ["forge_gateway.cli:main"]
    assert entry_points[0].load() is cli.main

    script_name = "gateway.exe" if sys.platform == "win32" else "gateway"
    console_script = Path(sys.executable).with_name(script_name)
    result = subprocess.run(
        [str(console_script), "--help"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Forge unified gateway node" in result.stdout
