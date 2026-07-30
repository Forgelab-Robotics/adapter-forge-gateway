from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))

from app.adapters.manifest_loader import load_action_manifest, load_action_manifests
from app.services.action_registry import ActionRegistry


def _write_manifest(path: Path, *, action_name: str = "grasp", robot_id: str = "piper") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""---
version: 1
robot_id: {robot_id}
policy_id: sam3
actions:
  {action_name}:
    command: grasp_simple
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
---
# SAM3
""",
        encoding="utf-8",
    )


def test_manifest_loader_parses_action_mapping(tmp_path: Path) -> None:
    manifest_path = tmp_path / "actions" / "piper" / "sam3.md"
    _write_manifest(manifest_path)

    manifest = load_action_manifest(manifest_path)
    action = manifest.actions["grasp"]

    assert manifest.robot_id == "piper"
    assert action.policy_id == "sam3"
    assert action.required_parameters == ["target_name"]
    assert action.input_mapping == {"target": "target_name"}


def test_manifest_loader_rejects_missing_robot_id(tmp_path: Path) -> None:
    manifest_path = tmp_path / "bad.md"
    manifest_path.write_text(
        """---
version: 1
policy_id: sam3
actions:
  grasp:
    command: grasp_simple
---
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="robot_id"):
        load_action_manifest(manifest_path)


def test_action_registry_rejects_duplicate_actions(tmp_path: Path) -> None:
    first = tmp_path / "a" / "sam3.md"
    second = tmp_path / "b" / "sam3.md"
    _write_manifest(first)
    _write_manifest(second, robot_id="piper2")

    with pytest.raises(ValueError, match="duplicate agent action"):
        load_action_manifests([first, second])


def test_action_registry_lists_supported_actions(tmp_path: Path) -> None:
    manifest_path = tmp_path / "actions" / "piper" / "sam3.md"
    _write_manifest(manifest_path)
    registry = ActionRegistry(load_action_manifests([manifest_path]))

    assert registry.supported_action_names() == ["grasp"]
    assert registry.get("grasp") is not None
