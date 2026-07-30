from pathlib import Path
import sys

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parents[1]))

from config import GatewayConfig, load_config


def test_config_parses_readiness(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "actions" / "piper"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "sam3.md").write_text(
        """---
version: 1
robot_id: piper
policy_id: sam3
actions:
  grasp:
    command: grasp_simple
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
---
# SAM3
""",
        encoding="utf-8",
    )
    config_path = tmp_path / "gateway.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "joint_order": ["j1", "j2"],
                "image_input_ids": ["image/front"],
                "port": 9002,
                "ws_send_timeout_sec": 0.5,
                "readiness": {
                    "require_proprio_state": True,
                    "require_images": True,
                    "require_state_client": True,
                    "image_stale_after_sec": 1.5,
                },
                "agent": {
                    "action_manifests": ["./actions/piper/sam3.md"],
                    "max_active_sessions": 99,
                },
            }
        ),
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.joint_order == ["j1", "j2"]
    assert cfg.image_input_ids == ["image/front"]
    assert cfg.port == 9002
    assert cfg.ws_send_timeout_sec == 0.5
    assert cfg.readiness.require_state_client is True
    assert cfg.readiness.image_stale_after_sec == 1.5
    assert cfg.agent.max_active_sessions == 1
    assert cfg.agent.actions["grasp"].policy_id == "sam3"
    assert cfg.agent.actions["grasp"].command == "grasp_simple"


def test_config_rejects_invalid_jpeg_quality() -> None:
    with pytest.raises(ValueError, match="jpeg_quality"):
        GatewayConfig.from_dict({"joint_order": ["j1"], "jpeg_quality": 101})


def test_config_example_parses() -> None:
    cfg = GatewayConfig.from_yaml_path(Path(__file__).parents[1] / "config.example.yaml")

    assert cfg.agent.actions["grasp"].policy_id == "sam3"
    assert cfg.agent.action_manifests
