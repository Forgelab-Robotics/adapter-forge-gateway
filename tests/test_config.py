from pathlib import Path

import pytest
import yaml

from forge_gateway.config import GatewayConfig, load_config


def test_config_parses_readiness(tmp_path: Path) -> None:
    config_path = tmp_path / "gateway.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "joint_order": ["j1", "j2"],
                "image_input_ids": ["image/front"],
                "port": 9002,
                "ws_send_timeout_sec": 0.5,
                "command_queue_capacity": 32,
                "readiness": {
                    "require_proprio_state": True,
                    "require_images": True,
                    "require_state_client": True,
                    "proprio_stale_after_sec": 3.0,
                    "image_stale_after_sec": 1.5,
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
    assert cfg.command_queue_capacity == 32
    assert cfg.readiness.require_state_client is True
    assert cfg.readiness.proprio_stale_after_sec == 3.0
    assert cfg.readiness.image_stale_after_sec == 1.5


def test_config_preserves_alias_and_documented_clamps() -> None:
    cfg = GatewayConfig.from_dict(
        {
            "joint_order": [],
            "image_input_ids": [],
            "broadcast_hz": 0.0,
            "image_broadcast_hz": 121.0,
            "ws_send_timeout_sec": 0.0,
            "readiness": {
                "proprio_stale_after_sec": 0.0,
                "image_stale_after_sec": -1.0,
            },
        }
    )

    assert cfg.state_broadcast_hz == 0.1
    assert cfg.image_broadcast_hz == 120.0
    assert cfg.ws_send_timeout_sec == 0.1
    assert cfg.readiness.proprio_stale_after_sec == 0.1
    assert cfg.readiness.image_stale_after_sec == 0.1


@pytest.mark.parametrize(
    ("data", "match"),
    [
        pytest.param(
            {"joint_order": ["j1"], "readiness": {"require_images": "false"}},
            "readiness.require_images",
            id="quoted-bool",
        ),
        pytest.param(
            {"joint_order": ["j1"], "agent": {}},
            "agent",
            id="removed-agent-config",
        ),
        pytest.param(
            {"joint_order": ["j1"], "jpeg_quailty": 85},
            "jpeg_quailty",
            id="unknown-gateway-key",
        ),
        pytest.param(
            {"joint_order": ["j1"], "readiness": {"require_image": True}},
            "require_image",
            id="unknown-readiness-key",
        ),
        pytest.param(
            {"joint_order": ["j1"], "readiness": []},
            "readiness",
            id="readiness-list",
        ),
        pytest.param(
            {"joint_order": ["j1"], "state_broadcast_hz": float("nan")},
            "state_broadcast_hz",
            id="nan-state-hz",
        ),
        pytest.param(
            {"joint_order": ["j1"], "image_broadcast_hz": float("inf")},
            "image_broadcast_hz",
            id="infinite-image-hz",
        ),
        pytest.param(
            {"joint_order": ["j1"], "ws_send_timeout_sec": float("-inf")},
            "ws_send_timeout_sec",
            id="infinite-ws-timeout",
        ),
        pytest.param(
            {
                "joint_order": ["j1"],
                "readiness": {"proprio_stale_after_sec": float("nan")},
            },
            "proprio_stale_after_sec",
            id="nan-proprio-staleness",
        ),
        pytest.param(
            {
                "joint_order": ["j1"],
                "readiness": {"image_stale_after_sec": float("inf")},
            },
            "image_stale_after_sec",
            id="infinite-image-staleness",
        ),
        pytest.param(
            {"joint_order": ["j1"], "state_broadcast_hz": True},
            "state_broadcast_hz",
            id="bool-number",
        ),
        pytest.param(
            {"joint_order": ["j1"], "port": 0},
            "port",
            id="port-too-low",
        ),
        pytest.param(
            {"joint_order": ["j1"], "port": 65536},
            "port",
            id="port-too-high",
        ),
        pytest.param(
            {"joint_order": ["j1"], "jpeg_quality": 101},
            "jpeg_quality",
            id="jpeg-quality-too-high",
        ),
        pytest.param(
            {"joint_order": ["j1", "j1"]},
            "joint_order",
            id="duplicate-joint-id",
        ),
        pytest.param(
            {"joint_order": [""]},
            "joint_order",
            id="empty-joint-id",
        ),
        pytest.param(
            {"joint_order": ["j1"], "image_input_ids": ["image/front"] * 2},
            "image_input_ids",
            id="duplicate-image-id",
        ),
        pytest.param(
            {"joint_order": ["j1"], "image_input_ids": [""]},
            "image_input_ids",
            id="empty-image-id",
        ),
        pytest.param(
            {"joint_order": ["j1"], "host": ""},
            "host",
            id="empty-host",
        ),
        pytest.param(
            {"joint_order": ["j1"], "policy_id": ""},
            "policy_id",
            id="empty-policy-id",
        ),
        pytest.param(
            {"joint_order": ["j1"], "command_queue_capacity": 0},
            "command_queue_capacity",
            id="zero-command-capacity",
        ),
        pytest.param(
            {
                "joint_order": ["j1"],
                "state_broadcast_hz": 50.0,
                "broadcast_hz": 50.0,
            },
            "broadcast_hz",
            id="ambiguous-state-hz-alias",
        ),
    ],
)
def test_config_rejects_invalid_values(
    data: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        GatewayConfig.from_dict(data)


@pytest.mark.parametrize(
    "yaml_text",
    [
        "joint_order: [j1]\njoint_order: [j2]\n",
        (
            "joint_order: [j1]\n"
            "readiness:\n"
            "  require_images: true\n"
            "  require_images: false\n"
        ),
        (
            "joint_order: [j1]\n"
            "tools:\n"
            "  enabled: true\n"
            "  enabled: false\n"
        ),
    ],
)
def test_yaml_rejects_duplicate_keys(tmp_path: Path, yaml_text: str) -> None:
    config_path = tmp_path / "duplicate.yaml"
    config_path.write_text(yaml_text, encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate YAML key"):
        GatewayConfig.from_yaml_path(config_path)


def test_config_example_parses() -> None:
    cfg = GatewayConfig.from_yaml_path(Path(__file__).parents[1] / "config.example.yaml")

    assert cfg.joint_order
    assert cfg.readiness.require_proprio_state is True
