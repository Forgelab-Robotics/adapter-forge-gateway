from fastapi.testclient import TestClient

from forge_gateway.adapters.http_app import create_app
from forge_gateway.config import GatewayConfig
from forge_gateway.services.runtime_service import GatewayRuntime


def test_legacy_agent_api_is_not_registered() -> None:
    runtime = GatewayRuntime(GatewayConfig.from_dict({"joint_order": ["joint1"]}))
    try:
        client = TestClient(create_app(runtime))
        paths = client.get("/openapi.json").json()["paths"]

        assert not any(path.startswith("/agent/") for path in paths)
        assert client.get("/agent/runtime/status").status_code == 404
        assert client.post("/agent/sessions", json={}).status_code == 404
    finally:
        runtime.close()
