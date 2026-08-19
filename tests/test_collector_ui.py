import re
from pathlib import Path

import pytest

TestClient = pytest.importorskip("fastapi.testclient").TestClient

from forge_gateway.adapters.http_app import STATIC_DIR, create_app


class FakeRuntime:
    pass


def test_collector_ui_is_mounted_at_gateway_root() -> None:
    app = create_app(FakeRuntime())
    client = TestClient(app)

    response = client.get("/")
    script_response = client.get("/static/app.js")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"
    assert "Forge Collector" in response.text
    assert script_response.status_code == 200
    assert "/record/control" in script_response.text
    assert any(getattr(route, "path", None) == "/static" for route in app.routes)


def test_collector_ui_assets_cover_gateway_collection_contract() -> None:
    index = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    script = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
    styles = (STATIC_DIR / "styles.css").read_text(encoding="utf-8")

    assert 'href="/static/styles.css"' in index
    assert 'src="/static/app.js"' in index
    assert "/ws/state" in script
    assert "/ws/images" in script
    assert 'postJson("/record/control"' in script
    assert 'postJson("/runtime/reset_scene"' in script
    assert 'action: "START"' in script
    assert 'action: "STOP"' in script
    assert 'action: "DISCARD"' in script
    assert "gateway-collector-ui" in script
    assert ".camera-grid" in styles

    html_ids = set(re.findall(r'id="([^"]+)"', index))
    script_ids = set(re.findall(r'\$\("([^"]+)"\)', script))
    assert script_ids
    assert script_ids <= html_ids


def test_pyinstaller_bundle_includes_collector_ui_assets() -> None:
    spec_path = Path(__file__).parents[1] / "scripts" / "gateway.spec"
    spec = spec_path.read_text(encoding="utf-8")

    assert '_entry = os.path.join(_src_dir, "forge_gateway", "__main__.py")' in spec
    assert '_resources_dir = os.path.join(_src_dir, "forge_gateway", "resources")' in spec
    assert '(_resources_dir, os.path.join("forge_gateway", "resources"))' in spec
