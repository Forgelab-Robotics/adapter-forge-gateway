from pathlib import Path


def test_gateway_api_docs_are_linked() -> None:
    project_root = Path(__file__).parents[1]
    api_doc = project_root / "docs" / "api.md"

    assert api_doc.is_file()
    assert "Gateway API 文档" in api_doc.read_text(encoding="utf-8")
    assert "[Gateway API 文档](docs/api.md)" in (
        project_root / "README.md"
    ).read_text(encoding="utf-8")
