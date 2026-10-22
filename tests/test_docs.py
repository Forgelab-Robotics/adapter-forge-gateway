import re
from pathlib import Path

import pytest


@pytest.mark.parametrize("readme_name", ["README.md", "README_zh.md"])
def test_gateway_api_docs_are_linked(readme_name: str) -> None:
    project_root = Path(__file__).parents[1]
    api_doc = project_root / "docs" / "api.md"

    assert api_doc.is_file()
    assert "Gateway API 文档" in api_doc.read_text(encoding="utf-8")
    readme = (project_root / readme_name).read_text(encoding="utf-8")
    assert re.search(r"\[[^\]]+\]\(docs/api\.md\)", readme)
    assert "[English](README.md)" in readme
    assert "[简体中文](README_zh.md)" in readme
