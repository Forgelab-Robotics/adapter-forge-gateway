# -*- mode: python ; coding: utf-8 -*-

import os

from PyInstaller.utils.hooks import collect_submodules

_spec_dir = os.path.dirname(os.path.abspath(SPEC))
_node_dir = os.path.dirname(_spec_dir)
_actions_dir = os.path.join(_node_dir, "actions")
_static_dir = os.path.join(_node_dir, "app", "static")
_sam3_manifest = os.path.join(_actions_dir, "piper", "sam3.md")

hiddenimports = (
    collect_submodules("app")
    + collect_submodules("uvicorn")
    + collect_submodules("fastapi")
    + collect_submodules("forge_msgs")
    + [
        "cv2",
        "dora",
        "pyarrow",
    ]
)

a = Analysis(
    [os.path.join(_node_dir, "main.py")],
    pathex=[_node_dir],
    binaries=[],
    datas=[
        (_sam3_manifest, os.path.join("actions", "piper")),
        (_static_dir, os.path.join("app", "static")),
    ],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="gateway",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
