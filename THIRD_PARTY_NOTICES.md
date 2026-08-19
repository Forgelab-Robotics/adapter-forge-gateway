# Third-party notices

The repository's original Python code, configuration, static resources, and
documentation are licensed under Apache License 2.0 unless a file states
otherwise.

## Runtime dependencies

Except for the vendored component identified below, runtime dependencies are
installed from package indexes and are not vendored into this source repository.
They retain their respective licenses:

- `dora-rs`: MIT
- `FastAPI`: MIT
- `Uvicorn`: BSD-3-Clause
- `forge-common` and `forge-msgs`: Apache-2.0
- `opencv-python`: Apache-2.0
- `PyYAML`: MIT

Transitive dependencies retain the licenses declared by their distributions.
Consult the locked environment and installed package metadata for the complete
dependency graph.

## Vendored forge-tool

- Component: `forge-tool`
- Version: `0.1.0`
- Upstream project identity: public `Forgelab-Robotics/forge`. Its current
  public branches do not contain `packages/tool`.
- Source used: a local Forge workspace snapshot identified by
  `8c32b03403518c4b3c6aeb3f834c726d06dd3c1c`
- Original source path: `packages/tool/src/forge_tool`
- Vendored path: `src/forge_tool`
- License: Apache License 2.0
- Modifications: redistributed in this repository instead of being installed
  as an external dependency. Tool behavior is unchanged; `dora.py` imports the
  local ToolMessage carrier described below because public `forge-msgs==1.0.1`
  does not export `ToolMessage` or `ToolMessageSizeError`.

The snapshot identifier records the provenance of the local source used here;
it does not imply that `packages/tool` can be retrieved from the current public
repository at that identifier.

## Vendored ToolMessage Arrow carrier

- Component: the `ToolMessage` Arrow carrier subset of `forge-msgs`
- Source used: the same local Forge workspace snapshot identified by
  `8c32b03403518c4b3c6aeb3f834c726d06dd3c1c`
- Original source path: `packages/msgs/src/forge_msgs/tool.py`
- Vendored path: `src/forge_tool/_tool_message.py`
- License: Apache License 2.0
- Modifications: only this carrier module is redistributed. Its
  `ensure_record_batch` dependency was reduced to a local helper supporting the
  carrier's accepted in-memory Arrow types, so no local package shadows or
  impersonates the public `forge_msgs` distribution.

## Build dependencies and binary releases

PyInstaller is used only to produce optional standalone executables and is not
vendored into this repository. A bundled executable contains runtime
dependencies and may create license, notice, source-offer, codec, or platform
obligations beyond source distribution. Review the complete bundled artifact
and all dependency licenses before publishing a binary release.
