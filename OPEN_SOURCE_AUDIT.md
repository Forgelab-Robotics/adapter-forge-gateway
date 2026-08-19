# Open-source release audit

Audit date: 2026-08-19

## Decision

The audited snapshot is suitable for publication with a clean initial Git
history. Project-owned code, configuration, static resources, and documentation
use Apache-2.0. The vendored Forge Tool protocol and ToolMessage carrier retain
their Apache-2.0 provenance as documented in `THIRD_PARTY_NOTICES.md`.

## Completed checks

- Public dependency resolution succeeds with `uv sync --frozen --all-groups`.
- `forge-common==1.0.1` and `forge-msgs==1.0.1` resolve from PyPI.
- The unpublished `forge-tool==0.1.0` protocol source and its required
  ToolMessage Arrow carrier are included under `src/forge_tool`.
- All 309 tests pass on Python 3.12 without physical hardware.
- CLI reports `forge-gateway 1.0.1`.
- `pip-audit` reports no known vulnerabilities in locked runtime dependencies.
- `detect-secrets` reports zero findings in the publishable source tree.
- Private repository URLs and machine-specific paths are absent from the
  publishable tree.
- No file approaches GitHub's 100 MiB hard limit; the largest file is under
  100 KiB.
- GitHub Actions, Dependabot, issue, pull-request, contribution, and security
  policy files are included.

## License findings

- Project-owned Python, configuration, static resources, tests, and
  documentation: Apache-2.0.
- Vendored `forge-tool` and ToolMessage carrier snapshot: Apache-2.0.
- Runtime dependencies are not otherwise vendored and retain the licenses
  declared by their distributions.
- `dora-rs`: MIT.
- FastAPI: MIT.
- Uvicorn: BSD-3-Clause.
- `forge-common` and `forge-msgs`: Apache-2.0.
- OpenCV Python wheels: Apache-2.0.
- PyYAML: MIT.
- PyInstaller is a build-only dependency under GPL-2.0-or-later with its
  special exception.

See `THIRD_PARTY_NOTICES.md` for provenance and binary-distribution cautions.

## Known limitations

- The Gateway control API does not provide authentication or TLS. Keep the
  default loopback binding, or deploy behind an authenticated encrypted proxy
  on a trusted network.
- CI and this audit do not command physical hardware.
- The vendored protocol snapshot should be replaced with a public package after
  an equivalent compatible `forge-tool` release becomes available.
- PyInstaller binary releases require a separate artifact-level license and
  security review.
- This audit is an engineering review, not legal advice, penetration testing,
  or safety certification.

## Publication model

Only the audited current snapshot is published. The public repository starts
with one initial commit on `master`; prior internal development history is not
included.
