# Contributing

Thank you for contributing to Forge Gateway.

## Development

Use Python 3.12 and `uv`:

```bash
uv sync --frozen --all-groups
uv run pytest -q
uv run python main.py --help
uv run python main.py --version
```

Keep changes focused and add tests for behavior changes. Update the API
documentation when routes, payloads, configuration, or protocol behavior
changes.

## Security and generated data

- Report vulnerabilities privately as described in `SECURITY.md`.
- Never commit credentials, private repository URLs, recordings, runtime state,
  machine-specific paths, or personal data.
- Keep the default loopback-only network boundary unless a change explicitly
  documents and tests its security impact.
- Do not run tests that command physical hardware without independent safety
  controls and an operator-accessible emergency stop.

By submitting a contribution, you agree that it is licensed under Apache
License 2.0.
