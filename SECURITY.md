# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
vulnerability reporting for this repository. Include affected versions,
reproduction steps, impact, and any suggested mitigation.

## Deployment boundary

Forge Gateway exposes control, recording, playback, runtime, Tool, and
WebSocket interfaces. It does not provide authentication or transport
encryption.

- Keep the default `127.0.0.1` bind address unless remote access is required.
- When binding to `0.0.0.0`, place the service behind an authenticated,
  encrypted reverse proxy and restrict it to a trusted network.
- Do not expose the service directly to the public internet.
- Treat configuration files, recordings, runtime state, and Tool results as
  potentially sensitive.
- Validate safety controls independently before connecting a physical robot.

Security fixes are supported on the latest released version.
