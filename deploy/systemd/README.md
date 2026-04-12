# Production Systemd Snapshot

This directory stores a snapshot of production `systemd` units for `inventory_service`.

Important:

- these files are based on confirmed server runtime wiring;
- they are kept in the repository as canonical templates and documentation;
- do not apply them directly to a server without review;
- server-local secrets or ad hoc environment wiring may exist outside these files.

Use this directory as:

- source of truth for current production unit layout;
- baseline for runtime stabilization work;
- reference when comparing repo state with server state.

Before applying any of these units:

1. compare them with the live server units;
2. review environment-specific values;
3. run `systemctl daemon-reload` only after deliberate deployment steps.
