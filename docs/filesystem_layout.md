# Filesystem Layout

## Purpose

Зафиксировать canonical production filesystem layout для `inventory_service` и явно отделить supported runtime directories от legacy drift.

## Canonical Directories

### `/root/inventory/temp`

- Purpose:
  - temporary runtime artifacts and per-enterprise intermediate files
- Status:
  - canonical
- Notes:
  - runtime temp files не являются source of truth

### `/root/inventory/state_cache`

- Purpose:
  - scheduler state, locks, retry queues, runtime caches
- Status:
  - canonical
- Notes:
  - stateful runtime path; ручная очистка возможна только осознанно

### `/root/inventory/backups`

- Purpose:
  - PostgreSQL backup dumps
- Status:
  - canonical
- Notes:
  - backup/restore docs и scripts опираются на этот путь

### `/root/inventory/logs`

- Purpose:
  - local file-based logs when they exist
- Status:
  - canonical secondary path
- Notes:
  - primary production log source всё равно `journalctl`

## Runtime Outputs Outside Canonical Paths

### `/root/inventory/app/services/temp`

- Status:
  - legacy
- Interpretation:
  - legacy temp subtree; не использовать как target path для новых runtime writes

### `/root/inventory/app/services/state_cache`

- Status:
  - legacy drift
- Interpretation:
  - duplicate runtime state subtree under service code directory

### `/root/inventory/app/services/input_raw`

- Status:
  - cwd-dependent runtime
- Interpretation:
  - raw captured payloads created by code paths that still depend on current working directory

### `/root/inventory/app/services/exports`

- Status:
  - runtime output
- Interpretation:
  - server-local export artifacts; не считать canonical storage contract без отдельной верификации

## Conventions

1. New runtime paths should resolve from repo root, not from service-local `cwd`.
2. New temp/state outputs should prefer canonical top-level directories.
3. `app/services/*` should remain primarily code, not a dumping ground for runtime artifacts.
