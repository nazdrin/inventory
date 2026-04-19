# Runtime Standards

## Purpose

Зафиксировать текущий production runtime standard для `inventory_service` после stabilization и cleanup.

Документ задаёт operational guardrails и не заменяет supplier-specific runbooks.

## Service Execution Standard

Все Python services должны запускаться только через module execution:

```bash
python -m app.services.<module_name>
```

Запрещённый pattern:

```bash
python file.py
```

Exception:

- `fastapi.service` использует `uvicorn app.main:app`
- `backup_db.service` запускает shell script

## WorkingDirectory Standard

Canonical `WorkingDirectory` для long-running Python services:

```bash
/root/inventory
```

Практический смысл:

- runtime не должен зависеть от того, из какой поддиректории был запущен процесс;
- относительные runtime paths должны резолвиться от repo root или explicit base paths, а не от случайного `cwd`.

## Environment Standard

Для production services обязательно:

- `PYTHONPATH=/root/inventory`
- `EnvironmentFile=/root/inventory/.env`

Operational rule:

- `.env` обязателен для production wiring;
- server-local env overrides допустимы только осознанно и должны быть documented отдельно.

## Filesystem Conventions

Canonical runtime directories:

- `/root/inventory/temp`
- `/root/inventory/state_cache`
- `/root/inventory/backups`
- `/root/inventory/logs`

Не считать canonical runtime paths:

- `app/services/temp`
- `app/services/state_cache`
- `app/services/input_raw`
- `app/services/exports`

Это либо legacy drift, либо cwd-dependent runtime artifacts.

## Backup Standard

Canonical backup entrypoint:

- `scripts/backup/backup_db.sh`

Manual shortcut допустим:

- `/root/inventory/backup_db.sh` as symlink compatibility path

Backup contract:

- local PostgreSQL backup обязателен;
- retention logic обязателен;
- Google Drive upload обязателен;
- `notify_backup` success/error hooks обязательны.

Operational rule:

- backup script нельзя менять без понимания:
  - local dump creation
  - retention
  - GDrive upload
  - notifications

## Logs Standard

Основной production log source:

- `systemd` / `journalctl`

File-based artifacts:

- `error_report_*.txt` считать legacy operational artifacts, а не canonical logging layer.

## Guardrails

1. Не добавлять новые cwd-dependent runtime paths без явного оправдания.
2. Для temp/state/log/backup paths prefer BASE_DIR-based or explicit absolute paths.
3. Не возвращать `python file.py` service launch style.
4. Не создавать новые runtime outputs внутри `app/services/`, если это можно избежать.
