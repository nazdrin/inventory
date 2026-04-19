# Runtime Cleanup Audit

## Purpose

Зафиксировать read-only audit файловой структуры production deployment `inventory_service` на сервере `/root/inventory` и выделить:

- явные временные и accidental артефакты;
- runtime cache/state;
- server-local legacy/double runtime trees;
- symlink-ы и подозрительные директории.

Документ не означает, что перечисленные файлы нужно удалять автоматически. Это только карта для последующей ручной верификации.

## Scope

Проверены:

- `/root/inventory`
- вложенные директории проекта
- `git status` на сервере
- symlink-ы
- runtime directories `temp`, `state_cache`, `backups`, `app/services/input_raw`
- server-local runtime subtrees внутри `app/services`

При классификации использованы:

- server filesystem inventory;
- repo references в коде и docs;
- `.gitignore`.

## 1. Safe To Delete

### `/root/inventory/ystemctl status backup_db.service --no-pager -l`

- Reason:
  - это случайный файл с именем shell-команды;
  - размер `359B`;
  - появился как результат accidental terminal output;
  - не найдено ни одного code/doc reference.
- Risk:
  - низкий.

### `/root/inventory/.DS_Store`

- Reason:
  - macOS metadata file;
  - не используется Linux runtime;
  - уже игнорируется repo.
- Risk:
  - низкий.

### `/root/inventory/app/.DS_Store`

- Reason:
  - macOS metadata file внутри source tree;
  - не участвует в runtime.
- Risk:
  - низкий.

### `__pycache__` directories

- Paths:
  - `/root/inventory/alembic/__pycache__`
  - `/root/inventory/app/__pycache__`
  - `/root/inventory/app/services/__pycache__`
- Reason:
  - стандартный Python bytecode cache;
  - безопасно регенерируется;
  - не является source of truth.
- Risk:
  - низкий, но на running server лучше чистить только осознанно и не во время диагностики инцидента.

## 2. Needs Verification

### `/root/inventory/scripts/backup/backup_db.sh.WORKING`

- Reason:
  - явный working-copy артефакт рядом с production backup script;
  - расширение `.WORKING` указывает на manual draft/backup;
  - code references отсутствуют.
- Risk:
  - средний, потому что файл лежит рядом с критичным backup path и может быть чьей-то ручной страховкой.

### `/root/inventory/backup_db.sh`

- Reason:
  - symlink: `/root/inventory/backup_db.sh -> /root/inventory/scripts/backup/backup_db.sh`;
  - текущий backup unit уже смотрит напрямую на `scripts/backup/backup_db.sh`;
  - symlink остаётся untracked на сервере.
- Risk:
  - средний: похоже на legacy compatibility shim, но возможен ручной operational use.

### `/root/inventory/.env.bak.2026-03-21-123052`

- Reason:
  - backup `.env`;
  - не является текущей конфигурацией;
  - содержит чувствительные данные.
- Risk:
  - средний: operationally useful as rollback artifact, but security-sensitive and easy to забыть.

### `/root/inventory/temp`

- Size:
  - около `114M`
- Reason:
  - repo/docs прямо называют `temp/` runtime artifacts;
  - внутри много enterprise-specific `catalog.json`, `stock.json`, debug snapshots, converted files, csv originals;
  - есть явные артефакты типа `stock.json.save`.
- Risk:
  - средний: директория явно не source of truth, но часть процессов пишет туда во время работы; без понимания lifecycle чистить нельзя.

### `/root/inventory/app/services/temp`

- Size:
  - около `7.2M`
- Reason:
  - server-local duplicate runtime tree внутри `app/services`;
  - содержит тот же тип артефактов (`catalog.json`, `stock.json`) что и top-level `temp/`;
  - в repo как canonical runtime path не описан.
- Risk:
  - высокий: похоже на legacy duplicate of runtime temp storage, но без верификации нельзя понять, жив ли ещё этот path.

### `/root/inventory/app/services/state_cache`

- Size:
  - около `2.1M`
- Reason:
  - server-local duplicate of top-level `state_cache/`;
  - содержит runtime state snapshots;
  - top-level `state_cache/` documented, а `app/services/state_cache/` в docs не фигурирует как canonical path.
- Risk:
  - высокий: вероятный legacy drift, но может использоваться кодом с `cwd=app/services`.

### `/root/inventory/app/services/input_raw`

- Size:
  - около `6.4M`
- Reason:
  - untracked raw payload storage;
  - код в `app/key_crm_data_service/key_crm_catalog_conv.py` и `key_crm_stock_conv.py` пишет `input_raw/<enterprise>` через `os.getcwd()`;
  - при `WorkingDirectory=/root/inventory/app/services` это как раз materializes в `app/services/input_raw`.
- Risk:
  - высокий: выглядит как runtime raw capture, а не мусор.

### `/root/inventory/app/services/exports`

- Size:
  - около `98M`
- Contents:
  - `catalog_2547.json`
  - `master_catalog_tabletki_2547.json`
- Reason:
  - server-local export storage inside `app/services`;
  - `exports/` игнорируется repo;
  - нужно отдельно понять, является ли это transient output или рабочим handoff path.
- Risk:
  - высокий: может быть downstream operational artifact.

### `/root/inventory/app/services/.salesdrive_category_cache_2547.json`

### `/root/inventory/app/services/.salesdrive_master_catalog_cache_2547.json`

- Reason:
  - hidden cache files;
  - код подтверждает generation через `os.getcwd()`:
    - `app/business/salesdrive_category_exporter.py`
    - `app/business/salesdrive_master_catalog_exporter.py`
  - при текущих server units это materializes inside `app/services`.
- Risk:
  - средний: это cache, но он явно связан с export flow.

### `/root/inventory/app/services/error_report_*.txt`

- Reason:
  - большое количество historical error reports вне `logs/`;
  - код scheduler-ов подтверждает генерацию `error_report_<timestamp>.txt` в current working directory;
  - на production их накопилось очень много.
- Risk:
  - средний: это candidate for cleanup policy, но сначала нужно понять, используются ли они как operational evidence/archive.

### `/root/inventory/app/services/catalog-Zoomagazin.json`

- Reason:
  - standalone JSON рядом с service code;
  - code reference не найден;
  - по имени похоже на manual/debug artifact.
- Risk:
  - средний: может быть забытый legacy sample/input.

### `/root/inventory/app/services/stock_logs.json`

- Reason:
  - файл игнорируется `.gitignore`;
  - code reference в текущем repo не найден;
  - лежит вне `logs/`.
- Risk:
  - средний: вероятный legacy log/output file.

### `/root/inventory/admin-panel/build`

- Size:
  - около `1.7M`
- Reason:
  - generated frontend build;
  - docs говорят, что production frontend обслуживается из `/usr/share/nginx/html`, а не напрямую из `admin-panel/build`;
  - build можно пересобрать.
- Risk:
  - средний: likely regenerable, но может использоваться как staging area при ручном deploy.

### `/root/inventory/admin-panel/node_modules`

- Size:
  - около `396M`
- Reason:
  - generated dependency tree;
  - not source of truth;
  - нужен только для future `npm install` / `npm run build` workflows.
- Risk:
  - средний: large disk consumer, но может быть operational convenience for manual frontend deploys.

## 3. Must Keep

### `/root/inventory/backups`

- Size:
  - около `292M`
- Reason:
  - documented production backup location;
  - содержит реальные PostgreSQL dumps;
  - backup flow и restore docs опираются на этот путь.
- Risk:
  - высокий: manual deletion directly reduces recovery options.

### `/root/inventory/state_cache`

- Size:
  - около `448K`
- Reason:
  - прямо задокументирован как runtime cache/state path;
  - code references подтверждают использование:
    - `master_catalog_scheduler.lock`
    - `master_catalog_scheduler_state.json`
    - `tabletki_cancel_retry_queue.json`
    - feed/drop state files
  - AGENTS explicitly says treat `state_cache/` as runtime cache, not source code.
- Risk:
  - высокий: ручная очистка может ломать scheduler state и retry queues.

### `/root/inventory/logs`

- Reason:
  - canonical log directory, even though currently small;
  - documented runtime path.
- Risk:
  - низкий как storage consumer, но keep as standard path.

### `/root/inventory/scripts/backup/backup_db.sh`

- Reason:
  - active production backup entrypoint.
- Risk:
  - критичный.

### `/root/inventory/scripts/backup/restore_db.sh`

- Reason:
  - documented restore entrypoint.
- Risk:
  - высокий.

### `/root/inventory/google_set/credentials.json`

- Reason:
  - backup/upload and Google integrations depend on credentials.
- Risk:
  - высокий operational risk and high security sensitivity.

### `/root/inventory/.env`

- Reason:
  - current production environment file.
- Risk:
  - критичный.

## 4. Symlinks Analysis

## Meaningful symlink

### `/root/inventory/backup_db.sh -> /root/inventory/scripts/backup/backup_db.sh`

- Purpose:
  - looks like compatibility shim after backup path migration.
- Confirmed usage:
  - current `backup_db.service` no longer needs it, because unit points directly to `/root/inventory/scripts/backup/backup_db.sh`.
- Classification:
  - `Needs verification`
- Risk:
  - medium, because it may still be used manually by operators or old notes.

## Routine internal symlinks

### `.venv` symlinks

- Examples:
  - `.venv/bin/python -> python3`
  - `.venv/bin/python3 -> /usr/bin/python3`
  - `.venv/lib64 -> lib`
- Classification:
  - `Must keep`
- Risk:
  - high if touched; these are normal virtualenv internals.

### `admin-panel/node_modules/.bin/*` symlinks

- Purpose:
  - standard npm tool shims.
- Classification:
  - follow `admin-panel/node_modules` classification.
- Risk:
  - low individually, but not useful to clean selectively.

## 5. Suspicious Directories

### `/root/inventory/temp`

- Reason:
  - top-level runtime temp store is expected, but contains many historical enterprise artifacts and debug variants.
- Risk:
  - medium.

### `/root/inventory/app/services/temp`

- Reason:
  - duplicate runtime temp subtree under service working directory;
  - strongly suggests old `cwd`-dependent writes are still materializing there.
- Risk:
  - high.

### `/root/inventory/state_cache`

- Reason:
  - canonical runtime state directory;
  - not cleanup target without workflow-aware policy.
- Risk:
  - high.

### `/root/inventory/app/services/state_cache`

- Reason:
  - duplicate state cache subtree under `app/services`;
  - likely legacy/runtime drift from `cwd`-dependent code.
- Risk:
  - high.

### `/root/inventory/app/services/input_raw`

- Reason:
  - raw captured payloads written by KeyCRM-related converters via `os.getcwd()`.
- Risk:
  - high.

### `/root/inventory/app/services/exports`

- Reason:
  - export payloads are accumulating under service code directory;
  - unclear whether they are transient, handoff files, or archive.
- Risk:
  - high.

### `/root/inventory/admin-panel/build`

- Reason:
  - generated frontend build artifact;
  - not current serving path by itself.
- Risk:
  - medium.

### `/root/inventory/admin-panel/node_modules`

- Reason:
  - regenerated dependency tree; large disk consumer.
- Risk:
  - medium.

## Key Findings

1. The clearest accidental garbage on server is:
   - `ystemctl status backup_db.service --no-pager -l`
   - `.DS_Store` files

2. The main runtime cleanup risk is not top-level `temp/` or `state_cache/`, but duplicated runtime trees under:
   - `app/services/temp`
   - `app/services/state_cache`
   - `app/services/input_raw`
   - `app/services/exports`

3. The main legacy-warning around backups is:
   - `backup_db.sh` symlink in project root
   - `scripts/backup/backup_db.sh.WORKING`

4. `app/services/error_report_*.txt` indicates long-running accumulation of error artifacts outside `logs/`, but they should be reviewed before any cleanup policy.

## Recommended Next Step

Before any cleanup action:

1. confirm which server-local paths are still written by active services:
   - `app/services/temp`
   - `app/services/state_cache`
   - `app/services/input_raw`
   - `app/services/exports`
2. diff `scripts/backup/backup_db.sh.WORKING` against the active backup script;
3. define explicit retention rules for:
   - `temp/`
   - `error_report_*.txt`
   - corrupted queue snapshots in `state_cache/`
4. only after that prepare a manual cleanup plan.
