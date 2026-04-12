# Backup And Restore

## Purpose

Зафиксировать текущий backup/restore flow проекта на основе реальных shell-скриптов и existing production docs.

## Scope

Документ покрывает:

- локальный PostgreSQL backup;
- retention;
- optional offsite copy;
- Google Drive upload hook;
- notification hook;
- restore flow и базовую проверку результата.

Документ не покрывает:

- server timer/unit configuration;
- политику disaster recovery за пределами этих скриптов;
- backup других артефактов кроме PostgreSQL dump.

## High-level Overview

Текущий backup flow реализован в [scripts/backup/backup_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/backup_db.sh):

1. создаёт gzip-compressed dump базы `inventory_db`
2. сохраняет файл в `/root/inventory/backups`
3. удаляет локальные backup-файлы старше 7 дней
4. при наличии remote env выполняет `scp` offsite copy
5. пытается загрузить backup в Google Drive
6. пытается отправить notification о результате

Restore flow реализован в [scripts/backup/restore_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/restore_db.sh).

## Backup

### Main script

```bash
cd /root/inventory
chmod +x scripts/backup/backup_db.sh
./scripts/backup/backup_db.sh
```

Подтверждённые runtime параметры из скрипта:

- backup directory: `/root/inventory/backups`
- DB name: `inventory_db`
- filename format: `backup_YYYY-MM-DD_HH-MM-SS.sql.gz`
- retention: 7 дней

### Actual flow

Скрипт делает:

```bash
pg_dump -U postgres inventory_db | gzip > /root/inventory/backups/backup_YYYY-MM-DD_HH-MM-SS.sql.gz
```

После успешного создания:

- удаляет старые `backup_*.sql.gz` старше `RETENTION_DAYS=7`
- при заданных `BACKUP_REMOTE_HOST` и `BACKUP_REMOTE_PATH` вызывает `scp`
- вызывает `upload_to_gdrive.py`
- вызывает `notify_backup.py success <file>`

Если backup падает:

- trap на `ERR` вызывает `notify_backup.py error "backup failed"`

### Offsite copy

Offsite copy выполняется только если заданы обе переменные:

- `BACKUP_REMOTE_HOST`
- `BACKUP_REMOTE_PATH`

В этом случае используется:

```bash
scp "${BACKUP_FILE}" "${BACKUP_REMOTE_HOST}:${BACKUP_REMOTE_PATH}/"
```

Если хотя бы одна переменная не задана, offsite copy пропускается.

### Google Drive upload

После локального backup скрипт пытается выполнить:

```bash
/root/inventory/.venv/bin/python /root/inventory/scripts/backup/upload_to_gdrive.py "${BACKUP_FILE}" || true
```

Подтверждённые условия для upload:

- должен существовать файл `google_set/credentials.json`
- должен быть задан `GOOGLE_DRIVE_BACKUP_FOLDER_ID`
- используется service account credentials
- scope: `https://www.googleapis.com/auth/drive.file`

Operational note:

- Google Drive upload не ломает backup whole-flow, потому что вызов завершается с `|| true`.

### Notifications

Скрипт уведомлений:

- success: `notify_backup.py success /path/to/file`
- error: `notify_backup.py error "message"`

Success notification содержит:

- имя файла
- размер файла в MB

Error notification содержит:

- текст ошибки

Operational note:

- notification attempt тоже не ломает already-created backup on success path.

## Restore

### Main script

```bash
cd /root/inventory
chmod +x scripts/backup/restore_db.sh
./scripts/backup/restore_db.sh /root/inventory/backups/backup_YYYY-MM-DD_HH-MM-SS.sql.gz test_restore
```

Подтверждённое поведение:

- принимает ровно два аргумента: backup path и target DB
- поддерживает `.sql.gz` и `.sql`
- если target DB не существует, создаёт её через `createdb`
- если target DB уже существует, выполняет restore в существующую БД

Restore commands by format:

```bash
gunzip -c backup.sql.gz | psql -U postgres -d target_db
psql -U postgres -d target_db < backup.sql
```

## Verification

После backup:

1. проверить наличие нового файла в `/root/inventory/backups`
2. проверить, что dump читается:

```bash
gunzip -c /root/inventory/backups/backup_YYYY-MM-DD_HH-MM-SS.sql.gz | head
```

3. если используется offsite copy, проверить наличие файла на удалённом хосте
4. если используется GDrive upload, проверить наличие файла в backup folder Google Drive

После restore:

```bash
psql -U postgres -d test_restore -c "\dt"
```

Дополнительно можно проверить размер и список таблиц:

```bash
psql -U postgres -d test_restore -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';"
```

## Production Restore Warning

Production restore - high-risk operation.

Минимальный безопасный порядок:

1. Остановить scheduler-ы и backend services, которые могут писать в БД.
2. Проверить отсутствие активных процессов записи в БД.
3. Сделать дополнительный свежий backup перед restore.
4. Выполнить restore только после этого.
5. Проверить таблицы и базовую доступность восстановленной БД.
6. Включать сервисы поэтапно: сначала backend/API, затем scheduler-ы по одному.

## Source Of Truth

- [scripts/backup/backup_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/backup_db.sh)
- [scripts/backup/restore_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/restore_db.sh)
- [scripts/backup/upload_to_gdrive.py](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/upload_to_gdrive.py)
- [scripts/backup/notify_backup.py](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/notify_backup.py)
- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md)

## Operational Notes

- `README_PROD.md` говорит о nightly backup через `systemd timer` в `03:00`, но timer/unit files отсутствуют в repo. Это external server truth.
- Скрипты жёстко ожидают production-like layout `/root/inventory/...`.
- `PGUSER` используется только в restore script; backup script жёстко использует `-U postgres`.

## Known Limitations / Risks

- Restore в существующую БД может привести к конфликтам и перезаписи данных.
- Backup не покрывает application files, `.env`, runtime state или external services.
- Offsite copy зависит от SSH/network availability.
- Google Drive upload зависит от credentials и `GOOGLE_DRIVE_BACKUP_FOLDER_ID`.
- Failure в GDrive upload или notification не фейлит успешный local backup, поэтому эти шаги нужно проверять отдельно.
