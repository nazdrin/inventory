# Backup and Restore

## Backup

- Основной backup создаётся скриптом `/root/inventory/scripts/backup/backup_db.sh`.
- Формат файла: `backup_YYYY-MM-DD_HH-MM-SS.sql.gz`.
- Локальное хранилище: `/root/inventory/backups`.
- Источник данных: PostgreSQL database `inventory_db`.
- Схема: `pg_dump -> gzip -> local file -> optional scp offsite copy`.
- Ротация: локальные backup-файлы старше 7 дней удаляются автоматически.

## Restore

- Restore выполняется скриптом `/root/inventory/scripts/backup/restore_db.sh`.
- Поддерживаются файлы `.sql.gz` и `.sql`.
- Целевая БД передаётся вторым аргументом.
- Если БД не существует, скрипт создаёт её через `createdb`.
- Если БД уже существует, restore выполняется в существующую БД.

Пример:

```bash
./scripts/backup/restore_db.sh /root/inventory/backups/backup_2026-04-11_03-00-00.sql.gz test_restore
```

## Где хранится

- Локально: `/root/inventory/backups`
- Offsite: `${BACKUP_REMOTE_HOST}:${BACKUP_REMOTE_PATH}` при заданных env-переменных

## Риски

- Restore в существующую БД может перезаписать данные и привести к конфликтам объектов.
- Backup не защищает от логических ошибок внутри уже сохранённых данных.
- Offsite copy зависит от доступности сети, SSH-доступа и прав на удалённый каталог.
- При restore в production БД требуется отдельное окно обслуживания и подтверждённый rollback plan.

## Как проверить

1. Запустить backup-скрипт вручную и убедиться, что новый файл появился в `/root/inventory/backups`.
2. Проверить, что backup открывается: `gunzip -c backup.sql.gz | head`.
3. Восстановить backup в тестовую БД, например `test_restore`.
4. Проверить список таблиц:

```bash
psql -U postgres -d test_restore -c "\dt"
```

5. При использовании offsite backup проверить наличие файла на удалённом хосте.
