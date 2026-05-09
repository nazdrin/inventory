# Inventory Service: Prod Runbook

Короткая памятка по прод-серверу.

## Подключение

```bash
ssh root@164.92.213.254
cd /root/inventory
source .venv/bin/activate
```

## Типовой деплой backend

Без миграций:

```bash
git pull origin main
pip install -r requirements.txt
sudo systemctl restart fastapi.service
```

С миграциями:

```bash
git pull origin main
source .venv/bin/activate
python -m alembic upgrade head
sudo systemctl restart fastapi.service
```

Если менялись unit-файлы:

```bash
sudo systemctl daemon-reload
```

## Frontend deploy

`npm start` не обновляет прод. Продовый frontend обслуживается из `/usr/share/nginx/html`.

```bash
cd /root/inventory/admin-panel
npm install
npm run build
sudo rm -rf /usr/share/nginx/html/*
sudo cp -r build/* /usr/share/nginx/html/
sudo nginx -t
sudo systemctl restart nginx
```

## Основные сервисы

Перезапуск:

```bash
sudo systemctl restart fastapi.service
sudo systemctl restart nginx
sudo systemctl restart stock_scheduler.service
sudo systemctl restart catalog_scheduler.service
sudo systemctl restart order_scheduler.service
sudo systemctl restart competitor_price_scheduler.service
sudo systemctl restart telegram_bot
sudo systemctl restart biotus_check_order_scheduler.service
sudo systemctl restart backup_db.service
sudo systemctl restart master-catalog-scheduler.service
sudo systemctl restart business_stock_scheduler.service
sudo systemctl restart tabletki-cancel-retry.service
sudo systemctl restart checkbox-receipt-retry.service
sudo systemctl restart checkbox-shift-scheduler.service
```

Статус и логи:

```bash
sudo systemctl status fastapi.service --no-pager -l
sudo systemctl status nginx --no-pager -l
sudo journalctl -u fastapi.service -f -o short-iso
sudo journalctl -u nginx -f
sudo journalctl -u stock_scheduler.service -f
sudo journalctl -u catalog_scheduler.service -f
sudo journalctl -u order_scheduler.service -f
sudo journalctl -u competitor_price_scheduler.service -f
sudo journalctl -u biotus_check_order_scheduler.service -f
sudo journalctl -u business_stock_scheduler.service -f
sudo journalctl -u master-catalog-scheduler.service -f
sudo journalctl -u tabletki-cancel-retry.service -f
sudo journalctl -u checkbox-receipt-retry.service -f
sudo journalctl -u checkbox-shift-scheduler.service -f
sudo journalctl -u telegram_bot -f
```

## Backup

```bash
/root/inventory/backups
```

- Backup-файлы лежат в `/root/inventory/backups`
- Формат имени: `backup_YYYY-MM-DD_HH-MM-SS.sql.gz`
- Запуск: `systemd timer` в `03:00`
- Основной скрипт: `/root/inventory/scripts/backup/backup_db.sh`

Ручной запуск:

```bash
cd /root/inventory
chmod +x scripts/backup/backup_db.sh
./scripts/backup/backup_db.sh
```

Если заданы `BACKUP_REMOTE_HOST` и `BACKUP_REMOTE_PATH`, после локального backup будет выполнен offsite copy через `scp`.

Если задан `GOOGLE_DRIVE_BACKUP_FOLDER_ID`, backup дополнительно загружается в Google Drive. Cloud retention управляется через `GOOGLE_DRIVE_BACKUP_RETENTION_COUNT` и по умолчанию оставляет последние `5` backup-файлов в Google Drive.

## Restore

Предупреждение:

- Restore может перезаписать данные
- Всегда сначала тестировать восстановление в test DB

Создать БД:

```bash
createdb -U postgres test_restore
```

Восстановление:

```bash
gunzip -c backup.sql.gz | psql -U postgres -d test_restore
```

Проверка:

```bash
psql -U postgres -d test_restore -c "\dt"
```

Через скрипт:

```bash
cd /root/inventory
chmod +x scripts/backup/restore_db.sh
./scripts/backup/restore_db.sh /root/inventory/backups/backup_YYYY-MM-DD_HH-MM-SS.sql.gz test_restore
```

## Работа с `.env`

Перед изменением:

```bash
cd /root/inventory
cp .env .env.bak.$(date +%F-%H%M%S)
nano .env
```

После изменения переменных нужно перезапускать только затронутые сервисы:

- `MASTER_*` -> `master-catalog-scheduler.service`
- `BIOTUS_*` -> `biotus_check_order_scheduler.service`
- `TABLETKI_*` -> `tabletki-cancel-retry.service`
- stock/catalog env -> соответствующий scheduler

## Полезные пути и адреса

- Проект: `/root/inventory`
- Frontend source: `/root/inventory/admin-panel`
- Runtime cache: `/root/inventory/state_cache`
- Backups: `/root/inventory/backups`
- App: [http://164.92.213.254](http://164.92.213.254)
- Backend: [http://164.92.213.254:8000](http://164.92.213.254:8000)
- Developer login: [http://164.92.213.254:8000/developer_panel/login](http://164.92.213.254:8000/developer_panel/login)
