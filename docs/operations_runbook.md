# Operations Runbook

## Purpose

Зафиксировать первую волну production-операций для `inventory_service` на основе кода и существующего `README_PROD.md`.

## Scope

Документ покрывает:

- deploy backend и frontend;
- restart matrix по основным сервисам;
- базовые health checks;
- где смотреть логи;
- что в production считается external server truth.

Документ не покрывает:

- полную серверную конфигурацию;
- содержимое `.env`;
- recovery сценарии по каждому supplier-specific контуру;
- точные `systemd` unit definitions, потому что они не хранятся в repo.

## High-level Overview

Production shape, подтверждённый кодом и текущими docs:

- backend запускается как FastAPI app из `app.main:app`;
- часть работы выполняется отдельными scheduler/service процессами;
- frontend собирается из `admin-panel/` и обслуживается вне dev server;
- backup/restore выполняются отдельными shell-скриптами в `scripts/backup/`.

`README_PROD.md` описывает production команды, но имена `systemd` unit-ов и nginx serving path являются external server truth: они задокументированы, но сами unit-файлы и nginx config в репозитории отсутствуют.

## Quick Commands

```bash
sudo systemctl status fastapi.service --no-pager -l
sudo systemctl status nginx --no-pager -l
sudo journalctl -u fastapi.service -f -o short-iso
sudo journalctl -u stock_scheduler.service -f
sudo journalctl -u order_scheduler.service -f
sudo journalctl -u master-catalog-scheduler.service -f
sudo systemctl restart fastapi.service
sudo systemctl restart stock_scheduler.service
sudo systemctl restart order_scheduler.service
sudo systemctl restart master-catalog-scheduler.service
sudo systemctl restart business_stock_scheduler.service
```

## Deploy

### Backend deploy

Без миграций:

```bash
cd /root/inventory
source .venv/bin/activate
git pull origin main
pip install -r requirements.txt
sudo systemctl restart fastapi.service
```

С миграциями:

```bash
cd /root/inventory
source .venv/bin/activate
git pull origin main
python -m alembic upgrade head
sudo systemctl restart fastapi.service
```

Если изменялись `systemd` unit-файлы на сервере:

```bash
sudo systemctl daemon-reload
```

Operational note:

- `create_tables()` вызывается на startup в [app/main.py](/Users/dmitrijnazdrin/inventory_service_1/app/main.py), но schema source of truth для production остаётся Alembic, а не implicit table creation.

### Frontend deploy

Подтверждённый documented flow:

```bash
cd /root/inventory/admin-panel
npm install
npm run build
sudo rm -rf /usr/share/nginx/html/*
sudo cp -r build/* /usr/share/nginx/html/
sudo nginx -t
sudo systemctl restart nginx
```

Operational notes:

- `npm start` используется только для локальной разработки и не обновляет production frontend.
- Путь `/usr/share/nginx/html` и факт обслуживания через nginx - external server truth из [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md).

## Restart Services

Подтверждённые application entrypoints:

- API: `app.main:app`
- catalog scheduler: `python -m app.services.catalog_scheduler_service`
- stock scheduler: `python -m app.services.stock_scheduler_service`
- business stock: `python -m app.services.business_stock_scheduler_service`
- order scheduler: `python -m app.services.order_scheduler_service`
- competitor pricing: `python -m app.services.competitor_price_scheduler`
- master catalog scheduler: `python -m app.services.master_catalog_scheduler_service`
- Biotus scheduler: `python -m app.services.biotus_check_order_scheduler`
- Tabletki cancel retry: `python -m app.services.tabletki_cancel_retry_service`
- Telegram bot: `python -m app.services.telegram_bot`

Documented production restarts:

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
```

External server truth:

- соответствие между Python entrypoints и unit names нужно подтверждать на сервере;
- в repo нет `*.service` или `*.timer` файлов.

## Health Checks

### Backend

Минимальные проверки после deploy/restart:

```bash
curl http://164.92.213.254:8000/
```

Что подтверждает код:

- корневой endpoint `/` существует в [app/main.py](/Users/dmitrijnazdrin/inventory_service_1/app/main.py)
- login endpoint расположен под `/developer_panel/login/` в [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)

Operational note:

- Для login в коде подтверждён `POST`, поэтому базовым probe здесь зафиксирован только корневой `/`.
- Для production use безопаснее дополнять probe проверкой логов сервиса после startup.

### Frontend

Минимальные проверки:

- открыть documented app URL `http://164.92.213.254`
- проверить загрузку главной страницы после `nginx` restart

### Database and migrations

После миграций:

```bash
cd /root/inventory
source .venv/bin/activate
python -m alembic current
```

Если нужен базовый smoke-check БД:

```bash
psql -U postgres -d inventory_db -c "\dt"
```

## Logs

Documented production log commands:

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
sudo journalctl -u telegram_bot -f
```

Repo-confirmed local log artifact:

- `LOG_DIR` используется в [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- documented default local file path: `./logs/salesdrive_webhook.log`

Operational note:

- Основной production log transport по docs - `journalctl`.
- File-based logs в repo не покрывают все процессы и не должны считаться единственным production source.

## Environment Change Impact

Подтверждённые restart hints из existing docs:

- `MASTER_*` -> restart `master-catalog-scheduler.service`
- `BIOTUS_*` -> restart `biotus_check_order_scheduler.service`
- `TABLETKI_*` -> restart `tabletki-cancel-retry.service`
- stock/catalog env -> restart соответствующего scheduler

Дополнительный grounded note:

- изменения `.env` влияют не только на scheduler-ы, но и на FastAPI/API behavior, если переменные читаются в request/runtime code paths;
- `business_settings`-driven behavior может менять runtime без изменения `.env`, если DB row существует.

## Production Paths And Addresses

Документированные полезные пути:

- project root: `/root/inventory`
- frontend source: `/root/inventory/admin-panel`
- runtime cache: `/root/inventory/state_cache`
- backups: `/root/inventory/backups`

Документированные адреса:

- app: [http://164.92.213.254](http://164.92.213.254)
- backend: [http://164.92.213.254:8000](http://164.92.213.254:8000)
- developer login: [http://164.92.213.254:8000/developer_panel/login](http://164.92.213.254:8000/developer_panel/login)

## Emergency Recovery (minimal flow)

1. Проверить API:

```bash
curl http://164.92.213.254:8000/
```

2. Проверить PostgreSQL:

```bash
psql -U postgres -d inventory_db -c "\dt"
```

3. Проверить ключевые scheduler-ы:

```bash
sudo systemctl status stock_scheduler.service --no-pager -l
sudo systemctl status order_scheduler.service --no-pager -l
sudo systemctl status master-catalog-scheduler.service --no-pager -l
```

4. Проверить наличие свежего backup в `/root/inventory/backups`.
5. Выполнить restore в test DB через `scripts/backup/restore_db.sh`.
6. Проверить таблицы в test DB:

```bash
psql -U postgres -d test_restore -c "\dt"
```

7. Только после проверки test restore принимать решение о production restore.

## Do Not Touch

- `state_cache/` - runtime state scheduler-ов и очередей; ручная очистка может сломать текущий stateful flow.
- `temp/` - временные runtime artifacts; не использовать как source of truth и не трогать без понимания активного процесса.
- `logs/` - operational evidence; не чистить во время инцидента до сбора симптомов.
- `backups/` - не удалять и не переписывать вручную, кроме штатной ротации и осознанных backup operations.

## Source Of Truth

- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md)
- [README.md](/Users/dmitrijnazdrin/inventory_service_1/README.md)
- [app/main.py](/Users/dmitrijnazdrin/inventory_service_1/app/main.py)
- [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- [app/services](/Users/dmitrijnazdrin/inventory_service_1/app/services)
- [scripts/backup/backup_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/backup_db.sh)
- [scripts/backup/restore_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/restore_db.sh)

## Operational Notes

- `systemd`, nginx и cron/timer wiring partially documented, but not versioned in this repo.
- В production troubleshooting сначала нужно различать API issue, scheduler issue и external integration issue; у них разные entrypoints и разные restart targets.
- `state_cache/` - runtime state, не документировать его как config source.

## Known Limitations / Risks

- Имена production unit-ов нельзя считать fully verified without server inspection.
- Health checks в этом документе ограничены тем, что подтверждено кодом и existing docs; полноценного `/health` endpoint в repo нет.
- Если на сервере используется дополнительная orchestration logic вне repo, этот документ её не видит.
- `README_PROD.md` остаётся краткой operational шпаргалкой; при расхождении между server reality и repo docs нужно подтверждать production state отдельно.
