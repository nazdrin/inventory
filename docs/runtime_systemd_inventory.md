# Runtime Systemd Inventory

## 1. Purpose

Зафиксировать подтверждённую карту production runtime wiring для `inventory_service` на основе:

- repo entrypoints и существующей документации;
- read-only inventory production server;
- текущих `systemd` service/timer definitions и фактически запущенных процессов.

Документ нужен как grounded baseline перед этапом stabilization / runtime cleanup.

## Runtime Standard

После runtime stabilization canonical standard для Python services такой:

- launch only via `python -m app.services.<module>`
- `WorkingDirectory=/root/inventory`
- `PYTHONPATH=/root/inventory`
- `EnvironmentFile=/root/inventory/.env`

Exceptions:

- `fastapi.service` stays on `uvicorn app.main:app`
- `backup_db.service` stays on shell script execution

## 2. Scope

Документ покрывает:

- application-related `systemd` services и timers на production server;
- их статус, `ExecStart`, `WorkingDirectory`, `EnvironmentFile`, `Restart` policy;
- связь unit -> repo entrypoint;
- расхождения между repo/docs и server wiring;
- legacy / suspicious runtime signals, которые стоит разбирать отдельно.

Документ не покрывает:

- nginx configuration details;
- содержимое `.env`;
- supplier-specific runtime logic внутри scheduler-ов;
- изменение или cleanup unit-файлов.

## 3. Active Services Inventory

### fastapi.service

- Unit name: `fastapi.service`
- Status: `active (running)`, `enabled`
- Purpose: основной FastAPI backend / developer panel API
- ExecStart: `/root/inventory/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000`
- WorkingDirectory: `/root/inventory`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `100ms`
- Timer relation: нет
- Repo entrypoint: [app/main.py](/Users/dmitrijnazdrin/inventory_service_1/app/main.py)
- Notes / mismatch / risk:
  - wiring совпадает с repo и `README_PROD.md`;
  - repo не хранит сам `fastapi.service` unit-файл.

### stock_scheduler.service

- Unit name: `stock_scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: основной stock ingestion scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/stock_scheduler_service.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `100ms`
- Timer relation: нет
- Repo entrypoint: [app/services/stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/stock_scheduler_service.py)
- Notes / mismatch / risk:
  - wiring совпадает с documented runtime;
  - используется прямой запуск `.py`, хотя repo docs часто описывают `python -m ...`;
  - в unit есть закомментированные legacy строки `DATABASE_URL` и старый пример `ExecStart`.

### catalog_scheduler.service

- Unit name: `catalog_scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: основной catalog ingestion scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/catalog_scheduler_service.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `100ms`
- Timer relation: нет
- Repo entrypoint: [app/services/catalog_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/catalog_scheduler_service.py)
- Notes / mismatch / risk:
  - wiring совпадает с repo entrypoint;
  - в unit сохранены закомментированные legacy строки `DATABASE_URL` и альтернативный `ExecStart`.

### order_scheduler.service

- Unit name: `order_scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: order fetch / processing scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/order_scheduler_service.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `5s`
- Timer relation: нет
- Repo entrypoint: [app/services/order_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/order_scheduler_service.py)
- Notes / mismatch / risk:
  - wiring совпадает с repo entrypoint;
  - unit-файл отсутствует в repo.

### competitor_price_scheduler.service

- Unit name: `competitor_price_scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: competitor price loading scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/competitor_price_scheduler.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `5s`
- Timer relation: нет
- Repo entrypoint: [app/services/competitor_price_scheduler.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/competitor_price_scheduler.py)
- Notes / mismatch / risk:
  - wiring совпадает с repo entrypoint;
  - unit-файл отсутствует в repo.

### biotus_check_order_scheduler.service

- Unit name: `biotus_check_order_scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: Biotus order/status follow-up scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/biotus_check_order_scheduler.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `3s`
- Timer relation: нет
- Repo entrypoint: [app/services/biotus_check_order_scheduler.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/biotus_check_order_scheduler.py)
- Notes / mismatch / risk:
  - wiring совпадает с repo entrypoint;
  - unit-файл отсутствует в repo.

### business_stock_scheduler.service

- Unit name: `business_stock_scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: отдельный Business stock scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/business_stock_scheduler_service.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `100ms`
- Timer relation: нет
- Repo entrypoint: [app/services/business_stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_stock_scheduler_service.py)
- Notes / mismatch / risk:
  - wiring совпадает с repo entrypoint;
  - сервис подтверждён и в repo docs, и на production server.

### master-catalog-scheduler.service

- Unit name: `master-catalog-scheduler.service`
- Status: `active (running)`, `enabled`
- Purpose: master catalog scheduler
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/master_catalog_scheduler_service.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `10s`
- Timer relation: нет
- Repo entrypoint: [app/services/master_catalog_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/master_catalog_scheduler_service.py)
- Notes / mismatch / risk:
  - server unit name использует kebab-case `master-catalog-scheduler.service`, а repo entrypoint и docs оперируют Python file name;
  - unit-файл отсутствует в repo.

### tabletki-cancel-retry.service

- Unit name: `tabletki-cancel-retry.service`
- Status: `active (running)`, `enabled`
- Purpose: retry loop для Tabletki cancel / warning flow
- ExecStart: `/root/inventory/.venv/bin/python -m app.services.tabletki_cancel_retry_service`
- WorkingDirectory: `/root/inventory`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `10s`
- Timer relation: нет
- Repo entrypoint: [app/services/tabletki_cancel_retry_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/tabletki_cancel_retry_service.py)
- Notes / mismatch / risk:
  - это единственный подтверждённый app service, который запускается через `-m`, а не через прямой путь к `.py`;
  - `WorkingDirectory=/root/inventory`, в отличие от большинства scheduler-ов;
  - `README_PROD.md` и repo docs подтверждают unit name, но repo не хранит сам unit-файл.

### telegram_bot.service

- Unit name: `telegram_bot.service`
- Status: `active (running)`, `disabled`
- Purpose: operational Telegram bot
- ExecStart: `/root/inventory/.venv/bin/python /root/inventory/app/services/telegram_bot.py`
- WorkingDirectory: `/root/inventory/app/services`
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=always`, restart delay `100ms`
- Timer relation: нет
- Repo entrypoint: [app/services/telegram_bot.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/telegram_bot.py)
- Notes / mismatch / risk:
  - runtime process активен, но unit state `disabled`;
  - это suspicious drift: сервис живёт в prod, но не включён в steady-state boot wiring;
  - `README_PROD.md` называет его `telegram_bot`, а фактический unit on server — `telegram_bot.service`.

### backup_db.service

- Unit name: `backup_db.service`
- Status: `inactive (dead)`, `static`
- Purpose: timer-triggered PostgreSQL backup job
- ExecStart: `/root/inventory/scripts/backup/backup_db.sh`
- WorkingDirectory: не задан
- EnvironmentFile: `/root/inventory/.env`
- Restart policy: `Restart=no`
- Timer relation: triggered by `backup_db.timer`
- Repo entrypoint: [scripts/backup/backup_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/backup_db.sh)
- Notes / mismatch / risk:
  - текущее server wiring уже смотрит на `scripts/backup/backup_db.sh`, что совпадает с repo и свежими docs;
  - unit дополнительно задаёт `Environment="PGPASSWORD=790318"` на сервере, что не хранится в repo docs как tracked unit truth;
  - `inactive (dead)` здесь не означает failure: по `systemctl show` last run завершился с `status=0` и service triggered by timer;
  - unit-файл отсутствует в repo.

## 4. Timers Inventory

### backup_db.timer

- Unit name: `backup_db.timer`
- Status: `active (waiting)`, `enabled`
- Purpose: nightly trigger для `backup_db.service`
- Timer target: `backup_db.service`
- Schedule: `OnCalendar=*-*-* 03:00:00`
- Persistent: `true`
- Repo entrypoint: indirectly maps to [scripts/backup/backup_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/backup_db.sh)
- Notes / mismatch / risk:
  - timer подтверждён на production server;
  - repo docs описывают nightly backup в `03:00`, что совпадает с server truth;
  - `*.timer` file отсутствует в repo.

## 5. Mapping: systemd unit -> repo entrypoint

| systemd unit | Repo entrypoint | Exists in repo | Notes |
| --- | --- | --- | --- |
| `fastapi.service` | `app/main.py` via `uvicorn app.main:app` | yes | matches repo/docs |
| `stock_scheduler.service` | `app/services/stock_scheduler_service.py` | yes | direct file exec |
| `catalog_scheduler.service` | `app/services/catalog_scheduler_service.py` | yes | direct file exec |
| `order_scheduler.service` | `app/services/order_scheduler_service.py` | yes | direct file exec |
| `competitor_price_scheduler.service` | `app/services/competitor_price_scheduler.py` | yes | direct file exec |
| `biotus_check_order_scheduler.service` | `app/services/biotus_check_order_scheduler.py` | yes | direct file exec |
| `business_stock_scheduler.service` | `app/services/business_stock_scheduler_service.py` | yes | direct file exec |
| `master-catalog-scheduler.service` | `app/services/master_catalog_scheduler_service.py` | yes | unit naming differs from file naming |
| `tabletki-cancel-retry.service` | `app/services/tabletki_cancel_retry_service.py` | yes | launched via `python -m` |
| `telegram_bot.service` | `app/services/telegram_bot.py` | yes | active but disabled |
| `backup_db.service` | `scripts/backup/backup_db.sh` | yes | timer-triggered oneshot-like flow |
| `backup_db.timer` | `scripts/backup/backup_db.sh` via `backup_db.service` | yes | nightly `03:00` |

## 6. Mismatches Between Server And Repo

### Repo does not track systemd units

Подтверждённый production runtime опирается на server-local unit files:

- `fastapi.service`
- `stock_scheduler.service`
- `catalog_scheduler.service`
- `order_scheduler.service`
- `competitor_price_scheduler.service`
- `biotus_check_order_scheduler.service`
- `business_stock_scheduler.service`
- `master-catalog-scheduler.service`
- `tabletki-cancel-retry.service`
- `telegram_bot.service`
- `backup_db.service`
- `backup_db.timer`

В repo `*.service` / `*.timer` files отсутствуют.

### README_PROD.md is only partially complete

`README_PROD.md` документирует основную runtime поверхность:

- `fastapi.service`
- scheduler services
- `backup_db.service`
- `master-catalog-scheduler.service`
- `business_stock_scheduler.service`
- `tabletki-cancel-retry.service`

Но он не фиксирует важные implementation details, которые живут только на server:

- реальные `WorkingDirectory`;
- restart delays;
- `telegram_bot.service` активен, но `disabled`;
- `backup_db.service` содержит server-local `PGPASSWORD` env line;
- direct-file execution vs `python -m` wiring differs per service.

### Docs mention active contours that are not wired as systemd units on server

Repo docs подтверждают дополнительные runtime contours:

- [app/services/balancer_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/balancer_scheduler_service.py)

Но production server inventory не показал `balancer_scheduler_service.service` или related timer. Это не значит, что contour obsolete, но как минимум он не входит в подтверждённый active systemd wiring на сервере.

### Backup documentation and server wiring are now aligned, but only externally

Свежие repo docs и current server unit совпадают по backup entrypoint:

- `/root/inventory/scripts/backup/backup_db.sh`

Однако это всё равно external truth, потому что:

- `backup_db.service` и `backup_db.timer` не хранятся в repo;
- unit содержит server-local auth wiring (`PGPASSWORD`) вне tracked configuration.

## 7. Legacy / Suspicious Wiring

### Commented legacy lines inside live units

В production unit-файлах обнаружены legacy/commented fragments:

- `stock_scheduler.service`:
  - commented `DATABASE_URL`
  - commented `ExecStart` fragment for `catalog_scheduler_service.py`
- `catalog_scheduler.service`:
  - commented `DATABASE_URL`
  - commented alternative `ExecStart` через `/bin/bash -c`

Это не active wiring, но это operational drift inside live server config.

### Mixed launch styles across services

На production server используются разные способы запуска:

- direct file execution:
  - `python /root/inventory/app/services/<name>.py`
- module execution:
  - `python -m app.services.tabletki_cancel_retry_service`
- uvicorn CLI:
  - `uvicorn app.main:app`

Это не ошибка само по себе, но это non-uniform runtime shape, которую нельзя считать self-documented только из repo.

### telegram_bot.service active but disabled

`telegram_bot.service`:

- `ActiveState=active`
- `UnitFileState=disabled`

Это один из strongest suspicious signals: текущий процесс живёт, но boot-time enablement не зафиксирован как steady-state systemd wiring.

### Server-only `PGPASSWORD` in backup unit

`backup_db.service` содержит:

- `Environment="PGPASSWORD=790318"`

Это operationally significant wiring detail, отсутствующий в tracked unit definitions, потому что tracked unit definitions отсутствуют в repo вообще.

### No production systemd tracking for balancer

Repo содержит [app/services/balancer_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/balancer_scheduler_service.py), но серверный inventory не показал ни service, ни timer для balancer. Это нужно трактовать как non-confirmed production runtime path.

## 8. Recommended Next Stabilization Step

Следующий grounded шаг после этой инвентаризации:

1. зафиксировать server-local `systemd` truth в repo как tracked artifacts или как минимум canonical templates;
2. отдельно подтвердить intended steady-state для:
   - `telegram_bot.service` enabled vs disabled
   - absence/presence of balancer runtime in prod
   - backup unit auth/env wiring
3. после этого уже делать cleanup legacy comments и приводить launch style к осознанной модели.

До этого момента любые runtime refactors лучше считать premature, потому что часть production truth живёт только на сервере.

## Source Of Truth

- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md)
- [README.md](/Users/dmitrijnazdrin/inventory_service_1/README.md)
- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md)
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md)
- [app/main.py](/Users/dmitrijnazdrin/inventory_service_1/app/main.py)
- [app/services](/Users/dmitrijnazdrin/inventory_service_1/app/services)
- [scripts/backup/backup_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/backup_db.sh)
- [scripts/backup/restore_db.sh](/Users/dmitrijnazdrin/inventory_service_1/scripts/backup/restore_db.sh)
- production `systemd` inventory captured in read-only mode on `2026-04-12`
