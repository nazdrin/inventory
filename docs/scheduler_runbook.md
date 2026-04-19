# Scheduler Runbook

## Purpose

Собрать grounded карту scheduler/process контуров `inventory_service` для эксплуатации и сопровождения.

## Scope

Документ покрывает:

- активные scheduler/process контуры;
- что делает каждый процесс;
- из каких источников он читает данные;
- куда пишет результат;
- признаки проблем и первичную диагностику.

Документ не покрывает:

- полный supplier-specific runtime по каждому adapter-у;
- точные systemd unit definitions;
- внутреннюю бизнес-логику каждого importer/exporter модуля.

## High-level Overview

Проект использует отдельные долгоживущие процессы вместо единого orchestration слоя. Большинство scheduler-ов:

- работают в бесконечном цикле;
- читают настройки из PostgreSQL и/или `.env`;
- пишут результат в БД, внешние API или runtime state;
- отправляют уведомления через `notification_service`.

Важно разделять:

- active scheduler paths;
- transitional fallback behavior;
- external server wiring, которого нет в repo.

## Scheduler Criticality

### High

- order scheduler
- business stock scheduler
- stock scheduler
- API app

Почему:

- напрямую влияют на заказы, остатки и текущие бизнес-операции.

### Medium

- master catalog scheduler
- competitor price scheduler
- Tabletki cancel retry service
- Biotus scheduler

Почему:

- влияют на downstream business behavior и качество данных, но не всегда блокируют базовую доступность системы немедленно.

### Low

- Telegram bot

Почему:

- operationally useful, но не является core execution path для API, stock или order continuity.

## Scheduler Inventory

### API app

Entrypoint:

- `python -m uvicorn app.main:app`

Что делает:

- поднимает FastAPI app;
- подключает router developer/admin surface;
- на startup вызывает `create_tables()`.

Читает:

- `.env`
- DB connection settings
- runtime code in `app/routes.py`

Пишет:

- HTTP responses
- local log output

Признаки проблем:

- root endpoint `/` недоступен;
- login/public webhook routes не обрабатываются;
- startup errors в logs.

### Catalog scheduler

Entrypoint:

- `python -m app.services.catalog_scheduler_service`

Что делает:

- раз в минуту отбирает `EnterpriseSettings` по `catalog_upload_frequency`;
- dispatch-ит обработчик по `data_format`;
- для `Business` использует старый общий path только если не включён `DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER`.

Читает:

- `enterprise_settings`
- `.env` timeout/cooldown flags
- integration adapters in `app/*_data_service`

Пишет:

- данные каталога через соответствующий runtime handler;
- error reports в локальные `error_report_*.txt` при исключениях;
- notifications через `send_notification`.

Признаки проблем:

- enterprise не попадает в очередь при ожидаемой частоте;
- repeated timeout/cooldown;
- unsupported `data_format`;
- цикл scheduler-а завершается или зависает на одном enterprise.

Transitional note:

- old Business catalog scheduler не является preferred path и должен документироваться отдельно от основной business stock/pricing модели.

### Stock scheduler

Entrypoint:

- `python -m app.services.stock_scheduler_service`

Что делает:

- раз в минуту отбирает `EnterpriseSettings` по `stock_upload_frequency`;
- dispatch-ит stock processor по `data_format`;
- явно пропускает `Business`, потому что для него есть отдельный scheduler.

Читает:

- `enterprise_settings`
- stock processors in `app/*_data_service`

Пишет:

- stock data через adapter-specific handlers;
- notifications и `error_report_*.txt`.

Признаки проблем:

- stock run не стартует при включённом `stock_enabled`;
- `Business` erroneously ожидается в этом scheduler-е;
- repeated adapter failures или пустые runs.

### Business stock scheduler

Entrypoint:

- `python -m app.services.business_stock_scheduler_service`

Что делает:

- находит enterprise с `data_format=Business`;
- берёт control plane из `business_settings`, а при отсутствии строки fallback-ит к старой логике через `EnterpriseSettings`;
- запускает `app.business.dropship_pipeline.run_pipeline(..., "stock")`.

Читает:

- `business_settings`
- `enterprise_settings`
- pricing/supplier data needed by `dropship_pipeline`

Пишет:

- business stock runtime result через `dropship_pipeline`;
- notifications при ошибках.

Признаки проблем:

- в БД нет или больше одного enterprise с `data_format=Business`;
- `business_stock_enabled=false`;
- misconfigured fallback interval;
- исключения в `dropship_pipeline`.

Transitional note:

- fallback к `EnterpriseSettings.stock_enabled` и `stock_upload_frequency` - transitional behavior, а не целевая control-plane модель.

### Order scheduler

Entrypoint:

- `python -m app.services.order_scheduler_service`

Что делает:

- раз в минуту ищет enterprises с `order_fetcher=True`;
- вызывает `fetch_orders_for_enterprise`;
- для `Business` дополнительно запускает обработку cancelled orders.

Читает:

- `enterprise_settings`
- external order sources via `order_fetcher`

Пишет:

- order data in downstream order logic;
- notifications on failure.

Признаки проблем:

- enterprise с `order_fetcher=True` не обрабатывается;
- fetch проходит, но cancel-processing падает;
- repeated order errors по одному enterprise.

### Competitor price scheduler

Entrypoint:

- `python -m app.services.competitor_price_scheduler`

Что делает:

- строит расписание по `COMPETITOR_SCHEDULER_WINDOW_START/END` и `COMPETITOR_SCHEDULER_INTERVAL_MINUTES`;
- раз в минуту проверяет, нужно ли запустить competitor loader;
- не допускает параллельных запусков loader-а.

Читает:

- env window and interval settings
- competitor loader in `app.business.competitor_price_loader`

Пишет:

- competitor price data через loader;
- optional success notifications;
- error notifications on failure.

Признаки проблем:

- неверный env interval/window;
- scheduler пропускает слоты;
- loader долго выполняется и блокирует следующий trigger;
- competitor data не обновляется в pricing contour.

### Master catalog scheduler

Entrypoint:

- `python -m app.services.master_catalog_scheduler_service`

Что делает:

- управляет weekly, daily publish и archive jobs;
- использует global lock и state file в `state_cache`;
- читает DB-first master settings через resolver;
- запускает `run_master_catalog_orchestrator`.

Читает:

- `business_settings` или env fallback
- state files in `state_cache`
- master catalog business logic and supplier modules

Пишет:

- orchestrator results;
- state file `state_cache/master_catalog_scheduler_state.json`;
- notifications for success/failure.

Признаки проблем:

- lock prevents start;
- invalid state file;
- DB settings inconsistency;
- daily/weekly/archive jobs не срабатывают в expected window.

Transitional note:

- если строка `business_settings` отсутствует, scheduler использует env fallback. Это допустимо, но не является целевой steady-state моделью.

### Biotus scheduler

Entrypoint:

- `python -m app.services.biotus_check_order_scheduler`

Что делает:

- периодически запускает `process_biotus_orders`;
- поддерживает day/night режим;
- получает effective enterprise через `master_business_settings_resolver`.

Читает:

- `business_settings` или env fallback
- `BIOTUS_*` env settings

Пишет:

- результаты обработки заказов Biotus в downstream order contour;
- logs с результатами run-а.

Признаки проблем:

- неправильная TZ или night mode;
- effective enterprise не резолвится;
- repeated skipped runs;
- исключения внутри `process_biotus_orders`.

### Tabletki cancel retry service

Entrypoint:

- `python -m app.services.tabletki_cancel_retry_service`

Что делает:

- периодически обрабатывает due items из очереди delayed retry;
- вызывает `process_due_tabletki_cancel_retries`;
- поддерживает single-run режим через `--once`.

Читает:

- DB session
- queue path `TABLETKI_CANCEL_RETRY_QUEUE_PATH`
- env `TABLETKI_CANCEL_RETRY_POLL_INTERVAL_SEC`

Пишет:

- retry effects в downstream order/cancel flow;
- runtime queue state;
- logs с результатом обработки due queue items.

Признаки проблем:

- queue does not drain;
- сервис не видит due items, хотя они ожидаются;
- repeated iteration failures;
- poll interval слишком большой или слишком маленький для текущей нагрузки.

### Telegram bot

Entrypoint:

- `python -m app.services.telegram_bot`

Что делает:

- обслуживает Telegram bot flow проекта.

Читает:

- Telegram env settings

Пишет:

- bot responses and notifications

Признаки проблем:

- bot start failure;
- no replies or notification delivery issues.

## Data Sources And Sinks

Общая схема для большинства scheduler-ов:

- primary structured source: PostgreSQL (`enterprise_settings`, `business_settings`, related tables)
- secondary source: `.env`
- external sources: Google Drive, FTP, feed/API integrations, SalesDrive, Biotus, Telegram
- sinks: PostgreSQL, external APIs, runtime state files, notifications, logs

## First-Triage Checklist

Если contour “не работает”, сначала проверить:

1. жив ли сам process/service;
2. резолвится ли target enterprise/settings;
3. не заблокирован ли contour feature flag-ом или fallback-state;
4. есть ли свежие ошибки в `journalctl`;
5. не завязан ли current failure на external integration availability.

## Source Of Truth

- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md)
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md)
- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md)
- [app/services/catalog_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/catalog_scheduler_service.py)
- [app/services/stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/stock_scheduler_service.py)
- [app/services/business_stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_stock_scheduler_service.py)
- [app/services/order_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/order_scheduler_service.py)
- [app/services/competitor_price_scheduler.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/competitor_price_scheduler.py)
- [app/services/master_catalog_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/master_catalog_scheduler_service.py)
- [app/services/biotus_check_order_scheduler.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/biotus_check_order_scheduler.py)

## Operational Notes

- Scheduler-ы не образуют единый orchestration framework; каждый contour нужно диагностировать отдельно.
- Для части процессов documented `systemd` service names есть только в existing docs, а не в tracked infra files.
- `state_cache/` содержит operational state отдельных контуров, но не является config source.

## Known Limitations / Risks

- Этот runbook intentionally не перечисляет все supplier modules поимённо.
- Для `tabletki_cancel_retry_service` первая волна фиксирует contour existence и operational role, но не полный internal algorithm.
- При отсутствии server-side unit files невозможно на 100% подтвердить production wiring только из repo.
