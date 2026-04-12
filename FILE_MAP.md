# FILE_MAP

## Назначение

Этот файл помогает быстро понять, где в кодовой базе лежат основные точки входа, критичные модули и зоны повышенного риска.

## Когда читать

- когда нужно найти, где реализован конкретный pipeline или интеграция;
- когда нужно оценить радиус влияния перед правкой;
- когда нужно быстро определить стартовые файлы для исследования;
- когда обсуждение упирается в конкретные модули, а не в high-level архитектуру.

## Связанные документы

- [README.md](/Users/dmitrijnazdrin/inventory_service_1/README.md)
- [AGENTS.md](/Users/dmitrijnazdrin/inventory_service_1/AGENTS.md)
- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md)
- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md)
- [CODEX_RULES.md](/Users/dmitrijnazdrin/inventory_service_1/CODEX_RULES.md)

## Точки входа

- `app/main.py` - FastAPI application, startup hook, подключение роутера.
- `app/routes.py` - developer panel API, login, CRUD по enterprise/developer/data_format, public webhook-и.
- `admin-panel/src/App.js` - entrypoint frontend-части.

## База данных и контракты

- `app/database.py` - async SQLAlchemy engine и session lifecycle.
- `app/models.py` - ORM-модели; менять осторожно, почти любое изменение требует проверки миграций и runtime-сценариев.
- `app/schemas.py` - Pydantic-схемы API и валидация входных данных.
- `alembic/env.py` - конфигурация миграций.
- `alembic/versions/*` - история изменений схемы.

## API и авторизация

- `app/auth.py` - работа с токеном и `SECRET_KEY`.
- `app/routes.py` - основной HTTP surface проекта.
- `app/business/salesdrive_webhook.py` - обработка webhook-ов SalesDrive.

## Шедулеры и сервисные процессы

- `app/services/catalog_scheduler_service.py` - периодическая обработка каталогов по `enterprise_settings`.
- `app/services/stock_scheduler_service.py` - периодическая обработка остатков.
- `app/services/order_scheduler_service.py` - загрузка заказов и post-processing.
- `app/services/order_fetcher.py` - логика получения заказов из внешней системы.
- `app/services/order_sender.py` - отправка/повторы/связанные операции по заказам.
- `app/services/competitor_price_scheduler.py` - расписание загрузки цен конкурентов.
- `app/services/master_catalog_scheduler_service.py` - расписание master catalog pipeline.
- `app/services/balancer_scheduler_service.py` - запуск balancer по временным сегментам.
- `app/services/tabletki_cancel_retry_service.py` - ретраи отмен/предупреждений для Tabletki.
- `app/services/notification_service.py` - уведомления и служебные сообщения.
- `app/services/telegram_bot.py` - Telegram bot.

## Главная бизнес-логика

- `app/business/dropship_pipeline.py` - один из самых критичных файлов; pricing, supplier feeds, offers, competitor logic.
- `app/business/import_catalog.py` - бизнес-импорт каталога для формата `Business`.
- `app/business/order_sender.py` - бизнес-обработка заказов и отмен.
- `app/business/competitor_price_loader.py` - загрузка и сохранение цен конкурентов.
- `app/business/master_catalog_orchestrator.py` - оркестратор master catalog сценариев.
- `app/business/tabletki_master_catalog_exporter.py` - экспорт мастер-каталога наружу.
- `app/business/salesdrive_master_catalog_exporter.py` - выгрузка master catalog в SalesDrive.
- `app/business/tabletki_master_catalog_loader.py` - загрузка/синхронизация master catalog из внешнего источника.

## Master catalog и supplier-модули

- `app/business/d1_*`, `d2_*`, ..., `d13_*` - sync/load/fallback модули по конкретным поставщикам.
- `app/business/master_content_select.py` - выбор контента.
- `app/business/master_main_image_select.py` - выбор главного изображения.
- `app/business/master_images_fallback_*` и `master_content_fallback_*` - fallback-процедуры.
- `app/business/barcode_matching.py` - сопоставление по barcode.

## Balancer

- `app/business/balancer/jobs.py` - orchestration balancer pipeline.
- `app/business/balancer/repository.py` - доступ к активным policy/rule данным.
- `app/business/balancer/live_logic.py` - runtime логика live-режима.
- `app/business/balancer/order_processor.py` - обработка заказов в контексте balancer.
- `app/business/balancer/policy.py` - policy logic.
- `app/business/balancer/config.yaml` - конфиг balancer.

## Интеграции по источникам

- `app/google_drive/google_drive_service.py` - загрузка catalog/stock из Google Drive.
- `app/ftp_data_service/ftp_catalog_conv.py` и `app/ftp_data_service/ftp_stock_conv.py` - FTP import.
- `app/ftp_multi_data_service/ftp_multi_conv.py` - multi-source FTP import.
- `app/dntrade_data_service/fetch_convert.py` и `stock_fetch_convert.py` - Dntrade catalog/stock.
- `app/key_crm_data_service/*` - KeyCRM catalog/stock/orders.
- `app/checkbox_data_service/*` - Checkbox import.
- `app/prom_data_service/*` - Prom import.
- `app/rozetka_data_service/*` - Rozetka import.
- `app/biotus_data_service/biotus_conv.py` - Biotus import.
- `app/torgsoft_google_data_service/*` - Torgsoft via Google Drive.
- `app/torgsoft_google_multi_data_service/*` - multi-tenant Torgsoft.

## Скрипты и вспомогательные утилиты

- `app/scripts/export_catalog_mapping_d6_mismatch.py` - экспорт mismatches по mapping.
- `app/scripts/ftp_down.py` - вспомогательная FTP-утилита.

## Файлы повышенного риска

- `app/models.py` - влияет на БД, миграции и ORM.
- `app/database.py` - влияет на подключение и lifecycle транзакций.
- `app/routes.py` - влияет на HTTP API и публичные webhook-и.
- `app/services/catalog_scheduler_service.py` и `app/services/stock_scheduler_service.py` - затрагивают массовую обработку данных.
- `app/business/dropship_pipeline.py` - центральная pricing/dropship логика.
- `app/business/master_catalog_orchestrator.py` - большой радиус влияния на master flows.
- `app/services/notification_service.py` - риск шумных уведомлений или ошибок DB/Telegram.
- `.env` - только как reference; не коммитить секреты и реальные значения.
