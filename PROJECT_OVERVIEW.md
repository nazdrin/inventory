# PROJECT_OVERVIEW

## Назначение

`inventory_service` - backend-сервис для загрузки, нормализации и публикации каталогов, остатков и заказов из разных источников. Проект объединяет несколько сценариев:

- импорт catalog/stock от поставщиков и клиентов;
- хранение настроек предприятий и сопоставлений филиалов;
- выгрузка данных во внешние системы;
- обработка заказов и webhook-событий;
- dropship/pricing логика;
- мастер-каталог и обогащение контента;
- загрузка цен конкурентов и balancer-пайплайн.

## Когда читать

- когда нужно быстро понять назначение системы;
- когда нужно разобраться в главных пайплайнах;
- когда нужно оценить затрагиваемые подсистемы до изменений;
- когда нужен high-level обзор перед чтением кода.

## Связанные документы

- [README.md](/Users/dmitrijnazdrin/inventory_service_1/README.md)
- [AGENTS.md](/Users/dmitrijnazdrin/inventory_service_1/AGENTS.md)
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md)
- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md)
- [CODEX_RULES.md](/Users/dmitrijnazdrin/inventory_service_1/CODEX_RULES.md)

## Общая архитектура

Проект ближе к монолиту на FastAPI с фоновыми сервисами.

- HTTP API и developer panel запускаются через FastAPI.
- Основная бизнес-логика сосредоточена в `app/business`.
- Интеграции по источникам разнесены по папкам `*_data_service`, `google_drive`, `ftp_*`.
- Планировщики и daemon-like процессы находятся в `app/services`.
- Данные хранятся в PostgreSQL, схема поддерживается через Alembic.
- Есть отдельный frontend `admin-panel` для настройки части сущностей.

## Основные модули

- `app/main.py` - точка входа FastAPI.
- `app/routes.py` - HTTP-роуты developer panel, CRUD-операции, login, public webhook-и.
- `app/database.py` - async engine SQLAlchemy, session factory, базовые DB helper-ы.
- `app/models.py` - ORM-модели таблиц.
- `app/schemas.py` - Pydantic-схемы API.
- `app/crud.py` - вспомогательные DB-операции.
- `app/services` - планировщики, сервисные процессы, order/notification/export logic.
- `app/business` - основная бизнес-логика dropship, master catalog, pricing, feeds, order processing.
- `app/*_data_service` - адаптеры конкретных источников данных.
- `alembic` - миграции БД.
- `admin-panel` - отдельный frontend для настройки.

## Ключевые потоки данных

### 1. Catalog pipeline

- планировщик читает `enterprise_settings`;
- по `data_format` выбирается нужный адаптер;
- адаптер забирает данные из API, FTP, Google Drive, feed или локального файла;
- данные конвертируются в внутренний формат;
- результат записывается в БД и/или готовится для внешней публикации.

### 2. Stock pipeline

- `stock_scheduler_service` циклически отбирает предприятия по `stock_upload_frequency`;
- для каждого предприятия вызывается обработчик по `data_format`;
- остатки нормализуются и пишутся в `inventory_stock` либо в связанные таблицы.

### 3. Orders pipeline

- `order_scheduler_service` находит предприятия с `order_fetcher=True`;
- `order_fetcher` забирает заказы из внешней системы;
- для enterprise с `Business` запускается дополнительная обработка отмен.

### 4. Dropship / pricing pipeline

- `app/business/dropship_pipeline.py` собирает фиды поставщиков;
- применяет правила ценообразования, competitor-based pricing и supplier schedule;
- пишет офферы и связанные сущности;
- использует данные конкурентов и balancer policy при наличии.

### 5. Master catalog pipeline

- `master_catalog_scheduler_service` управляет daily/weekly/interval job-ами;
- `master_catalog_orchestrator` выполняет импорт, fallback, enrichment, export;
- отдельные модули синхронизируют barcode/content/images по поставщикам D1-D13 и др.

### 6. Competitor pricing

- `competitor_price_scheduler` по окну времени запускает загрузчик цен конкурентов;
- данные используются в pricing-логике dropship pipeline.

### 7. Balancer

- `balancer_scheduler_service` запускает pipeline на временных границах сегментов;
- `app/business/balancer` хранит правила, сегменты, репозиторий и обработку.

## Основные сущности БД

- `enterprise_settings` - настройки предприятий и частоты загрузки.
- `mapping_branch` - сопоставление branch/store/folder.
- `developer_settings` - настройки developer panel и login.
- `inventory_data` - каталог по предприятиям.
- `inventory_stock` - остатки по branch и коду.
- `catalog_mapping` - централизованное сопоставление каталога.
- `dropship_enterprises` - конфигурация dropship-поставщиков.
- `competitor_prices` - цены конкурентов.
- дополнительные master/balancer/offer-таблицы также определены в модели проекта и миграциях.

## Точки запуска

- API: `python3 -m uvicorn app.main:app --reload`
- Catalog scheduler: `python -m app.services.catalog_scheduler_service`
- Stock scheduler: `python -m app.services.stock_scheduler_service`
- Order scheduler: `python -m app.services.order_scheduler_service`
- Competitor scheduler: `python -m app.services.competitor_price_scheduler`
- Telegram bot: `python -m app.services.telegram_bot`
- Master catalog scheduler: `python -m app.services.master_catalog_scheduler_service`
- Balancer scheduler: `python -m app.services.balancer_scheduler_service`
- Tabletki cancel retry: `python -m app.services.tabletki_cancel_retry_service`

## Что важно понимать перед изменениями

- В проекте много интеграций и env-driven поведения.
- Один и тот же pipeline часто зависит сразу от БД, файловой системы, Google Drive или FTP.
- Логика поставщиков часто разнесена по отдельным файлам с похожими именами, но разной семантикой.
- Изменения в `app/models.py`, `app/routes.py`, шедулерах и `dropship_pipeline.py` имеют широкий радиус влияния.
