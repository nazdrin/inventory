# Inventory Service

## Purpose

`inventory_service` - stateful backend-монолит для загрузки, нормализации, публикации и сопровождения catalog/stock/order данных из нескольких внешних источников.

Этот README - entrypoint в документацию. Он не заменяет operational runbook и не описывает все специальные случаи по поставщикам.

## Scope

Документ покрывает:

- что это за система;
- какие контуры в проекте считаются активными;
- где лежат основные точки входа;
- какие документы читать дальше.

Документ не покрывает:

- полный production runbook;
- все env и DB настройки;
- детальную логику scheduler-ов;
- supplier-specific особенности по всем интеграциям.

## High-level Overview

Проект запускается как FastAPI-приложение с несколькими daemon-like scheduler/process контурами.

Подтверждённые active contours:

- API / developer panel backend: `app/main.py`, `app/routes.py`
- catalog ingestion: `app/services/catalog_scheduler_service.py`
- stock ingestion: `app/services/stock_scheduler_service.py`
- business stock: `app/services/business_stock_scheduler_service.py`
- orders / cancellations / webhooks: `app/services/order_scheduler_service.py`, `app/business/order_sender.py`, `app/business/salesdrive_webhook.py`
- pricing / dropship: `app/business/dropship_pipeline.py`
- master catalog: `app/services/master_catalog_scheduler_service.py`, `app/business/master_catalog_orchestrator.py`
- competitor pricing: `app/services/competitor_price_scheduler.py`
- backup / restore: `scripts/backup/backup_db.sh`, `scripts/backup/restore_db.sh`
- admin-panel frontend: `admin-panel/`

Transitional behavior, которое не нужно считать основным потоком:

- старый `Business` catalog path в общем catalog scheduler-е; он может быть отключён через `DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER`
- env fallback для части business/master/biotus/pricing-настроек, если строка `business_settings` отсутствует или невалидна
- legacy-семантика `Blank` как специального формата

## Main Entry Points

Backend API:

```bash
cd /Users/dmitrijnazdrin/inventory_service_1
source .venv/bin/activate
python3 -m uvicorn app.main:app --reload
```

Frontend:

```bash
cd /Users/dmitrijnazdrin/inventory_service_1/admin-panel
npm start
```

Основные scheduler entrypoints:

```bash
python -m app.services.catalog_scheduler_service
python -m app.services.stock_scheduler_service
python -m app.services.business_stock_scheduler_service
python -m app.services.order_scheduler_service
python -m app.services.competitor_price_scheduler
python -m app.services.master_catalog_scheduler_service
python -m app.services.biotus_check_order_scheduler
python -m app.services.tabletki_cancel_retry_service
python -m app.services.telegram_bot
```

## Documentation Map

Core project context:

- [AGENTS.md](/Users/dmitrijnazdrin/inventory_service_1/AGENTS.md) - repo operating rules and safety constraints
- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md) - architecture and active contours
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md) - key modules and high-risk files
- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md) - env variable meaning without secret values
- [CODEX_RULES.md](/Users/dmitrijnazdrin/inventory_service_1/CODEX_RULES.md) - architectural analysis rules

Production and operations:

- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md) - short production cheat sheet
- [docs/operations_runbook.md](/Users/dmitrijnazdrin/inventory_service_1/docs/operations_runbook.md) - production operations runbook
- [docs/scheduler_runbook.md](/Users/dmitrijnazdrin/inventory_service_1/docs/scheduler_runbook.md) - scheduler inventory and troubleshooting
- [docs/configuration_reference.md](/Users/dmitrijnazdrin/inventory_service_1/docs/configuration_reference.md) - env vs DB configuration model
- [docs/backup_and_restore.md](/Users/dmitrijnazdrin/inventory_service_1/docs/backup_and_restore.md) - current backup/restore flow

Process and git workflow:

- [GIT_WORKFLOW.md](/Users/dmitrijnazdrin/inventory_service_1/GIT_WORKFLOW.md) - only approved git scenarios for this repo

## Operational Notes

- Это stateful система: многие контуры зависят от PostgreSQL, `enterprise_settings`, `business_settings`, `.env`, runtime cache и внешних интеграций.
- Не все production truths хранятся в репозитории. Например, `systemd` unit names и nginx deployment details описаны в docs, но сами unit-файлы в repo отсутствуют.
- При изменениях в `app/models.py`, `app/routes.py`, `app/services/*scheduler*`, `app/business/dropship_pipeline.py`, `app/business/master_catalog_orchestrator.py` нужен отдельный impact check.
- `state_cache/`, `temp/`, `logs/`, `admin-panel/build/`, `node_modules/` не являются source of truth для документации.

## Source Of Truth

- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md)
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md)
- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md)
- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md)
- [app/main.py](/Users/dmitrijnazdrin/inventory_service_1/app/main.py)
- [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- [app/services](/Users/dmitrijnazdrin/inventory_service_1/app/services)
- [app/business](/Users/dmitrijnazdrin/inventory_service_1/app/business)

## Known Limitations / Risks

- README intentionally keeps only the first-wave map; it is not a full system handbook.
- Supplier-specific behavior is distributed across many files and is not exhaustively described here.
- `README_PROD.md` and runbooks depend partly on external server truth; if production service names drift, docs must be re-validated on the server.
- Presence of env fallback means that documented runtime behavior may differ depending on whether `business_settings` row exists in the target DB.
