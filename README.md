# Inventory Service

Основной README для локальной разработки. Прод-операции вынесены в [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md).

## Git workflow

- Для commit и push в `develop`: `сделай git develop по правилам из AGENTS.md`
- Для release merge `develop -> main`: `сделай merge main по правилам из AGENTS.md`

Подробные правила вынесены в [AGENTS.md](/Users/dmitrijnazdrin/inventory_service_1/AGENTS.md) и [GIT_WORKFLOW.md](/Users/dmitrijnazdrin/inventory_service_1/GIT_WORKFLOW.md).

## Project docs map

- [AGENTS.md](/Users/dmitrijnazdrin/inventory_service_1/AGENTS.md): как агент должен работать в этом репозитории.
- [GIT_WORKFLOW.md](/Users/dmitrijnazdrin/inventory_service_1/GIT_WORKFLOW.md): единственные допустимые git-сценарии.
- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md): архитектура, пайплайны, сущности и точки запуска.
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md): карта критичных файлов и модулей.
- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md): справочник ключевых env-переменных без значений.
- [CODEX_RULES.md](/Users/dmitrijnazdrin/inventory_service_1/CODEX_RULES.md): стиль архитектурного анализа и формат ответов.
- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md): продовый runbook.

## Локальный запуск

```bash
cd /Users/dmitrijnazdrin/inventory_service_1
source .venv/bin/activate
pip install -r requirements.txt
python3 -m uvicorn app.main:app --reload
```

Frontend:

```bash
npm start
```

## Фоновые сервисы

Запускать по необходимости:

```bash
python -m app.services.catalog_scheduler_service
python -m app.services.stock_scheduler_service
python -m app.services.order_scheduler_service
python -m app.services.competitor_price_scheduler
python -m app.services.telegram_bot
python app/services/biotus_check_order_scheduler.py
python app/services/business_stock_scheduler.py
python -m app.services.business_stock_scheduler.service
```

## PostgreSQL

Подключение:

```bash
psql -U postgres -d inventory_db
```

Полезные команды:

```sql
\dt
\q
SELECT * FROM enterprise_settings;
SELECT * FROM developer_settings;
SELECT * FROM mapping_branch;
SELECT * FROM dropship_enterprises;
SELECT * FROM catalog_mapping;
SELECT * FROM offers;
SELECT * FROM client_notifications;
SELECT * FROM competitor_prices;
```

## Примечания по проекту

- `competitors_min_price.py` и `salesdrive_catalog_uploader.py` вынесены в отдельные проекты и не должны возвращаться в этот репозиторий.
- `state_cache/` считается runtime-кэшем и не должен попадать в git.
- `catalog_mapping_d2_mismatch.xlsx` считается неиспользуемым файлом.
