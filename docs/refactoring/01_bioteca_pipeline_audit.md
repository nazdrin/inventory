# Bioteca Pipeline Audit

## 1. Scope

Покрывает [app/bioteca_data_service/bioteca_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/bioteca_data_service/bioteca_conv.py) и общий persistence path.

## 2. Related files and dependencies

- API source: `https://connect.ainur.app/api/v4/product`
- Auth: `EnterpriseSettings.token`
- Multi-store routing: `MappingBranch.store_id -> branch`
- Async HTTP через `aiohttp`

## 3. Current catalog flow

- `run_service(..., "catalog")` читает token и все mapping branches
- Для каждого `store_id` по отдельности собираются страницы через `fetch_products_for_store()`
- `transform_catalog()` дедуплицирует записи по `code`
- JSON сохраняется в `TEMP_FILE_PATH` и уходит в `process_database_service`

## 4. Current stock flow

- Используются те же API fetch loops по каждому `store_id`
- `transform_stock()` не агрегирует остатки между stores: каждая строка мапится в свой branch
- `qty` берётся из `product.stock[store_id]`
- Потом идёт shared write/export path

## 5. Findings

### DB inefficiencies

- Mapping branches грузятся отдельно, потом общий save path снова читает enterprise settings.

### Heavy transformations

- Полный gather `products_by_store` в памяти для всех stores до transform.

### Config/env issues

- API page increment строится через `offset += PAGE_LIMIT`, не через фактический размер ответа.
- Нет explicit max pages / repeat-page guard.

### Structure/code issues

- Catalog и stock уже ближе к reusable adapter pattern, чем многие другие модули.
- Но fetch/store orchestration и transform всё ещё живут в одном файле.

### Reliability issues

- Ошибка одного store логируется, но run идёт дальше; это повышает availability, но может скрывать partial data.
- Нет итогового явного флага partial-success.

### Logging/observability gaps

- Логи лучше, чем у многих других adapters, но нет единого финального summary с failed stores.

## 6. Risk classification

- Medium: partial-success semantics остаётся conscious design choice, но уже явно сигнализируется в summary logs.
- Low: paging risk снижен через `MAX_PAGES_PER_STORE` и repeat-page guard.
- Low: structure в целом уже лучше Dntrade и многих XML/FTP adapters.

## 7. Current status after safe pass

Уже сделано:

1. Добавлен final summary с processed/failed stores.
2. Добавлен явный `partial success` warning с `failed_store_ids`.
3. Добавлены paging guards: `MAX_PAGES_PER_STORE` и защита от repeated page.

Текущий practical status:

- adapter можно считать закрытым на текущем этапе;
- возвращаться стоит только при новых инцидентах или изменении API/paging контракта.

## 8. Notes about differences from Dntrade

- Это один из самых зрелых adapters в проекте: async HTTP, явный multi-store fetch, dedupe в catalog.
- Повторяются shared persistence issues, но unique risk здесь скорее в partial-success semantics, а не в сырой архитектуре.
