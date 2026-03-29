# HProfit Pipeline Audit

## 1. Scope

Покрывает [app/hprofit_data_service/hprofit_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/hprofit_data_service/hprofit_conv.py) и общий persistence layer.

## 2. Related files and dependencies

- Feed URL хранится в `EnterpriseSettings.token`
- Stock branch из `MappingBranch.branch`
- XML fetch через `requests`

## 3. Current catalog flow

- `run_service(..., "catalog")` читает URL из БД
- XML скачивается и разбирается в `parse_xml_feed()`
- Catalog строится из `productId/productName/barcode`
- JSON пишется в `temp/<enterprise>/catalog.json`, затем уходит в `process_database_service`

## 4. Current stock flow

- Тот же XML input
- Branch lookup отдельным запросом
- `transform_stock()` использует `quantity - reserve`, reserve всегда `0`
- Затем идёт обычный shared write/export path

## 5. Findings

### DB inefficiencies

- Повторяется Dntrade pattern: branch/settings читаются отдельно, затем снова на общем save path.

### Heavy transformations

- XML полностью парсится в промежуточный список dict, потом ещё раз сериализуется.

### Config/env issues

- Feed URL перегружает `EnterpriseSettings.token`.
- `save_to_json()` использует жёсткий `temp/`, а не env-driven temp path.

### Structure/code issues

- Catalog и stock делят один parser, но остальная orchestration остаётся дублированной.

### Reliability issues

- Sync HTTP без timeout/retry настройки.
- Ошибка XML parsing приводит к падению всего run без degradation path.

### Logging/observability gaps

- Есть минимум логов; нет summary по offers/records.

## 6. Risk classification

- Medium: feed URL всё ещё хранится в token field.
- Low: hardcoded temp path уже убран; network path получил timeout/retry.
- Low: logic простая и меньше divergence, чем у Dntrade.

## 7. Current status after safe pass

Уже сделано:

1. Добавлены summary logs и timeout/retry.
2. Temp path переведён на `TEMP_FILE_PATH`.
3. Удалён мёртвый legacy-файл `# feed_converter2.py`.

Текущий practical status:

- adapter закрыт на текущем этапе;
- отдельная унификация XML feed adapters может рассматриваться только как optional cleanup.

## 8. Notes about differences from Dntrade

- Намного проще Dntrade: нет pagination, store loops и custom scheduler protection.
- Уникальные проблемы: hardcoded `temp/` и feed URL в `token`.
