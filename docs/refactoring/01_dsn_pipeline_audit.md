# DSN Pipeline Audit

## 1. Scope

Покрывает:
- [app/dsn_data_service/dsn_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/dsn_data_service/dsn_conv.py)
- [app/dsn_data_service/dsn_common.py](/Users/dmitrijnazdrin/inventory_service_1/app/dsn_data_service/dsn_common.py)
- общий write path в [app/services/database_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/database_service.py)

## 2. Related files and dependencies

- Feed URL берётся из `EnterpriseSettings.token`
- Источник: XML feed через `requests`
- Stock branch берётся из `MappingBranch.branch`
- Debug JSON пишется в `TEMP_FILE_PATH`

## 3. Current catalog flow

- Scheduler вызывает единый `run_service(..., "catalog")`
- `dsn_common.py` читает `EnterpriseSettings.token`
- XML скачивается через общий helper с timeout/retry
- Catalog parse path разбирает `<offer>`, извлекает `id`, `name`, `vendor`, barcode из `description`
- Debug JSON пишется только при включённом debug-env; рабочий JSON уходит в `process_database_service`

## 4. Current stock flow

- Тот же feed URL
- Branch читается явно и обязателен для stock path
- `parse_stock_data()` извлекает `price` и `quantity_in_stock`, отрицательные qty режет в `0`
- Debug JSON условный; затем идёт итоговый stock JSON
- Затем идёт общий delete/export/save path

## 5. Findings

### DB inefficiencies

- Повторяется проблема Dntrade: catalog и stock отдельно читают settings/branch, затем общий слой снова читает settings.

### Heavy transformations

- Полный XML -> debug JSON -> working JSON -> DB round-trip.
- Debug JSON создаётся всегда, а не только при debug режиме.

### Config/env issues

- `EnterpriseSettings.token` перегружен как feed URL.
- Нет timeout/retry/env управления для XML download.

### Structure/code issues

- Catalog и stock почти зеркальны, но логика разнесена и частично дублируется.
- Barcode extraction из `description` завязана на конкретный regex `Штрихкод:` и может быть хрупкой.

### Reliability issues

- Синхронный `requests.get()` в async flow.
- При ошибке скачивания кидается exception без локального recovery.
- Stock fallback branch=`"unknown"` может дойти до persistence path.

### Logging/observability gaps

- Есть базовые log/debug files, но нет summary по размеру входного XML и числу записей.

## 6. Risk classification

- Medium: feed URL всё ещё перегружает `EnterpriseSettings.token`.
- Low: fallback branch=`unknown` уже убран; debug JSON стал условным.
- Low: catalog/stock дублирование снижено через unified adapter module.

## 7. Current status after safe pass

Уже сделано:

1. `DSN` объединён в единый adapter module `dsn_conv.py` с общим helper.
2. Введена явная валидация branch перед save.
3. Debug JSON стал условным.
4. Добавлены timeout/retry и summary logs без смены data contract.

Текущий practical status:

- `DSN` закрыт на текущем этапе;
- возвращаться стоит только при инцидентах вокруг feed contract или barcode parsing assumptions.

## 8. Notes about differences from Dntrade

- Повторяются shared проблемы `database_service`.
- В отличие от Dntrade, здесь нет API pagination, но есть XML-specific хрупкость и постоянные debug side effects.
