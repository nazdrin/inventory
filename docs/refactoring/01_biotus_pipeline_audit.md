# Biotus Pipeline Audit

## 1. Scope

Покрывает [app/biotus_data_service/biotus_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/biotus_data_service/biotus_conv.py) и общий persistence layer.

## 2. Related files and dependencies

- Feed URL из `EnterpriseSettings.token`
- Branch из `MappingBranch.branch`
- XML feed parser работает по `<item>` и полям `sku/name/vendor/barcode/price_rsp_uah/in_stock`

## 3. Current catalog flow

- Feed URL читается из БД
- XML скачивается через `requests`
- Parser строит промежуточные dict с `productId/productName/brand/...`
- `transform_catalog()` формирует standard catalog payload
- JSON сохраняется в `temp/<enterprise>/catalog.json`, затем идёт общий persistence path

## 4. Current stock flow

- Тот же XML input
- Branch читается отдельным запросом
- `transform_stock()` использует `quantity - reserve`, где reserve всегда `0`
- Затем идёт shared write/export path

## 5. Findings

### DB inefficiencies

- Повторяются shared проблемы Dntrade: branch/settings read до adapter save, затем повторный DB access выше.

### Heavy transformations

- XML -> intermediate dicts -> transformed dicts -> JSON -> DB.

### Config/env issues

- Feed URL перегружает `EnterpriseSettings.token`.
- Temp path жёстко задан как `temp/`, не через `TEMP_FILE_PATH`.

### Structure/code issues

- В коде остались комментарии-заглушки вида "вставить название секции", что снижает доверие к стабильности контракта.

### Reliability issues

- Sync requests without timeout/retry config.
- Parser жёстко привязан к конкретным XML field names без fallback/validation summary.

### Logging/observability gaps

- Почти нет structured summary-логов по input/output counts.

## 6. Risk classification

- Medium: feed URL всё ещё перегружает `EnterpriseSettings.token`.
- Low: draft-like comments уже убраны.
- Low: hardcoded temp path и sync HTTP reliability риски уже снижены через env/temp handling и retry/timeout.

## 7. Current status after safe pass

Уже сделано:

1. Убраны двусмысленные template-like comments.
2. Добавлены basic summary logs по download/parse/transform/run.
3. Temp path унифицирован через `TEMP_FILE_PATH`.
4. XML download path получил timeout/retry/backoff.

Текущий practical status:

- `Biotus` закрыт на текущем этапе;
- возвращаться стоит только если появится новый explicit cleanup goal вокруг order-side operational surface.

## 8. Notes about differences from Dntrade

- Контур проще и не имеет pagination/store loops.
- Уникальный риск: код выглядит partially-template-based и требует schema confirmation.
