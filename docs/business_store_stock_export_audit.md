# Business Store Stock Export Audit

## 1. Scope

Этот документ фиксирует аудит текущего legacy Business stock export path и итоговую техническую схему для manual store-aware stock exporter:

- manual store-aware stock exporter для одного `BusinessStore`;
- без scheduler integration;
- без изменения current legacy runtime;
- без изменения `dropship_pipeline.py`, `mapping_branch`, DB schema или order runtime.

Текущий статус:

- manual store-aware stock exporter реализован как отдельный CLI-only path;
- source для него — `app/business/business_store_stock_preview.py`;
- live send по-прежнему требует явного `--send --confirm`;
- scheduler integration не добавлялась.

## 2. Что проверено

Проверены:

- `app/services/business_stock_scheduler_service.py`
- `app/business/dropship_pipeline.py`
- `app/services/database_service.py`
- `app/services/stock_export_service.py`
- `app/services/stock_update_service.py`
- `app/models.py`
- `app/business/business_store_stock_preview.py`
- `app/routes.py`
- `ENV_REFERENCE.md`
- `docs/business_multistore_architecture.md`
- `docs/business_store_catalog_identity.md`
- `docs/business_stores_ui_handoff.md`

## 3. Legacy Stock Scheduler Flow

### 3.1 Кто запускает stock export

Текущий Business stock scheduler находится в:

- `app/services/business_stock_scheduler_service.py`

Entry point:

- `schedule_business_stock_tasks()`
- `run_business_stock_once()`

Фактический запуск stock export происходит через:

- `await run_pipeline(enterprise_code, "stock")`

где `run_pipeline` определён в:

- `app/business/dropship_pipeline.py`

### 3.2 Как выбирается enterprise_code

Scheduler:

1. Загружает все `EnterpriseSettings`.
2. Фильтрует только `data_format='Business'`.
3. Если Business enterprise нет:
   - skip.
4. Если их больше одного:
   - resolution = `ambiguous`
   - scheduler skip.
5. Если ровно один:
   - использует его `enterprise_code`.

Это поведение реализовано через:

- `_load_business_enterprises()`
- `_resolve_business_enterprise(...)`

### 3.3 Какие флаги проверяются

Текущий scheduler gate:

- `BusinessSettings.business_stock_enabled`
- `BusinessSettings.business_stock_interval_seconds`

Fallback, если `business_settings` row отсутствует:

- `EnterpriseSettings.stock_enabled`
- `EnterpriseSettings.stock_upload_frequency`

Дополнительно:

- `_is_stock_due(...)` использует `EnterpriseSettings.last_stock_upload` и `stock_upload_frequency`
- fallback path запускает stock только когда пришло время очередного запуска

### 3.4 Почему scheduler пропускает запуск при нескольких Business enterprises

Причина жёстко зашита в:

- `_resolve_business_enterprise(...)`

Если найдено более одного `EnterpriseSettings` c `data_format='Business'`, scheduler считает состояние неоднозначным и делает skip.

Это одна из ключевых причин, почему store-aware path нельзя встраивать в текущий single-enterprise scheduler без переработки control-plane.

## 4. Legacy Stock Payload Building

### 4.1 Где строится payload

Текущий stock payload строится в:

- `app/business/dropship_pipeline.py`
- `build_stock_payload(session, enterprise_code)`

Перед этим формируется best-offer set в:

- `build_best_offers_by_city(session)`

### 4.2 Как выбирается best offer

Legacy best-offer selection:

- только из `Offer.stock > 0`
- группировка по `(city, product_code)`
- ranking:
  - `stock_priority_flag DESC`
  - `price ASC`
  - `supplier.priority DESC`
  - `stock DESC`
  - `updated_at DESC`

Источники ranking:

- `Offer`
- `DropshipEnterprise.priority`
- env override `STOCK_PRIORITY_SUPPLIERS`

Это важное отличие от current store preview:

- preview сейчас использует локальную approximation;
- legacy runtime additionally учитывает `DropshipEnterprise.priority` и special stock-priority suppliers.

### 4.3 Какая форма item payload

`build_stock_payload(...)` возвращает список объектов такого вида:

```json
{
  "branch": "30630",
  "code": "123456",
  "price": 367.0,
  "qty": 4,
  "price_reserve": 367.0
}
```

Факты:

- внешний код товара сейчас = `product_code` из `Offer`, то есть legacy internal code
- количество = `stock`
- цена = `price`
- `price_reserve` = то же самое значение, что `price`
- branch берётся не из `Offer`, а через `mapping_branch`

### 4.4 Где участвует city -> branch mapping

В `dropship_pipeline.py`:

- `_load_branch_mapping(session, enterprise_code)` читает `mapping_branch`
- возвращает `{store_id(city) -> branch}`

Затем:

- `row["city"]` из best offer
- `branch = city2branch.get(city)`

Если branch не найден:

- запись пропускается
- `skipped_no_branch += 1`

Следствие:

- legacy path привязан к `mapping_branch.store_id == Offer.city`
- `mapping_branch.branch` является runtime branch target
- `BusinessStore.tabletki_branch` в legacy path не участвует

## 5. Legacy Sender Path

### 5.1 Как отправляется stock

После `build_stock_payload(...)` runtime делает:

- `_dump_payload_to_file(...)`
- `process_database_service(file_path, "stock", enterprise_code)`

То есть current sender path идёт не напрямую на endpoint, а через DB-service layer.

### 5.2 Что делает process_database_service для stock

`app/services/database_service.py`

Stock flow:

1. загрузка payload из JSON file
2. `stock_pre_delete_guard`
3. `delete_old_stock`
4. `apply_discount_rate`
5. опционально `update_stock`, если `EnterpriseSettings.stock_correction=true`
6. `stock_pre_persistence_validation`
7. `export_stock` -> `process_stock_file(...)`
8. `save_stock`
9. `flush_stock`
10. `update_last_upload`
11. `commit`

Это критично:

- current legacy stock export связан с persistence side effects;
- он не является pure sender;
- он меняет таблицу `InventoryStock` и `last_stock_upload`.

### 5.3 Что валидируется в DB-service

`_validate_stock_phase(...)` требует:

- `branch`
- `code`
- `price`
- `qty`

Также проверяет:

- `price_reserve <= price`
- `qty >= 0`
- уникальность пары `(branch, code)`

### 5.4 Что делает update_stock

`app/services/stock_update_service.py`

Если `stock_correction=true`, legacy flow:

- идёт в `DeveloperSettings.endpoint_orders`
- делает `GET /api/orders/{branch}/4`
- уменьшает `qty` на основе данных API

Следствие:

- legacy stock export включает дополнительную online side effect/adjustment логику;
- manual store-aware exporter не должен случайно унаследовать этот path, если задача требует read-only overlay export от текущего preview state.

### 5.5 Финальный sender

`app/services/stock_export_service.py`

`process_stock_file(...)`:

- группирует flat rows в:

```json
{
  "Branches": [
    {
      "Code": "30630",
      "DateTime": "20.04.2026 13:00:00",
      "Rests": [
        {
          "Code": "123456",
          "Price": 367.0,
          "Qty": 4,
          "PriceReserve": 367.0
        }
      ]
    }
  ]
}
```

- endpoint:
  - `DeveloperSettings.endpoint_stock + "/Import/Rests"`
- auth:
  - basic auth
  - `EnterpriseSettings.tabletki_login`
  - `EnterpriseSettings.tabletki_password`

Response handling:

- возвращает `(status_code, response_text)`
- явного retry нет
- batching нет
- success/failure mostly логируется и нотифицируется

## 6. Role of MappingBranch

`mapping_branch` fields:

- `enterprise_code`
- `store_id`
- `branch`

Current legacy role:

- `store_id` хранит legacy city/scope value
- `branch` хранит runtime branch target
- `enterprise_code` ограничивает mapping конкретным enterprise runtime

Почему future store-aware exporter не должен использовать `mapping_branch`:

- `mapping_branch` является legacy runtime bridge, а не store overlay identity;
- store-aware overlay уже имеет собственный target:
  - `BusinessStore.tabletki_enterprise_code`
  - `BusinessStore.tabletki_branch`
- manual exporter для одного `BusinessStore` должен отправлять в branch выбранного store, а не в branch, резолвленный по legacy city mapping

Как не сломать legacy path:

- legacy stock runtime продолжает использовать `mapping_branch` без изменений;
- store-aware exporter идёт отдельным CLI/manual path;
- никакой общий helper не должен тихо переключать legacy stock на `BusinessStore.tabletki_branch`

## 7. Current BusinessStore Stock Preview

Текущий preview builder:

- `app/business/business_store_stock_preview.py`

Сейчас он отдаёт:

- `internal_product_code`
- `external_product_code`
- `supplier_code`
- `qty`
- `base_price`
- `markup_percent`
- `final_store_price_preview`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `exportable`
- `reasons`

Также summary:

- `offer_rows_total`
- `candidate_products`
- `exportable_products`
- `missing_code_mapping`
- `missing_price_adjustment`
- `markup_applied_products`

### 7.1 Хватает ли preview для реального sender path

Для direct manual sender path почти хватает.

Уже есть всё ключевое для store-aware semantics:

- внешний код
- qty
- overlay final price
- branch target
- exportable flag

Но для технической реализации sender не хватает нескольких вещей:

- row-level field, явно соответствующего legacy `branch`
  - сейчас есть `tabletki_branch`, но не alias `branch`
- row-level field `price_reserve`
  - legacy sender требует `PriceReserve`
- top-level/group-ready structure не построена
  - current preview flat

Дополнительно желательно:

- хранить/возвращать `base_price_rounded_for_payload`, если в будущем появится отдельное payload rounding rule
- отдельный `payload_item_preview`, если потребуется 1:1 сравнение с legacy JSON

### 7.2 Важное архитектурное наблюдение

Current stock preview already models the right store-aware economics:

- `Code` должен быть `external_product_code`
- `Price` должен быть `final_store_price_preview`
- target branch должен быть `BusinessStore.tabletki_branch`

Значит future manual exporter должен использовать preview as source of truth, а не заново считать overlay pricing внутри sender.

## 8. Recommended Target Design

### 8.1 Future module

Рекомендуемый будущий модуль:

- `app/business/business_store_stock_exporter.py`

Основная функция:

```python
export_business_store_stock(
    session,
    store_id: int,
    dry_run: bool = True,
    limit: int | None = None,
    require_confirm: bool = True,
) -> dict
```

### 8.2 Core rules

- source = `build_store_stock_payload_preview(...)`
- брать только `exportable=true`
- `Code` = `external_product_code`
- `Qty` = `qty`
- `Price` = `final_store_price_preview`
- `PriceReserve`:
  - на первом шаге лучше делать равным `final_store_price_preview`
  - чтобы не нарушать legacy validator `price_reserve <= price`
- `Branch` = `BusinessStore.tabletki_branch`
- target enterprise = `BusinessStore.tabletki_enterprise_code`
- не использовать `mapping_branch` для store-aware target
- не изменять `offers`
- не создавать mappings
- не создавать price adjustments
- live send только через явный manual path

### 8.3 Recommended sender path

Для store-aware exporter не рекомендуется переиспользовать полный `process_database_service("stock", ...)` path.

Причины:

- он удаляет/перезаписывает `InventoryStock`
- он обновляет `last_stock_upload`
- он применяет `discount_rate`
- он может запускать `stock_correction/update_stock`
- он опирается на legacy `enterprise_code` semantics

Рекомендуемый путь:

1. preview builder
2. filter exportable rows
3. transform в legacy-compatible `Branches/Rests` JSON
4. direct call sender-level function наподобие `send_to_endpoint(...)`
5. dry-run summary/sample JSON
6. live send only with confirm

То есть:

- payload shape желательно совместим с `stock_export_service`
- side effects `database_service` желательно не использовать

## 9. CLI Proposal

Рекомендуемый будущий CLI:

- `app/scripts/business_store_stock_export.py`

Аргументы:

- `--store-id`
- `--store-code`
- `--dry-run`
- `--send`
- `--confirm`
- `--limit`
- `--output-json`

Правила:

- default = dry-run
- `--send` без `--confirm` запрещён
- `--dry-run` и `--send` одновременно запрещены
- если указан `store-code`, резолвить store по нему
- результат печатать JSON summary

## 10. Endpoint / UI Recommendation

На следующем этапе не рекомендуется добавлять live stock send в UI.

Рекомендация:

- UI оставить с текущим `Stock preview`
- live send оставить только в CLI
- developer endpoint можно обсуждать позже отдельно, после фактической проверки manual CLI path

## 11. Risks

Обязательно учитывать:

- неправильный stock payload format:
  - `Branches/Rests` must match current Tabletki stock API expectations
- отправка stock не в тот branch:
  - особенно если случайно использовать `mapping_branch.branch` вместо `BusinessStore.tabletki_branch`
- использование internal code вместо external code
- использование `base_price` вместо `final_store_price_preview`
- изменение `offers.price` вместо overlay final price
- конфликт с current `PRICE_JITTER` / balancer semantics
  - current preview already bypasses balancer/jitter runtime path
  - future exporter must decide consciously whether preview price is authoritative
- двойная выгрузка legacy и store-aware stock для одного scope
- scheduler conflict при нескольких Business enterprises
- order flow пока не знает reverse mapping from external store code back to internal code
- accidental reuse of `process_database_service("stock", ...)` may introduce unwanted deletes, updates, and last-upload side effects

## 12. Required Payload Mapping for Future Exporter

Минимальное целевое соответствие:

| store-aware source | future stock payload |
| --- | --- |
| `external_product_code` | `Code` |
| `qty` | `Qty` |
| `final_store_price_preview` | `Price` |
| `final_store_price_preview` | `PriceReserve` |
| `BusinessStore.tabletki_branch` | `Branches[].Code` |

Top-level payload target:

```json
{
  "Branches": [
    {
      "Code": "30630",
      "DateTime": "20.04.2026 13:00:00",
      "Rests": [
        {
          "Code": "8C411335BA",
          "Price": 367,
          "Qty": 4,
          "PriceReserve": 367
        }
      ]
    }
  ]
}
```

## 13. Files for Future Implementation

Точные файлы для следующего этапа:

- new:
  - `app/business/business_store_stock_exporter.py`
  - `app/scripts/business_store_stock_export.py`
- maybe update:
  - `app/business/business_store_stock_preview.py`
  - `docs/business_store_catalog_identity.md`
  - `docs/business_multistore_architecture.md`
  - `docs/business_stores_ui_handoff.md`

Файлы, которые не должны меняться в рамках manual exporter:

- `app/business/dropship_pipeline.py`
- `app/services/business_stock_scheduler_service.py`
- `app/services/database_service.py`
- `app/services/stock_export_service.py`
- `mapping_branch` runtime logic
- DB schema / Alembic

## 14. Implementation Checklist for Next Prompt

1. Confirm final sender path:
   - direct sender over `endpoint_stock`, not `process_database_service`
2. Add store-aware stock exporter module
3. Reuse `build_store_stock_payload_preview(...)`
4. Add missing preview fields if needed:
   - `branch`
   - `price_reserve`
5. Build `Branches/Rests` payload
6. Add dry-run JSON output
7. Add CLI with `--send --confirm`
8. Validate no DB mutations in dry-run
9. Validate no `offers.price` mutation
10. Test against `business_364` in dry-run

## 15. Conclusion

Legacy Business stock export today is tightly coupled to:

- single-enterprise scheduler resolution
- `mapping_branch`
- `dropship_pipeline`
- `process_database_service`
- `stock_export_service`
- enterprise credentials from `EnterpriseSettings`

Store-aware manual stock export should therefore be implemented as a separate explicit operator path, similar in spirit to manual store-aware catalog export:

- one explicit `BusinessStore`
- preview as source of truth
- direct sender path
- no scheduler
- no DB side effects
- no `mapping_branch` target resolution
