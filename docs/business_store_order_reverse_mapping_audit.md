# Business Store Order Reverse Mapping Audit

## 1. Scope

Этот документ фиксирует аудит текущего order runtime и target plan для future store-aware reverse mapping в Business multistore.

Цель:

- понять, как сейчас order flow читает branch, `goodsCode` и `enterprise_code`;
- определить, где `goodsCode` уже трактуется как canonical internal product code;
- описать, как безопасно добавить future reverse mapping:
  - `external_product_code -> internal_product_code`
  - через `business_store_product_codes`;
- не менять текущий runtime на этом шаге.

Current status after Stage 1:

- isolated pure reverse mapper is implemented in `app/business/business_store_order_mapper.py`;
- CLI test harness is implemented in `python -m app.scripts.business_store_order_mapping_test`;
- mapper is read-only and not connected to live order runtime;
- `order_fetcher`, `auto_confirm`, `order_sender`, and scheduler remain unchanged.

Current status after Stage 2:

- integration simulation is implemented in `app/business/business_store_order_integration_simulator.py`;
- CLI simulation harness is implemented in `python -m app.scripts.business_store_order_integration_simulation`;
- simulation runs the existing mapper first and then checks downstream readiness in legacy read paths;
- simulation is still pure/read-only and not connected to live order runtime.

Auto-confirm strategy for future runtime integration is documented separately in:

- `docs/business_store_order_autoconfirm_strategy.md`

Current status after Stage 3:

- `order_fetcher.py` can now call `normalize_store_order_payload(...)` behind feature flag `BUSINESS_STORE_ORDER_MAPPING_ENABLED`;
- legacy orders still keep their old path;
- store-aware normalized orders bypass legacy `process_orders(...)`;
- `mapping_error` orders are logged and skipped from normal downstream processing;
- `auto_confirm.py`, `app/business/order_sender.py`, and `app/services/order_sender.py` remain unchanged.

Current status after Stage 5:

- store-aware inbound order flow still resolves store by branch / `BusinessStore`;
- reverse product code lookup can now switch behind `BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED`;
- default rollback path remains store-level:
  - `BusinessStoreProductCode.store_id`
- enterprise-level mode uses:
  - `BusinessEnterpriseProductCode.enterprise_code`
- normalized payload shape stays the same:
  - `rows[].goodsCode = internal_product_code`
  - `rows[].originalGoodsCodeExternal = incoming external goodsCode`
  - `rows[]["_businessStoreId"] = store.id`

Ограничения этого шага:

- не менять `order_fetcher.py`;
- не менять `order_sender.py`;
- не менять scheduler;
- не менять DB schema;
- не менять `business_store_product_codes`;
- не менять SalesDrive payload;
- не вызывать внешние API.

## 2. Что проверено

Проверены:

- `app/services/order_scheduler_service.py`
- `app/services/order_fetcher.py`
- `app/services/auto_confirm.py`
- `app/services/order_sender.py`
- `app/business/order_sender.py`
- `app/models.py`
- `app/routes.py`
- `app/schemas.py`
- `docs/business_multistore_architecture.md`
- `docs/business_store_catalog_identity.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_stores_ui_handoff.md`
- `ENV_REFERENCE.md`

Дополнительно проверен поиск по проекту:

- `fetch_orders_for_enterprise`
- `goodsCode`
- `MappingBranch`
- `supplierlist`
- `SalesDrive`
- `auto_confirm`
- `branch`
- `enterprise_code`

## 3. Current Order Scheduler / Fetch Flow

### 3.1 Кто запускает order fetch

Текущий планировщик заказов:

- `app/services/order_scheduler_service.py`

Главный цикл:

- `schedule_order_fetcher_tasks()`

Он:

1. раз в минуту выбирает все `EnterpriseSettings`, где `order_fetcher=True`;
2. для каждого `enterprise_code` вызывает `fetch_orders_for_enterprise(...)`;
3. после fetch для `data_format='Business'` отдельно вызывает `process_cancelled_orders_service(...)`.

### 3.2 Как выбирается enterprise

В отличие от `business_stock_scheduler_service`, текущий order scheduler:

- не ищет один "primary Business enterprise";
- не использует `business_settings`;
- не проверяет ambiguity;
- просто запускается по всем строкам `EnterpriseSettings.order_fetcher=True`.

Следствие:

- если появятся несколько `EnterpriseSettings` с `data_format='Business'` и `order_fetcher=True`, scheduler будет обрабатывать все их параллельно;
- отдельной защиты от multistore ambiguity сейчас нет;
- risk profile выше, чем в legacy Business stock scheduler.

### 3.3 Какие flags участвуют

Текущий fetch flow использует:

- `EnterpriseSettings.order_fetcher`
- `EnterpriseSettings.auto_confirm`
- `EnterpriseSettings.data_format`
- `EnterpriseSettings.tabletki_login`
- `EnterpriseSettings.tabletki_password`
- `DeveloperSettings.endpoint_orders`

Роли:

- `order_fetcher=True` включает enterprise в polling;
- `auto_confirm=True` включает ветку auto-confirm / reserve-confirm logic;
- `data_format` выбирает processor:
  - `Business` -> `app.business.order_sender.process_and_send_order`
  - другие -> другие processors/status checkers.

### 3.4 Как выбираются branches

`fetch_orders_for_enterprise(...)` читает:

- `MappingBranch.branch`
- по `MappingBranch.enterprise_code == enterprise_code`

Дальше для каждого branch делает:

- `GET {endpoint_orders}/api/Orders/{branch}/{status}`

Текущий order fetch path therefore:

- полностью привязан к `mapping_branch`;
- не использует `BusinessStore.tabletki_branch`;
- не использует `BusinessStore.tabletki_enterprise_code`.

## 4. Current Incoming Tabletki Order Shape

### 4.1 Какие поля реально использует runtime

По коду runtime order payload использует:

- `order["id"]`
- `order["code"]`
- `order["branchID"]`
- `order["customerPhone"]`
- `order["rows"]`
- `row["goodsCode"]`
- `row["goodsName"]`
- `row["goodsProducer"]`
- `row["qty"]`
- `row["price"]`
- `deliveryData[*].key/value`

Дополнительно в отдельных местах:

- `tabletkiOrder` / `TabletkiOrder`
- `statusID`

### 4.2 Где лежит branch

Branch приходит и используется в двух формах:

- branch в URL fetch path:
  - `/api/Orders/{branch}/{status}`
- branch в самом payload:
  - `order["branchID"]`

Current runtime не делает reverse lookup branch -> store overlay.

### 4.3 Где лежит товарный код

Товарный код приходит в:

- `row["goodsCode"]`

Именно это поле сейчас считается primary product identity во всём downstream order runtime.

### 4.4 Предположение current runtime

Текущий runtime не содержит слоя:

- `external goodsCode -> internal product_code`

По факту он предполагает, что:

- `goodsCode` уже совместим с internal `InventoryStock.code`;
- `goodsCode` уже совместим с `Offer.product_code`;
- `goodsCode` уже совместим с `CatalogMapping.ID`;
- `goodsCode` уже совместим с `CatalogSupplierMapping.sku`;
- `goodsCode` уже совместим с `MasterCatalog.sku`.

Следствие:

- если после store-aware catalog export Tabletki начнёт присылать внешний код магазина, например `8C411335BA`, текущий runtime не сможет безопасно обработать такой заказ как Business legacy order.

## 5. Where goodsCode Is Used

### 5.1 Auto-confirm / stock availability

`app/services/auto_confirm.py`

Логика:

- берёт `order["branchID"]`
- берёт `row["goodsCode"]`
- ищет `InventoryStock` по:
  - `InventoryStock.branch == branchID`
  - `InventoryStock.code == goodsCode`

Следствие:

- если `goodsCode` станет store external code, auto-confirm не найдёт legacy stock rows;
- это один из первых guaranteed breakpoints.

### 5.2 Tabletki confirm/cancel payload

`app/services/order_sender.py`

При подтверждении/отказе обратно в Tabletki используются те же входящие `goodsCode`:

- `_build_cancel_payload(...)`
- `send_orders_to_tabletki(...)`

Следствие:

- original external code может понадобиться для обратных status/cancel calls;
- после reverse mapping нельзя просто терять original incoming `goodsCode`.

### 5.3 Supplier selection in Business flow

`app/business/order_sender.py`

Почти весь supplier resolution строится на `r.goodsCode`:

- `_fetch_supplier_by_price(...)`
- `_find_suppliers_within_tolerance(...)`
- `_find_nearest_supplier_by_price(...)`
- `_prefetch_offers_for_products(...)`
- `_fetch_supplier_price(...)`
- `_fetch_supplier_wholesale_price(...)`
- `_fetch_stock_qty(...)`

Все эти функции ожидают:

- `Offer.product_code == goodsCode`

Следствие:

- внешний store code instantly ломает supplier resolution и margin logic.

### 5.4 Catalog mapping / supplier mapping

`app/business/order_sender.py`

Для order line enrichment используются:

- `CatalogMapping.ID == goodsCode`
- `CatalogSupplierMapping.sku == goodsCode`
- `MasterCatalog.sku == goodsCode`

Функции:

- `_fetch_sku_from_catalog_mapping(...)`
- `_fetch_sku_from_master_mapping(...)`
- `_fetch_barcode_and_supplier_code(...)`
- `_fetch_barcode_and_supplier_code_master(...)`
- `_fetch_sku_for_order_line(...)`
- `_fetch_barcode_and_supplier_code_for_order_line(...)`

Следствие:

- без reverse mapping заказ с external code не сможет корректно построить SalesDrive product rows.

## 6. Current SalesDrive Payload Implications

### 6.1 Что уходит в SalesDrive

`build_salesdrive_payload(...)` и `_build_products_block(...)` формируют SalesDrive payload.

Для product rows сейчас уходит:

- `id = r.goodsCode`
- `sku = r.goodsCode`
- `barcode` и `description` подтягиваются через mapping lookup от `goodsCode`
- `name = goodsName`
- `costPerItem = row.price`
- `amount = row.qty`

На root-level также уходят:

- `externalId = order["id"]`
- `branch = branch`
- `tabletkiOrder = order["code"]`
- `supplier`
- `supplierlist`

### 6.2 Что это значит для multistore

SalesDrive payload сейчас implicitly expects:

- `goodsCode` already internal / canonical;
- supplier lookup and SKU resolution already work from that code.

Если в Business order flow попадёт внешний store code:

- `id` и `sku` в SalesDrive станут внешними Tabletki codes;
- supplier resolution пойдёт по неправильному product identity;
- downstream SalesDrive semantics become unsafe.

Следствие:

- до `build_salesdrive_payload(...)` внутренняя логика должна уже видеть `internal_product_code`;
- original external Tabletki code нужно сохранить отдельно для audit/debug/cancel/status continuity.

## 7. Store Identification For Incoming Order

### 7.1 Какие ключи реально есть сейчас

Current runtime точно имеет:

- enterprise context from scheduler/fetcher:
  - `enterprise_code`
- branch context from fetch loop:
  - `branch` from `MappingBranch.branch`
- order payload branch:
  - `order["branchID"]`

Current code не показывает явного `tabletki_enterprise_code` внутри incoming order payload.

### 7.2 Best future key

Наиболее безопасный будущий ключ идентификации store:

1. if incoming payload/transport context ever exposes both values:
   - `(BusinessStore.tabletki_enterprise_code, BusinessStore.tabletki_branch)`
2. otherwise:
   - `BusinessStore.tabletki_branch`

Почему:

- `tabletki_branch` уже является реальным target при store-aware catalog/stock export;
- в `BusinessStore` уже есть unique partial index:
  - `uq_business_stores_tabletki_identity(tabletki_enterprise_code, tabletki_branch)`
- branch alone может быть достаточно в пределах одного enterprise, но pair безопаснее.

### 7.3 Fallback model

Future order mapper должен работать так:

- если branch/store resolution уверенно находит `BusinessStore`:
  - включать store-aware reverse mapping;
- если store не найден:
  - не трогать `goodsCode`;
  - использовать legacy flow;
- если найден store, но mapping не найден:
  - no auto-confirm;
  - no outbound send to SalesDrive;
  - явный warning/error path.

## 8. Reverse Mapping Design

### 8.1 Target module

Рекомендуемый будущий модуль:

- `app/business/business_store_order_mapper.py`

### 8.2 Target helper functions

Implemented Stage 1 functions:

- `resolve_business_store_for_order(...)`
- `map_external_order_code_to_internal(...)`
- `normalize_store_order_payload(...)`

### 8.3 Expected behavior

Если incoming order принадлежит store-aware `BusinessStore`:

1. определить store по `tabletki_branch`, а при наличии и по паре `(tabletki_enterprise_code, tabletki_branch)`;
2. для каждого `row["goodsCode"]` найти:
   - `BusinessStoreProductCode.store_id == resolved_store.id`
   - `BusinessStoreProductCode.external_product_code == incoming goodsCode`
3. заменить рабочий `goodsCode` на `internal_product_code`;
4. сохранить original external code в отдельном debug/original field в памяти payload normalization layer;
5. дальше передавать в current internal order logic уже internal code.

Current implemented preservation field:

- `row["originalGoodsCodeExternal"]`

Current debug field:

- `row["_businessStoreId"]`

Если mapping не найден:

- не auto-confirm;
- не отправлять заказ дальше как normal Business order;
- логировать clear warning;
- возможно переводить order в manual review path.

Если order legacy:

- не трогать `goodsCode`;
- current runtime идёт как есть.

## 9. DB / Index Assessment

### 9.1 Хватает ли текущей таблицы

Для самой reverse mapping логики на первом этапе текущей таблицы достаточно:

- `business_store_product_codes`

Потому что уже есть:

- `unique(store_id, internal_product_code)`
- `unique(store_id, external_product_code)`

Следствие:

- глобально искать `external_product_code` нельзя;
- scoped lookup by `store_id` already well-defined.

### 9.2 Нужен ли новый index

Для correctness:

- дополнительная миграция не обязательна уже сейчас;
- `unique(store_id, external_product_code)` уже даёт нужный lookup path.

Для future performance:

- отдельный named index `(store_id, external_product_code)` может быть необязателен, если unique constraint already materializes index in PostgreSQL;
- сначала можно обойтись без новой миграции.

### 9.3 Нужна ли новая таблица логов

На первом этапе:

- нет, необязательно.

Возможный future enhancement:

- отдельный order-mapping log или persisted original-external-code field,
- но это уже второй шаг после proof-of-concept mapper.

## 10. What Will Definitely Break Without Reverse Mapping

Без reverse mapping store-aware orders сломают:

- `auto_confirm` availability checks against `InventoryStock`
- Business supplier selection via `Offer`
- `CatalogMapping` / `CatalogSupplierMapping` / `MasterCatalog` lookups
- SalesDrive payload `id` / `sku` semantics
- margin-based and mixed-supplier decision logic

Дополнительный risk:

- cancel/status updates обратно в Tabletki могут потребовать original external `goodsCode`, значит его нельзя потерять после normalization.

## 11. Recommended Migration Plan

### Stage 1

Read-only helper layer only:

- add `business_store_order_mapper.py`
- add pure store resolution
- add pure reverse mapping from external code to internal code
- no scheduler change
- no fetcher/sender integration yet

### Stage 2

Add isolated manual test harness:

- mock incoming Tabletki order payload
- run mapper only
- verify mapped payload
- verify legacy payload untouched

### Stage 3

Integrate mapper into order fetch/runtime before:

- `process_orders(...)`
- `process_and_send_order(...)`

Точка внедрения должна быть ранней:

- сразу после получения incoming order и before any use of `goodsCode`.

### Stage 4

Add explicit error path for:

- store resolved but missing external->internal mapping
- duplicate/ambiguous store resolution
- original external code retention for cancel/status paths

## 12. Manual Test Plan For Next Step

Без live runtime, безопасный тест должен быть таким:

1. mock incoming order for `business_364`
2. row contains:
   - `goodsCode = external_product_code`
3. resolve store by `tabletki_branch=30630`
4. map external code through `BusinessStoreProductCode`
5. verify:
   - mapped working code = `internal_product_code`
   - original external code preserved in debug/original field
6. run same helper on legacy order:
   - payload remains unchanged
7. run missing-mapping case:
   - returns explicit error/reason

Рекомендуемый future CLI:

- `app/scripts/business_store_order_mapping_test.py`

Suggested args:

- `--store-code`
- `--external-code`
- `--output-json`

## 13. Risks

- incoming orders with external code will break current auto-confirm logic
- wrong store resolution will map to wrong internal code
- identical external codes across stores are safe only if lookup is scoped by `store_id`
- global lookup by `external_product_code` is unsafe
- legacy orders must not pass through store-aware reverse mapping without confident store resolution
- SalesDrive can receive wrong product identity if mapping is skipped
- cancel/status flows may still need original external Tabletki code
- catalog/stock are already store-aware, but orders are not yet

## 14. Files For Future Implementation

Recommended future implementation files:

- `app/business/business_store_order_mapper.py`
- `app/services/order_fetcher.py`
- `app/services/auto_confirm.py`
- `app/business/order_sender.py`
- `app/services/order_sender.py`
- optional test helper:
  - `app/scripts/business_store_order_mapping_test.py`

Possible docs to update on next implementation step:

- `docs/business_multistore_architecture.md`
- `docs/business_store_catalog_identity.md`
- `docs/business_stores_ui_handoff.md`

## 15. Conclusion

Current Business order runtime is still fully legacy with respect to product identity:

- fetch path is enterprise + `mapping_branch` based;
- incoming `goodsCode` is assumed to already be internal;
- all stock, supplier, mapping, and SalesDrive logic relies on that assumption.

Therefore store-aware catalog/stock can go live before orders only if:

- order fetch path remains legacy,
- or a reverse mapping layer is added before any downstream use of `goodsCode`.

The safest next step is not to patch isolated lookups one by one, but to add one normalization layer early in order processing:

- resolve store;
- map external code to internal code;
- preserve original external code;
- only then pass normalized payload into legacy internals.

## 16. Stage 1 CLI Usage

Implemented test harness:

- `python -m app.scripts.business_store_order_mapping_test`

Examples:

```bash
python -m app.scripts.business_store_order_mapping_test \
  --store-code business_364 \
  --external-code 8C411335BA \
  --output-json
```

```bash
python -m app.scripts.business_store_order_mapping_test \
  --tabletki-branch 30630 \
  --external-code 8C411335BA \
  --output-json
```

```bash
python -m app.scripts.business_store_order_mapping_test \
  --store-code business_364 \
  --external-code NOT_EXISTING_CODE \
  --output-json
```

Expected semantics:

- `status="ok"` for valid store-scoped external mapping
- `status="mapping_error"` for missing mapping
- `status="legacy_passthrough"` when store is not resolved

Stage 1 explicitly does not integrate the mapper into:

- `process_orders(...)`
- `process_and_send_order(...)`
- `order_fetcher`
- scheduler runtime

## 17. Stage 2 Integration Simulation

Implemented simulator:

- `app/business/business_store_order_integration_simulator.py`

Implemented CLI:

- `python -m app.scripts.business_store_order_integration_simulation`

The simulation:

1. builds a mock incoming Tabletki order with external `goodsCode`;
2. runs `normalize_store_order_payload(...)`;
3. if mapper succeeds, validates downstream readiness in:
   - `Offer`
   - `CatalogSupplierMapping`
   - `MasterCatalog`
   - `CatalogMapping`
   - `InventoryStock`
4. returns a diagnostic report without touching live runtime.

Expected status semantics:

- `ok`
  - mapper succeeded
  - `Offer` found the internal code
  - `MasterCatalog` or `CatalogSupplierMapping` also found the internal code
  - `InventoryStock` for normalized branch exists
- `warning`
  - mapper succeeded
  - downstream catalog/offer readiness exists
  - but `InventoryStock` for branch is missing
- `error`
  - mapper failed
  - or internal code is not represented in the key legacy read paths

Important interpretation:

- missing `InventoryStock` is not proof that reverse mapping is wrong;
- it is a warning that legacy `auto_confirm.py` still depends on `InventoryStock` and will require special handling when runtime integration is discussed.

Examples:

```bash
python -m app.scripts.business_store_order_integration_simulation \
  --store-code business_364 \
  --external-code 8C411335BA \
  --output-json
```

```bash
python -m app.scripts.business_store_order_integration_simulation \
  --tabletki-branch 30630 \
  --external-code 8C411335BA \
  --output-json
```

```bash
python -m app.scripts.business_store_order_integration_simulation \
  --store-code business_364 \
  --external-code NOT_EXISTING_CODE \
  --output-json
```

Recommended next integration decision:

- decide the exact normalization point before `process_orders(...)` and `process_and_send_order(...)`;
- decide how legacy `InventoryStock` dependency should behave for store-aware orders.

Current runtime note:

- the normalization point is now inserted in `order_fetcher.py` before any downstream Business use of `goodsCode`, but only when `BUSINESS_STORE_ORDER_MAPPING_ENABLED=true`;
- dedicated store-aware availability checker is still not implemented.

## 18. Status 2 Audit For Store-Aware Orders

Current legacy status `2` path:

- in `app/services/order_fetcher.py`
- when `enterprise.auto_confirm == false`
- for incoming `status == 0`
- legacy orders do:
  1. outbound processor
  2. `order["statusID"] = 2.0`
  3. `send_single_order_status_2(...)`

Current store-aware gap:

- normalized store-aware orders are processed in a separate branch
- after successful outbound Business processing they only log success
- they do not call:
  - `send_single_order_status_2(...)`
  - or `send_orders_to_tabletki(...)`

Operational consequence:

- the order remains on Tabletki in status `0`
- next scheduler run can fetch the same order again from `/api/Orders/{branch}/0`
- the same order can be resent to SalesDrive

Current sender shape:

- `send_single_order_status_2(...)` sends the whole `order` object to `POST /api/orders`
- it only filters rows by positive `qty` / `qtyShip`
- it does not rewrite `goodsCode`
- it preserves top-level `branchID`

This means:

- if called with a normalized store-aware order as-is, it would send internal `goodsCode` back to Tabletki

Recommended safe rule for future implementation:

- SalesDrive path keeps normalized internal `goodsCode`
- Tabletki status `2` path restores:
  - `row["goodsCode"] = row["originalGoodsCodeExternal"]`
  - when `originalGoodsCodeExternal` exists
- legacy rows without preserved external code remain unchanged

Safe insertion point:

- `app/services/order_fetcher.py`
- immediately after successful `process_and_send_order(...)`
- only for store-aware normalized orders
- only for incoming status `0`

Success signal today:

- `process_and_send_order(...)` does not return explicit success metadata
- practical success condition is absence of exception

Recommended feature flag:

- `BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED=false`

Most likely files for the next step:

- `app/services/order_fetcher.py`
- optional helper in `app/business/business_store_order_mapper.py`
- `app/services/order_sender.py`
- `ENV_REFERENCE.md`

Current implementation status:

- helper `restore_tabletki_goods_codes_for_status(...)` now exists in `app/business/business_store_order_mapper.py`
- `order_fetcher.py` can now send Tabletki status `2` for store-aware normalized orders behind `BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED`
- SalesDrive path still uses normalized internal `goodsCode`
- Tabletki status `2` path restores external `goodsCode` from `originalGoodsCodeExternal`
- legacy order flow remains unchanged

Related follow-up:

- outbound SalesDrive/webhook status mapping for status `4` / `4.1` / `6` / `7` is audited separately in `docs/business_store_outbound_status_mapping_audit.md`
- current Stage 1 there: isolated outbound mapper resolves store by `BusinessStore.tabletki_branch` because Business contour expects `mapping_branch.branch` and `tabletki_branch` alignment
- long-term stronger path still remains persisted order-to-store link via `externalId` / `tabletkiOrder`
