# Business Store Order Auto-Confirm Strategy

## 1. Scope

Этот документ фиксирует аудит текущего `auto_confirm` flow и выбор безопасной стратегии для future store-aware orders после reverse mapping.

Ограничения этого шага:

- runtime не меняется;
- `order_fetcher.py` не меняется;
- `auto_confirm.py` не меняется;
- `order_sender.py` не меняется;
- scheduler не меняется;
- DB schema не меняется;
- `InventoryStock` не заполняется для store-aware branch;
- внешние API не вызываются.

## 2. Что проверено

Проверены:

- `app/services/auto_confirm.py`
- `app/services/order_fetcher.py`
- `app/services/order_sender.py`
- `app/business/order_sender.py`
- `app/business/business_store_order_mapper.py`
- `app/business/business_store_order_integration_simulator.py`
- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/models.py`
- `docs/business_store_order_reverse_mapping_audit.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_multistore_architecture.md`
- `ENV_REFERENCE.md`

## 3. Current auto_confirm Flow

### 3.1 Где вызывается auto_confirm

`auto_confirm` вызывается в:

- `app/services/order_fetcher.py`

Текущий path:

1. `fetch_orders_for_enterprise(...)`
2. для каждого `branch` из `mapping_branch`
3. запрашиваются статусы `0, 2, 4, 4.1`
4. если `enterprise.auto_confirm == true` и `status in [0, 2]`
   - вызывается outbound processor (`Business` -> `process_and_send_order(...)`)
   - затем вызывается `process_orders(session, data)`
   - затем результат отправляется обратно в Tabletki через `send_orders_to_tabletki(...)`

### 3.2 Какие статусы обрабатываются

Auto-confirm path реально участвует для incoming orders со статусами:

- `0`
- `2`

Для этих заказов:

- сначала order идёт в seller-side processor;
- потом `process_orders(...)` формирует confirm/cancel rows;
- потом это уходит обратно в Tabletki.

### 3.3 Как проверяется наличие остатков

`app/services/auto_confirm.py`

`process_orders(...)`:

- читает `order["branchID"]`
- читает `row["goodsCode"]`
- делает lookup только в `InventoryStock`:
  - `InventoryStock.branch == branchID`
  - `InventoryStock.code == goodsCode`
- других источников availability не использует

### 3.4 Что происходит при нехватке остатка

Если stock row отсутствует или `qty == 0`:

- строка помечается как `not_available`
- в outgoing row остаются:
  - `goodsCode`
  - `qty`

Если остатка достаточно:

- outgoing row содержит:
  - `goodsCode`
  - `goodsName`
  - `goodsProducer`
  - `qtyShip`
  - `priceShip`

Если остатка меньше запроса:

- outgoing row отправляется как partial shipment:
  - `qtyShip = stock_entry.qty`

Итоговый order status:

- `7`, если все строки `not_available`
- `4`, если хотя бы одна строка available/partial

### 3.5 Какие payload-ы отправляются обратно в Tabletki

`app/services/order_sender.py`

`send_orders_to_tabletki(...)`:

- для `statusID in [4, 6]` отправляет confirm payload в `/api/orders`
- для `statusID == 7` или нулевых `qtyShip` отправляет cancel payload в `/api/Orders/cancelledOrders`

Важно:

- outgoing payload использует `goodsCode` из уже обработанного order rows;
- cancel flow тоже использует `goodsCode`;
- original external Tabletki code therefore нельзя терять, если order стал store-aware.

## 4. Почему InventoryStock Dependency Критична

### 4.1 Current dependency

`auto_confirm.py` зависит только от `InventoryStock`.

После reverse mapping:

- `goodsCode` может стать internal `product_code`
- это правильно для `Offer`, `CatalogMapping`, `CatalogSupplierMapping`, `MasterCatalog`
- но не решает проблему branch-local stock availability, если `InventoryStock` для нового store-aware branch не существует

### 4.2 Почему InventoryStock может отсутствовать

Это не баг, а intentional design текущего manual store-aware stock path.

`manual store-aware stock exporter`:

- не вызывает `process_database_service("stock", ...)`
- не вызывает `save_stock`
- не пишет `InventoryStock`
- не обновляет `last_stock_upload`

То есть store-aware stock может быть уже live в Tabletki branch, но legacy local `InventoryStock` для того же branch всё ещё пуст.

### 4.3 Практический эффект

Stage 2 simulation уже показал типичный кейс:

- reverse mapping successful
- `Offer`, `CatalogSupplierMapping`, `MasterCatalog`, `CatalogMapping` found
- `InventoryStock` for normalized branch not found

Следствие:

- legacy `auto_confirm` может ошибочно считать такой store-aware order not available;
- это делает прямую runtime integration через current `process_orders(...)` unsafe.

## 5. Strategy Comparison

## 5.1 Strategy A

Временно bypass `auto_confirm` для store-aware orders.

Model:

- reverse mapping выполняется;
- order идёт дальше в Business/SalesDrive flow;
- legacy `process_orders(...)` не вызывается для store-aware orders;
- confirm/cancel обратно в Tabletki current auto-confirm path не делает.

Плюсы:

- минимальный runtime risk;
- не требует записи в `InventoryStock`;
- не требует немедленно redesign availability checker;
- не ломает legacy enterprises/orders;
- защищает от ложных `not_available`/`cancel` из-за пустого `InventoryStock`.

Минусы:

- store-aware orders не получат current automatic confirm behavior;
- потребуется manual or later dedicated confirm logic;
- operational latency выше, пока не будет следующего этапа.

Риски:

- если просто bypass без отдельного operator process, order lifecycle может стать менее автоматизированным;
- нужно явно отличать store-aware order от legacy order before auto-confirm branch.

## 5.2 Strategy B

Сделать отдельный store-aware stock availability checker.

Model:

- reverse mapping выполняется;
- вместо `InventoryStock` checker uses:
  - `Offer`
  - `store.legacy_scope_key`
  - possibly same best-offer semantics as store stock preview/export
- confirm/cancel обратно в Tabletki должен использовать original external `goodsCode`, если API требует именно его.

Плюсы:

- closest functional replacement for future store-aware auto-confirm;
- не требует писать в `InventoryStock`;
- согласуется со store-aware stock source of truth.

Минусы:

- это уже новая business logic, а не просто wiring;
- нужно решить точную availability semantics:
  - best offer selection
  - partial qty rules
  - use of markup or not
  - branch/store scoping
- confirm/cancel payload needs explicit handling of original external code.

Риски:

- если checker semantics будут отличаться от реально отправленного stock payload, можно подтвердить то, чего branch уже не увидит в Tabletki;
- легко accidentally продублировать или рассинхронизировать stock selection logic.

## 5.3 Strategy C

Писать store-aware stock в `InventoryStock` для нового branch.

Варианты:

- `branch = BusinessStore.tabletki_branch`, `code = internal_product_code`
- `branch = BusinessStore.tabletki_branch`, `code = external_product_code`

Плюсы:

- формально позволяет переиспользовать current `auto_confirm.py`

Минусы:

- нарушает текущее intentional separation между manual store-aware export и legacy stock persistence;
- возвращает side effects, от которых специальной архитектурой уже ушли;
- неясно, какой code писать:
  - internal code нужен внутренней логике
  - external code нужен branch-facing Tabletki identity
- легко загрязнить legacy stock contour.

Риски:

- сильный риск загрязнения `InventoryStock`;
- риск сломать legacy analytics / assumptions / stock correction behavior;
- риск появления двух разных semantic layers в одной таблице;
- риск перепутать branch/code semantics for legacy orders.

Вывод:

- Strategy C на первом этапе не рекомендована.

## 6. Recommended Strategy

### 6.1 First safe runtime step

Рекомендуемая первая стратегия:

- `Strategy A`: bypass legacy auto-confirm for store-aware orders

Причина:

- это единственный путь, который не требует ни записи в legacy persistence, ни новой availability logic прямо в момент первого runtime подключения reverse mapping;
- он минимизирует blast radius;
- он не создаёт ложных cancel/partial decisions по пустому `InventoryStock`.

### 6.2 Second step after that

Следующий этап после безопасного bypass:

- `Strategy B`: dedicated store-aware availability checker

То есть roadmap:

1. сначала safe routing + bypass
2. потом separate checker
3. только потом optional store-aware auto-confirm

### 6.3 What not to do

Не рекомендуется на первом runtime step:

- не писать store-aware stock в `InventoryStock`
- не пускать store-aware orders напрямую в current `process_orders(...)`
- не пытаться reuse legacy `auto_confirm` only by swapping `goodsCode` to internal code

## 7. Future Integration Point

### 7.1 Где вставлять normalize_store_order_payload

Будущая точка интеграции:

- `app/services/order_fetcher.py`
- сразу после получения incoming `order` из Tabletki
- до любого downstream use of `goodsCode`

### 7.2 Где ставить guard для mapping_error

Guard нужен сразу после normalization:

- если `mapping_error`
  - не вызывать `process_orders(...)`
  - не вызывать `process_and_send_order(...)` как normal path
  - логировать и отправлять order в manual review / explicit warning path

### 7.3 Где решать auto_confirm bypass/checker

Точка decision:

- в `fetch_orders_for_enterprise(...)`
- after normalization and store detection
- before `process_orders(session, data)`

Pseudo-contract:

- legacy order -> current `process_orders(...)`
- store-aware order -> bypass current auto-confirm on first stage
- later:
  - store-aware order -> dedicated checker

### 7.4 Где сохранять original external goodsCode

Current mapper already preserves:

- `row["originalGoodsCodeExternal"]`

Это поле должно сохраняться through in-memory normalized payload path until:

- confirm/cancel/status sender decides which code Tabletki expects back

## 8. Future Implementation Checklist

1. Integrate `normalize_store_order_payload(...)` into `order_fetcher.py`
2. Detect whether resolved order belongs to store-aware `BusinessStore`
3. Add explicit guard:
   - `mapping_error` => no downstream processing
4. Add stage-1 bypass for legacy `auto_confirm`
5. Preserve `originalGoodsCodeExternal` through confirm/cancel path
6. Design dedicated store-aware availability checker
7. Only after checker is verified, discuss store-aware auto-confirm enablement

## 9. Risk Matrix

| Risk | Severity | Why |
| --- | --- | --- |
| False `not_available` because `InventoryStock` empty for new branch | high | current `auto_confirm.py` depends only on `InventoryStock` |
| Wrong confirm/cancel code sent back to Tabletki | high | original external `goodsCode` may be required |
| Writing store-aware rows into `InventoryStock` pollutes legacy contour | high | breaks current architecture boundary |
| Reverse mapping ok but downstream auto-confirm still wrong | high | internal code alone does not solve branch stock persistence |
| Store-aware orders accidentally treated as legacy | medium | missing guard in `order_fetcher` |
| Divergent availability semantics between checker and stock export | medium | if Strategy B is added carelessly |

## 10. Conclusion

Current `auto_confirm` is tightly coupled to legacy `InventoryStock`.

Because store-aware stock export intentionally does not populate `InventoryStock`, direct reuse of legacy `auto_confirm` after reverse mapping is unsafe.

Recommended path:

- first runtime integration step: reverse mapping + bypass legacy auto-confirm for store-aware orders
- second step: separate store-aware availability checker
- do not write store-aware stock into `InventoryStock` on the first stage

Current implementation status:

- Stage 1 reverse mapper exists
- Stage 2 integration simulator exists
- Stage 3 runtime wiring in `order_fetcher.py` exists behind `BUSINESS_STORE_ORDER_MAPPING_ENABLED`
- when the flag is enabled:
  - legacy orders keep old behavior
  - store-aware orders are normalized and bypass legacy `process_orders(...)`
  - mapping errors are skipped from normal downstream processing
- dedicated availability checker is still not implemented
