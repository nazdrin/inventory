# Store-Native Offers Migration Design

## 1. Executive summary

Текущая pricing-модель исторически держится на глобальной таблице `offers`, где ключом scope является `city`.

Это работает для legacy baseline-контура, но плохо масштабируется под целевую Business-модель, где:

- operational target уже определяется магазином / branch;
- store-level routing и overlays живут в `BusinessStore`;
- baseline и custom runtime расходятся по ownership, но до сих пор сходятся в одном глобальном `offers` storage;
- order/supplier selection продолжает читать `offers` как глобальный источник истины без store dimension.

Постоянная стратегия “добавлять ещё один overlay поверх city-based offers” не выглядит хорошей целевой архитектурой. Она увеличивает число late adapters, но не решает главную проблему: runtime readers до сих пор не знают, что такое store-native effective offer.

Поэтому целевая модель должна быть такой:

- supplier остаётся глобальной master-сущностью;
- market scope остаётся отдельной сущностью для competitor / balancer / threshold logic;
- store получает store-supplier overrides;
- итоговые offers становятся store-native;
- stock runtime читает final store-level price/qty уже из store-native source of truth;
- order runtime мигрируется позже и отдельно, без резкого cutover.

Рекомендуемый migration path:

- не переписывать текущую таблицу `offers` in-place;
- вводить staged модель через новую store-native сущность (`offers_v2` или аналог);
- сначала ввести `business_store_supplier_settings`;
- потом делать dual-write / compare;
- readers переключать поэтапно и по allowlist.

## 2. Текущее состояние

### 2.1 Как сейчас устроены `offers`

Текущая таблица `offers`:

- хранит `product_code`, `supplier_code`, `city`, `price`, `wholesale_price`, `stock`;
- имеет `UniqueConstraint(product_code, supplier_code, city)`;
- индексируется по `city`, `product_code`, `price`;
- является global city-scoped price/stock snapshot, а не store-native сущностью.

Ключевые определения:

- `app/models.py`
  - `Offer`
  - `CompetitorPrice`
  - `DropshipEnterprise`

### 2.2 Где используется `city`

Сейчас `city` участвует в нескольких разных ролях:

- как supplier market scope:
  - `DropshipEnterprise.city`
  - `_split_cities(ent.city)` в `app/business/dropship_pipeline.py`
- как competitor scope:
  - `CompetitorPrice.city`
  - `app/business/competitor_price_loader.py`
- как balancer scope:
  - `balancer_policy_log.city`
  - `balancer_segment_stats.city`
  - `balancer_order_facts.city`
  - `balancer_test_state.city`
- как legacy stock export scope:
  - `Offer.city`
  - `mapping_branch.store_id -> branch`
- как store stock source selector в custom dry-run/runtime:
  - `BusinessStore.legacy_scope_key`
  - `Offer.city == legacy_scope_key`
- как source for UI legacy scope options:
  - `GET /business-stores/meta/legacy-scopes`
  - `Offer.city` aggregation in `app/routes.py`

### 2.3 Как baseline/custom используют pricing today

`baseline`:

- stock идёт через `app/business/dropship_pipeline.py`;
- pipeline refreshes global `offers` by supplier + city;
- competitor logic и balancer logic работают по `city`;
- branch routing делается позже через `mapping_branch.store_id -> branch`;
- baseline stock payload до недавнего времени вообще не зависел от `BusinessStore`, кроме нового late store markup overlay;
- catalog path не использует store-native pricing storage.

`custom`:

- store-aware stock preview/export читает `Offer.city == BusinessStore.legacy_scope_key`;
- then applies store-level code mapping and store price adjustments;
- то есть даже custom stock сейчас не читает store-native offers, а строит store payload поверх legacy city-scoped offers.

## 3. Dependency audit

### 3.1 DB schema

Критичные city-based сущности:

- `offers`
  - global city-based stock/price snapshot
- `competitor_prices`
  - key `(code, city)`
- `dropship_enterprises.city`
  - supplier scope definition, potentially multi-city
- balancer tables
  - all scoped by `city`
- `business_stores.legacy_scope_key`
  - store -> city linkage for store-aware stock/catalog previews

Критичность:

- `offers`: критично
- `competitor_prices`: критично
- `dropship_enterprises.city`: критично
- balancer tables: критично
- `legacy_scope_key`: transitional, но всё ещё runtime-relevant для custom stock

### 3.2 Pricing runtime

Главный pricing engine находится в `app/business/dropship_pipeline.py`.

Там `city` участвует напрямую в:

- загрузке competitor prices по `(product_code, city)`;
- выборе active balancer policy по `(supplier_code, city)`;
- расчёте threshold / no-competitor / under-competitor branches;
- batched upsert в `offers` per supplier + city;
- выборе best offer по `(city, product_code)` в `build_best_offers_by_city()`.

Это означает:

- city-based pricing сейчас не просто storage detail;
- это core dimension pricing engine.

### 3.3 Balancer / threshold logic

Balancer сегодня полностью scope-based, не store-based.

Фактически он опирается на:

- `city`
- `supplier`
- `segment`
- price bands

Через:

- `app/business/balancer/repository.py`
- `app/business/balancer/jobs.py`
- `app/business/balancer/live_logic.py`
- `app/business/dropship_pipeline.py`

Это критичная зона. Её нельзя просто “переключить на branch/store” без отдельной модели scope.

### 3.4 Competitor logic

Competitor delivery prices хранятся и обновляются по `city`:

- `app/business/competitor_price_loader.py`
- `CompetitorPrice(code, city, competitor_price)`

Это явно market scope, а не store identity.

### 3.5 Stock export

Baseline stock:

- `build_best_offers_by_city()`
- `build_stock_payload_with_markup_overlay_report()`
- branch routing через `mapping_branch.store_id`

Custom stock:

- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/services/business_store_stock_publish_service.py`

Custom stock сейчас фактически делает:

- city-scoped best-offer selection from `offers`
- then store overlays

То есть final stock payload пока ещё не store-native at source.

### 3.6 Order routing / downstream

Это самый важный non-obvious dependency.

`app/business/order_sender.py` использует `offers` глобально без city/store dimension:

- `_fetch_supplier_by_price()`
- `_fetch_stock_qty()`
- `_pick_supplier_for_single_item()`
- `_fetch_supplier_price()`
- `_fetch_supplier_wholesale_price()`
- `_prefetch_offers_for_products()`

Текущий order contour implicitly assumes:

- `offers` are globally queryable by `product_code` / `supplier_code`;
- supplier selection не привязан к store-native offer row.

Это значит:

- прямой cutover `offers` на store-native schema сломает или изменит order behavior, если order readers не будут оставлены на legacy path.

### 3.7 Admin panel / operator settings

UI ownership today:

- `BusinessSettingsPage` владеет global pricing settings (`BusinessSettings`);
- `BusinessStoresPage` владеет enterprise runtime и store overlays;
- `SuppliersPage` / `DropshipEnterprisePanel` по сути владеют supplier-side scope fields, включая `city`.

Следствие:

- будущая store-native architecture потребует отдельного store-supplier config owner screen или section;
- просто добавить это в текущую store form можно, но это быстро перегрузит страницу.

### 3.8 Integrations

Критичные downstream integrations:

- Tabletki stock export
- SalesDrive order send
- competitor feed load
- balancer order facts / stats

Из них store-native pricing напрямую first-order влияет прежде всего на:

- stock export
- order supplier selection
- pricing observability / balancer compare

## 4. Целевая архитектура

### 4.1 Supplier master config

Глобальным должно остаться:

- supplier identity (`DropshipEnterprise.code`)
- feed source / parser / active flags
- supplier-level default pricing parameters
- market scope coverage
- competitor/balancer participation

Это не store-owned.

### 4.2 Store-supplier config

На уровень `store + supplier` должны переехать именно operational overrides:

- store-specific enable/disable for supplier
- store-specific extra markup / price adjustment strategy
- optional priority overrides for supplier inside store
- optional stock availability filters if появятся позже

Это отдельная сущность. Store-level общая markup today слишком грубая, если target model действительно supplier-aware.

### 4.3 Store-native offers

Новая final offer сущность должна хранить effective stock/price per store.

Минимальный смысл такой строки:

- `store_id`
- `enterprise_code`
- `branch`
- `product_code`
- `supplier_code`
- `market_scope_key`
- `base_price`
- `effective_price`
- `wholesale_price`
- `stock`
- metadata about pricing source / competitor scope / supplier scope

Главное:

- store-native offers должны быть final source for stock runtime;
- не обязательно final source for competitor logic;
- не обязательно immediate source for orders on day one.

### 4.4 Market scope / competitor scope

`city` не нужно пытаться уничтожить как концепт.

Лучше разделить две сущности:

- market scope
  - competitor market
  - balancer pricing scope
  - threshold context
- operational target
  - store / branch

В target architecture `city` должен остаться как pricing market scope, но перестать быть основным ключом final exportable `offers`.

## 5. Предлагаемая схема хранения

### Вариант A — in-place migration existing `offers`

Идея:

- расширить текущую таблицу `offers`;
- добавить `store_id`, `enterprise_code`, `branch`, `market_scope_key`;
- постепенно перестроить unique/index model;
- readers переводить на новый shape.

Плюсы:

- одна таблица;
- нет dual-table complexity;
- проще финальное состояние.

Минусы:

- слишком большой blast radius;
- order readers, stock readers, legacy reports и UI мета-запросы внезапно окажутся на одной мутирующей таблице;
- rollback будет болезненным;
- migration потребует очень аккуратной backwards compatibility на всех readers сразу.

Риск:

- высокий.

Rollback:

- сложный.

Влияние на runtime:

- высокое сразу.

### Вариант B — staged migration через `offers_v2`

Идея:

- оставить текущую `offers` как legacy source;
- создать новую store-native offers сущность;
- сначала dual-write или dual-build;
- сравнивать payloads;
- переводить readers по allowlist.

Плюсы:

- минимальный blast radius;
- можно держать legacy orders и legacy reports на старой таблице;
- stock runtime можно переводить отдельно от order runtime;
- проще rollback: cutover flag назад на legacy readers.

Минусы:

- временное удвоение storage/runtime complexity;
- нужен explicit compare tooling;
- потребуется discipline в naming и ownership.

Риск:

- средний и управляемый.

Rollback:

- простой.

Влияние на runtime:

- поэтапное.

### Recommended option

Рекомендован `Вариант B`.

Причина:

- текущая таблица `offers` слишком глубоко встроена в baseline stock, custom stock preview, order sender и UI meta-sources;
- staged `offers_v2` позволяет сначала мигрировать stock runtime, не ломая order contour.

## 6. Recommended target data model

### 6.1 `business_store_supplier_settings`

Новая верхнеуровневая сущность должна связывать:

- `store_id`
- `supplier_code`

И хранить:

- `is_active`
- optional per-store-supplier markup mode / min / max / strategy
- optional priority override
- optional market scope override, если появится обоснование
- timestamps / source metadata

### 6.2 `offers_v2` / store-native offers

Рекомендуемый верхнеуровневый shape:

- primary id
- `store_id`
- `enterprise_code`
- `branch`
- `product_code`
- `supplier_code`
- `market_scope_key`
- `base_price`
- `effective_price`
- `wholesale_price`
- `stock`
- `pricing_policy_source`
- `competitor_scope_key`
- `updated_at`

Recommended uniqueness:

- `(store_id, product_code, supplier_code)`

Возможный optional convenience uniqueness:

- `(enterprise_code, branch, product_code, supplier_code)`

### 6.3 Legacy scope relation

`market_scope_key` should not disappear.

It should explicitly represent:

- competitor market / balancer market / supplier city scope

and not be overloaded with:

- target branch
- store identity

### 6.4 What stays global

Оставить глобальными:

- `DropshipEnterprise`
- `CompetitorPrice`
- balancer policy/state tables
- `BusinessSettings` pricing policy

Не надо в первой фазе переносить competitor/balancer tables на store level.

## 7. Migration strategy

### Phase 0 — dependency freeze and observability

- зафиксировать current readers of legacy `offers`;
- добавить explicit compare/report tooling для old vs new stock payloads;
- определить enterprise allowlist for experiments.

### Phase 1 — introduce store-supplier config

- добавить `business_store_supplier_settings`;
- без runtime cutover;
- заполнить initial defaults from current store-level markup model;
- зафиксировать ownership in admin-panel.

### Phase 2 — introduce `offers_v2`

- создать store-native schema;
- пока не трогать readers;
- создать builder/writer path, который на основе:
  - global supplier data
  - market scope pricing
  - store-supplier overrides
  - branch/store assignment
  пишет final rows into `offers_v2`.

### Phase 3 — dual-build / compare

- legacy `offers` continue unchanged;
- `offers_v2` строится параллельно;
- baseline и custom stock payloads сравниваются read-only;
- discrepancies логируются по enterprise/store/branch/product sample.

### Phase 4 — switch custom stock readers

- сначала перевести только custom stock preview/export;
- затем custom stock live path;
- baseline оставить на legacy path.

### Phase 5 — optional baseline stock migration

- только после того как `offers_v2` и compare стабильны;
- baseline можно переводить отдельно:
  - либо сразу на store-native stock read,
  - либо на hybrid adapter, если baseline routing должен остаться legacy.

### Phase 6 — order runtime migration

- order sender readers переводятся отдельно;
- либо остаются на legacy `offers` дольше;
- либо получают explicit source selection between `offers` and `offers_v2`.

### Phase 7 — cleanup legacy city-bound reads

- только после успешного cutover stock + orders;
- можно убирать `Offer.city` как core runtime driver.

## 8. Runtime cutover strategy

### 8.1 Stock runtime

Переводить первым именно stock runtime.

Порядок:

- custom stock preview
- custom dry-run publish
- custom live publish
- baseline preview
- baseline live publish

Причина:

- stock runtime already has clearer enterprise/store ownership;
- order runtime has more implicit coupling to global `offers`.

### 8.2 Baseline

Baseline нельзя резко ломать.

Safest path:

- keep baseline pricing market scope logic city-based;
- build store-native rows from that same market scope;
- keep branch routing unchanged on first cutover.

### 8.3 Custom

Custom — лучший первый consumer для `offers_v2`, потому что:

- он уже conceptually store-aware;
- current implementation всё равно делает city → store overlay adaptation;
- new storage just removes that impedance mismatch.

### 8.4 Feature flags / allowlist

Нужны explicit runtime switches:

- global feature flag for `offers_v2` build
- enterprise allowlist for custom read cutover
- separate flag for baseline read cutover
- separate flag for order runtime migration

Не надо делать one-shot global switch.

## 9. UI / operator model

High-level ownership:

- `BusinessSettingsPage`
  - остаётся global pricing policy / jitter / threshold control-plane
- `BusinessStoresPage`
  - остаётся enterprise runtime + store overlays
- supplier/store-supplier configuration
  - должен иметь отдельный owner block, скорее store-centric with supplier subsettings

Оператору не нужно показывать `offers_v2` как raw table.

Нужны более прикладные экраны:

- store supplier overrides
- store effective pricing diagnostics
- compare preview old vs new source

## 10. Risks

### Data consistency risks

- divergence between legacy `offers` and `offers_v2`
- duplicated source-of-truth period
- stale store-supplier overrides

### Pricing drift risks

- effective store price may diverge from legacy city price unexpectedly
- competitor caps / threshold floors may be applied at wrong stage if market scope is lost

### Performance risks

- row count explosion when moving from city-scoped offers to store-native offers
- more expensive refresh/build jobs

### Scheduler risks

- dual-build increases runtime cost
- compare jobs can create latency spikes if not isolated

### Rollback risks

- moderate under staged approach
- high under in-place migration

### Operator confusion risks

- if UI mixes market scope and store target again
- if supplier-level vs store-supplier-level pricing ownership is unclear

## 11. Recommendation

Рекомендую:

- принять staged migration через `offers_v2`;
- не трогать current `offers` in-place;
- first isolate missing config layer: `business_store_supplier_settings`;
- only after that introduce store-native final offers builder;
- switch custom stock readers first;
- orders migrate later.

Что intentionally не делать в первой фазе:

- не переписывать balancer around branch/store;
- не переносить competitor prices на store level;
- не переключать order sender на новую таблицу;
- не смешивать эту миграцию с catalog identity work.

## 12. Concrete next Codex step

Следующий safest implementation step:

- `design accepted -> add store-supplier config only`

Почему не `offers_v2` сразу:

- сейчас в модели ещё нет полноценной store-supplier override сущности;
- если сначала создать `offers_v2`, придётся закладывать часть target semantics на неполной configuration model;
- store-supplier config — это low-blast-radius foundation step, после которого `offers_v2` будет проектироваться намного точнее.

После этого уже следующий шаг:

- `add offers_v2 schema + builder in shadow mode`

