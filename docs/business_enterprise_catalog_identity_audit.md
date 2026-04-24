# Business Enterprise Catalog Identity Audit

## 1. Scope

Этот документ фиксирует архитектурный аудит перед переходом от текущей store-level catalog identity к enterprise-level catalog identity в Business multistore contour.

В этом шаге:

- runtime не меняется;
- DB schema не меняется;
- UI не меняется;
- scheduler-ы не меняются;
- webhook/runtime integration не меняется;
- внешние API не вызываются.

## 2. Executive Summary

Текущее состояние:

- catalog identity сейчас store-level:
  - catalog preview/export/publish ищут codes и names через `store_id`;
  - catalog target branch тоже сейчас store-level и берётся из `BusinessStore.tabletki_branch`;
- stock export уже разделён на store-level routing и store-level pricing:
  - branch = `BusinessStore.tabletki_branch`;
  - code lookup = `BusinessStoreProductCode.store_id`;
  - price adjustment lookup = `BusinessStoreProductPriceAdjustment.store_id`;
- inbound/outbound order mapping тоже сейчас store-scoped:
  - branch резолвит `BusinessStore`;
  - code lookup идёт через `BusinessStoreProductCode.store_id`.

Целевая модель:

- catalog identity должна стать enterprise-level;
- catalog publish target branch должен стать enterprise-level:
  - `EnterpriseSettings.branch_id`;
- product code/name mappings должны стать enterprise-level и одинаковыми для всех магазинов одного `enterprise_code`;
- stores должны остаться store-level только для:
  - stock routing;
  - stock pricing;
  - offer scope;
  - order routing;
  - migration flags.

Главный вывод:

- текущий контур ещё не разделяет "catalog identity" и "store overlay";
- для перехода нужен новый enterprise-level слой mappings, после чего catalog logic переносится на enterprise scope, а stock/order logic начинает использовать `store -> enterprise` bridge.

## 3. What Was Checked

- `app/models.py`
- `app/business/business_store_catalog_preview.py`
- `app/business/business_store_catalog_exporter.py`
- `app/services/business_store_catalog_publish_service.py`
- `app/scripts/business_store_catalog_publish.py`
- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/services/business_store_stock_publish_service.py`
- `app/business/business_store_code_generator.py`
- `app/business/business_store_name_generator.py`
- `app/business/business_store_order_mapper.py`
- `app/business/business_store_tabletki_outbound_mapper.py`
- `app/services/order_fetcher.py`
- `app/business/salesdrive_webhook.py`
- `app/services/master_catalog_scheduler_service.py`
- `admin-panel/src/pages/BusinessStoresPage.jsx`
- `docs/business_multistore_architecture.md`
- `docs/business_store_catalog_identity.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_store_order_reverse_mapping_audit.md`
- `docs/business_store_outbound_status_mapping_audit.md`
- `ENV_REFERENCE.md`

## 4. Current Store-Level Identity Model

### 4.1 Current DB ownership

Current store-scoped identity tables:

- `business_store_product_codes`
- `business_store_product_names`
- `business_store_product_price_adjustments`

Current ownership:

- `BusinessStoreProductCode`
  - unique by `(store_id, internal_product_code)`
  - unique by `(store_id, external_product_code)`
- `BusinessStoreProductName`
  - unique by `(store_id, internal_product_code)`
- `BusinessStoreProductPriceAdjustment`
  - unique by `(store_id, internal_product_code)`

This means:

- code identity is different per store;
- name identity is different per store;
- price adjustment is different per store.

### 4.2 Where catalog currently uses `BusinessStore.tabletki_branch`

Current catalog preview/export path is explicitly store-targeted:

- `app/business/business_store_catalog_preview.py`
  - returns store context with `tabletki_enterprise_code` and `tabletki_branch`;
- `app/business/business_store_catalog_exporter.py`
  - validates `store.tabletki_enterprise_code`;
  - validates `store.tabletki_branch`;
  - builds endpoint:
    - `.../Import/Ref/{store.tabletki_branch}`;
- `app/services/business_store_catalog_publish_service.py`
  - eligibility requires:
    - `store.tabletki_branch`
    - `store.tabletki_enterprise_code`.

Current result:

- catalog publish target is store branch, not enterprise branch.

### 4.3 Where catalog code/name mapping is currently store-scoped

Catalog code lookup:

- `app/business/business_store_catalog_preview.py`
  - `_load_store_product_code_map(session, store_id)`
  - `BusinessStoreProductCode.store_id == store_id`

Catalog name lookup:

- `app/business/business_store_catalog_preview.py`
  - `_load_store_product_name_map(session, store_id)`
  - `BusinessStoreProductName.store_id == store_id`

Catalog identity behavior:

- `code_strategy` and `name_strategy` are read from `BusinessStore`;
- `is_legacy_default` is read from `BusinessStore`;
- if `code_strategy != legacy_same`, external code is resolved through store mapping;
- if `name_strategy == supplier_random`, external name is resolved through store mapping.

### 4.4 Where stock code mapping is currently store-scoped

Stock code lookup:

- `app/business/business_store_stock_preview.py`
  - `_load_store_product_code_map(session, store_id)`
  - `BusinessStoreProductCode.store_id == store_id`

Stock price adjustment lookup:

- `app/business/business_store_stock_preview.py`
  - `_load_store_product_price_adjustment_map(session, store_id)`
  - `BusinessStoreProductPriceAdjustment.store_id == store_id`

Stock export target:

- `app/business/business_store_stock_exporter.py`
  - branch = `BusinessStore.tabletki_branch`

Current result:

- stock is already partly in the right shape for future target:
  - routing is store-level;
  - pricing is store-level;
- but code identity is still wrong for future architecture because it depends on `store_id`.

### 4.5 Where inbound order reverse mapping is currently store-scoped

Store resolution:

- `app/business/business_store_order_mapper.py`
  - `resolve_business_store_for_order(...)`
  - resolves by:
    - `(tabletki_enterprise_code, tabletki_branch)`
    - or `tabletki_branch`

Code reverse mapping:

- `app/business/business_store_order_mapper.py`
  - `map_external_order_code_to_internal(...)`
  - lookup:
    - `BusinessStoreProductCode.store_id == resolved_store.id`
    - `external_product_code == incoming goodsCode`

Current result:

- store is branch-scoped;
- code mapping is store-scoped.

### 4.6 Where outbound status mapping is currently store-scoped

Store resolution:

- `app/business/business_store_tabletki_outbound_mapper.py`
  - `resolve_business_store_by_tabletki_branch(...)`
  - resolves active `BusinessStore` by `tabletki_branch`

Code restoration:

- `app/business/business_store_tabletki_outbound_mapper.py`
  - `map_internal_code_to_store_external(...)`
  - lookup:
    - `BusinessStoreProductCode.store_id == store.id`
    - `internal_product_code == parameter/sku`

Runtime integration:

- `app/business/salesdrive_webhook.py`
  - main `/webhooks/salesdrive` path can already call isolated outbound mapper

Current result:

- webhook branch resolves store;
- internal -> external conversion is store-scoped.

## 5. What Must Become Enterprise-Level

These concerns should move from `BusinessStore` scope to `enterprise_code` scope:

- catalog external code identity;
- catalog external name identity;
- code generation strategy;
- name generation strategy;
- catalog publish target branch;
- catalog publish readiness from identity perspective.

These concerns should remain store-level:

- stock branch routing;
- stock qty/price shaping;
- extra markup;
- offer scope / `legacy_scope_key`;
- order store resolution by branch;
- migration state per store;
- store participation flags for stock/orders.

## 6. Proposed DB Target Model

### 6.1 New enterprise-level identity tables

Recommended new tables:

1. `business_enterprise_product_codes`
2. `business_enterprise_product_names`

Optional later helper table:

3. `business_enterprise_catalog_settings`
  - only if identity-specific settings should be separated from `BusinessStore` and `EnterpriseSettings`
  - not required for first migration step

### 6.2 `business_enterprise_product_codes`

Recommended fields:

- `id`
- `enterprise_code`
- `internal_product_code`
- `external_product_code`
- `code_source`
- `is_active`
- `created_at`
- `updated_at`

Recommended constraints:

- FK:
  - `enterprise_code -> enterprise_settings.enterprise_code`
- unique:
  - `(enterprise_code, internal_product_code)`
  - `(enterprise_code, external_product_code)`
- indexes:
  - `enterprise_code`
  - `internal_product_code`
  - `external_product_code`
  - `is_active`

Recommended semantics:

- one stable external code per internal SKU per enterprise;
- same code reused by all stores of the enterprise;
- stock and orders use this same external code.

### 6.3 `business_enterprise_product_names`

Recommended fields:

- `id`
- `enterprise_code`
- `internal_product_code`
- `external_product_name`
- `name_source`
- `source_supplier_id`
- `source_supplier_code`
- `source_supplier_product_id`
- `source_supplier_product_name_raw`
- `is_active`
- `created_at`
- `updated_at`

Recommended constraints:

- FK:
  - `enterprise_code -> enterprise_settings.enterprise_code`
- unique:
  - `(enterprise_code, internal_product_code)`
- indexes:
  - `enterprise_code`
  - `internal_product_code`
  - `(source_supplier_id, source_supplier_code)`
  - `is_active`

Recommended semantics:

- one stable external product name per internal SKU per enterprise;
- same name reused by all stores of the enterprise catalog.

### 6.4 What stays store-level

Keep as store-scoped:

- `business_store_product_price_adjustments`

Keep on `BusinessStore`:

- `legacy_scope_key`
- `tabletki_branch`
- `stock_enabled`
- `orders_enabled`
- `extra_markup_*`
- `takes_over_legacy_scope`
- `migration_status`

### 6.5 Fields that should likely move out of `BusinessStore`

These fields become architecturally misleading once catalog identity is enterprise-level:

- `BusinessStore.code_strategy`
- `BusinessStore.code_prefix`
- `BusinessStore.name_strategy`
- `BusinessStore.catalog_only_in_stock` only if catalog itself becomes purely enterprise-wide without store assortment filtering

Recommendation:

- in phase 1 keep them for compatibility;
- in phase 2 deprecate them from store-level UI/runtime;
- replace with enterprise-level catalog identity settings.

## 7. Target Runtime Model

### 7.1 Catalog exporter target

Future catalog exporter should use:

- target branch:
  - `EnterpriseSettings.branch_id`
- code mapping:
  - `business_enterprise_product_codes`
- name mapping:
  - `business_enterprise_product_names`

Future catalog exporter should not use:

- `BusinessStore.tabletki_branch`
- `BusinessStoreProductCode`
- `BusinessStoreProductName`

Implication:

- current `business_store_catalog_preview.py`
- current `business_store_catalog_exporter.py`
- current `business_store_catalog_publish_service.py`

all need redesign or replacement, because they are currently explicitly store-scoped.

### 7.2 Stock exporter target

Future stock exporter should use:

- target branch:
  - `BusinessStore.tabletki_branch`
- external code:
  - enterprise-level code from `business_enterprise_product_codes`
- qty/price:
  - still store-level, based on:
    - `legacy_scope_key`
    - `extra_markup_*`

Future stock exporter should keep:

- `BusinessStoreProductPriceAdjustment` as store-level

Future stock exporter should stop using:

- `BusinessStoreProductCode` by `store_id`

### 7.3 Order reverse mapper target

Future inbound order flow:

1. resolve store by branch
2. read `store.enterprise_code`
3. map external code to internal through enterprise-level code mapping

This means `normalize_store_order_payload(...)` should become:

- store resolution = store-level
- code reverse mapping = enterprise-level

### 7.4 Outbound status mapper target

Future outbound webhook/status flow:

1. resolve store by webhook branch
2. read `store.enterprise_code`
3. convert internal code to external through enterprise-level code mapping

This means `restore_salesdrive_products_for_tabletki_outbound(...)` should become:

- store resolution = store-level
- internal -> external mapping = enterprise-level

## 8. Safe Migration Strategy For Current `business_364`

Current known case:

- `store_id = 2`
- `enterprise_code = 364`
- working E2E already depends on current store-scoped mappings

Safe migration principle:

- first duplicate identity from store scope to enterprise scope;
- only then switch readers one surface at a time;
- do not delete current store mappings until every reader is switched.

### 8.1 Migration of code mappings

Source:

- `business_store_product_codes`
- filter:
  - `store_id = 2`

Target:

- `business_enterprise_product_codes`
- `enterprise_code = '364'`

Required checks:

- every `internal_product_code` is unique in source store;
- every `external_product_code` is unique in source store;
- no conflicting rows already exist in enterprise-level target;
- external code values are copied exactly as-is.

### 8.2 Migration of name mappings

Source:

- `business_store_product_names`
- filter:
  - `store_id = 2`

Target:

- `business_enterprise_product_names`
- `enterprise_code = '364'`

Required checks:

- no duplicate `internal_product_code` in target;
- existing chosen names are preserved exactly;
- source metadata fields are copied unchanged.

### 8.3 Backward-compatible rollout

Recommended migration order:

1. add new enterprise-level tables
2. backfill from `store_id=2`
3. build isolated simulator/read-only checks
4. switch catalog preview/export to enterprise-level identity
5. switch stock code lookup to enterprise-level identity
6. switch inbound order reverse mapping
7. switch outbound status mapping
8. only then deprecate store-level code/name tables from runtime

## 9. Required Code Changes By Area

### 9.1 Models / schema

Future implementation files:

- `app/models.py`
- `alembic/versions/*`

Add:

- `BusinessEnterpriseProductCode`
- `BusinessEnterpriseProductName`

Potential deprecation later:

- `BusinessStore.code_strategy`
- `BusinessStore.code_prefix`
- `BusinessStore.name_strategy`

### 9.2 Catalog preview/export/publish

Future implementation files:

- `app/business/business_store_catalog_preview.py`
- `app/business/business_store_catalog_exporter.py`
- `app/services/business_store_catalog_publish_service.py`
- `app/scripts/business_store_catalog_publish.py`
- possibly a new `app/business/business_enterprise_catalog_preview.py`
- possibly a new `app/business/business_enterprise_catalog_exporter.py`

Expected direction:

- either replace current store-scoped catalog preview/export
- or introduce enterprise-scoped preview/export modules and migrate callers

### 9.3 Stock preview/export/publish

Future implementation files:

- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/services/business_store_stock_publish_service.py`
- `app/scripts/business_store_stock_publish.py`

Expected direction:

- keep branch/price/qty store-aware;
- replace code lookup with enterprise-level lookup.

### 9.4 Generators

Future implementation files:

- `app/business/business_store_code_generator.py`
- `app/business/business_store_name_generator.py`

Expected direction:

- introduce enterprise-scoped generators or rename modules to enterprise identity generators;
- generation seed should stop depending on `store_code` / `store_id`;
- generation seed should depend on `enterprise_code + internal_product_code`.

### 9.5 Order inbound/outbound mapping

Future implementation files:

- `app/business/business_store_order_mapper.py`
- `app/business/business_store_tabletki_outbound_mapper.py`
- `app/services/order_fetcher.py`
- `app/business/salesdrive_webhook.py`

Expected direction:

- keep branch-based store resolution;
- replace store-level code lookup with enterprise-level code lookup.

### 9.6 Scheduler layer

Future implementation files:

- `app/services/master_catalog_scheduler_service.py`

Expected direction:

- catalog scheduler hook should publish enterprise-level catalog identity, not store-level catalog overlays;
- stock scheduler can remain store-aware.

### 9.7 UI

Future implementation files:

- `admin-panel/src/pages/BusinessStoresPage.jsx`
- potentially a new enterprise-level catalog identity page or embedded enterprise section

UI target:

- enterprise-level block:
  - external code strategy
  - external name strategy
  - catalog target branch
  - code/name generation actions
- store-level block:
  - stock scope
  - branch
  - price adjustments
  - orders
  - migration flags

Store-level fields to remove or de-emphasize later:

- `code_strategy`
- `name_strategy`
- catalog identity texts/actions tied to store

## 10. Feature Flags

Current flags that should remain during migration:

- `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED`
- `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN`
- `BUSINESS_STORE_ORDER_MAPPING_ENABLED`
- `BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED`
- `BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED`
- `BUSINESS_STORE_STOCK_SCHEDULER_ENABLED`
- `BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN`
- refresh-before-publish stock flags

Recommended future migration flags:

- `BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=false`
- `BUSINESS_ENTERPRISE_CATALOG_EXPORT_ENABLED=false`
- `BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=false`

Principle:

- switch readers by feature flag;
- do not switch catalog, stock, inbound orders, and outbound orders in one step.

Catalog gate cleanup:

- при `BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=true` operator-facing catalog gate = `EnterpriseSettings.catalog_enabled`;
- `BusinessStore.catalog_enabled` больше не должен блокировать enterprise-level catalog eligibility;
- поле остаётся в БД как deprecated compatibility field для rollback/storage;
- catalog reports now expose:
  - `enterprise_catalog_enabled`
  - `store_catalog_enabled_deprecated`
  - `catalog_gate_source`

Enterprise catalog assortment scope:

- `catalog_only_in_stock` физически пока хранится в `BusinessStore`;
- в enterprise catalog mode это поле трактуется как enterprise-level assortment setting через главный магазин каталога;
- главный магазин каталога определяется как active `BusinessStore` where:
  - `enterprise_code == EnterpriseSettings.enterprise_code`
  - `tabletki_branch == EnterpriseSettings.branch_id`
- selected store больше не должен определять catalog assortment в enterprise mode;
- если главный магазин не найден:
  - `missing_catalog_scope_store`
- если найдено несколько:
  - `ambiguous_catalog_scope_store`

## 11. Risks

Main risks:

- catalog published to wrong branch if old `store.tabletki_branch` routing remains;
- stock code mismatch if catalog is switched first but stock still reads store-level codes;
- inbound order reverse mapping failure if external code has already been moved to enterprise-level but runtime still queries by `store_id`;
- outbound webhook mapping failure if webhook path still reads store-level codes;
- UI ambiguity if catalog identity controls remain mixed with store-level settings;
- rollback complexity if old and new mappings diverge.

## 12. Rollback Strategy

Safe rollback path:

1. keep store-level tables untouched during migration
2. backfill enterprise-level tables as copies
3. enable new readers behind flags
4. if issue appears:
   - disable new reader flag
   - revert to current store-level readers
5. only after stabilization consider cleanup/deprecation of old readers

This means:

- no destructive migration in phase 1;
- no immediate delete from:
  - `business_store_product_codes`
  - `business_store_product_names`

## 13. Phased Implementation Plan

### Stage 1

- add enterprise-level identity tables
- backfill current `business_364` mappings from `store_id=2`
- keep all runtime readers unchanged

### Stage 2

- create isolated enterprise-level catalog identity preview/simulator
- compare:
  - old store-level catalog payload
  - new enterprise-level catalog payload

### Stage 3

- switch catalog preview/export/publish to enterprise-level identity
- target branch = `EnterpriseSettings.branch_id`

### Stage 4

- switch stock preview/export to enterprise-level code lookup
- keep store-level pricing and routing

### Stage 5

- switch inbound order reverse mapping to:
  - store by branch
  - code lookup by `enterprise_code`

### Stage 6

- switch outbound status mapping to:
  - store by branch
  - code lookup by `enterprise_code`

### Stage 7

- simplify UI:
  - enterprise-level catalog identity controls
  - store-level stock/pricing/order controls

### Stage 8

- deprecate old store-level code/name runtime readers
- decide whether old tables remain for audit history or are archived

## 14. Recommended Immediate Next Step

The safest next implementation step is not runtime wiring.

The safest next step is:

- add new enterprise-level code/name tables;
- backfill `business_364`;
- build isolated comparison preview for catalog identity.

That gives a reversible checkpoint before any catalog/stock/order runtime reader changes.

## 15. Stage 1 Status

Stage 1 is now implemented as schema and tooling only:

- `BusinessEnterpriseProductCode` model added;
- `BusinessEnterpriseProductName` model added;
- Alembic migration added for new enterprise-level identity tables;
- backfill CLI added:
  - `python -m app.scripts.business_enterprise_catalog_identity_backfill`
- comparison CLI added:
  - `python -m app.scripts.business_enterprise_catalog_identity_compare`
- runtime readers remain unchanged and still read store-level mappings;
- rollback path still remains:
  - `business_store_product_codes`
  - `business_store_product_names`

## 16. Stage 2 Status

Stage 2 is now implemented as read-only comparison tooling:

- enterprise-level catalog preview added:
  - `app/business/business_enterprise_catalog_preview.py`
- enterprise vs store preview comparison CLI added:
  - `python -m app.scripts.business_enterprise_catalog_preview_compare`
- enterprise preview uses:
  - `EnterpriseSettings.branch_id`
  - `BusinessEnterpriseProductCode`
  - `BusinessEnterpriseProductName`
- enterprise preview now supports two assortment modes:
  - `master_all`
    - diagnostic mode over the full non-archived `MasterCatalog`
  - `store_compatible`
    - compatibility mode that reuses the same candidate assortment scope as current store preview
    - for `catalog_only_in_stock=true` this means:
      - non-archived `MasterCatalog`
      - intersected with `Offer.product_code`
      - filtered by `Offer.city == BusinessStore.legacy_scope_key`
      - filtered by `Offer.stock > 0`
- comparison CLI now defaults to `store_compatible`, because Stage 2 is meant to validate identity replacement only, not assortment expansion;
- `master_all` remains available as a diagnostic mode to inspect enterprise mapping coverage outside current store assortment;
- reason-family comparison now normalizes:
  - `missing_code_mapping` == `missing_enterprise_code_mapping`
  - `missing_name_mapping` == `missing_enterprise_name_mapping`
- existing runtime readers still remain store-level;
- existing store catalog preview/export/publish paths are unchanged;
- no external API calls are made by enterprise preview or comparison CLI.

## 17. Stage 3 Status

Stage 3 is now implemented behind feature flag:

- `BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=false`
  - rollback/default mode
  - Business store catalog preview/export/publish still use store-level identity
  - code/name lookup:
    - `BusinessStoreProductCode`
    - `BusinessStoreProductName`
  - target branch:
    - `BusinessStore.tabletki_branch`
- `BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=true`
  - Business store catalog preview/export/publish switch to enterprise-level identity
  - code/name lookup:
    - `BusinessEnterpriseProductCode`
    - `BusinessEnterpriseProductName`
  - target branch:
    - `EnterpriseSettings.branch_id`
  - assortment mode:
    - `store_compatible`
    - candidate scope remains aligned with current store preview/export behavior

Important boundary:

- only catalog preview/export/publish are switched in Stage 3;
- stock readers remain store-level;
- inbound/outbound order readers remain store-level;
- UI is unchanged in this step.

## 18. Stage 4 Status

Stage 4 is now implemented behind feature flag:

- `BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED=false`
  - rollback/default mode
  - store-aware stock preview/export/publish still read product codes via:
    - `BusinessStoreProductCode.store_id`
- `BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED=true`
  - store-aware stock preview/export/publish switch only code lookup to:
    - `BusinessEnterpriseProductCode.enterprise_code`

Important boundary:

- stock branch remains store-level:
  - `BusinessStore.tabletki_branch`
- stock scope remains store-level:
  - `BusinessStore.legacy_scope_key`
- best offer selection remains based on `Offer`
- stock pricing and extra markup remain store-level:
  - `BusinessStoreProductPriceAdjustment.store_id`
- catalog, inbound order mapping, outbound status mapping, and UI are unchanged in this step.

## 19. Stage 5 Status

Stage 5 is now implemented behind feature flag:

- `BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=false`
  - rollback/default mode
  - store-aware inbound reverse lookup still uses:
    - `BusinessStoreProductCode.store_id`
- `BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=true`
  - store-aware inbound reverse lookup switches to:
    - `BusinessEnterpriseProductCode.enterprise_code`

Important boundary:

- `BUSINESS_STORE_ORDER_MAPPING_ENABLED` still remains the main gate for store-aware inbound order flow;
- store resolution remains store-level:
  - branch -> `BusinessStore`
- only reverse product code lookup becomes enterprise-level;
- outbound status mapper remains store-level until later stage;
- catalog and stock keep their separate feature flags.

## 20. Stage 6 Status

Stage 6 is now implemented behind feature flag:

- `BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=false`
  - rollback/default mode
  - outbound Tabletki status mapper still restores codes via:
    - `BusinessStoreProductCode.store_id`
- `BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=true`
  - outbound Tabletki status mapper can restore codes via:
    - `BusinessEnterpriseProductCode.enterprise_code`

Important boundary:

- `BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED` still remains the main gate for the runtime mapper in `/webhooks/salesdrive`;
- store resolution remains branch-based:
  - branch -> `BusinessStore`
- only `internal -> external` product code lookup changes in this step;
- `salesdrive-simple` remains unchanged;
- catalog, stock, inbound order mapping, and UI are unchanged in this step.
