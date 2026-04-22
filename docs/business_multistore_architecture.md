# Business Multistore Architecture

## 1. Scope and Constraints

This document fixes the target configuration model for:

- `enterprise_settings`
- `business_settings`
- `business_stores`
- `mapping_branch`

It is intentionally limited to architecture, ownership, UI/API boundaries, and future runtime direction.

Store-level catalog identity and price markup target model is documented separately in:

- [docs/business_store_catalog_identity.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_catalog_identity.md)
- [docs/business_enterprise_catalog_identity_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_enterprise_catalog_identity_audit.md)
- [docs/business_store_stock_export_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_stock_export_audit.md)
- [docs/business_store_offers_refresh_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_offers_refresh_audit.md)
- [docs/business_store_order_reverse_mapping_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_order_reverse_mapping_audit.md)
- [docs/business_store_order_autoconfirm_strategy.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_order_autoconfirm_strategy.md)

Current status after the latest foundation step:

- DB/API/UI/dry-run support for store-level names and stable extra markup exists;
- read-only store-aware catalog payload preview exists;
- read-only store-aware stock payload preview exists;
- these overlays are still preparation-only and dry-run-only;
- current master publish/runtime/scheduler flows remain unchanged.

Explicitly out of scope in this step:

- no live export runtime implementation;
- no scheduler behavior changes;
- no changes in `app/business/dropship_pipeline.py`;
- no changes in order fetch/import/order sender runtime;
- no DB schema changes or migrations.

Catalog preview clarification:

- `POST /developer_panel/business-stores/{store_id}/catalog-preview` builds a hypothetical store-aware catalog payload;
- it reads `master_catalog` plus store mappings only;
- it does not call `tabletki_master_catalog_exporter`;
- it does not send data to Tabletki;
- it does not modify runtime or scheduler behavior.

Stock preview clarification:

- `POST /developer_panel/business-stores/{store_id}/stock-preview` builds a hypothetical store-aware stock payload;
- it reads `offers` plus store code mappings and store price adjustment mappings only;
- it does not call `dropship_pipeline` or `business_stock_scheduler_service`;
- it does not send data to Tabletki;
- it does not modify `offers.price`;
- it does not modify runtime or scheduler behavior.

Manual store-aware catalog export clarification:

- a separate CLI-only path exists for one explicit `BusinessStore`;
- it is implemented outside `master_catalog_orchestrator` and outside `tabletki_master_catalog_exporter`;
- it uses store-aware catalog preview as the export source;
- it sends only exportable rows;
- it targets `BusinessStore.tabletki_branch`, not `EnterpriseSettings.branch_id`;
- default mode is dry-run;
- live send requires explicit CLI confirmation;
- scheduler behavior remains unchanged.

Manual store-aware stock export clarification:

- a separate CLI-only path exists for one explicit `BusinessStore`;
- it is implemented outside `business_stock_scheduler_service`, `dropship_pipeline`, and `process_database_service`;
- it uses store-aware stock preview as the export source;
- it sends only exportable rows;
- it targets `BusinessStore.tabletki_branch`, not `mapping_branch.branch`;
- `Price` and `PriceReserve` are taken from store-aware `final_store_price_preview`;
- default mode is dry-run;
- live send requires explicit CLI confirmation;
- scheduler behavior remains unchanged.

Refresh-only offers clarification:

- a separate refresh-only helper now exists in `app/business/dropship_pipeline.py`;
- a separate manual CLI exists in `app/scripts/business_offers_refresh.py`;
- this path updates `offers` only;
- it does not call `process_database_service("stock", ...)`;
- it does not write `InventoryStock`;
- it does not send stock to Tabletki;
- current legacy `run_pipeline(..., "stock")` still keeps its previous behavior by running refresh first and stock export second.
- the separate store-aware stock scheduler can now optionally run refresh-before-publish behind dedicated env flags;
- default scheduler mode still remains publish-only and dry-run-safe.

Store-aware order reverse mapping clarification:

- an isolated read-only helper exists in `app/business/business_store_order_mapper.py`;
- a read-only integration simulator also exists in `app/business/business_store_order_integration_simulator.py`;
- runtime wiring in `app/services/order_fetcher.py` also exists behind `BUSINESS_STORE_ORDER_MAPPING_ENABLED`;
- it resolves `BusinessStore` and maps external store codes back to internal product codes;
- it preserves the original external `goodsCode` in normalized payload output;
- it can also verify downstream readiness in legacy order read paths without runtime integration;
- legacy orders keep old behavior by default;
- store-aware orders currently bypass legacy `auto_confirm` when the feature flag is enabled;
- store-aware orders can now use a separate Tabletki status `2` path after successful outbound processing behind `BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED`;
- dedicated store-aware availability checker is still a later step.

## 2. What Was Checked

Reviewed code and docs:

- `app/models.py`
- `app/schemas.py`
- `app/routes.py`
- `app/business/dropship_pipeline.py`
- `app/business/business_store_export_dry_run.py`
- `app/business/master_catalog_orchestrator.py`
- `app/business/tabletki_master_catalog_exporter.py`
- `app/business/import_catalog.py`
- `app/business/order_sender.py`
- `app/services/business_stock_scheduler_service.py`
- `app/services/master_catalog_scheduler_service.py`
- `app/services/order_fetcher.py`
- `app/services/order_sender.py`
- `admin-panel/src/pages/EnterprisePanel.js`
- `admin-panel/src/pages/BusinessSettingsPage.jsx`
- `admin-panel/src/pages/BusinessStoresPage.jsx`
- `admin-panel/src/pages/MappingBranchPage.js`
- `docs/business_stores.md`
- `docs/business_stores_ui_handoff.md`

## 3. Current Problem

The current system is halfway between two models:

- the real runtime still uses `enterprise_settings` as the Business enterprise profile;
- `business_settings` mixes control-plane fields, business pipeline scheduling, and enterprise-specific operational fields;
- `business_stores` is already positioned as an overlay, but the UI can still be interpreted as if it creates another Business master enterprise;
- some fields now exist conceptually in more than one place, which creates ownership ambiguity before the next UI/API/runtime step.

The most important correction is this:

- a Business enterprise must be created in `enterprise_settings`;
- `BusinessStore` must not create or replace a master Business enterprise;
- `BusinessStore` is only a store-level overlay bound to an existing `enterprise_settings` row where `data_format='Business'`.

## 4. Target Model

### 4.1 EnterpriseSettings

`enterprise_settings` remains the master runtime profile for the Business enterprise.

It owns:

- runtime identity;
- current live credentials and access;
- current live scheduler gating;
- current live order intake gating;
- current legacy/master target branch.

For the Business contour this remains the only authoritative source for current runtime until a separate store-aware runtime is explicitly implemented.

### 4.2 BusinessSettings

`business_settings` is not a per-enterprise entity.

It should be treated as a singleton Business pipeline control-plane table for:

- primary Business enterprise selector;
- master daily/weekly publish selectors and schedule;
- Business stock scheduler control;
- BIOTUS fallback handling;
- pricing configuration.

It should not be treated as the canonical home for enterprise credentials or operational enterprise fields.

### 4.3 BusinessStore

`business_stores` is a store-level overlay on top of a selected Business enterprise profile.

It owns:

- store identity and display fields;
- legal and tax metadata for the store/legal entity;
- future external routing identity for Tabletki and SalesDrive;
- future per-store flags;
- legacy scope linkage;
- product code strategy and external code mapping behavior;
- migration lifecycle markers.

It does not own:

- master Business enterprise creation;
- current shared Tabletki credentials;
- current global scheduler gating;
- current legacy runtime branch routing in `mapping_branch`.

### 4.4 MappingBranch

`mapping_branch` stays as a legacy runtime bridge.

It currently powers:

- stock export routing in legacy Business stock payload building;
- order fetch branch iteration.

It must not be silently repurposed into canonical store configuration.

## 5. EnterpriseSettings Audit

### 5.1 Fields that are master/runtime settings

These are currently runtime-owned or runtime-critical for Business:

| field | role now | comments |
| --- | --- | --- |
| `enterprise_code` | primary runtime identity | used across schedulers/export/import/order flows |
| `enterprise_name` | display identity | UI label and operator-facing selector |
| `branch_id` | legacy/master publish branch | used by Tabletki master catalog export |
| `tabletki_login` | live credential | used by order fetch and outbound calls |
| `tabletki_password` | live credential | used by order fetch and outbound calls |
| `token` | integration access secret | used as enterprise access token / Business operational field |
| `catalog_enabled` | live global catalog gate | current runtime switch |
| `stock_enabled` | live global stock gate | current runtime switch and fallback for business stock scheduler |
| `order_fetcher` | live order intake gate | current order scheduler switch |
| `auto_confirm` | live order behavior | used in `order_fetcher` flow |
| `data_format` | processor selector | current runtime dispatch key |
| `stock_upload_frequency` | current stock cadence | used directly or as fallback |
| `catalog_upload_frequency` | current catalog cadence | current scheduler cadence |
| `stock_correction` | current stock policy | exposed through Business settings operational scope |

### 5.2 Fields used by schedulers/runtime

Observed live use:

- `catalog_enabled`
  - catalog scheduler gating
- `stock_enabled`
  - stock scheduler gating
  - fallback gating for `business_stock_scheduler_service`
- `order_fetcher`
  - order scheduler target selection
  - order fetch gating
- `auto_confirm`
  - order fetch processing behavior
- `stock_upload_frequency`
  - stock scheduler cadence
  - fallback for Business stock scheduler interval
- `catalog_upload_frequency`
  - catalog scheduler cadence
- `data_format`
  - processor selection for order sender / status handling
- `branch_id`
  - used by `tabletki_master_catalog_exporter`

### 5.3 Fields used for Tabletki credentials/access

- `tabletki_login`
- `tabletki_password`
- `token`
- `branch_id`

These must stay in `enterprise_settings` for the current runtime model.

### 5.4 Fields that must not be duplicated in BusinessStore

These should remain single-owner in `enterprise_settings` for the current phase:

- `enterprise_code`
  - as runtime identity of the base enterprise, not as external store account code
- `enterprise_name`
  - as enterprise profile name
- `branch_id`
  - as current legacy/master branch target
- `tabletki_login`
- `tabletki_password`
- `token`
- `catalog_enabled`
- `stock_enabled`
- `order_fetcher`
- `auto_confirm`
- `data_format`
- `stock_upload_frequency`
- `catalog_upload_frequency`

### 5.5 What BusinessStore may use as defaults

When a new overlay is created for a selected Business enterprise, these defaults are valid:

- `business_stores.enterprise_code` <- `enterprise_settings.enterprise_code`
- `store_name` <- `enterprise_settings.enterprise_name`
- `tabletki_enterprise_code` <- `enterprise_settings.enterprise_code`
- `tabletki_branch` <- `enterprise_settings.branch_id`
- `catalog_enabled` / `stock_enabled`
  - optionally prefilled from enterprise flags for convenience, but still interpreted as store-level future flags

These are defaults only, not evidence that ownership moved.

## 6. BusinessSettingsPage Audit

### 6.1 What the page is doing now

`BusinessSettingsPage.jsx` currently combines three different concerns:

1. Business pipeline control-plane
2. enterprise-specific operational fields
3. pricing and BIOTUS policy

Backend routes confirm this split:

- `/business/settings/master-scope`
  - writes `business_settings`
- `/business/settings/enterprise-operational-scope`
  - writes `enterprise_settings`
- `/business/settings/pricing-scope`
  - writes `business_settings`

So the page is already logically mixed, not cleanly scoped.

### 6.2 What should remain on BusinessSettingsPage

Keep on `BusinessSettingsPage`:

- `business_enterprise_code`
- `daily_publish_enterprise_code_override`
- `weekly_salesdrive_enterprise_code_override`
- `business_stock_enabled`
- `business_stock_interval_seconds`
- `master_weekly_*`
- `master_daily_publish_*`
- `master_archive_*`
- BIOTUS fallback fields
- pricing fields

These are global Business pipeline control-plane settings.

### 6.3 What should move out conceptually

The following blocks are enterprise-specific and should be treated as belonging to `EnterpriseSettings`, even if the current page still edits them temporarily:

- `branch_id`
- `tabletki_login`
- `tabletki_password`
- `token`
- `order_fetcher`
- `auto_confirm`
- `stock_correction`

These should ultimately live only on Enterprise Settings UI for the selected Business enterprise.

### 6.4 Section-level target classification

| BusinessSettingsPage block | target owner |
| --- | --- |
| `Предприятие (Business)` | split: selector fields stay in `business_settings`; enterprise operational fields move to `enterprise_settings` |
| `Основные параметры рабочего предприятия` | `enterprise_settings` |
| `Сток (Stock)` | split: scheduler control in `business_settings`; runtime stock policy in `enterprise_settings`; future store flags in `business_stores` |
| `Заказы` | split: `order_fetcher` and `auto_confirm` in `enterprise_settings`; BIOTUS fallback in `business_settings`; future per-store orders flag in `business_stores` |
| `Основной контур заказов и дополнительная обработка` | split as above |
| `Integration / Access` | enterprise credentials belong to `enterprise_settings`; store routing identities belong to `business_stores`; pipeline selectors stay in `business_settings` |

### 6.5 Future cleanup of BusinessSettingsPage

Future UI cleanup should:

- remove enterprise credentials from this page;
- remove enterprise operational toggles from this page;
- keep only Business pipeline control-plane and pricing;
- display the selected primary Business enterprise as a reference, not as a full enterprise editor.

## 7. Business Stores UI Audit

### 7.1 What is correct in the current direction

The page already moves in the right direction:

- top-level selection of an existing Business enterprise;
- overlay creation instead of free-form enterprise creation;
- defaulting from enterprise to store overlay;
- dry-run and missing-code generation as non-live preparation tools;
- separation of SalesDrive legacy fields into a lower-importance block.

### 7.2 What is still misleading or incomplete

The page still needs a stronger ownership model:

- it should explicitly say this page configures a store overlay, not a Business enterprise;
- enterprise-owned settings must not be editable here;
- store-owned future flags must be clearly labeled as inactive until store-aware runtime exists;
- SalesDrive legacy fields should be visually secondary.

### 7.3 Why `legal_entity_name` / `tax_identifier` may appear not to save

Based on the current code, the storage contract itself is present:

- DB model contains both fields;
- create/update schemas contain both fields;
- POST/PUT routes write both fields;
- response schema returns both fields.

So this is not primarily a DB schema gap and not primarily a backend route omission.

The most probable current issue is frontend state behavior on the page:

- the page always derives the visible form from `primaryStoreForEnterprise = storesForSelectedEnterprise[0]`;
- the `useEffect` tied to `selectedEnterprise` and `primaryStoreForEnterprise` resets `selectedStoreId` and `draft` to the first overlay for the enterprise;
- if multiple `BusinessStore` rows exist for one enterprise, editing a non-first row can be visually overwritten after reload, which looks like fields did not save.

Conclusion:

- likely frontend issue first;
- not a schema issue;
- backend update path for these two fields exists.

Secondary UX issue:

- the list table does not show `legal_entity_name` / `tax_identifier`, so even successful persistence is hard to verify visually.

### 7.4 Editable vs read-only vs hidden

Editable on `Business Stores`:

- `store_name`
- `legal_entity_name`
- `tax_identifier`
- `legacy_scope_key`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `salesdrive_enterprise_id`
- `code_strategy`
- `code_prefix`
- `is_legacy_default`
- `catalog_enabled`
- `stock_enabled`
- `orders_enabled`
- `catalog_only_in_stock`
- `migration_status`
- `is_active`

Editable only during initial creation or before dry-run policy freeze:

- `store_code`
- `enterprise_code` should not be manually editable at all, only derived from selection

High-risk and should become guarded/read-only after later live phases:

- `legacy_scope_key`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `salesdrive_enterprise_id`
- `code_strategy`
- `code_prefix`
- `takes_over_legacy_scope`

Read-only on this page:

- selected Business enterprise identity from `enterprise_settings`
- enterprise global runtime flags summary
- current enterprise credentials
- current enterprise branch/runtime gating

Hide or move under `legacy / advanced`:

- `salesdrive_enterprise_code`
- `salesdrive_store_name`
- eventually `is_legacy_default`
- eventually `code_prefix` unless `prefix_mapping` is selected

## 8. MappingBranch Audit

### 8.1 How it is used now

Current stock export path:

- `dropship_pipeline._load_branch_mapping()` loads `{store_id -> branch}` for an `enterprise_code`;
- `build_stock_payload()` builds best offers by `Offer.city`;
- `Offer.city` is used as lookup key into `mapping_branch.store_id`;
- resolved `branch` is sent in stock payload.

Current order fetch path:

- `order_fetcher.fetch_orders_for_enterprise()` loads all `MappingBranch.branch` rows for the enterprise;
- it fetches Tabletki orders per branch;
- it uses enterprise credentials from `EnterpriseSettings`.

### 8.2 Meaning of fields now

- `mapping_branch.enterprise_code`
  - legacy runtime profile owner
- `mapping_branch.branch`
  - stable branch identity for legacy stock/order paths
- `mapping_branch.store_id`
  - overloaded legacy scope key; in Business flow it effectively matches `offers.city`

### 8.3 Relationship to `legacy_scope_key`

For Business migration:

- `business_stores.legacy_scope_key` should point to the operational legacy scope;
- today that often equals `mapping_branch.store_id`;
- this does not mean `mapping_branch.store_id` should be renamed or repurposed yet.

### 8.4 Risks of changing MappingBranch now

If `mapping_branch` semantics are changed prematurely:

- stock export can stop routing products to branches;
- order fetch can stop reading correct branches;
- existing Business city/scope logic can split from actual branch routing;
- silent data drift can appear between `offers.city`, `mapping_branch.store_id`, and UI assumptions.

Therefore:

- no runtime rewrite of `mapping_branch`;
- no assumption that `business_stores.tabletki_branch` replaces `mapping_branch.branch` yet.

### 8.5 Future coexistence model

Future store-aware runtime should:

- keep legacy `mapping_branch` unchanged for old path;
- use `business_stores.tabletki_branch` as the store-aware target branch;
- use `business_stores.legacy_scope_key` only to discover or exclude the old scope during migration;
- introduce explicit gating before any legacy exclusion is applied.

## 9. Catalog Future Architecture

### 9.1 Current situation

Current master catalog path is already separated from regular stock runtime:

- `master_catalog_scheduler_service` schedules jobs;
- `master_catalog_orchestrator` resolves the target Business enterprise through `business_settings`;
- `tabletki_master_catalog_exporter` sends catalog to `branch_id` of `enterprise_settings`.

### 9.2 Target future model

Target separation:

- legacy Business catalog path
  - current enterprise-level behavior
- master daily publish path
  - explicit master catalog publication path
- future store-aware catalog publication path
  - per `BusinessStore`

### 9.3 Desired behavior

`master_catalog_scheduler_service` should continue to own daily publish scheduling.

Future publishing model:

- primary Business enterprise
  - receives catalog with base/internal codes when acting as the base profile
- additional `BusinessStore` overlays
  - receive catalog with external codes from `BusinessStoreProductCode`
- `catalog_only_in_stock`
  - filters assortment at store export stage
- future scheduler-owned store publish
  - should be implemented as a separate service over `business_store_catalog_exporter`
  - should be connected behind dedicated feature flags
  - should not be embedded into `tabletki_master_catalog_exporter`
  - standalone service and CLI are now implemented
  - post-daily-publish scheduler hook is now wired behind default-off flags
  - legacy master publish still remains the primary path

Important separation:

- legacy Business catalog flow and master publish flow must remain distinct concepts;
- do not overload `business_enterprise_code` and daily publish override with store-level routing.
- see `docs/business_store_catalog_scheduler_audit.md` for the staged scheduler plan.

## 10. Stock Future Architecture

### 10.1 Current situation

`business_stock_scheduler_service` currently resolves a single Business enterprise and then calls:

- `run_pipeline(enterprise_code, "stock")`

That still uses:

- legacy `mapping_branch`
- `Offer.city`
- legacy stock payload builder

### 10.2 Target future model

Future store-aware stock export should be a separate path, not an in-place rewrite of legacy payload logic.

Target inputs per store:

- `business_stores.enterprise_code`
- `business_stores.legacy_scope_key`
- `business_stores.tabletki_branch`
  - with fallback from `enterprise_settings.branch_id` or legacy mapping during migration planning only
- `BusinessStoreProductCode.external_product_code`

### 10.3 Required compatibility rule

The current main Business stock contour must continue to work without changes until the new path is explicitly enabled.

Therefore:

- do not break `build_stock_payload()`;
- do not replace `mapping_branch` consumption in legacy path;
- add store-aware stock export as a new branch of runtime later.
- standalone multi-store stock publish service and CLI are now implemented;
- separate store-aware stock scheduler service is now implemented;
- it remains isolated from legacy `business_stock_scheduler_service`;
- see `docs/business_store_stock_scheduler_audit.md` for the recommended separate scheduler strategy and offers-freshness constraints.

## 11. Orders Future Architecture

### 11.1 Current situation

Current order runtime is enterprise-level:

- intake gate is `EnterpriseSettings.order_fetcher`;
- credentials come from `EnterpriseSettings`;
- branches come from `mapping_branch.branch`;
- Business order sender uses internal product identity assumptions from the current flow.

### 11.2 Future direction

Later, `orders_enabled` on `BusinessStore` should become the per-store gate for store-aware order handling.

Future reverse mapping must:

- identify the target store by external identity
  - likely `tabletki_enterprise_code + tabletki_branch` or equivalent external routing pair
- map `external_product_code -> internal_product_code`
  - through `BusinessStoreProductCode`

Critical rule:

- never confuse `external_product_code` with the internal/base product code;
- internal code stays canonical in project data;
- external code is store-specific integration identity.

Current action:

- do not modify order runtime in this phase.

## 12. Two-Level Flags Model

### 12.1 Owner tables

Global `enterprise_settings`:

- `catalog_enabled`
- `stock_enabled`
- `order_fetcher`

Store-level `business_stores`:

- `catalog_enabled`
- `stock_enabled`
- `orders_enabled`
- `catalog_only_in_stock`

### 12.2 How they should work now

Now:

- only `enterprise_settings` flags have live runtime effect;
- `business_stores` flags are planning metadata for the future store-aware runtime;
- UI must say this explicitly.

### 12.3 How they should work in the future

Future runtime permission model:

- catalog allowed only if:
  - enterprise `catalog_enabled = true`
  - store `catalog_enabled = true`
- stock allowed only if:
  - enterprise `stock_enabled = true`
  - store `stock_enabled = true`
- store-aware orders allowed only if:
  - enterprise `order_fetcher = true` for the base intake contour, or a future equivalent global order-runtime gate is enabled
  - store `orders_enabled = true`

`catalog_only_in_stock`:

- does not replace global gating;
- it constrains assortment selection only when store catalog export exists.

### 12.4 UI rules

UI should show:

- enterprise-level flags as global runtime gates;
- store-level flags as future per-store gates;
- a warning when store flags are enabled but global enterprise gate is off;
- a warning that store flags do not affect current runtime yet.

### 12.5 BusinessSettingsPage cleanup

In future:

- `order_fetcher` should disappear from Business Settings page and remain on Enterprise Settings page;
- store-level flags should not appear on Business Settings page at all;
- Business Settings page should stop duplicating enterprise runtime switches.

## 13. Data Ownership Matrix

| Field | Owning table | UI page | Runtime now | Runtime future | Comment |
| --- | --- | --- | --- | --- | --- |
| `enterprise_code` | `enterprise_settings` | Enterprise Settings | primary identity | primary base enterprise identity | master enterprise profile |
| `enterprise_name` | `enterprise_settings` | Enterprise Settings | display/runtime label | display/runtime label | base enterprise name |
| `branch_id` | `enterprise_settings` | Enterprise Settings | live branch target for master catalog | fallback/base branch only | do not duplicate into store ownership |
| `tabletki_login` | `enterprise_settings` | Enterprise Settings | live credential | base shared credential until store-aware auth exists | do not move now |
| `tabletki_password` | `enterprise_settings` | Enterprise Settings | live credential | base shared credential until store-aware auth exists | do not move now |
| `token` | `enterprise_settings` | Enterprise Settings | live integration access | base integration access | do not duplicate in `business_stores` |
| `catalog_enabled` | `enterprise_settings` | Enterprise Settings | live global gate | live global gate | level 1 flag |
| `stock_enabled` | `enterprise_settings` | Enterprise Settings | live global gate | live global gate | level 1 flag |
| `order_fetcher` | `enterprise_settings` | Enterprise Settings | live order intake gate | live global order gate | level 1 flag |
| `BusinessSettings.business_enterprise_code` | `business_settings` | Business Settings | control-plane selector | control-plane selector | primary Business enterprise for pipeline family |
| `BusinessSettings.daily_publish_enterprise_code_override` | `business_settings` | Business Settings | master publish selector override | same | not a store selector |
| `business_stores.enterprise_code` | `business_stores` | Business Stores | overlay link only | base enterprise link for store runtime | reference to existing Business enterprise |
| `business_stores.legacy_scope_key` | `business_stores` | Business Stores | dry-run/planning only | migration and store runtime scope link | often matches `mapping_branch.store_id` today |
| `business_stores.tabletki_enterprise_code` | `business_stores` | Business Stores | dry-run/planning only | store external Tabletki identity | external account identity |
| `business_stores.tabletki_branch` | `business_stores` | Business Stores | dry-run/planning only | store target branch | future store branch, not current legacy branch owner |
| `business_stores.legal_entity_name` | `business_stores` | Business Stores | metadata only | legal/integration metadata | should save and remain editable pre-live |
| `business_stores.tax_identifier` | `business_stores` | Business Stores | metadata only | legal/integration metadata | should save and remain editable pre-live |
| `business_stores.salesdrive_enterprise_id` | `business_stores` | Business Stores | metadata only | store external SalesDrive identity | prefer numeric primary identity |
| `business_stores.code_strategy` | `business_stores` | Business Stores | dry-run/generator behavior | store external code policy | freeze before live |
| `business_stores.catalog_enabled` | `business_stores` | Business Stores | no live effect yet | per-store catalog gate | level 2 flag |
| `business_stores.stock_enabled` | `business_stores` | Business Stores | no live effect yet | per-store stock gate | level 2 flag |
| `business_stores.orders_enabled` | `business_stores` | Business Stores | no live effect yet | per-store order gate | level 2 flag |
| `business_stores.catalog_only_in_stock` | `business_stores` | Business Stores | dry-run filter intent only | store catalog assortment filter | not a global gate |
| `business_stores.takes_over_legacy_scope` | `business_stores` | Business Stores | informational/high-risk flag only | explicit migration routing gate | must not change legacy runtime by itself |
| `mapping_branch.enterprise_code` | `mapping_branch` | Mapping Branch | legacy runtime owner | legacy path owner only | keep stable |
| `mapping_branch.branch` | `mapping_branch` | Mapping Branch | live stock/order branch routing | legacy path only | stable branch identity |
| `mapping_branch.store_id` | `mapping_branch` | Mapping Branch | live legacy scope key | legacy path only | overloaded field, often matches `offers.city` |

## 14. Concrete UI/API Fixes for the Next Prompt

These are intentionally not implemented in this task.

### 14.1 Rename / wording

- rename the main CTA from `Создать настройки для выбранного Business-предприятия`
  to `Создать overlay продавца для выбранного Business-предприятия`
  or shorter:
  `Создать overlay для выбранного предприятия`
- use `Legacy scope` consistently instead of mixing with city semantics
- label enterprise summary as `Базовый Business enterprise profile`

### 14.2 Hide / move / make read-only

- remove enterprise credentials from Business Settings page over time
- show enterprise credentials only on Enterprise Settings page
- keep `enterprise_code` read-only on Business Stores page
- keep global enterprise flags read-only on Business Stores page
- move `salesdrive_enterprise_code` and `salesdrive_store_name` into `legacy / advanced`
- hide `code_prefix` unless `code_strategy='prefix_mapping'`

### 14.3 Autofill

Autofill on overlay creation:

- `store_code`
- `store_name`
- `enterprise_code`
- `tabletki_enterprise_code`
- `tabletki_branch`

Optional helper autofill:

- suggest `legacy_scope_key` if a unique matching legacy mapping exists
- suggest `catalog_enabled` and `stock_enabled` from enterprise flags, but clearly mark them as store-level future flags

### 14.4 Why `ЄДРПОУ / РНОКПП` appears not to save

For the next UI/API prompt, inspect/fix first:

- Business Stores page form reset logic for multiple overlays per enterprise
- selection logic that always reverts to `storesForSelectedEnterprise[0]`
- post-save reload flow
- add table visibility or confirmation for `legal_entity_name` / `tax_identifier`

Expected fix direction:

- preserve selected store after reload;
- do not auto-reset to the first overlay when a different overlay is open.

### 14.5 Where things belong

On Business Stores page:

- overlay identity
- legal/tax metadata
- external routing identities
- code strategy
- store-level flags
- migration markers
- dry-run and code generation

On Enterprise Settings page:

- enterprise creation and identity
- enterprise credentials
- `branch_id`
- global runtime flags
- enterprise schedule frequencies
- current operational runtime fields

On Business Settings page:

- global Business pipeline control-plane
- master publish selectors and schedule
- Business stock scheduler control
- BIOTUS fallback policy
- pricing

## 15. Migration Plan

1. Freeze ownership model in docs.
2. Clean up UI responsibilities without changing runtime.
3. Remove enterprise-specific duplication from Business Settings page.
4. Make Business Stores page explicitly overlay-only.
5. Preserve legacy `mapping_branch` runtime unchanged.
6. Add separate store-aware catalog path later.
7. Add separate store-aware stock path later.
8. Add reverse mapping and store-aware order routing only after store catalog/stock identity is stable.

## 16. Risks

- ownership ambiguity between `enterprise_settings`, `business_settings`, and `business_stores`;
- accidental duplication of enterprise runtime flags in store overlay UI;
- premature changes to `mapping_branch`;
- confusion between internal `enterprise_code` and external `tabletki_enterprise_code`;
- confusion between SalesDrive enterprise id and supplier id;
- allowing code strategy changes after mappings are generated;
- enabling `takes_over_legacy_scope` before store runtime exists.

## 17. Next Tasks

- refine Enterprise Settings UI to clearly own enterprise runtime fields;
- trim Business Settings page down to control-plane and pricing;
- fix Business Stores page selection/save behavior for multiple overlays;
- add explicit read-only summaries and warnings for global vs store-level flags;
- define the API contract for future store-aware catalog export;
- define the API contract for future store-aware stock export;
- define reverse mapping contract for future store-aware orders;
- only after that implement runtime changes in a separate prompt.

For the next multistore identity step, the target additions are:

- keep `master_catalog` as base source of truth;
- keep current master publish path unchanged;
- treat assortment, external codes, external names, and extra markup as store-level overlay;
- store external codes and external names in stable mapping tables;
- apply extra store markup only in a future store-aware export layer, not in current pricing runtime.

## 18. Business Stores Page Responsibilities

After the current UI refactor, `Business Stores` is expected to edit two owners on one screen:

1. `enterprise_settings` for the selected Business enterprise
2. `business_stores` overlay rows linked to that enterprise

### 18.1 Enterprise-owned block on Business Stores page

The page may edit and save these fields into `enterprise_settings`:

- `enterprise_name`
- `branch_id`
- `stock_upload_frequency`
- `catalog_upload_frequency`
- `tabletki_login`
- `tabletki_password`
- `token`
- `catalog_enabled`
- `stock_enabled`
- `order_fetcher`
- `auto_confirm`
- `stock_correction`

UI rule:

- these fields must save through enterprise settings update flow;
- changes must be visible on Enterprise Settings page because they belong to the same underlying row.

### 18.2 Store-owned block on Business Stores page

The page may edit and save these fields into `business_stores`:

- `store_code`
- `store_name`
- `legal_entity_name`
- `tax_identifier`
- `legacy_scope_key`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `salesdrive_enterprise_id`
- `migration_status`
- `is_active`
- `code_strategy`
- `code_prefix`
- `is_legacy_default`
- `catalog_enabled`
- `stock_enabled`
- `orders_enabled`
- `catalog_only_in_stock`
- `takes_over_legacy_scope`

### 18.3 Runtime meaning of flags on the page

Already live enterprise-level runtime flags:

- `enterprise_settings.catalog_enabled`
- `enterprise_settings.stock_enabled`
- `enterprise_settings.order_fetcher`
- `enterprise_settings.auto_confirm`
- `enterprise_settings.stock_correction`

Future-only store-level flags:

- `business_stores.catalog_enabled`
- `business_stores.stock_enabled`
- `business_stores.orders_enabled`
- `business_stores.catalog_only_in_stock`
- `business_stores.takes_over_legacy_scope`

UI must keep these two levels visually separated.

### 18.4 BusinessSettingsPage status

`BusinessSettingsPage` is intentionally not cleaned up in this step.

For now:

- it still contains some enterprise-specific fields;
- those fields overlap conceptually with `enterprise_settings`;
- future cleanup should remove these enterprise-specific blocks and leave only pipeline control-plane plus pricing.

## 19. Additional Multistore Identity Direction

The next store-level architecture step should follow these rules:

- assortment:
  - keep using `business_stores.catalog_only_in_stock` as the stored flag;
  - let UI render it as `all products / only in-stock`;
- codes:
  - keep existing `code_strategy` / `is_legacy_default` / `business_store_product_codes`;
- names:
  - add a future `name_strategy` with `base` and `supplier_random`;
  - add a separate `business_store_product_names` table;
  - never auto-overwrite existing generated name mappings;
- price:
  - add future store-level extra markup fields on `BusinessStore`;
  - apply them only in a future store-aware export layer;
  - do not modify current `dropship_pipeline` and master publish runtime.

Primary source priority for future store-level supplier names:

1. `catalog_supplier_mapping.supplier_product_name_raw`
2. `raw_supplier_feed_products.name_raw` as optional fallback
3. `catalog_mapping.Name_D*` only as legacy fallback
4. `master_catalog.name_ua/name_ru` as base-name fallback

This keeps new multistore identity aligned with the newer master catalog model and avoids introducing new dependence on legacy `catalog_mapping`.

## 20. Outbound Status Mapping Note

Store-aware inbound order normalization and separate Tabletki status `2` send are already available behind feature flags.

The next outbound order-status problem is different:

- SalesDrive webhook payload carries `branch` sourced from `MappingBranch.branch`
- in the current Business contour this branch is expected to stay aligned with `BusinessStore.tabletki_branch`
- therefore isolated outbound mapping may currently resolve store by Tabletki branch
- webhook runtime integration is still a separate later step

Current architectural recommendation:

- add a dedicated outbound mapper layer for Tabletki-facing payloads
- current Stage 1 resolver is `BusinessStore.tabletki_branch`
- long-term stronger fallback remains persisted order-to-store link based on `externalId` / `tabletkiOrder`
- only then convert internal product codes back into store external codes

Current runtime status:

- outbound code restoration is now wired only into main SalesDrive webhook `/webhooks/salesdrive`
- this wiring is gated by `BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED`
- `/webhooks/salesdrive-simple/{branch}` is intentionally unchanged
- `mapping_error` blocks only the affected outbound Tabletki status send and does not crash the webhook processor

See:

- `docs/business_store_outbound_status_mapping_audit.md`

## 21. Enterprise-Level Catalog Identity Migration Note

A separate audit now fixes the next architectural shift:

- catalog identity should move from `store_id` scope to `enterprise_code` scope
- catalog publish target should move from `BusinessStore.tabletki_branch` to `EnterpriseSettings.branch_id`
- stock routing should remain store-aware
- inbound/outbound order store resolution should remain branch-based, but code mapping should become enterprise-level

See:

- [docs/business_enterprise_catalog_identity_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_enterprise_catalog_identity_audit.md)

Current implementation status for that migration:

- Stage 1 is implemented as schema + tooling only;
- enterprise-level catalog identity tables now exist in models/migration;
- backfill and comparison CLI tooling now exists;
- catalog/stock/order/outbound runtime readers still remain store-level.
