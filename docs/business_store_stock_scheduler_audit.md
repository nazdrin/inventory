# Business Store Stock Scheduler Audit

## Scope

Этот документ фиксирует:

- как сейчас работает legacy Business stock scheduler;
- какие зависимости и side effects есть у текущего stock runtime;
- почему store-aware stock publish нельзя безопасно смешивать с legacy stock path;
- где проходит безопасная граница для потенциального refresh-only обновления `offers`;
- какой staged rollout нужен для future store-aware stock scheduler.

Этот документ не вносит runtime-изменений.

Отдельный аудит refresh-only boundary:

- [docs/business_store_offers_refresh_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_offers_refresh_audit.md)

Current status:

- refresh-only offers helper and CLI now exist;
- store-aware stock scheduler still remains publish-only;
- store-aware stock scheduler now supports optional refresh-before-publish via env flags;
- default mode still remains publish-only without refresh.

Refresh-before-publish flags:

- `BUSINESS_STORE_STOCK_REFRESH_OFFERS_BEFORE_PUBLISH=false`
- `BUSINESS_STORE_STOCK_REFRESH_ENTERPRISE_CODE=`
- `BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL=false`

Current scheduler behavior:

- if refresh flag is disabled:
  - scheduler behaves exactly as before
- if refresh flag is enabled:
  - scheduler runs offers refresh first
  - `status=ok` -> publish proceeds
  - `status=partial` + allow_partial=false -> publish skipped
  - `status=partial` + allow_partial=true -> publish proceeds with warning
  - `status=error` -> publish skipped

This still does not connect the store-aware scheduler to:

- `process_database_service`
- `InventoryStock`
- legacy `business_stock_scheduler_service`

## Checked Files

- `app/services/business_stock_scheduler_service.py`
- `app/business/dropship_pipeline.py`
- `app/services/database_service.py`
- `app/services/stock_export_service.py`
- `app/services/stock_update_service.py`
- `app/business/business_store_stock_exporter.py`
- `app/business/business_store_stock_preview.py`
- `app/services/business_store_catalog_publish_service.py`
- `app/scripts/business_store_stock_export.py`
- `app/models.py`
- `ENV_REFERENCE.md`
- `docs/business_multistore_architecture.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_store_catalog_scheduler_audit.md`
- `docs/business_store_catalog_identity.md`

## 1. Current Business Stock Scheduler Flow

Current scheduler owner:

- `app/services/business_stock_scheduler_service.py`

Flow:

1. scheduler loads `BusinessSettings` row if present
2. scheduler loads all `EnterpriseSettings`
3. keeps only rows where `data_format='Business'`
4. resolves a single target enterprise
5. runs:
   - `run_pipeline(enterprise_code, "stock")`

## Control flags

Current scheduler reads:

- `BusinessSettings.business_stock_enabled`
- `BusinessSettings.business_stock_interval_seconds`
- fallback `EnterpriseSettings.stock_enabled`
- fallback `EnterpriseSettings.stock_upload_frequency`
- `EnterpriseSettings.last_stock_upload`

Resolution model:

- if `business_settings` row exists
  - scheduler uses DB-level control
  - interval from `business_stock_interval_seconds`
  - enable/disable from `business_stock_enabled`
- otherwise
  - fallback to enterprise-level cadence
  - uses `stock_enabled`
  - uses `stock_upload_frequency`
  - checks `_is_stock_due(...)`

## Ambiguity behavior

Current scheduler assumes exactly one Business enterprise.

If `EnterpriseSettings` contains more than one row with `data_format='Business'`, scheduler returns:

- resolution = `ambiguous`
- and skips the run

This makes current legacy stock scheduler unsuitable as a direct host for per-store fan-out logic.

## 2. Legacy Stock Runtime Dependencies

Legacy runtime path:

1. `business_stock_scheduler_service.run_business_stock_once()`
2. `dropship_pipeline.run_pipeline(enterprise_code, "stock")`
3. `dropship_pipeline.build_stock_payload(...)`
4. `process_database_service(file_path, "stock", enterprise_code)`
5. stock side effects + send

## Where assortment and branch are resolved

Legacy `build_stock_payload(...)` uses:

- `Offer.city`
- `MappingBranch.store_id -> branch`

Resulting flat rows are shaped as:

```json
{
  "branch": "30630",
  "code": "1000331",
  "price": 367.0,
  "qty": 157,
  "price_reserve": 367.0
}
```

This is legacy runtime identity:

- branch from `mapping_branch`
- code from legacy internal product identity
- price and reserve from legacy pipeline decisions

## process_database_service side effects

`process_database_service("stock", enterprise_code)` performs multiple side effects:

- validates stock payload
- deletes old `InventoryStock` rows for the enterprise
- applies `discount_rate`
- may run `update_stock(...)` if `stock_correction=true`
- formats and sends payload through `stock_export_service.process_stock_file(...)`
- saves new `InventoryStock`
- updates `EnterpriseSettings.last_stock_upload`
- commits transaction

This path is not a pure sender.

## Why this matters

Store-aware stock export must not reuse this full path, because it would introduce:

- `InventoryStock` writes
- delete/save behavior for legacy stock cache
- `last_stock_upload` mutation
- optional `stock_correction`
- branch/code semantics tied to `mapping_branch`

## 3. Store-Aware Stock Path Already Available

Current isolated store-aware stock path:

- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/scripts/business_store_stock_export.py`

## Current behavior

Source:

- `build_store_stock_payload_preview(...)`

Target:

- `BusinessStore.tabletki_branch`
- `BusinessStore.tabletki_enterprise_code`

Payload item:

```json
{
  "Code": "8C411335BA",
  "Price": 367,
  "Qty": 157,
  "PriceReserve": 367
}
```

Meaning:

- `Code = external_product_code`
- `Price = final_store_price_preview`
- `PriceReserve = final_store_price_preview`
- `Qty = qty`

Important properties:

- no `process_database_service`
- no `InventoryStock` writes
- no `offers.price` mutation
- no `mapping_branch` target routing
- no scheduler coupling

## 4. Why Store-Aware Stock Publish Must Stay Separate

Store-aware stock publish must remain separate from legacy stock runtime because:

1. legacy scheduler is single-enterprise and ambiguous with multiple Business enterprises
2. legacy path resolves target branch via `mapping_branch`, not `BusinessStore.tabletki_branch`
3. legacy path writes `InventoryStock` and mutates upload timestamps
4. store-aware stock price is an overlay:
   - `final_store_price_preview`
   - based on price adjustments
   - outside current `dropship_pipeline`
5. store-aware code identity is external:
   - `BusinessStoreProductCode.external_product_code`
   - not the internal code used in legacy path

## 5. Proposed Future Service

Implemented standalone multi-store layer:

- `app/services/business_store_stock_publish_service.py`
- `app/scripts/business_store_stock_publish.py`

Recommended future scheduler module:

- `app/services/business_store_stock_publish_service.py`

Implemented functions:

- `get_eligible_business_store_stocks(...)`
- `publish_enabled_business_store_stocks(...)`

Recommended execution source:

- `export_business_store_stock(...)`

This mirrors the catalog rollout pattern:

- first standalone service
- then standalone CLI
- only later scheduler wiring

## 6. Eligibility Rules

Recommended eligibility rules:

- `BusinessStore.is_active = true`
- `BusinessStore.stock_enabled = true`
- linked `EnterpriseSettings.stock_enabled = true`
- `BusinessStore.is_legacy_default = false` by default
- `BusinessStore.migration_status` in stock-ready states
- `BusinessStore.tabletki_branch` not empty
- `BusinessStore.tabletki_enterprise_code` not empty
- exportable rows exist in stock preview

Additional stock-specific conditions:

- if code mappings are missing and preview has no exportable rows
  - skip
- if `extra_markup_enabled=true` and price adjustment mappings are missing
  - skip

Recommended allowed states:

- `dry_run`
- `catalog_stock_live`
- `orders_live`

Recommended skip reasons:

- `inactive_store`
- `store_stock_disabled`
- `enterprise_stock_disabled`
- `legacy_default_excluded`
- `migration_status_not_stock_ready`
- `missing_tabletki_branch`
- `missing_tabletki_enterprise_code`
- `missing_exportable_rows`
- `missing_code_mapping`
- `missing_price_adjustment`
- `missing_enterprise_settings`

## 7. Feature Flags

Scheduler flags:

- `BUSINESS_STORE_STOCK_SCHEDULER_ENABLED=false`
- `BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN=true`

Recommended semantics:

- disabled by default
- if enabled, dry-run by default
- live send only when dry-run flag is explicitly disabled

## 8. Scheduler Strategy Recommendation

## Option A: Hook into existing `business_stock_scheduler_service`

Pros:

- one existing scheduler loop
- can run after legacy stock refresh

Cons:

- current scheduler is single-enterprise and ambiguous
- legacy loop can skip entirely when multiple Business enterprises exist
- store-aware fan-out logic would inherit assumptions from legacy stock pipeline
- strong risk of mixing legacy and store-aware logging, cadence and failure semantics

## Option B: Separate `business_store_stock_scheduler_service.py`

Pros:

- clean separation from legacy single-enterprise assumptions
- per-store fan-out fits naturally
- no pressure to reuse `run_pipeline(..., "stock")`
- easier dry-run/live rollout
- clearer future observability and independent failure handling

Cons:

- one more scheduler service to operate
- needs its own cadence and freshness policy

## Recommendation

Safer staged strategy:

1. implement separate multi-store stock publish service
2. add multi-store stock CLI
3. only then add separate scheduler service:
   - `app/services/business_store_stock_scheduler_service.py`

This is safer than hooking directly into legacy `business_stock_scheduler_service`.

Reason:

- legacy scheduler ambiguity is already a known failure mode;
- store-aware stock is per-store and should not depend on single-enterprise resolution;
- store-aware sender should remain isolated from `process_database_service`.

## 9. Relationship to Offers Freshness

Store-aware stock publish already uses:

- current `Offer` rows

That means store-aware stock freshness depends on how fresh `offers` already are.

Important implication:

- if legacy `run_pipeline(..., "stock")` is skipped because Business enterprise resolution is ambiguous, then `offers` may become stale
- store-aware stock publish would still be able to send, but from stale offer data

Therefore future stock scheduler architecture should separate two concerns:

1. offer refresh
2. store-aware stock send

Recommended initial rule:

- do not make store-aware stock scheduler responsible for refreshing `offers`
- document that it relies on existing `offers` state
- before live rollout, define an explicit freshness policy for `offers`

Open architecture question for next stage:

- whether to keep relying on external offer refresh already happening elsewhere
- or to introduce a dedicated precondition/health-check before store-aware stock publish

## 10. Report Format

Future multi-store stock publish report should include:

- `total_stores_found`
- `eligible_stores`
- `skipped_stores`
- `published_stores`
- `failed_stores`

Per store:

- `store_id`
- `store_code`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `candidate_products`
- `exportable_products`
- `skipped_products`
- `sent_products`
- `endpoint_preview`
- `status`
- `warnings`
- `errors`
- `skip_reason`

This should mirror the catalog publish reporting model for operational consistency.

## 11. Risks

Main risks:

1. store stock send with stale `offers`
2. double stock send to the same branch
3. stock publish not aligned with latest catalog external codes
4. sending internal code instead of external code
5. accidental reuse of `mapping_branch`
6. accidental `InventoryStock` writes
7. accidental reuse of `process_database_service("stock", ...)`
8. legacy scheduler ambiguity when multiple Business enterprises exist
9. price mismatch because store markup is applied outside `dropship_pipeline`
10. partial migration where catalog/stock are store-aware but orders are only partially integrated

## 12. Files for Future Implementation

Current implementation files:

- `app/services/business_store_stock_publish_service.py`
- `app/scripts/business_store_stock_publish.py`

Future scheduler implementation files:

- `app/services/business_store_stock_scheduler_service.py`
- `ENV_REFERENCE.md`
- `docs/business_multistore_architecture.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_stores_ui_handoff.md`

Files that should remain unchanged in that stage unless explicitly re-scoped:

- `app/services/business_stock_scheduler_service.py`
- `app/business/dropship_pipeline.py`
- `app/services/database_service.py`
- `app/services/stock_export_service.py`
- `app/services/stock_update_service.py`
- `mapping_branch` runtime logic
- DB schema / Alembic

## 13. Staged Implementation Plan

Recommended order:

1. add standalone multi-store stock publish service
2. add standalone multi-store CLI
3. verify dry-run reports for all candidate stores
4. verify store-level code and price-adjustment gating
5. define offers freshness policy
6. add separate stock scheduler service
7. enable dry-run mode first
8. enable live send only after branch-level validation

## 14. Final Recommendation

Do not hook store-aware stock publish directly into current legacy `business_stock_scheduler_service`.

Preferred path:

- separate multi-store service
- separate CLI
- separate scheduler later
- future flags:
  - `BUSINESS_STORE_STOCK_SCHEDULER_ENABLED`
  - `BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN`

This keeps:

- legacy stock runtime stable
- `process_database_service` untouched
- `InventoryStock` untouched
- `mapping_branch` untouched
- store-aware stock semantics isolated and auditable

## 15. Current Status

Implemented on this stage:

- standalone multi-store stock publish service
- standalone multi-store stock CLI
- separate `app/services/business_store_stock_scheduler_service.py`
- default dry-run behavior
- live send only through explicit `--send --confirm`
- base/legacy-default stores excluded by default
- separate scheduler implemented and still isolated from legacy `business_stock_scheduler_service`
- service relies on current `Offer` state and does not refresh offers

Scheduler behavior:

- `BUSINESS_STORE_STOCK_SCHEDULER_ENABLED=false`
  - service exits gracefully
- `BUSINESS_STORE_STOCK_SCHEDULER_ENABLED=true`
  - scheduler runs its own loop
- `BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN=true`
  - report-only mode, no external send
- `BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN=false`
  - live send is allowed through env-only confirmation
- `BUSINESS_STORE_STOCK_SCHEDULER_INTERVAL_SECONDS`
  - controls the loop cadence
  - minimum enforced to `30` seconds
