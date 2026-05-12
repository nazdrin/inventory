# Business Store Catalog Scheduler Audit

## Scope

Этот документ фиксирует:

- как сейчас работает current master catalog scheduler;
- почему store-aware catalog publish нельзя встраивать в текущий legacy/master publish path;
- какой отдельный service нужен для будущего безопасного scheduler integration;
- какие feature flags и gating rules нужны для staged rollout.

Этот документ относится только к catalog publish. Stock publish, order runtime и manual exporters здесь не меняются.

## Checked Files

- `app/services/master_catalog_scheduler_service.py`
- `app/services/master_business_settings_resolver.py`
- `app/business/master_catalog_orchestrator.py`
- `app/business/tabletki_master_catalog_exporter.py`
- `app/business/business_store_catalog_exporter.py`
- `app/business/business_store_catalog_preview.py`
- `app/services/business_stock_scheduler_service.py`
- `app/models.py`
- `ENV_REFERENCE.md`
- `docs/business_multistore_architecture.md`
- `docs/business_store_catalog_identity.md`
- `docs/business_stores_ui_handoff.md`

## Current Master Catalog Scheduler Flow

## 1. Scheduler owner

Current master catalog scheduling belongs to:

- `app/services/master_catalog_scheduler_service.py`

The scheduler:

- is enabled by `MASTER_SCHEDULER_ENABLED`;
- keeps its own lock and state file;
- runs three independent job families:
  - weekly enrichment
  - daily publish
  - periodic archive import

## 2. Settings source

Scheduler settings are resolved through:

- `app/services/master_business_settings_resolver.py`

Resolution model:

- primary source: `business_settings` DB row;
- fallback source: env vars if `business_settings` row is missing.

Relevant fields:

- `BusinessSettings.business_enterprise_code`
- `BusinessSettings.daily_publish_enterprise_code_override`
- `BusinessSettings.weekly_salesdrive_enterprise_code_override`
- `BusinessSettings.master_daily_publish_enabled`
- `BusinessSettings.master_daily_publish_hour`
- `BusinessSettings.master_daily_publish_minute`
- `BusinessSettings.master_daily_publish_limit`
- `BusinessSettings.master_weekly_*`

Effective daily publish enterprise is:

- `daily_publish_enterprise_code_override`
- otherwise `business_enterprise_code`

## 3. Daily publish execution

Daily publish path in current runtime:

1. `master_catalog_scheduler_service._maybe_run_daily_publish(...)`
2. `_run_daily_publish(...)`
3. `_run_orchestrator_job("daily_publish", mode="publish", send=True, ...)`
4. `run_master_catalog_orchestrator(mode="publish", enterprise=...)`
5. `master_catalog_orchestrator._build_publish_steps(...)`
6. `export_master_catalog_to_tabletki(...)`

This means current daily publish is a single-enterprise job with one resolved `enterprise_code`.

## 4. Weekly path

Weekly enrichment is separate:

1. enrichment of `master_catalog` inputs
2. SalesDrive weekly export

It does not own store-aware routing and is not the right place for per-store catalog identity logic.

## Current Master Publish Target Model

## 1. Target enterprise

Current publish target is resolved from:

- `business_settings.business_enterprise_code`
- optional `daily_publish_enterprise_code_override`

This is an enterprise-level selector, not a store selector.

## 2. Target branch

Current live branch is taken from:

- `EnterpriseSettings.branch_id`

inside:

- `app/business/tabletki_master_catalog_exporter.py`

Specifically, exporter sends to:

- `DeveloperSettings.endpoint_catalog + "/Import/Ref/{enterprise_settings.branch_id}"`

## 3. Auth and endpoint

Current master publish uses:

- endpoint base: `DeveloperSettings.endpoint_catalog`
- auth login: `EnterpriseSettings.tabletki_login`
- auth password: `EnterpriseSettings.tabletki_password`

## 4. Payload shape

Current master catalog payload is built in:

- `tabletki_master_catalog_exporter._build_offer_payload(...)`

Current item identity:

- `Code = MasterCatalog.sku`
- `Name = resolved base name from MasterCatalog`

This is a base/internal-code publish path, not a store overlay path.

## Why Store-Aware Catalog Publish Must Stay Separate

Store-aware catalog publish must not be embedded into:

- `master_catalog_orchestrator.py`
- `tabletki_master_catalog_exporter.py`

Reasons:

1. Current master publish is single-enterprise and branch-fixed through `EnterpriseSettings.branch_id`.
2. Store-aware publish needs per-store target routing:
   - `BusinessStore.tabletki_enterprise_code`
   - `BusinessStore.tabletki_branch`
3. Store-aware publish needs per-store identity overlay:
   - `BusinessStoreProductCode`
   - `BusinessStoreProductName`
4. Store-aware publish must skip or warn on missing mappings, not silently fall back inside legacy exporter.
5. Mixing the routes would blur control-plane selectors:
   - `business_enterprise_code`
   - `daily_publish_enterprise_code_override`
   These selectors must stay enterprise-level only.
6. Legacy master publish must remain predictable even if store overlays are partially configured or broken.

## Existing Safe Building Blocks

Already implemented and safe to reuse:

- `app/business/business_store_catalog_preview.py`
  - read-only source-of-truth preview over `MasterCatalog + BusinessStore mappings`
- `app/business/business_store_catalog_exporter.py`
  - manual exporter for one store
  - supports dry-run and live send
  - already uses:
    - `BusinessStore.tabletki_branch`
    - `BusinessStore.tabletki_enterprise_code`
    - external codes/names

That exporter is the correct future source for scheduler-owned store publish.

## Proposed Future Service

Implemented separate service:

- `app/services/business_store_catalog_publish_service.py`

Candidate functions:

- `publish_enabled_business_store_catalogs(...)`
- `publish_single_business_store_catalog(...)`

Implemented now:

- `get_eligible_business_store_catalogs(...)`
- `publish_enabled_business_store_catalogs(...)`
- CLI: `app/scripts/business_store_catalog_publish.py`

## Source and execution model

The new service should:

- iterate eligible `BusinessStore` rows;
- call `export_business_store_catalog(...)`;
- default to dry-run capable behavior;
- collect per-store report data;
- avoid direct dependency on `tabletki_master_catalog_exporter`.

The service should not:

- modify `master_catalog_orchestrator`;
- modify `tabletki_master_catalog_exporter`;
- generate missing mappings;
- reinterpret `business_settings` enterprise overrides as store selectors.

## Proposed Eligibility Rules

A store should be eligible only if all conditions hold:

- `BusinessStore.is_active = true`
- `BusinessStore.catalog_enabled = true`
- linked `EnterpriseSettings.catalog_enabled = true`
- `BusinessStore.is_legacy_default = false`
- `BusinessStore.migration_status` is in allowed publish-ready states
- `BusinessStore.tabletki_branch` is not empty
- `BusinessStore.tabletki_enterprise_code` is not empty

Implemented publish-ready migration states:

- `dry_run`
- `catalog_stock_live`
- `orders_live`

Recommended exclusion on first scheduler stage:

- stores with `is_legacy_default = true`

Reason:

- the base/primary enterprise already has a legacy master publish path;
- sending it again through store-aware scheduler creates duplicate-routing risk.

## Base Enterprise Handling

First safe rollout rule:

- primary legacy/base enterprise continues to publish through the current master daily publish path;
- store-aware scheduler publishes only non-legacy overlays.

Do not use these fields as store selectors:

- `BusinessSettings.business_enterprise_code`
- `BusinessSettings.daily_publish_enterprise_code_override`

They remain control-plane selectors for the legacy/master path only.

## Proposed Feature Flags

Scheduler hook flags:

- `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED=false`
- `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN=true`

Implemented behavior:

- if `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED=false`
  - no store-aware scheduler activity
- if `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED=true` and `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN=true`
  - scheduler executes per-store dry-runs and logs/report-only output
- if `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED=true` and `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN=false`
  - scheduler is allowed to perform live send

This provides a staged rollout:

1. code present but disabled
2. dry-run logging in scheduler window
3. live send only after verification

## Recommended Connection Point

Recommended staged plan:

1. implement separate service first
2. implement optional manual CLI for all eligible stores
3. only then add a hook in `master_catalog_scheduler_service`

Implemented hook location in scheduler:

- after successful legacy daily publish
- as a separate flagged block
- with independent per-store reporting

Why after legacy daily publish:

- current master publish keeps priority;
- store-aware failures must not block base catalog publication;
- dry-run/live reporting can stay isolated.

Why not inside `master_catalog_orchestrator`:

- orchestrator is mode-based and enterprise-oriented;
- store-aware publish is per-store fan-out logic with different routing semantics.

## Error Handling and Reporting

Future scheduler report should include:

- total stores found
- stores eligible
- stores skipped
- stores published dry-run
- stores published live

Per store:

- `store_id`
- `store_code`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `candidate_products`
- `exportable_products`
- `skipped_products`
- `endpoint_preview`
- `status`
- `warnings`
- `errors`

Recommended skip reasons:

- inactive store
- catalog disabled on store
- catalog disabled on enterprise
- legacy default store excluded
- migration status not publish-ready
- missing target branch
- missing target enterprise code
- missing exportable rows
- missing credentials

## Risks

Main risks for future implementation:

1. Double catalog publish into the same branch.
2. Catalog publish into the wrong branch because of mixed legacy/store routing.
3. Store publish before code/name mappings are generated.
4. Scheduler dry-run/live confusion.
5. Store publish failure blocking legacy master publish.
6. Legacy master publish failure incorrectly blocking dry-run reporting for stores.
7. Reusing `daily_publish_enterprise_code_override` as a fake store selector.
8. Accidentally publishing the base enterprise through both legacy and store-aware paths.
9. Repeated store publish in the same scheduler window without separate state handling.

## Recommended Next Implementation Checklist

Current implementation files:

- `app/services/business_store_catalog_publish_service.py`
- `app/scripts/business_store_catalog_publish.py`

Future scheduler integration files:

- `app/services/master_catalog_scheduler_service.py`
- `ENV_REFERENCE.md`
- `docs/business_multistore_architecture.md`
- `docs/business_store_catalog_identity.md`
- `docs/business_stores_ui_handoff.md`

Recommended sequence:

1. implement dedicated publish service
2. add multi-store dry-run CLI
3. verify reporting and eligible-store gating
4. add scheduler feature flags
5. connect service after successful legacy daily publish
6. enable scheduler in dry-run mode first
7. enable live send only after branch-level validation

## Final Recommendation

Safe first production architecture:

- keep current master catalog scheduler unchanged in behavior;
- use separate `business_store_catalog_publish_service`;
- source it from `export_business_store_catalog(...)`;
- gate it by:
  - `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED`
  - `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN`
- exclude base/legacy-default store on first stage;
- connect it only as a separate post-publish hook, not inside `tabletki_master_catalog_exporter` and not inside `master_catalog_orchestrator`.

## Current Status

Implemented on this stage:

- standalone multi-store publish service;
- standalone multi-store CLI;
- scheduler post-daily-publish hook behind feature flags;
- default dry-run behavior;
- live send only through explicit `--send --confirm`;
- base/legacy-default stores excluded by default;
- scheduler hook is connected but disabled by default.

Scheduler hook rules:

- `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED=false`
  - no store-aware scheduler publish runs
- `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED=true`
  - hook runs only after successful legacy daily publish
- `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN=true`
  - hook executes report-only dry-run
- `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN=false`
  - hook is allowed to perform live send through the separate publish service

Failure isolation:

- legacy daily publish remains the primary job;
- if legacy daily publish fails, store-aware hook is skipped;
- if store-aware hook fails, the scheduler loop continues and legacy daily publish result remains unchanged.
