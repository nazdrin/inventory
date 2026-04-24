# Business baseline stock mode and store scope persistence audit

Date: 2026-04-23

Scope:
- audit only;
- no runtime changes;
- no DB schema changes;
- no UI code changes;
- no external API calls.

## 1. Problem statement

After the enterprise catalog migration the catalog identity is enterprise-level, while stock, orders, routing and pricing overlays remain store-level for the Business multistore contour.

Two separate issues are now coupled in operator experience:

1. On `BusinessStoresPage`, store-level fields such as `legacy_scope_key`, `stock_enabled`, `orders_enabled`, `salesdrive_enterprise_id`, `extra_markup_*` and `tabletki_branch` can be shown or saved as if they belonged to the wrong selected store after switching stores.
2. Baseline enterprises such as `223` need behavior equivalent to the current legacy stock contour: base codes, base names, full/base catalog, current pricing/jitter logic, and stock publishing across branches without manual store overlay tuning for every branch.

Read-only data check for enterprise `223`:

```json
{
  "enterprise": {
    "enterprise_code": "223",
    "branch_id": "30421",
    "stock_enabled": false,
    "catalog_enabled": false
  },
  "mapping_branch": [
    {"branch": "30421", "store_id": "Lviv"},
    {"branch": "30422", "store_id": "Ivano-Frankivsk"},
    {"branch": "30423", "store_id": "Kyiv"},
    {"branch": "30491", "store_id": "Kremenchuk"}
  ],
  "stores": [
    {
      "id": 1,
      "store_code": "business_223",
      "tabletki_branch": "30421",
      "legacy_scope_key": "Ivano-Frankivsk",
      "is_active": true,
      "stock_enabled": true,
      "orders_enabled": false,
      "salesdrive_enterprise_id": 1,
      "migration_status": "draft"
    },
    {
      "id": 3,
      "store_code": "business_223_30422",
      "tabletki_branch": "30422",
      "legacy_scope_key": null,
      "is_active": true,
      "stock_enabled": false,
      "orders_enabled": false,
      "salesdrive_enterprise_id": null,
      "migration_status": "draft"
    },
    {
      "id": 4,
      "store_code": "business_223_30423",
      "tabletki_branch": "30423",
      "legacy_scope_key": null,
      "is_active": true,
      "stock_enabled": false,
      "orders_enabled": false,
      "salesdrive_enterprise_id": null,
      "migration_status": "draft"
    },
    {
      "id": 5,
      "store_code": "business_223_30491",
      "tabletki_branch": "30491",
      "legacy_scope_key": null,
      "is_active": true,
      "stock_enabled": false,
      "orders_enabled": false,
      "salesdrive_enterprise_id": null,
      "migration_status": "draft"
    }
  ],
  "duplicates": []
}
```

Important observation: branch sync has aligned store rows with `mapping_branch`, but only one store has `legacy_scope_key`. For `branch=30421`, the store scope is `Ivano-Frankivsk`, while `mapping_branch.store_id` is `Lviv`. This is either a real operator mistake or stale UI save behavior, and it is exactly the kind of drift that makes baseline enterprises risky to run through manual overlays.

## 2. UI/store save bug audit

Relevant files:
- `admin-panel/src/pages/BusinessStoresPage.jsx`
- `app/routes.py`
- `app/schemas.py`

### Current frontend state flow

`BusinessStoresPage.jsx` uses one shared `storeDraft` object for all stores:

- `selectOverlay(store)` sets:
  - `selectedStoreId = store.id`;
  - `storeDraft = buildStoreDraftFromStore(store)`.
- `loadEnterpriseContext` effect depends on:
  - `selectedEnterpriseCode`;
  - `selectedStoreId`;
  - `storesForSelectedEnterprise`.
- That effect asynchronously calls `getEnterpriseByCode(selectedEnterpriseCode)` and then sets `storeDraft` again from:
  - selected store if found;
  - first store if selected id is missing;
  - a new draft if no stores exist.
- The new-store branch auto-select effect can also rewrite `storeDraft.tabletki_branch`, `store_code`, `store_name`, `enterprise_code` and `tabletki_enterprise_code`.

### Root cause

The root cause is a frontend state ownership/race problem, not an obvious backend persistence bug.

The page has no request cancellation, no request generation id, no dirty-state guard and no explicit `storeDraftStoreId`. Because `loadEnterpriseContext` is async, an older run can finish after a newer store selection and overwrite the visible `storeDraft` with stale data. Since the same `storeDraft` object is used for all store fields, the UI can visually show the last selected/loaded `legacy_scope_key` or related values under a different selected row.

The problem can become a save/update bug when the operator saves while the visible draft no longer matches the intended store. `handleSaveStore` sends `buildStorePayload(storeDraft, selectedEnterpriseCode)` to `/business-stores/{selectedStoreId}`. If `selectedStoreId` and `storeDraft` are out of sync, the backend correctly writes the wrong draft into the selected row.

### Backend save path

The backend update path is conventional and does not show field leakage by itself:

- `BusinessStoreUpdate` fields are optional.
- `update_business_store` uses `payload.model_dump(exclude_unset=True)` and applies only sent fields via `setattr`.
- Branch validation checks `enterprise_code`, `tabletki_branch` and `is_active` when needed.

Therefore the persistence layer writes what the frontend sends. The suspicious part is the frontend draft source, not `BusinessStoreUpdate`.

### Affected fields

Any field carried by `storeDraft` can be affected:

- `legacy_scope_key`;
- `stock_enabled`;
- `orders_enabled`;
- `salesdrive_enterprise_id`;
- `tabletki_branch`;
- `extra_markup_enabled`;
- `extra_markup_min`;
- `extra_markup_max`;
- `migration_status`;
- compatibility fields still included in payload, such as `catalog_only_in_stock`, `code_strategy`, `name_strategy`.

### Minimal safe fix

Recommended UI fix:

1. Track draft ownership explicitly:
   - add `storeDraftStoreId`;
   - set it together with `storeDraft`;
   - refuse save if `selectedStoreId !== storeDraftStoreId` for existing stores.
2. Add async request guard to `loadEnterpriseContext`:
   - generation counter or cancellation flag;
   - only the latest request may update `enterpriseDraft` and `storeDraft`.
3. Do not let `loadEnterpriseContext` overwrite a user-edited existing `storeDraft` after store selection unless store list actually changed and the selected store object was refreshed after save.
4. On store selection, clear preview/action state and build draft only from the clicked store.
5. Keep the new-store branch auto-fill effect limited to unsaved drafts only.

Radius of impact: UI-only, mostly `BusinessStoresPage.jsx`. Backend route behavior can remain unchanged.

## 3. Current baseline stock behavior audit

Relevant files:
- `app/services/business_stock_scheduler_service.py`
- `app/business/dropship_pipeline.py`
- `app/services/database_service.py`
- `app/services/stock_export_service.py`

The current legacy Business stock scheduler is enterprise-level:

1. `run_business_stock_once()` resolves one Business enterprise through `BusinessSettings` or fallback enterprise settings.
2. If enabled and due, it calls `run_pipeline(enterprise_code, "stock")`.
3. `run_pipeline` refreshes offers and calls `generate_and_send_stock`.
4. `generate_and_send_stock` calls `build_stock_payload(session, enterprise_code)` and then `process_database_service(..., "stock", enterprise_code)`.

The baseline stock payload is built by `dropship_pipeline.build_stock_payload`:

- it selects best offers by `(Offer.city, Offer.product_code)`;
- it uses only offers with `Offer.stock > 0`;
- tie-breakers include supplier stock priority, price, supplier priority, stock and `updated_at`;
- it loads branch routing from `mapping_branch` with `{store_id: branch}`;
- it emits rows with:
  - `branch`;
  - internal/base `product_code`;
  - `price`;
  - `qty`;
  - `price_reserve`.

This contour does not depend on `BusinessStore`.

Current pricing/jitter is part of `dropship_pipeline` and `process_database_service`. Reusing this contour preserves the behavior operators currently expect for baseline enterprises.

## 4. Store-aware stock mode audit

Relevant files:
- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/services/business_store_stock_publish_service.py`

Store-aware stock is a different contour. It requires a configured `BusinessStore`:

- `store.is_active = true`;
- `store.stock_enabled = true`;
- `enterprise.stock_enabled = true`;
- `store.migration_status` in stock-ready states;
- `store.tabletki_branch` present;
- `store.tabletki_enterprise_code` present;
- `store.legacy_scope_key` present for offer scope;
- product code mapping available according to the current code mapping mode;
- optional store-level price adjustments and extra markup.

Offer scope in store-aware preview is `Offer.city == store.legacy_scope_key`. The preview explicitly warns that best offer selection is locally approximated and does not import `app.business.dropship_pipeline`.

This contour is appropriate for Business multistore scenario B, where each store has own stock scope, branch, pricing overlay and optional extra markup. It is operationally expensive and risky for baseline enterprises that should behave "as before".

## 5. Architectural options

### Option A: force baseline through store-aware mode

Approach:
- create one `BusinessStore` per `mapping_branch.branch`;
- manually choose `legacy_scope_key` for each store;
- enable `stock_enabled`;
- keep extra markup disabled;
- use store-aware stock publish.

Pros:
- one runtime model for all stores;
- uses already implemented store-aware reporting and branch sync;
- branch selection is visible in UI.

Cons:
- baseline operators must configure every store even when the business requirement is "same as current stock";
- easy to misconfigure `legacy_scope_key`, as shown by current `223` state;
- current store-aware preview/export is not exactly the legacy `dropship_pipeline` algorithm;
- adds rollout friction for enterprises that do not need store-level pricing/markup/order routing.

Risks:
- silent branch/scope mismatch;
- partial enablement, where only one branch is configured;
- different stock quantities/prices than the legacy contour;
- increased support load in the operator UI.

### Option B: enterprise-level baseline stock export mode

Approach:
- add an explicit enterprise-level mode for baseline stock export;
- reuse the current legacy stock algorithm;
- source branch list from `mapping_branch`;
- keep current branch-city/scope linkage through `mapping_branch.store_id`;
- keep existing pricing/jitter and `process_database_service` behavior;
- do not require `BusinessStore` overlays for baseline stock publishing.

Pros:
- preserves known production behavior;
- avoids manual per-store overlay setup for baseline enterprises;
- uses `mapping_branch`, which is already the branch source of truth;
- lower risk for enterprises with base codes/base names/base prices;
- can coexist with store-aware Business mode.

Cons:
- two stock modes must be documented and selected explicitly;
- requires clear operator/admin visibility so baseline mode is not confused with store-aware mode;
- if an enterprise later needs store-specific markup/orders, it must be migrated intentionally to store-aware mode.

Risks:
- accidental mode misconfiguration if controlled only by env;
- scheduler needs explicit mode logging;
- baseline mode must be kept read-only/dry-run testable before live use.

## 6. Recommendation

Use both modes deliberately:

- Baseline enterprise, e.g. `223`: use enterprise-level baseline stock mode based on the existing legacy stock algorithm.
- Business enterprise, e.g. `364`: use store-aware stock mode.

Do not force baseline enterprises through store-aware overlays as the primary path. It is more operationally risky than preserving the existing enterprise-level stock algorithm.

Recommended control model:

1. Short-term, no DB schema:
   - env/config allowlist such as `BUSINESS_BASELINE_STOCK_ENTERPRISE_CODES=223,...`;
   - explicit report fields: `stock_mode=baseline_legacy` or `stock_mode=store_aware`;
   - no auto-detection from `BusinessStore` existence.
2. Longer-term:
   - dedicated settings field in `BusinessSettings` or `EnterpriseSettings`, e.g. `business_stock_mode`;
   - allowed values: `baseline_legacy`, `store_aware`;
   - UI shows the selected mode at enterprise level.

The UI store bug should still be fixed, because store-aware Business enterprises need correct overlay editing. But fixing that bug should not be used as the reason to make baseline enterprises depend on store overlays.

## 7. Minimal rollout plan

Phase 1: fix UI draft ownership.

- Add `storeDraftStoreId`.
- Add async request generation/cancellation in enterprise context loading.
- Prevent stale async `setStoreDraft` from overwriting the currently selected store.
- Block save when selected store and draft owner do not match.
- Regression: switch between stores with different scopes/flags and verify no visual leakage.

Phase 2: add baseline stock dry-run/report.

- Add a read-only preview/simulator around `dropship_pipeline.build_stock_payload`.
- Report:
  - `stock_mode=baseline_legacy`;
  - enterprise code/name;
  - mapping branch count;
  - output branch count;
  - rows count;
  - skipped rows without branch mapping;
  - sample rows.
- Do not call `process_database_service` in dry-run.

Implementation status:

- Implemented as `app/services/business_baseline_stock_preview_service.py`.
- CLI: `python -m app.scripts.business_baseline_stock_preview --enterprise-code 223 --output-json`.
- The preview reuses `dropship_pipeline.build_stock_payload`.
- It is read-only:
  - no `process_database_service`;
  - no scheduler refresh;
  - no live send;
  - no external API calls;
  - no `BusinessStore` dependency.
- Scheduler/runtime switching is not implemented yet.

Phase 3: add explicit mode selection.

- Short-term env allowlist is acceptable.
- Scheduler must log mode and skip reasons.
- Store-aware stock publish remains unchanged for Business multistore enterprises.

Implementation status:

- Phase A control-plane visibility exists via `app/services/business_stock_mode_service.py`.
- Short-term env allowlist: `BUSINESS_BASELINE_STOCK_ENTERPRISE_CODES`.
- BusinessSettingsPage shows read-only resolved stock mode.
- Scheduler/live publish are still not switched by this mode.

Phase 4: regression.

- UI: switching stores does not leak `legacy_scope_key`, `stock_enabled`, `orders_enabled`, `salesdrive_enterprise_id`, `extra_markup_*`, `tabletki_branch`.
- Baseline dry-run for `223`: uses `mapping_branch`, not `BusinessStore`.
- Store-aware dry-run for `364`: unchanged.
- No external API calls in tests.

## 8. Concrete next implementation task

Recommended next implementation prompt:

```text
Задача: исправить state/save bug на BusinessStoresPage и добавить read-only baseline stock preview для enterprise-level legacy stock mode.

Требования:
- UI: добавить ownership guard для storeDraft, request generation guard для async loadEnterpriseContext, запрет save при mismatch selectedStoreId/storeDraftStoreId.
- UI: не менять backend payload shape.
- Backend: добавить read-only CLI/service для baseline stock preview, переиспользующий dropship_pipeline.build_stock_payload без process_database_service и без внешних API.
- Runtime scheduler не переключать.
- DB schema не менять.
- Проверки: frontend build, python compileall, baseline preview для 223, store-aware dry-run для 364.
```

Final recommendation: do bugfix + baseline enterprise stock mode. Do not use store-aware overlays as the default baseline rollout mechanism.
