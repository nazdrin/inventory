# Business baseline stock mode implementation audit

Date: 2026-04-23

Scope:
- audit only;
- no runtime changes;
- no DB schema changes;
- no UI code changes;
- no external API calls.

## 1. Goal

The Business contour now needs two explicit stock models:

1. Baseline enterprise stock mode.
   - Enterprise-level stock publish.
   - Uses the current legacy stock algorithm.
   - Uses `mapping_branch` as branch routing source.
   - Uses current pricing/jitter path.
   - Does not require `BusinessStore` overlays.

2. Store-aware business stock mode.
   - Store-level branch/scope/price overlay.
   - Uses `BusinessStore.tabletki_branch`.
   - Uses `BusinessStore.legacy_scope_key`.
   - Can use enterprise-level product code mapping.
   - Supports store-level extra markup and future multistore order routing.

Baseline enterprises like `223` should not depend on store overlays because their expected business behavior is "work as current legacy stock". Forcing them through store-aware overlays adds manual scope assignment, rollout-state configuration and possible extra-markup/code-mapping semantics that do not belong to a baseline stock rollout.

## 2. Current baseline preview status

Already implemented:

- service: `app/services/business_baseline_stock_preview_service.py`;
- CLI: `app/scripts/business_baseline_stock_preview.py`;
- function: `build_business_baseline_stock_preview(session, enterprise_code, limit=None)`;
- source algorithm: `dropship_pipeline.build_stock_payload`;
- report mode: `stock_mode="baseline_legacy"`;
- explicit report flags:
  - `depends_on_business_stores=false`;
  - `uses_process_database_service=false`;
  - `external_api_calls=false`;
  - `price_source="legacy_algorithm"`.

Confirmed local result for `223`:

- `status=ok`;
- `stock_mode=baseline_legacy`;
- `mapping_branch_rows=4`;
- `output_branches_count=4`;
- `branches_in_payload=["30421","30422","30423","30491"]`;
- `rows_total=17069`;
- `missing_mapping_rows_count=0`;
- no external API calls.

Not implemented yet:

- no runtime scheduler mode switch;
- no live send path gated by baseline mode;
- no UI action/button;
- no persisted mode field;
- no operator-visible mode selector;
- no report integration into Business Settings or Business Stores page.

The preview proves technical feasibility, but it is not an operator-usable mode. Operators still need a clear control plane: what mode is active, what will be published, and which action actually sends stock.

## 3. UI problem status

The store overlay UI had a state ownership bug where `storeDraft` could be overwritten by stale async enterprise/store reloads after switching stores. A frontend fix was added to isolate `storeDraft` by `storeDraftStoreId` and block save on owner mismatch, but manual replay is not yet a stable basis for baseline rollout.

Even after that fix, baseline stock mode should be independent from the store form:

- baseline enterprises should not require `legacy_scope_key` on every `BusinessStore`;
- baseline enterprises should not require `BusinessStore.stock_enabled`;
- baseline enterprises should not require store-level `migration_status`;
- baseline enterprises should not be affected by store-level extra markup fields;
- store-aware UI correctness remains important for Business multistore enterprises, but it must not be the baseline stock control mechanism.

## 4. Where the baseline mode should live

### Option A: Enterprise-level toggle/button on BusinessStoresPage

Description:
- Add a visible enterprise-level stock section on BusinessStoresPage.
- Show current mode and baseline preview/publish actions near the enterprise catalog/stock settings.

Pros:
- Good operator clarity while configuring Business stores.
- Close to `mapping_branch` and store overlay context.
- Easy to explain: "Stores are for store-aware mode; baseline mode ignores overlays."

Cons:
- BusinessStoresPage is already overloaded.
- Risk of mixing baseline enterprise stock actions with store-aware overlay actions.
- Requires careful UI copy to avoid accidental live publish.

Backend complexity:
- moderate; needs endpoint/action for baseline preview/publish.

Migration risk:
- medium if live button is exposed too early.

### Option B: Enterprise-level toggle on Enterprise Settings page

Description:
- Put baseline stock mode controls in generic Enterprise Settings / EnterprisePanel.

Pros:
- Conceptually enterprise-level.
- Avoids store overlay UI confusion.

Cons:
- Operators working with Business contour may not discover the setting.
- EnterprisePanel likely has broader non-Business semantics.
- Could spread Business-specific controls across pages.

Backend complexity:
- moderate.

Migration risk:
- medium due to weaker Business-context guardrails.

### Option C: Mode selector in BusinessSettingsPage

Description:
- Add a Business stock mode selector to the existing Business control-plane page.
- The selector controls scheduler/runtime behavior for the resolved Business enterprise.

Pros:
- Best architectural fit for runtime control.
- BusinessSettings already owns `business_stock_enabled` and `business_stock_interval_seconds`.
- Clear place to show scheduler mode, source and enabled state.
- Easier to keep live actions behind explicit Business runtime controls.

Cons:
- Does not directly show store branch overlays.
- Needs UI wording to tell operators that baseline mode ignores BusinessStore overlays.

Backend complexity:
- low to medium if using env short-term; medium if persisted in DB later.

Migration risk:
- lowest among UI options because it aligns with existing scheduler control-plane.

### Option D: Env allowlist only

Description:
- Introduce env such as `BUSINESS_BASELINE_STOCK_ENTERPRISE_CODES=223`.
- No operator UI toggle at first.

Pros:
- No DB schema change.
- Lowest initial implementation risk.
- Good for limited pilot.
- Easy rollback by removing enterprise code from env.

Cons:
- Poor operator visibility.
- Requires deploy/config access.
- Not self-documenting in UI.
- Can drift from BusinessSettings UI unless surfaced as read-only badge.

Backend complexity:
- low.

Migration risk:
- low technically, medium operationally if not reported clearly.

### Option E: Hybrid

Description:
- Short-term: env allowlist + read-only UI badge/report.
- Long-term: persisted enterprise-level setting.

Pros:
- Safe rollout without schema change.
- Operators can see active mode before DB model is finalized.
- Clean migration path to persisted selector later.

Cons:
- Two-step implementation.
- Temporary duplication between env and future DB setting.

Backend complexity:
- low short-term, medium long-term.

Migration risk:
- lowest practical path.

## 5. Recommended control model

Recommended short-term model:

- Use env/config allowlist:
  - `BUSINESS_BASELINE_STOCK_ENTERPRISE_CODES=223,...`
- Add runtime resolver:
  - if target enterprise code is in allowlist: `stock_mode="baseline_legacy"`;
  - otherwise: existing behavior remains unchanged.
- Add read-only mode badge/summary in BusinessSettingsPage:
  - "Stock mode: baseline legacy" or "Stock mode: store-aware / legacy default";
  - source: `env`;
  - explanation that baseline mode ignores `BusinessStore` overlays.
- Do not add a live UI publish button in the first runtime step.

Recommended long-term model:

- Add a persisted enterprise-level setting:
  - preferred field: `business_stock_mode`;
  - location: `BusinessSettings` if it controls Business runtime globally;
  - allowed values:
    - `baseline_legacy`;
    - `store_aware`.
- Surface it in BusinessSettingsPage as the primary mode selector.
- BusinessStoresPage should show a read-only badge only:
  - "This enterprise uses baseline stock mode; store stock fields are ignored for baseline stock publish."

Actions:

- Add "Preview baseline stock" early because it is read-only and already backed by the new service.
- Add "Run baseline stock publish" only after runtime support has confirm guards, dry-run parity and clear mode logging.
- Do not reuse store-aware buttons for baseline publish.

## 6. Runtime architecture for baseline mode

Target baseline runtime:

- Source algorithm:
  - `dropship_pipeline.build_stock_payload`.
- Branch routing:
  - `mapping_branch`.
- City/scope linkage:
  - current legacy mechanism through `mapping_branch.store_id`.
- Pricing/jitter:
  - current legacy pricing path unchanged.
- Send path:
  - existing `process_database_service(..., "stock", enterprise_code)` for live baseline publish.
- No dependency on:
  - `BusinessStore.stock_enabled`;
  - `BusinessStore.legacy_scope_key`;
  - `BusinessStore.migration_status`;
  - `BusinessStore.tabletki_branch`;
  - store-level extra markup;
  - store-level price adjustments.

Scheduler/reporting expectations:

- Every report must include:
  - `stock_mode`;
  - `stock_mode_source`;
  - `enterprise_code`;
  - `mapping_branch_rows`;
  - `output_branches_count`;
  - `rows_total`;
  - `sent_rows` for live send;
  - `dry_run`;
  - `external_api_calls`;
  - warnings/errors.
- Logs must clearly distinguish:
  - `baseline_legacy` using legacy enterprise algorithm;
  - `store_aware` using BusinessStore overlays.
- If multiple Business enterprises exist, baseline mode should not silently pick one unless the target is explicit through BusinessSettings or CLI.

## 7. Interaction with store-aware mode

Coexistence is safe if mode selection is explicit.

Baseline enterprise:

- `stock_mode=baseline_legacy`;
- branch routing from `mapping_branch`;
- product codes are baseline/internal codes;
- pricing is legacy algorithm;
- store overlays are not required for stock publish.

Business enterprise:

- `stock_mode=store_aware`;
- branch/scope from `BusinessStore`;
- product code mapping can be enterprise-level behind existing flags;
- store-level markup/price overlays can apply;
- store/order routing remain store-level.

Misconfiguration prevention:

- Never auto-detect mode from `BusinessStore` existence.
- Never auto-detect mode from missing `legacy_scope_key`.
- Never let both baseline and store-aware scheduler publish the same enterprise in the same run.
- Report mode and source in every CLI/scheduler result.
- In UI, disable or visually de-emphasize store stock overlay controls when baseline mode is active, but keep them editable only if the enterprise is being prepared for store-aware migration.

## 8. Minimal implementation plan

### Phase A: explicit mode concept and runtime selector

- Add stock mode resolver:
  - `baseline_legacy` if enterprise in env allowlist;
  - default remains current behavior.
- Add CLI/report endpoint for current mode.
- Add BusinessSettingsPage read-only badge.
- Route scheduler/manual stock publish through the mode-aware selector.

Implementation status:

- Implemented unified resolver: `app/services/business_runtime_mode_service.py`.
- Persisted source of truth: `EnterpriseSettings.business_runtime_mode`.
- CLI: `python -m app.scripts.business_stock_mode_status --enterprise-code 223 --output-json`.
- BusinessStoresPage now shows `Режим предприятия` as an editable enterprise-level selector.
- Runtime selector is implemented:
  - `baseline` uses the legacy catalog path plus the existing legacy dropship stock pipeline;
  - `custom` uses enterprise-level catalog identity plus the BusinessStore stock publish contour;
  - scheduler processes multiple Business enterprises one by one through the selector;
  - `EnterpriseSettings.business_stock_mode` remains only as a legacy compatibility field during the transition.

### Phase B: operator actions

- Add "Preview baseline stock" action first.
- Add "Run baseline stock publish" only with confirm modal and visible mode/source.
- Operator docs:
  - baseline mode ignores store overlays;
  - store-aware mode requires per-store scope/branch/pricing configuration.

## 9. Concrete next implementation task

Recommended next prompt:

```text
Задача: добавить operator actions для enterprise-level baseline stock mode.

Сделать:
- добавить UI action "Preview baseline stock";
- live action добавлять только после confirm modal;
- показывать `stock_mode`, `stock_mode_source` и source env;
- DB schema не менять;
- external APIs не вызывать в preview.

Проверки:
- compileall app;
- BusinessSettingsPage build;
- baseline preview для 223 остаётся ok;
- store-aware dry-run для 364 остаётся ok.
```

After Phase A, implement Phase B as a separate operator-action task with live UI send guarded by explicit confirm and rollback plan.

## 10. Non-goals

This audit does not implement:

- DB schema changes;
- runtime scheduler switch;
- baseline live send;
- UI code changes;
- external API calls;
- store-aware stock rewrite;
- order runtime changes;
- catalog runtime changes;
- outbound status runtime changes;
- replacing `mapping_branch` in legacy baseline stock.
