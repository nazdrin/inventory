# Business final preprod test audit

Date: 2026-04-23

## 1. Scope

This audit covers the current state of two Business contours:

- enterprise `223` as a baseline enterprise;
- enterprise `364` as a Business multistore enterprise.

Checked areas:

- stock mode switch;
- baseline legacy stock dry-run/preview;
- store-aware stock dry-run/preview;
- inbound order reverse mapping;
- outbound Tabletki status code restoration;
- Business SalesDrive payload fields;
- branch-sync consistency;
- UI build sanity;
- scheduler/report sanity by code path and dry-run reports;
- catalog only in non-live preview/compare mode.

Not checked:

- live catalog publish;
- live stock send;
- real Tabletki API calls;
- real SalesDrive API calls;
- real webhook delivery.

## 2. Enterprise 223 audit

### 2.1 Enterprise-level settings

Read-only DB snapshot:

| Field | Value |
|---|---|
| `enterprise_code` | `223` |
| `enterprise_name` | `test--` |
| `branch_id` | `30421` |
| `catalog_enabled` | `false` |
| `stock_enabled` | `false` |
| `order_fetcher` | `false` |
| `business_stock_mode` | `baseline_legacy` |

Current stores:

| Store | Branch | Scope | Active | Stock | Orders | SalesDrive ID | Status |
|---|---:|---|---|---|---|---|---|
| `business_223` | `30421` | `Lviv` | true | true | true | `1` | `draft` |
| `business_223_30422` | `30422` | empty | true | false | false | empty | `draft` |
| `business_223_30423` | `30423` | empty | true | false | false | empty | `draft` |
| `business_223_30491` | `30491` | empty | true | false | false | empty | `draft` |

### 2.2 Stock mode

Command:

```bash
.venv/bin/python -m app.scripts.business_stock_mode_status --enterprise-code 223 --output-json
```

Result:

- `stock_mode=baseline_legacy`;
- `stock_mode_source=enterprise_settings`;
- `is_baseline_mode=true`;
- runtime switch enabled.

Important finding:

- Report source is now aligned with storage: `enterprise_settings.business_stock_mode`.

### 2.3 Stock result

Command:

```bash
.venv/bin/python -m app.scripts.business_stock_publish \
  --enterprise-code 223 \
  --dry-run \
  --limit 20 \
  --output-json
```

Result:

| Field | Value |
|---|---|
| `status` | `ok` |
| `stock_mode` | `baseline_legacy` |
| `stock_mode_source` | `enterprise_settings` |
| `runtime_path` | `legacy_dropship_pipeline_preview` |
| `mapping_branch_rows` | `4` |
| `output_branches_count` | `4` |
| `branches_in_payload` | `30421`, `30422`, `30423`, `30491` |
| `rows_total` | `17069` |
| `missing_mapping_rows_count` | `0` |
| `sent_products` | `0` |
| `external_api_calls` | `false` |
| `warnings` | none |
| `errors` | none |

Confirmed:

- baseline dry-run uses the old `dropship_pipeline.build_stock_payload` path;
- branch/city routing comes from `mapping_branch`;
- stock rows are independent of `BusinessStore.legacy_scope_key`, `BusinessStore.stock_enabled`, and store markup;
- no live send was performed.

### 2.4 Store overlay irrelevance for stock

Confirmed by dry-run output:

- `depends_on_business_stores=false`;
- baseline stock preview produced rows for all mapping branches even though only one 223 store has `legacy_scope_key`;
- empty store scopes for `30422`, `30423`, `30491` did not block baseline stock.

Risk:

- Store rows for 223 still exist and some are active. This is acceptable for branch overlay/admin visibility, but operators need clear UI messaging that these stock fields do not affect baseline stock.

### 2.5 Orders

Command:

```bash
BUSINESS_STORE_ORDER_MAPPING_ENABLED=true \
BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=true \
.venv/bin/python -m app.scripts.business_salesdrive_payload_test \
  --store-code business_223 \
  --external-code 1000331 \
  --output-json
```

Result:

- `status=ok`;
- `store_code=business_223`;
- `enterprise_code=223`;
- `branch=30421`;
- `organizationId=1`;
- `organizationId_source=store_salesdrive_enterprise_id`;
- `payment_method=Післяплата`;
- `shipping_method=Nova Poshta`;
- `simulator_status=mapping_error`.

Interpretation:

- SalesDrive payload builder itself is correct for `223`: it uses store SalesDrive ID and fixed payment method.
- Reverse mapping simulator is not ready for `223` with `external-code=1000331`; this is acceptable if `223` is not intended to use the new store-aware order contour.
- If `223` must receive orders through the new store-aware mapper later, product code mappings and order mode need a separate rollout.

## 3. Enterprise 364 audit

### 3.1 Enterprise-level settings

Read-only DB snapshot:

| Field | Value |
|---|---|
| `enterprise_code` | `364` |
| `enterprise_name` | `Петренко New` |
| `branch_id` | `30630` |
| `catalog_enabled` | `true` |
| `stock_enabled` | `true` |
| `order_fetcher` | `true` |
| `business_stock_mode` | `store_aware` |

Current store:

| Store | Branch | Scope | Active | Stock | Orders | SalesDrive ID | Status |
|---|---:|---|---|---|---|---|---|
| `business_364` | `30630` | `Kyiv` | true | true | true | empty | `catalog_stock_live` |

### 3.2 Stock mode

Command:

```bash
.venv/bin/python -m app.scripts.business_stock_mode_status --enterprise-code 364 --output-json
```

Result:

- `stock_mode=store_aware`;
- `stock_mode_source=enterprise_settings`;
- `is_baseline_mode=false`.

Finding:

- Report source is now aligned with storage: `enterprise_settings.business_stock_mode`.

### 3.3 Store-aware stock

Command:

```bash
BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED=true \
.venv/bin/python -m app.scripts.business_stock_publish \
  --enterprise-code 364 \
  --dry-run \
  --limit 20 \
  --output-json
```

Result:

| Field | Value |
|---|---|
| `status` | `ok` |
| `stock_mode` | `store_aware` |
| `stock_mode_source` | `enterprise_settings` |
| `runtime_path` | `business_store_stock_publish` |
| `code_mapping_mode` | `enterprise_level` |
| `identity_mode` | `enterprise_level` |
| `total_stores_found` | `1` |
| `eligible_stores` | `1` |
| `target_branch` | `30630` |
| `target_branch_source` | `business_store` |
| `candidate_products` | `4543` |
| `exportable_products` | `20` with limit |
| `skipped_products` | `39` |
| `sent_products` | `0` |
| `errors` | none |

Warnings:

- store-aware stock publish relies on current offers state and does not refresh offers;
- local preview approximates best-offer selection and does not import full `dropship_pipeline`;
- dry-run made no external API calls;
- enterprise-level stock code mapping is enabled.

Interpretation:

- Store-aware stock contour for 364 is ready for controlled manual testing.
- Offers freshness remains an operational constraint.

### 3.4 Orders

Inbound reverse mapping command:

```bash
BUSINESS_STORE_ORDER_MAPPING_ENABLED=true \
BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=true \
.venv/bin/python -m app.scripts.business_store_order_mapping_test \
  --store-code business_364 \
  --external-code 464087B31A \
  --output-json
```

Result:

- `status=ok`;
- `code_mapping_mode=enterprise_level`;
- `store_code=business_364`;
- `enterprise_code=364`;
- `branch=30630`;
- `input_external_code=464087B31A`;
- `mapped_internal_code=1040084`;
- `originalGoodsCodeExternal=464087B31A`;
- `mapped_rows=1`;
- no warnings/errors.

Outbound status mapping command:

```bash
BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED=true \
BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=true \
.venv/bin/python -m app.scripts.business_store_outbound_status_mapping_test \
  --store-code business_364 \
  --internal-code 1040084 \
  --output-json
```

Result:

- `status=ok`;
- `code_mapping_mode=enterprise_level`;
- `store_found=true`;
- `store_code=business_364`;
- `enterprise_code=364`;
- `branch=30630`;
- `mapped_products=1`;
- `parameter: 1040084 -> 464087B31A`;
- `sku: 1040084 -> 464087B31A`;
- no errors.

SalesDrive payload command:

```bash
BUSINESS_STORE_ORDER_MAPPING_ENABLED=true \
BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=true \
.venv/bin/python -m app.scripts.business_salesdrive_payload_test \
  --store-code business_364 \
  --external-code 464087B31A \
  --output-json
```

Result:

- `status=ok`;
- `store_code=business_364`;
- `enterprise_code=364`;
- `branch=30630`;
- `organizationId=1`;
- `organizationId_source=fallback_default`;
- `payment_method=Післяплата`;
- `shipping_method=Nova Poshta`;
- warning: `missing_salesdrive_enterprise_id`.

Important finding:

- `business_364.salesdrive_enterprise_id` is empty, so order payload uses fallback `organizationId=1`.
- This is the main remaining data/config gap for 364 orders before production.

### 3.5 End-to-end order contour readiness

Confirmed without external API calls:

- branch -> store resolution works for `30630`;
- inbound external -> internal mapping works through enterprise-level codes;
- outbound internal -> external mapping works through enterprise-level codes;
- SalesDrive payload builder adds `payment_method=Післяплата`.

Partial confidence:

- No real Tabletki order fetch was called.
- No real SalesDrive send was called.
- No real outbound webhook event was sent to Tabletki.
- `salesdrive_enterprise_id` missing for `business_364` must be fixed before live order payload testing.

## 4. Branch sync / data consistency

Commands:

```bash
.venv/bin/python -m app.scripts.business_store_branch_sync --enterprise-code 223 --dry-run --output-json
.venv/bin/python -m app.scripts.business_store_branch_sync --enterprise-code 364 --dry-run --output-json
.venv/bin/python -m app.scripts.business_store_branch_sync --dry-run --output-json
```

Enterprise `223`:

- `status=ok`;
- `mapping_branch_rows=4`;
- `stores_found=4`;
- `duplicates=0`;
- `missing_stores_to_create=0`;
- `orphan_stores_to_deactivate=0`;
- no warnings/errors.

Enterprise `364`:

- `status=ok`;
- `mapping_branch_rows=1`;
- `stores_found=1`;
- `duplicates=0`;
- `missing_stores_to_create=0`;
- `orphan_stores_to_deactivate=0`;
- no warnings/errors.

All enterprise scan:

- `status=ok`;
- `enterprises_scanned=2`;
- `mapping_branch_rows=12`;
- `stores_found=5`;
- `duplicates=0`;
- `missing_stores_to_create=0`;
- `orphan_stores_to_deactivate=0`;
- warnings for `mapping_branch` rows whose `enterprise_code` does not exist in `EnterpriseSettings`: `238`, `256`, `326`, `360`.

Interpretation:

- 223 and 364 are aligned with `mapping_branch`.
- Global `mapping_branch` data still contains rows for missing enterprises; this is not a blocker for 223/364 but should be cleaned or documented.

## 5. UI sanity

Command:

```bash
cd admin-panel && npm run build
```

Result:

- build passed;
- warning: Browserslist/caniuse-lite data is old;
- existing eslint warning: `src/api/developerApi.js` line 163 anonymous default export.

BusinessStoresPage logical audit:

- enterprise block contains the real editable `Режим остатков предприятия` selector;
- values map to `baseline_legacy` and `store_aware`;
- baseline mode shows a warning in the store block that store stock settings do not affect stock publish.

BusinessSettingsPage logical audit:

- The old read-only stock mode control was removed from BusinessSettingsPage after this audit.
- The real editable selector remains only in BusinessStoresPage.

## 6. Catalog non-live sanity only

Command:

```bash
BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=true \
.venv/bin/python -m app.scripts.business_enterprise_catalog_preview_compare \
  --store-code business_364 \
  --limit 20 \
  --output-json
```

Result:

- `status=ok`;
- `assortment_mode=store_compatible`;
- old candidate products: `4543`;
- new candidate products: `4543`;
- old exportable products: `4504`;
- new exportable products: `4504`;
- `missing_in_new=0`;
- `missing_in_old=0`;
- `different_codes=0`;
- `different_names=0`;
- `different_exportable_flags=0`;
- `different_reasons=0`;
- `branch_same=true`.

Catalog live publish:

- not tested by design;
- manual catalog verification remains pending.

## 7. Risks / open issues

1. `business_364.salesdrive_enterprise_id` is empty.

- SalesDrive payload uses fallback `organizationId=1`.
- This is acceptable as compatibility, but not ideal for production order routing.
- Recommended fix: fill `salesdrive_enterprise_id` for `business_364` before live order testing.

3. `223` has `catalog_enabled=false`, `stock_enabled=false`, `order_fetcher=false`.

- Baseline stock dry-run still works because it uses the dedicated baseline preview path.
- Scheduler/live behavior depends on scheduler gates and should be manually reviewed before enabling live baseline stock for 223.

4. Store-aware stock offers freshness is still an operational constraint.

- Dry-run warns that store-aware publish relies on current offers state and does not refresh offers.
- This should be reflected in runbooks.

5. Global mapping_branch has stale rows for missing enterprises.

- Not a blocker for 223/364.
- Should be cleaned or kept documented.

6. Live external integrations remain unverified in this audit.

- No real Tabletki/SalesDrive calls were made.
- Final production readiness still requires controlled manual live tests.

## 8. Final verdict

| Area | Status | Reason |
|---|---|---|
| `223` baseline stock contour | ready for controlled manual testing | Dry-run legacy stock path is clean, branch routing covers 4 branches, no missing mapping rows, no dependence on store overlays. Live scheduler gates still need manual review. |
| `364` business multistore contour | partially ready | Stock and mapping simulators are clean. Main gap is missing `salesdrive_enterprise_id`, causing SalesDrive `organizationId` fallback. |
| UI/control-plane readiness | ready for controlled manual testing | BusinessStoresPage has the real selector, BusinessSettingsPage duplicate indicator is removed, and build passes. |

## 9. Exact next steps

Manual steps:

- Verify live catalog manually as planned; do not automate it from Codex.
- Fill `business_364.salesdrive_enterprise_id` with the correct SalesDrive organization ID.
- Decide whether `223` live scheduler should be enabled despite `EnterpriseSettings.stock_enabled=false`, or update the relevant control-plane gates before live baseline stock.

Codex-suitable next tasks:

- Add a safe scheduler dry-run/status command that reports what scheduler would do without calling live send.
- Add a small config/data audit for missing SalesDrive IDs across active BusinessStores.

Do not touch for now:

- enterprise catalog identity logic, until manual catalog live verification is completed;
- inbound/outbound mapping code for 364, which passed simulator checks;
- baseline stock algorithm/pricing/jitter, which should remain unchanged.

