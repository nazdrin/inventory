# Business Enterprise Identity E2E Regression

Дата прогона: 2026-04-22  
Контур: `business_364` / `enterprise_code=364` / `store_id=2` / `tabletki_branch=30630`

## Scope

Проверялся backend regression/smoke-test после Stage 1-6 migration к enterprise-level catalog identity:

- Stage 1: enterprise-level code/name tables + backfill
- Stage 2: enterprise catalog preview compare
- Stage 3: catalog preview/export/publish gate
- Stage 4: stock code mapping gate
- Stage 5: inbound order reverse mapping gate
- Stage 6: outbound status mapping gate

Ограничения прогона:

- live send не выполнялся;
- внешние API не вызывались;
- использовались только import/compile и DB-backed dry-run/CLI checks;
- runtime code, schema, env и UI в этом прогоне не менялись.

## Commands Run

### Compile / import

```bash
python3 -m compileall app
.venv/bin/python -c "import app.business.business_enterprise_catalog_preview; print('enterprise catalog preview ok')"
.venv/bin/python -c "import app.business.business_enterprise_catalog_exporter; print('enterprise catalog exporter ok')"
.venv/bin/python -c "import app.business.business_store_stock_preview; print('stock preview ok')"
.venv/bin/python -c "import app.business.business_store_order_mapper; print('inbound order mapper ok')"
.venv/bin/python -c "import app.business.business_store_tabletki_outbound_mapper; print('outbound mapper ok')"
```

### Stage 1 / Stage 2 compares

```bash
.venv/bin/python -m app.scripts.business_enterprise_catalog_identity_compare --store-code business_364 --output-json
.venv/bin/python -m app.scripts.business_enterprise_catalog_preview_compare --store-code business_364 --output-json
```

### Catalog regression

```bash
env BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=false \
  .venv/bin/python -m app.scripts.business_store_catalog_publish \
  --store-code business_364 --dry-run --limit 20 --output-json

env BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_catalog_publish \
  --store-code business_364 --dry-run --limit 20 --output-json

env BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_catalog_publish \
  --store-code business_364 --dry-run --output-json
```

### Stock regression

```bash
env BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED=false \
  .venv/bin/python -m app.scripts.business_store_stock_publish \
  --store-code business_364 --dry-run --limit 20 --output-json

env BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_stock_publish \
  --store-code business_364 --dry-run --limit 20 --output-json

env BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_stock_publish \
  --store-code business_364 --dry-run --output-json
```

### Inbound order mapping regression

```bash
env BUSINESS_STORE_ORDER_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=false \
  .venv/bin/python -m app.scripts.business_store_order_mapping_test \
  --store-code business_364 --external-code 464087B31A --output-json

env BUSINESS_STORE_ORDER_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_order_mapping_test \
  --store-code business_364 --external-code 464087B31A --output-json

env BUSINESS_STORE_ORDER_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_ORDER_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_order_mapping_test \
  --store-code business_364 --external-code NOT_EXISTING --output-json
```

### Outbound status mapping regression

```bash
env BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=false \
  .venv/bin/python -m app.scripts.business_store_outbound_status_mapping_test \
  --store-code business_364 --internal-code 1040084 --output-json

env BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_outbound_status_mapping_test \
  --store-code business_364 --internal-code 1040084 --output-json

env BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_outbound_status_mapping_test \
  --store-code business_364 --internal-code NOT_EXISTING --output-json

env BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED=true BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=true \
  .venv/bin/python -m app.scripts.business_store_outbound_status_mapping_test \
  --payload-json-file /tmp/salesdrive_status4_payload.json --output-json
```

## Pass / Fail Table

| Check | Result | Key facts |
| --- | --- | --- |
| `compileall` | PASS | `python3 -m compileall app` completed without errors |
| Import checks | PASS | preview/exporter/stock/inbound/outbound modules import cleanly |
| Enterprise identity compare | PASS | `codes 4561/4561 matched`, `names 4561/4561 matched`, no missing/different/extra |
| Enterprise preview compare | PASS | `assortment_mode=store_compatible`, old/new `candidate_products=4543`, `exportable_products=4504`, `branch_same=true`, `missing_in_new=0`, `missing_in_old=0`, `different_codes=0`, `different_names=0` |
| Catalog rollback dry-run | PASS | `identity_mode=store_level`, `target_branch_source=business_store`, `target_branch=30630`, `exportable_products=20`, `sent_products=0` |
| Catalog enterprise dry-run limit 20 | PASS | `identity_mode=enterprise_level`, `assortment_mode=store_compatible`, `target_branch_source=enterprise_settings`, `target_branch=30630`, `exportable_products=20`, `sent_products=0` |
| Catalog enterprise full dry-run | PASS | `candidate_products=4543`, `exportable_products=4504`, `skipped_products=39`, `sent_products=0`, no accidental expansion to `77350` |
| Stock rollback dry-run | PASS | `code_mapping_mode=store_level`, `target_branch=30630`, `exportable_products=20`, `sent_products=0` |
| Stock enterprise dry-run limit 20 | PASS | `code_mapping_mode=enterprise_level`, `target_branch=30630`, `exportable_products=20`, `sent_products=0` |
| Stock enterprise full dry-run | PASS | `candidate_products=4543`, `exportable_products=4504`, `skipped_products=39`, `sent_products=0` |
| Inbound rollback mapping | PASS | `code_mapping_mode=store_level`, `464087B31A -> 1040084`, `originalGoodsCodeExternal` preserved |
| Inbound enterprise mapping | PASS | `code_mapping_mode=enterprise_level`, `464087B31A -> 1040084`, normalized payload equivalent |
| Inbound enterprise missing code | PASS | `status=mapping_error`, reason `missing_enterprise_external_code_mapping`, no crash |
| Outbound rollback mapping | PASS | `code_mapping_mode=store_level`, `1040084 -> 464087B31A` in both `parameter` and `sku` |
| Outbound enterprise mapping | PASS | `code_mapping_mode=enterprise_level`, `1040084 -> 464087B31A` in both `parameter` and `sku` |
| Outbound enterprise missing code | PASS | `status=mapping_error`, reason `missing_enterprise_internal_code_mapping`, no crash |
| Outbound payload-file transform | PASS | `statusId`, `branch`, `externalId`, `tabletkiOrder` unchanged; `parameter` and `sku` transformed to `464087B31A` |

## Key Results

### 1. Enterprise identity compare

```json
{
  "status": "ok",
  "store_id": 2,
  "store_code": "business_364",
  "enterprise_code": "364",
  "code_counts": {
    "store_total": 4561,
    "enterprise_total": 4561,
    "matched": 4561,
    "missing_in_enterprise": 0,
    "different_values": 0,
    "extra_enterprise": 0
  },
  "name_counts": {
    "store_total": 4561,
    "enterprise_total": 4561,
    "matched": 4561,
    "missing_in_enterprise": 0,
    "different_values": 0,
    "extra_enterprise": 0
  }
}
```

### 2. Catalog preview compare

```json
{
  "status": "ok",
  "assortment_mode": "store_compatible",
  "old_preview": {
    "candidate_products": 4543,
    "exportable_products": 4504,
    "tabletki_branch": "30630"
  },
  "new_preview": {
    "candidate_products": 4543,
    "exportable_products": 4504,
    "tabletki_branch": "30630"
  },
  "comparison": {
    "missing_in_new": 0,
    "missing_in_old": 0,
    "different_codes": 0,
    "different_names": 0,
    "different_exportable_flags": 0,
    "different_reasons": 0,
    "reason_mismatch_normalized": 0,
    "raw_reason_differences": 39,
    "branch_same": true
  }
}
```

### 3. Catalog dry-run summary

- Rollback mode:
  - `identity_mode=store_level`
  - `target_branch_source=business_store`
  - `target_branch=30630`
  - `catalog_scope_source=selected_store_legacy`
  - `candidate_products=4543`
  - `exportable_products=20`
  - `sent_products=0`
- Enterprise mode full dry-run:
  - `identity_mode=enterprise_level`
  - `assortment_mode=store_compatible`
  - `target_branch_source=enterprise_settings`
  - `target_branch=30630`
  - `catalog_scope_store_code=business_364`
  - `catalog_scope_branch=30630`
  - `catalog_scope_key=Kyiv`
  - `catalog_scope_source=enterprise_branch_match`
  - `catalog_only_in_stock_source=catalog_scope_store`
  - `candidate_products=4543`
  - `exportable_products=4504`
  - `skipped_products=39`
  - `sent_products=0`

### 4. Stock dry-run summary

- Rollback mode:
  - `code_mapping_mode=store_level`
  - `target_branch=30630`
  - `candidate_products=4543`
  - `exportable_products=20`
  - `sent_products=0`
- Enterprise mode full dry-run:
  - `code_mapping_mode=enterprise_level`
  - `target_branch=30630`
  - `candidate_products=4543`
  - `exportable_products=4504`
  - `skipped_products=39`
  - `sent_products=0`

### 5. Inbound / outbound mapping summaries

- Inbound:
  - rollback and enterprise modes both map `464087B31A -> 1040084`
  - missing case stays `mapping_error` and does not crash
- Outbound:
  - rollback and enterprise modes both map `1040084 -> 464087B31A`
  - both `products[].parameter` and `products[].sku` are transformed
  - missing case blocks only that mapping and does not crash

## Deviations

### Non-blocking deviation: raw reason labels differ in preview compare

`business_enterprise_catalog_preview_compare` showed:

- `different_reasons = 0`
- `reason_mismatch_normalized = 0`
- `raw_reason_differences = 39`

Это ожидаемо и не считается functional drift:

- old preview uses `missing_code_mapping` / `missing_name_mapping`
- new preview uses `missing_enterprise_code_mapping` / `missing_enterprise_name_mapping`

То есть отличается только naming family, а не exportability или candidate scope.

### Expected stock preview warning

Во всех stock dry-run виден ожидаемый warning:

- `Store-aware stock publish relies on current offers state and does not refresh offers.`

Это текущая архитектурная особенность, не regression.

### Not tested: temporary DB mutation for `store.catalog_enabled=false`

Сценарий:

- `BUSINESS_ENTERPRISE_CATALOG_IDENTITY_ENABLED=true`
- `EnterpriseSettings.catalog_enabled=true`
- `BusinessStore.catalog_enabled=false`

не прогонялся, потому что в этом regression run было принято правило не мутировать БД даже временно.

Вместо этого проверена новая report semantics:

- enterprise mode now returns `catalog_gate_source=enterprise_settings`
- rollback mode returns `catalog_gate_source=store_and_enterprise_legacy`

Этого достаточно для smoke-level подтверждения, но не заменяет отдельный controlled DB-backed experiment, если понадобится доказательство на уровне runtime data mutation.

## External API Calls

В этом прогоне внешние API не вызывались.

- Все catalog/stock проверки были `--dry-run`
- Inbound/outbound order checks выполнялись локальными simulator/mapper CLI
- Webhook route не дергался
- Live publish / live status send / live order fetch не запускались

## Conclusion

### Ready / not ready for UI cleanup

`Conditionally ready`

Backend после Stage 1-6 выглядит стабильным для следующего UI refactor-а, но есть один важный compatibility issue:

- `business_stores.catalog_enabled` всё ещё участвует в текущем catalog eligibility и не может быть просто удалён из operator UI без отдельного backend cleanup или безопасного hidden/deprecated handling.

То есть:

- можно переходить к UI cleanup planning и label/layout refactor;
- нельзя сразу делать полный operator-visible removal `store.catalog_enabled` без отдельного backend-aware шага.

### Ready / not ready for limited live test

`Ready for limited live test`

Основания:

- identity compare clean;
- preview compare clean;
- catalog rollback/enterprise dry-run clean;
- stock rollback/enterprise dry-run clean;
- inbound/outbound rollback/enterprise mapping clean;
- all checks gated by rollback flags;
- no evidence of candidate-scope drift or code-translation drift for `business_364`.

Рекомендуемый live rollout order:

1. limited live stock/code-mapping check
2. limited live inbound order mapping check
3. limited live outbound status mapping check

с включением только нужных feature flags и с контролем логов по `code_mapping_mode`.
