# Business Store Outbound Status Mapping Audit

## 1. Scope

This audit covers SalesDrive-originated webhook/status payloads that later send status updates back to Tabletki for Business multistore orders.

Goal:

- identify which webhook endpoints exist;
- identify which payload fields are actually used;
- determine whether current webhook payload can safely resolve `BusinessStore`;
- define a future outbound mapper for:
  - internal product code -> external store product code;
  - optional internal/base product name -> store-facing product name.

Stage update:

- initial isolated outbound mapper now exists in `app/business/business_store_tabletki_outbound_mapper.py`
- CLI simulation now exists in `app/scripts/business_store_outbound_status_mapping_test.py`
- runtime wiring now exists only in `app/business/salesdrive_webhook.py`
- `/webhooks/salesdrive-simple/{branch}` remains unchanged
- runtime wiring is gated by `BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED=false`
- outbound code lookup now also has a second-stage flag:
  - `BUSINESS_ENTERPRISE_OUTBOUND_STATUS_CODE_MAPPING_ENABLED=false`
- default rollback path remains store-level:
  - `BusinessStoreProductCode.store_id`
- optional enterprise-level mode uses:
  - `BusinessEnterpriseProductCode.enterprise_code`

## 2. Files Reviewed

- `app/routes.py`
- `app/services/order_sender.py`
- `app/business/order_sender.py`
- `app/services/order_fetcher.py`
- `app/services/auto_confirm.py`
- `app/business/business_store_order_mapper.py`
- `app/business/salesdrive_webhook.py`
- `app/salesdrive_simple/webhook.py`
- `app/models.py`
- `docs/business_store_order_reverse_mapping_audit.md`
- `docs/business_store_order_autoconfirm_strategy.md`
- `docs/business_multistore_architecture.md`

## 3. Webhook Endpoints Found

### 3.1 `/webhooks/salesdrive`

Location:

- `app/routes.py`
- handler: `salesdrive_webhook(...)`
- background task: `app.business.salesdrive_webhook.process_salesdrive_webhook(payload)`

Expected payload:

- raw SalesDrive webhook JSON with top-level `info` and `data`

Current fields actually used by processor:

- `data.statusId`
- `data.externalId`
- `data.id`
- `data.branch`
- fallback `data.utmSource`
- `data.products`
- `data.tabletkiOrder` / `data.TabletkiOrder`
- `data.ord_delivery_data`
- `data.contacts`
- `data.rejectionReason`

What it does:

- resolves `enterprise_code` by `MappingBranch.branch == data.branch`
- converts `products[].parameter` into Tabletki `rows[].goodsCode`
- sends confirm/cancel statuses back to Tabletki through `send_orders_to_tabletki(...)`
- optionally sends TTN through `send_ttn(...)`

### 3.2 `/webhooks/salesdrive-simple/{branch}`

Location:

- `app/routes.py`
- handler: `salesdrive_simple_webhook(...)`
- background task: `app.salesdrive_simple.webhook.process_salesdrive_simple_webhook(payload, branch)`

Expected payload:

- raw SalesDriveSimple webhook JSON
- branch is also passed in URL path

Current fields actually used by processor:

- URL `branch`
- `data.statusId`
- `data.externalId`
- `data.id`
- `data.branch`
- `data.utmSource`
- `data.products`
- `data.ord_delivery_data`
- `data.contacts`

What it does:

- resolves `enterprise_code` by URL branch through `MappingBranch.branch`
- warns if payload `branch` differs from URL branch
- converts `products[].parameter` into Tabletki `rows[].goodsCode`
- sends confirm/cancel statuses back to Tabletki through `send_orders_to_tabletki(...)`
- optionally sends TTN through `send_ttn(...)`

## 4. Current SalesDrive Order Payload Identity

When Business order runtime creates the SalesDrive payload in `app/business/order_sender.py`:

- `externalId = order["id"]`
- `tabletkiOrder = order["code"]`
- `branch = branch`
- `sajt = branch`
- `city = BRANCH_CITY_MAP[branch]` with optional supplier city decoration

Important:

- this `branch` is the fetch-loop branch passed from `order_fetcher.py`;
- `order_fetcher.py` gets that branch from `MappingBranch.branch`;
- this is not sourced from `BusinessStore.tabletki_branch`.

Business multistore decision for current rollout:

- `mapping_branch.branch` is expected to stay synchronized with `BusinessStore.tabletki_branch`
- for the current Business contour, outbound mapper may resolve the store by Tabletki branch
- current Stage 1 uses webhook `branch` -> `BusinessStore.tabletki_branch`

Important limitation:

- this is a rollout assumption for the current contour
- if branch alignment ever stops being guaranteed, runtime integration should switch to persisted order link

## 5. Meaning Of Key Webhook Fields

### 5.1 `branch`

Current meaning in code:

- copied from `MappingBranch.branch` at the moment the SalesDrive payload is created

Current webhook usage:

- used to resolve `enterprise_code`
- used as `branchID` in outbound Tabletki payload object

Conclusion for current Stage 1:

- branch-based resolution is allowed for isolated outbound simulation
- resolver should use `BusinessStore.tabletki_branch`
- optional `enterprise_code` filter remains useful to avoid future ambiguity

Long-term note:

- persisted order link is still the more robust architecture if branch semantics diverge later

### 5.2 `city`

Current meaning in code:

- presentation field derived from `BRANCH_CITY_MAP`
- may be decorated with supplier city tag such as `Kyiv (Київ)` or similar variants

Conclusion:

- not a stable identity field
- may loosely correlate with `legacy_scope_key`
- should not be used as primary `BusinessStore` resolver

### 5.3 `externalId`

Current meaning in code:

- copied from incoming Tabletki order `order["id"]`
- this is the strongest stable order-level identity currently present in both directions

Conclusion:

- best long-term candidate for persisted order-to-store link
- not required for current isolated Stage 1 simulation because branch alignment is now considered valid in Business contour

### 5.4 `tabletkiOrder`

Current meaning in code:

- copied from incoming Tabletki order `order["code"]`
- user-facing numeric order code

Conclusion:

- useful secondary order identity
- should be stored together with `externalId`
- not strong enough alone as the only future key

### 5.5 `products[].parameter`

Current meaning in code:

- current webhook processors use `parameter` as source of Tabletki `goodsCode`
- `productId` is not used for Tabletki code resolution

Conclusion:

- `parameter` is the primary product-code field that must be transformed for outbound Tabletki statuses
- `sku` should likely be transformed together for consistency if the outbound payload builder ever uses it

### 5.6 `products[].sku`

Current webhook processors today:

- do not use `sku`

But for future store-aware outbound mapper:

- `sku` should be treated as a sibling code field
- if outbound payload contains both `parameter` and `sku`, both should be converted from internal code to store external code

## 6. How Outbound Tabletki Statuses Are Built Today

Current outbound sender is `app/services/order_sender.py`.

### 6.1 Confirm / status 4 or 6

`send_orders_to_tabletki(...)`:

- sends to `POST /api/orders`
- keeps rows with `qtyShip > 0`
- sends payload as-is

Important:

- no code remapping happens inside sender
- whatever is in `rows[].goodsCode` is sent to Tabletki unchanged

### 6.2 Cancel / status 7

`send_orders_to_tabletki(...)` for cancels:

- sends to `POST /api/Orders/cancelledOrders`
- `_build_cancel_payload(...)` uses:
  - `id`
  - `rows[].goodsCode`
  - `rows[].qty` or `qtyShip`

Important:

- cancel path also sends `goodsCode` unchanged

### 6.3 Status 2

`send_single_order_status_2(...)`:

- sends to `POST /api/orders`
- filters rows by positive `qty` / `qtyShip`
- sends payload as-is

Important:

- the same raw-code rule applies here too

## 7. Why Current Webhook Path Is Not Store-Aware

Current SalesDrive webhook processors resolve only:

- `enterprise_code` via `MappingBranch.branch`

They do not resolve:

- `BusinessStore`
- `store_id`
- `store_code`
- `tabletki_branch`

They also do not persist any order-level link such as:

- `externalId -> store_id`
- `tabletkiOrder -> store_id`

So current outbound status handlers have no reliable store scope for:

- internal -> external product code mapping
- optional store-specific name restoration

## 8. Current Stage 1 Store Resolution Strategy

Current Stage 1 resolver:

- `webhook branch` or `salesdrive-simple URL branch`
- interpreted as `BusinessStore.tabletki_branch`
- optional enterprise narrowing by `BusinessStore.enterprise_code`

Isolated implementation:

- `resolve_business_store_by_tabletki_branch(...)`
- `restore_salesdrive_products_for_tabletki_outbound(...)`

Current mapper behavior:

- if no store is resolved:
  - `legacy_passthrough`
- if `code_strategy = legacy_same` or `is_legacy_default = true`:
  - `legacy_passthrough`
- otherwise:
  - map `products[].parameter`
  - map `products[].sku`
  - if any product mapping is missing:
    - return `mapping_error`
    - automatic outbound send should be blocked in future runtime integration

## 9. Best Long-Term Store Resolution Strategy

### Ranked options

1. Persisted order link by `externalId` plus `tabletkiOrder`
2. Direct Tabletki branch if webhook field is explicitly guaranteed to carry `BusinessStore.tabletki_branch`
3. City only as weak fallback / diagnostics
4. Supplier-based fields should not be used
5. Organization / stock ids should not be used without explicit separate mapping

### A. Webhook `branch`

Rating:

- not safe as primary store resolver

Reason:

- current code sets it from `MappingBranch.branch`
- example values already differ from `BusinessStore.tabletki_branch`

### B. Webhook `city`

Rating:

- weak diagnostic-only field

Reason:

- presentation text
- formatting may vary

### C. `externalId` / `tabletkiOrder` plus persisted link

Rating:

- safest long-term strategy

Reason:

- both values originate from original Tabletki order
- they survive SalesDrive roundtrip
- they are order-scoped, not presentation-scoped

### D. `supplierlist` / `supplier`

Rating:

- unsafe

Reason:

- supplier identity does not identify store
- supplier can change independently of store

### E. `organizationId`

Rating:

- unsafe today

Reason:

- current Business sender hardcodes `"organizationId": "1"`
- no BusinessStore mapping exists

### F. `products[].stockId`

Rating:

- unsafe

Reason:

- current SalesDrive payload uses root `stockId` only for D14 override
- not a BusinessStore identity

## 10. Recommended Future Persistence

Before live runtime integration for outbound store-aware statuses, add a persisted order-store link.

Recommended future table:

- `business_store_order_links`
  - `id`
  - `store_id`
  - `tabletki_order_id` or `external_id`
  - `tabletki_order_code`
  - `tabletki_branch`
  - `enterprise_code`
  - `created_at`
  - `updated_at`
  - optional raw/debug metadata

Recommended creation moment:

- at order fetch time
- right after successful store-aware normalization
- while `externalId`, `tabletkiOrder`, `tabletki_branch`, `store_id` are all known together

Current status:

- persisted link is postponed
- current isolated mapper relies on branch alignment instead

If that alignment ever becomes non-guaranteed:

- outbound webhook should stop resolving store by branch alone
- runtime integration should require persisted order-store link

## 11. Implemented Stage 1 Mapper

Implemented module:

- `app/business/business_store_tabletki_outbound_mapper.py`

Implemented functions:

- `resolve_business_store_by_tabletki_branch(...)`
- `map_internal_code_to_store_external(...)`
- `restore_salesdrive_products_for_tabletki_outbound(...)`

Implemented CLI:

- `app/scripts/business_store_outbound_status_mapping_test.py`

Examples:

- `python -m app.scripts.business_store_outbound_status_mapping_test --branch 30630 --internal-code 1040321 --output-json`
- `python -m app.scripts.business_store_outbound_status_mapping_test --store-code business_364 --internal-code 1040321 --output-json`

Current Stage 1 limits:

- no webhook runtime integration yet
- no external API calls
- no sender integration yet
- names are not transformed by default
- missing mapping returns `mapping_error`

## 12. Proposed Outbound Mapper

Recommended future module:

- `app/business/business_store_tabletki_outbound_mapper.py`

Candidate functions:

- `resolve_business_store_for_outbound_status(payload, enterprise_code=None)`
- `restore_tabletki_product_codes_for_outbound_status(session, payload, store)`
- `map_internal_code_to_external_for_store(session, store_id, internal_code)`
- `maybe_restore_store_product_names(session, payload, store)`

Rules:

- if store cannot be resolved:
  - legacy passthrough
- if store is resolved and `code_strategy = legacy_same`:
  - passthrough
- if store is resolved and code remap is required:
  - for each product:
    - take internal code from `parameter`, else `sku`
    - lookup `BusinessStoreProductCode` by `store_id + internal_product_code`
    - replace Tabletki-facing code fields with `external_product_code`
- if required mapping is missing:
  - do not auto-send outbound status
  - return `mapping_error`
  - log clear warning

Name restoration:

- optional and secondary to code restoration
- use `BusinessStoreProductName` only if outbound Tabletki payload actually requires name consistency
- if no store-specific name mapping exists, keeping base name is safer than guessing

## 13. Product Fields That Need Transformation

Primary required code fields:

- `products[].parameter`
- `products[].sku` if present in outbound payload generation path
- `rows[].goodsCode` for direct Tabletki sender payloads

Optional name fields:

- `products[].text`
- `products[].documentName`
- `rows[].goodsName`

Primary rule:

- do not transform SalesDrive-facing payload
- transform only the Tabletki-facing outbound payload copy

## 14. Phased Plan

### Stage 1

- this audit

### Stage 2

- webhook runtime integration into one outbound status path
- before calling existing Tabletki sender helper
- with auto-block on `mapping_error`

Implemented scope for current Stage 2:

- only `/webhooks/salesdrive`
- only `app/business/salesdrive_webhook.py`
- before `send_orders_to_tabletki(...)`
- TTN path remains untouched
- `salesdrive-simple` path remains untouched
- route and processor now both tolerate:
  - `{"data": {...}}`
  - `{"data": [{...}, {...}]}`
- per-event errors do not stop processing of sibling events in the same webhook payload
- successful mapper logs now include first-product `parameter` / `sku` before and after transformation

### Stage 3

- optionally persist order-to-store link during order fetch normalization

### Stage 4

- add name restoration only if Tabletki-facing product names must match store catalog

### Stage 5

- apply to all outbound Tabletki statuses that carry products

## 15. Runtime-Safe Conclusions

- current isolated Stage 1 mapper resolves store by `BusinessStore.tabletki_branch`
- this is acceptable only because current Business contour explicitly aligns `mapping_branch.branch` with `BusinessStore.tabletki_branch`
- current `city` should still not be treated as store identity
- `externalId` + `tabletkiOrder` remain the best future persisted order-store keys
- current outbound Tabletki senders pass `goodsCode` through unchanged
- therefore outbound store-aware code transformation must happen before calling existing Tabletki sender helpers
- runtime webhook integration is now implemented only for `/webhooks/salesdrive` behind `BUSINESS_STORE_OUTBOUND_STATUS_MAPPING_ENABLED`
- when mapper returns `mapping_error`, outbound Tabletki status send is skipped for that webhook event without crashing the webhook processor
