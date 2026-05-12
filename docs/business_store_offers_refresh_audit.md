# Business Store Offers Refresh Audit

## Scope

Этот документ фиксирует аудит `app/business/dropship_pipeline.py` с одной конкретной целью:

- понять, можно ли безопасно выделить refresh-only режим;
- обновлять `offers`;
- не запускать stock export;
- не вызывать `process_database_service("stock", ...)`;
- не писать `InventoryStock`;
- не отправлять stock во внешние системы.

Документ не вносит runtime-изменений.

Current status:

- refresh-only helper is now implemented in `app/business/dropship_pipeline.py`;
- service wrapper is implemented in `app/services/business_offers_refresh_service.py`;
- manual CLI is implemented in `app/scripts/business_offers_refresh.py`;
- refresh-only path updates `offers` only;
- it does not build stock payload and does not send stock.
- store-aware stock scheduler can now optionally call refresh-before-publish through env-gated orchestration.

## Checked Files

- `app/business/dropship_pipeline.py`
- `app/services/business_stock_scheduler_service.py`
- `app/services/business_store_stock_scheduler_service.py`
- `app/services/business_store_stock_publish_service.py`
- `app/business/business_store_stock_preview.py`
- `app/business/business_store_stock_exporter.py`
- `app/services/database_service.py`
- `app/services/stock_export_service.py`
- `app/models.py`
- `ENV_REFERENCE.md`
- `docs/business_store_stock_scheduler_audit.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_multistore_architecture.md`

## 1. Current Dropship Pipeline Flow

Current entry point:

- `app/business/dropship_pipeline.py`
- `run_pipeline(enterprise_code=None, file_type=None)`

High-level flow:

1. load pricing runtime snapshot:
   - `load_business_pricing_settings_snapshot(session)`
2. sanitary cleanup:
   - `fetch_suppliers_to_clear(...)`
   - `clear_offers_for_supplier(...)`
   - `session.commit()`
3. load active suppliers:
   - `fetch_active_enterprises(...)`
4. for each active `DropshipEnterprise`:
   - check schedule block with `is_supplier_blocked(...)`
   - if blocked:
     - delete old supplier offers
     - `session.commit()`
   - else run `process_supplier(...)`
   - then `session.commit()`
5. only after all suppliers are processed:
   - if `enterprise_code` and `file_type == "stock"`:
     - `generate_and_send_stock(session, enterprise_code)`

This means the pipeline is already structurally split into two phases:

- phase A: refresh `offers`
- phase B: build/send stock

## 2. Supplier Processing Path

`process_supplier(...)` does the full offers refresh for one supplier.

Per-supplier flow:

1. remove all old offers for supplier:
   - `clear_offers_for_supplier(session, code)`
2. load raw feed from parser:
   - `_call_parser_kw(parser, session, ent)`
3. map supplier item code to internal product identity:
   - `map_supplier_codes(...)`
   - direct / legacy / master mapping backend
4. apply offer block rules:
   - `fetch_active_offer_blocks(...)`
5. split supplier `city` field:
   - `_split_cities(ent.city or "")`
6. preload competitor prices:
   - `CompetitorPrice`
7. resolve pricing policy context:
   - balancer repository or SQL fallback
8. for each `(city, mapped item)`:
   - compute base band / threshold
   - apply competitor logic
   - apply supplier rounding
   - optionally apply price jitter
   - produce row:
     - `product_code`
     - `supplier_code`
     - `city`
     - `price`
     - `wholesale_price`
     - `stock`
9. write rows to `offers`:
   - `bulk_upsert_offers(...)`

Important point:

- all pricing, balancer, competitor and jitter logic is applied before `offers` are written;
- `offers` already contain the base runtime stock/price state later used by both:
  - legacy stock payload;
  - store-aware stock preview/export.

## 3. Exact Safe Boundary

The last safe boundary for a future refresh-only mode is:

- after the supplier loop in `run_pipeline(...)` finishes and commits all supplier updates;
- before this block starts:

```python
if enterprise_code and file_type:
    ft = file_type.lower()
    if ft == "stock":
        await generate_and_send_stock(session, enterprise_code)
```

Operationally this means:

- `offers` are already refreshed;
- no stock payload has been built yet;
- no JSON stock file has been dumped yet;
- `process_database_service("stock", ...)` has not started yet;
- `InventoryStock` has not been touched yet;
- no stock has been sent to Tabletki yet.

This is the cleanest extraction point for a future refresh-only path.

## 4. Where Stock Export Starts

Stock export starts only in `generate_and_send_stock(...)`.

That path does:

1. `build_stock_payload(session, enterprise_code)`
2. `_dump_payload_to_file(...)`
3. `process_database_service(file_path, "stock", enterprise_code)`

Then `process_database_service` introduces legacy side effects:

- `delete_old_stock`
- optional `update_stock`
- `process_stock_file(...)` outbound send
- `save_stock`
- `flush_stock`
- `last_stock_upload`
- `commit`

So the refresh-only boundary is clearly before `generate_and_send_stock(...)`, not inside `process_database_service`.

## 5. Can Refresh-Only Be Extracted Safely?

Short answer:

- yes, architecturally it can be extracted safely;
- but only as a split of the existing phase A / phase B boundary;
- not by trying to reuse the stock export path with flags.

Why this is safe:

- supplier parsers, mapping, pricing, competitor logic and `bulk_upsert_offers(...)` already run before stock export;
- `run_pipeline(...)` already treats offers refresh as a separate completed phase;
- store-aware stock publish already reads from `offers`, so a refresh-only layer would match the current target architecture.

Why this must stay separate from stock runtime:

- `build_stock_payload(...)` introduces `mapping_branch` routing and legacy internal codes;
- `process_database_service("stock", ...)` mutates `InventoryStock` and upload state;
- store-aware stock publish must not inherit those side effects.

## 6. Recommended Future Extraction Model

Recommended future split:

- pure offers refresh helper/service
- legacy stock sender remains separate
- store-aware stock publish remains separate

Suggested future shape:

- `refresh_business_offers(...)`
- `run_pipeline(..., file_type="stock")` becomes:
  1. refresh offers
  2. optionally build/send legacy stock

Recommended future helper boundaries:

1. `refresh_offers_for_all_active_suppliers(session, enterprise_code=None) -> report`
2. `generate_and_send_stock(session, enterprise_code) -> None`
3. keep store-aware stock publish in:
   - `app/services/business_store_stock_publish_service.py`
   - `app/services/business_store_stock_scheduler_service.py`

This would allow one shared upstream refresh and two downstream fan-out paths:

1. legacy stock path
2. store-aware per-store stock path

## 7. Constraints for a Future Refresh-Only Mode

A future refresh-only implementation must preserve these current semantics:

- supplier-by-supplier cleanup before parser import
- per-supplier commit/rollback isolation
- blocked supplier cleanup behavior
- current mapping backend selection
- current pricing/balancer/jitter logic
- no `process_database_service`
- no `InventoryStock`
- no `mapping_branch` routing
- no outbound Tabletki send

It must also preserve current partial-failure behavior:

- one failed supplier does not roll back all already committed suppliers

## 8. Known Risks

### Partial refresh semantics

Current pipeline commits per supplier.

That means a refresh-only mode would still be partially successful if:

- some suppliers updated successfully;
- one later supplier failed.

This is already current behavior, but it matters for monitoring.

### Shared offers state

`offers` is one shared base table.

A refresh-only mode improves freshness for:

- legacy stock payload building;
- store-aware stock preview/export;
- order downstream reads using `Offer`.

But it also means:

- base offers are global, not store-specific;
- store-aware markup still stays outside `dropship_pipeline`.

### Supplier cleanup side effects

`process_supplier(...)` starts with `clear_offers_for_supplier(...)`.

So a failed parser after cleanup can temporarily leave a supplier with no offers until the next successful run.

That is not new, but a future refresh-only scheduler must document it.

### No freshness guarantee by itself

A refresh-only service updates `offers`, but does not guarantee:

- legacy stock was published;
- store-aware stock was published;
- catalog and stock remain synchronized in the target external systems.

It only refreshes the internal base state.

## 9. Recommendation

Recommended staged direction:

1. do not modify current `business_stock_scheduler_service`
2. do not try to add a flag inside `process_database_service`
3. extract refresh-only at the `run_pipeline(...)` phase boundary
4. keep legacy stock publish and store-aware stock publish as separate downstream steps
5. let store-aware stock scheduler depend on refreshed `offers`, not on legacy stock export side effects

Preferred future rollout:

- first: isolated refresh-only service or helper
- then: manual/CLI refresh run
- later: dedicated scheduler for refresh-only cadence
- only after that: define orchestration between
  - refresh-only
  - legacy stock
  - store-aware stock

## 10. Files for Future Implementation

Most likely future touch points:

- `app/business/dropship_pipeline.py`
- `app/services/business_stock_scheduler_service.py`
- `app/services/business_store_stock_scheduler_service.py`
- optional new service:
  - `app/services/business_offers_refresh_service.py`
- optional CLI:
  - `app/scripts/business_offers_refresh.py`

These changes should be evaluated together with:

- `docs/business_store_stock_scheduler_audit.md`
- `docs/business_store_stock_export_audit.md`
- `docs/business_multistore_architecture.md`

## 11. Current Conclusion

Safe extraction is feasible.

The correct boundary is:

- after offers refresh completes in `run_pipeline(...)`
- before `generate_and_send_stock(...)` begins

That is the point where:

- `offers` are already current;
- stock export side effects have not started yet.

## 12. Implemented Refresh-Only Layer

Implemented public helper:

- `refresh_business_offers(...)`

Implemented shared internal helper:

- `_refresh_business_offers_in_session(...)`

Implemented service wrapper:

- `run_business_offers_refresh_once(...)`

Implemented CLI:

- `python -m app.scripts.business_offers_refresh --enterprise-code 223 --output-json`
- `python -m app.scripts.business_offers_refresh --output-json`

Current guarantees of the refresh-only path:

- updates `offers` only
- does not call `generate_and_send_stock(...)`
- does not call `build_stock_payload(...)`
- does not call `process_database_service("stock", ...)`
- does not write `InventoryStock`
- does not send stock to Tabletki

Current scheduler orchestration on top of refresh-only:

- `BUSINESS_STORE_STOCK_REFRESH_OFFERS_BEFORE_PUBLISH=false`
  - scheduler keeps old publish-only behavior
- `BUSINESS_STORE_STOCK_REFRESH_OFFERS_BEFORE_PUBLISH=true`
  - scheduler runs refresh before publish
- `BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL=false`
  - `partial` refresh skips publish
- `BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL=true`
  - `partial` refresh may still allow publish with warning
- `error` refresh always skips publish

Legacy behavior preserved:

- `run_pipeline(enterprise_code, "stock")` still performs:
  1. refresh offers
  2. then legacy stock export
- per-supplier commit/rollback semantics are unchanged
- partial supplier success remains possible and is now reflected in the refresh report
