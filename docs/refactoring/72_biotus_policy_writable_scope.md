# Biotus Policy Writable Scope

## 1. Scope

Этот шаг добавляет bounded writable scope только для Biotus fallback/status policy в `business_settings`.

Включено:
- storage support for four Biotus policy fields
- safe migration/backfill from current env
- resolver DB-first read for these fields
- `biotus_check_order.py` DB-first policy read
- limited writable scope in `Business Settings` page for section `Заказы / Biotus`

Не включено:
- `BIOTUS_TIME_*`
- `BIOTUS_TZ`
- `BIOTUS_SCHEDULER_INTERVAL_SECONDS`
- `BIOTUS_NIGHT_*`
- `BIOTUS_NO_SSL_VERIFY`
- `BIOTUS_DRY_RUN`
- `ALLOWED_SUPPLIERS`
- `TABLETKI_CANCEL_REASON_DEFAULT`
- pricing / supplier settings / old Business contour

## 2. Storage

В `business_settings` добавлены поля:

- `biotus_enable_unhandled_fallback`
- `biotus_unhandled_order_timeout_minutes`
- `biotus_fallback_additional_status_ids`
- `biotus_duplicate_status_id`

`biotus_fallback_additional_status_ids` хранится в DB как `integer[]`.

Причина выбора:

- avoids CSV parsing chaos in storage
- keeps semantic type explicit
- UI still edits the value as a comma-separated list for operator convenience

## 3. Migration

Добавлена отдельная migration только под эти 4 поля.

Семантика migration:

- add nullable columns
- backfill existing `business_settings` row from current env
- set `NOT NULL` + server defaults
- add bounded check constraints for timeout / duplicate status / non-empty status id array

Никакие другие поля migration не затрагивает.

## 4. Resolver / runtime

`master_business_settings_resolver.py` теперь читает эти 4 Biotus policy fields из DB, если `business_settings` row существует.

Fallback rules:

1. если row существует:
- policy читается из DB
- env не используется как альтернативный source of truth

2. если row отсутствует:
- используется прежний env fallback

Time-window block и scheduler/runtime flags intentionally remain env-driven.

`biotus_check_order.py` переведён на resolver-backed policy read без изменения бизнес-логики обработки заказов.

## 5. Backend write contract

Writable scope интегрирован в существующий controlled singleton update path.

`BusinessSettingsUpdateSchema` теперь включает:

- `biotus_enable_unhandled_fallback`
- `biotus_unhandled_order_timeout_minutes`
- `biotus_fallback_additional_status_ids`
- `biotus_duplicate_status_id`

Validation:

- timeout >= 0
- duplicate status id >= 1
- additional status ids must be non-empty positive integers

No generic patch semantics were added.

## 6. Frontend

`Business Settings` page now allows editing section `Заказы / Biotus`, but only for these four fields.

UI rules:

- checkbox for fallback enabled
- numeric fields for timeout and duplicate status id
- comma-separated input for additional status ids
- explicit help text states that DB stores the list as `integer[]`

Timing window / runtime flags stay visible but read-only.

## 7. Risks

Residual risks:

- SalesDrive status ids remain external integration constants; API validation can ensure shape, but not semantic correctness in SalesDrive
- existing save flow still uses one full control-plane payload, so target/master/Biotus bounded fields are saved together rather than section-patch style
- deploy-before-migrate order is still operationally sensitive because ORM/page surface expects the new columns after this step

## 8. Next step

Следующий рекомендуемый шаг:

- cleanup pass for Business Settings wording/provenance so the UI no longer refers to “master-only” writable support

После этого можно отдельно решить:

- нужен ли отдельный endpoint per section
- нужен ли вообще перенос mixed Biotus time-window block в control-plane
