# Business Stores

## 1. Назначение Business Stores

- `BusinessStore` не заменяет текущий Business runtime-контур.
- `enterprise_settings` остаётся базовым control-plane и runtime profile для scheduler/export/order flows.
- `business_stores` — store-level overlay для поэтапного перехода от legacy `city/scope` к продавцам, витринам и юрлицам.
- `business_store_product_codes` — стабильный mapping `internal_product_code <-> external_product_code` per store.
- `business_store_product_names` — стабильный mapping `internal_product_code <-> external_product_name` per store.
- `business_store_product_price_adjustments` — стабильный mapping `internal_product_code <-> markup_percent` per store.
- На текущем этапе dry-run строит отчёты и при явном флаге может генерировать code mappings, name mappings и stable markup adjustments, но ничего не отправляет наружу.

## 1.1 Current implementation status

На текущем этапе уже реализовано:

- `BusinessStore.name_strategy`
- `BusinessStore.extra_markup_*`
- генерация `missing codes`
- генерация `missing names`
- генерация `missing price adjustments`
- emergency cleanup для `business_store_product_names`
- dry-run preview для catalog identity и store-level markup

Не реализовано:

- live store-aware catalog export
- live store-aware stock export
- runtime применение markup
- order runtime integration

## 2. EnterpriseSettings vs BusinessStore

### EnterpriseSettings

`enterprise_settings` сегодня используется как global/runtime control-plane:

- `enterprise_code` — базовый runtime identity почти для всех scheduler и export/import paths.
- `enterprise_name` — display label и базовая сущность предприятия.
- `branch_id` — legacy catalog/master publish target branch.
- `tabletki_login` / `tabletki_password` — текущие credentials для Tabletki order/catalog/stock flows.
- `token` — access token для части integrations и Business settings runtime.
- `catalog_enabled` / `stock_enabled` / `order_fetcher` / `auto_confirm` — live runtime flags.
- `data_format` — выбор processor-а в scheduler-ах.
- `stock_upload_frequency` / `catalog_upload_frequency` — scheduler cadence.

Эти поля нельзя переносить в `business_stores` на первом этапе, потому что они уже используются в:

- [app/services/catalog_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/catalog_scheduler_service.py)
- [app/services/stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/stock_scheduler_service.py)
- [app/services/business_stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_stock_scheduler_service.py)
- [app/services/order_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/order_scheduler_service.py)
- [app/services/order_fetcher.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/order_fetcher.py)
- [app/business/import_catalog.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/import_catalog.py)
- [app/business/tabletki_master_catalog_exporter.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/tabletki_master_catalog_exporter.py)
- [app/business/master_catalog_orchestrator.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/master_catalog_orchestrator.py)

### BusinessStore

`business_stores` должен хранить только store-level overlay:

- identity продавца;
- внешний Tabletki/SalesDrive routing;
- per-store enable flags;
- связь с legacy scope;
- strategy/mapping внешних product codes;
- migration metadata.

`BusinessStore.enterprise_code` — это ссылка на базовый Business enterprise profile в `enterprise_settings`, а не замена этого профиля.

## 3. MappingBranch and Legacy Scope

### Что означает `mapping_branch` сейчас

По текущему коду:

- `mapping_branch.branch` — стабильный runtime branch identity.
- `mapping_branch.store_id` — raw storage value с перегруженной семантикой.
- В Business stock pipeline `mapping_branch.store_id` фактически используется как legacy scope key (`city`).
- В order fetch `mapping_branch.branch` используется для чтения заказов из Tabletki.
- В runtime read-model уже зафиксировано, что `branch` — stable identity, а `store_id` нельзя blindly rename в storage.

Подтверждающие места:

- [app/business/dropship_pipeline.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/dropship_pipeline.py): `_load_branch_mapping()`, `build_stock_payload()`
- [app/services/order_fetcher.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/order_fetcher.py)
- [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- [admin-panel/src/pages/MappingBranchPage.js](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/pages/MappingBranchPage.js)

### Правильная модель на период миграции

- `mapping_branch` остаётся legacy runtime bridge.
- `business_stores.legacy_scope_key` указывает на старый operational scope, который сегодня часто совпадает со значением `mapping_branch.store_id`.
- `business_stores.tabletki_branch` — будущий store-level branch для нового live export/import.
- До подключения нового store runtime удалять или менять `mapping_branch` нельзя.

### Как должен работать future takeover

- `takes_over_legacy_scope=true` не должен сам по себе выключать старый export.
- Сначала нужен новый live export для конкретного store.
- Только после этого legacy export по `legacy_scope_key` может быть явно исключён.
- Исключение должно происходить через новый runtime gating, а не через silent rewrite `mapping_branch`.

## 4. Legacy Scope (`city`)

`city` пока остаётся internal operational scope:

- `offers.city`
- `dropship_enterprises.city`
- `competitor_prices.city`
- `balancer_*`.`city`
- часть Business order/payload logic

Подтверждение:

- [app/business/dropship_pipeline.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/dropship_pipeline.py)
- [app/models.py](/Users/dmitrijnazdrin/inventory_service_1/app/models.py)
- [admin-panel/src/pages/SuppliersPage.jsx](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/pages/SuppliersPage.jsx)

Правило:

- глобальный rename `city -> store` не делать;
- `legacy_scope_key` в `business_stores` должен трактоваться как "старый scope", а не как новый canonical store id;
- будущий UI должен показывать это именно как `Legacy scope`, а не как `City`, даже если исторические значения выглядят как `Kyiv/Lviv`.

## 5. Two-level Flags Model

### Уровень 1. Global runtime

В `enterprise_settings`:

- `catalog_enabled`
- `stock_enabled`
- `order_fetcher`

Это глобальные Business runtime switches. Сейчас именно они реально включают или выключают live behavior.

### Уровень 2. Store overlay

В `business_stores`:

- `catalog_enabled`
- `stock_enabled`
- `orders_enabled`

Это per-store switches для будущего store-aware runtime.

### Итоговая модель

- live действие разрешено только если включены оба уровня;
- `enterprise_settings` flags блокируют весь Business runtime profile;
- `business_stores` flags блокируют только конкретного продавца внутри этого profile;
- пока store runtime не подключён, store-level flags информационные и не должны менять текущее поведение.

### Что показать в будущем UI

- global warning: `EnterpriseSettings gates the whole Business runtime`
- per-store warning: `Store flags have no live effect until store runtime is enabled`
- dangerous toggles:
  - `enterprise_settings.catalog_enabled`
  - `enterprise_settings.stock_enabled`
  - `enterprise_settings.order_fetcher`
  - future `business_stores.takes_over_legacy_scope`

## 6. Tabletki Identity Model

Разделять две сущности:

- `enterprise_settings.enterprise_code` — внутренний runtime identity проекта.
- `business_stores.tabletki_enterprise_code` — внешний enterprise/account identity в Tabletki для конкретного store.

Текущая модель:

- credentials берутся из `EnterpriseSettings.tabletki_login/tabletki_password/token`;
- `branch_id` из `EnterpriseSettings` используется в legacy/master publish path;
- `MappingBranch.branch` используется в legacy orders/stock path.

На первом этапе credentials не переносим в `business_stores`, потому что текущие scheduler/export/order paths уже читают их из `enterprise_settings`, а live store runtime ещё не подключён.

Будущая роль полей `business_stores`:

- `tabletki_enterprise_code` — target external enterprise/account for store-level export/import;
- `tabletki_branch` — target external branch for store-level stock/orders;
- использовать их нужно только в новом store-aware runtime, а не в legacy flows.

Главное правило:

- не путать внутренний `enterprise_settings.enterprise_code` с внешним `business_stores.tabletki_enterprise_code`.

## 7. SalesDrive Identity Model

`BusinessStore` уже содержит:

- `salesdrive_enterprise_code` — legacy/string identity field;
- `salesdrive_enterprise_id` — numeric enterprise id;
- `salesdrive_store_name` — display/store label.

Правильная трактовка:

- `salesdrive_enterprise_id` — будущий основной numeric ID предприятия SalesDrive;
- `salesdrive_enterprise_code` — legacy/string compatibility field;
- `salesdrive_store_name` — display/integration helper, не identity.

Риски путаницы:

- numeric `salesdrive_enterprise_id` легко перепутать с supplier id (`dropship_enterprises.salesdrive_supplier_id`);
- supplier id, SalesDrive enterprise id и external store identity — это разные сущности и должны быть разведены в UI.

## 8. Product Code Strategy

Поддерживаемые стратегии:

- `legacy_same`
- `opaque_mapping`
- `prefix_mapping`

Правила:

- для базового legacy продавца вроде Петренко/Likodar — `legacy_same`;
- для новых продавцов — `opaque_mapping`;
- `prefix_mapping` допустим только как осознанный интеграционный компромисс, а не default.

`internal_product_code` должен оставаться внутренним кодом системы.

`external_product_code` должен использоваться только для внешнего Tabletki catalog/stock/orders.

`BusinessStoreProductCode` обязателен для будущего reverse mapping в order flow.

Критичное правило:

- после генерации и тем более после live нельзя свободно менять `code_strategy`, `code_prefix` или semantics внешнего кода.

## 9. Migration Lifecycle

Статусы:

- `draft`
  - store создан, runtime не подключён
  - можно свободно менять identity/integration поля
- `dry_run`
  - можно строить отчёты и проверять mappings
  - live behavior не меняется
- `stock_live`
  - store-level stock path уже live
  - менять branch/code strategy опасно
- `catalog_stock_live`
  - live и stock, и catalog
  - identity/code fields должны быть фактически frozen
- `orders_live`
  - полный store lifecycle с orders/reverse mapping
  - любые изменения identity/code fields high-risk
- `disabled`
  - store отключён как planned live entity
  - historical mappings должны сохраняться

Правило:

- `migration_status` сам по себе не включает runtime;
- это lifecycle marker и UI state;
- реальное поведение должны определять feature flags, global enterprise flags и future store runtime routing.

## 10. Field Editability Matrix

| field | table | source | editable in draft | editable in dry_run | editable in live | runtime effect now | future runtime effect | risk/comment |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `catalog_enabled` | `enterprise_settings` | existing runtime | yes | yes | yes | yes | yes | global kill switch for catalog |
| `stock_enabled` | `enterprise_settings` | existing runtime | yes | yes | yes | yes | yes | global kill switch for stock |
| `order_fetcher` | `enterprise_settings` | existing runtime | yes | yes | yes | yes | yes | global order intake switch |
| `store_code` | `business_stores` | store identity | yes | risky | no | no | yes | stable store slug, should be immutable after dry-run |
| `store_name` | `business_stores` | store identity/display | yes | yes | limited | no | yes | display-safe, but avoid frequent live renames |
| `legal_entity_name` | `business_stores` | integration/legal | yes | yes | limited | no | yes | legal document/display impact |
| `tax_identifier` | `business_stores` | integration/legal | yes | yes | limited | no | yes | external legal identity risk |
| `enterprise_code` | `business_stores` | link to base profile | yes | risky | no | no | yes | wrong link routes store to wrong Business profile |
| `legacy_scope_key` | `business_stores` | legacy overlay | yes | risky | no | no | yes | wrong value can duplicate or skip legacy scope |
| `tabletki_enterprise_code` | `business_stores` | external Tabletki identity | yes | yes | no | no | yes | wrong target account risk |
| `tabletki_branch` | `business_stores` | external Tabletki branch | yes | yes | no | no | yes | wrong branch risk |
| `salesdrive_enterprise_id` | `business_stores` | SalesDrive identity | yes | yes | no | no | yes | do not confuse with supplier id |
| `salesdrive_store_name` | `business_stores` | display/integration | yes | yes | limited | no | yes | mostly metadata |
| `code_strategy` | `business_stores` | code policy | yes | risky | no | no | yes | must freeze before first live mapping use |
| `code_prefix` | `business_stores` | code policy | yes | risky | no | no | yes | only relevant for `prefix_mapping` |
| `is_legacy_default` | `business_stores` | migration/code policy | yes | risky | no | no | yes | controls `legacy_same` semantics |
| `catalog_enabled` | `business_stores` | store runtime flag | yes | yes | yes | no | yes | no effect until store runtime exists |
| `stock_enabled` | `business_stores` | store runtime flag | yes | yes | yes | no | yes | no effect until store runtime exists |
| `orders_enabled` | `business_stores` | store runtime flag | yes | yes | yes | no | yes | no effect until store runtime exists |
| `catalog_only_in_stock` | `business_stores` | store catalog rule | yes | yes | risky | no | yes | affects export assortment |
| `takes_over_legacy_scope` | `business_stores` | migration routing | yes | yes | guarded only | no | yes | dangerous, can suppress legacy export later |
| `migration_status` | `business_stores` | lifecycle marker | yes | yes | yes | no | indirect/UI only | informational, not a switch by itself |

## 11. Future UI/API Notes

### Future UI: `Business sellers`

Table columns:

- `store_code`
- `store_name`
- `enterprise_code`
- `legacy_scope_key`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `salesdrive_enterprise_id`
- `code_strategy`
- `migration_status`
- `is_active`
- `catalog_enabled`
- `stock_enabled`
- `orders_enabled`
- `takes_over_legacy_scope`

Create/edit form:

- editable in `draft`:
  - identity
  - integration identities
  - code strategy
  - overlay flags
- editable in `dry_run` with warning:
  - `legacy_scope_key`
  - `tabletki_enterprise_code`
  - `tabletki_branch`
  - `salesdrive_enterprise_id`
  - `code_strategy`
  - `is_legacy_default`
- read-only in live states:
  - `store_code`
  - `enterprise_code`
  - `legacy_scope_key`
  - `tabletki_enterprise_code`
  - `tabletki_branch`
  - `salesdrive_enterprise_id`
  - `code_strategy`
  - `code_prefix`
  - `is_legacy_default`

Dropdowns/meta:

- Business profile options from `enterprise_settings where data_format='Business'`
- `legacy_scope_key` options from distinct `offers.city`
- `code_strategy` enum
- `migration_status` enum

Warnings/confirm:

- changing `legacy_scope_key`
- enabling `takes_over_legacy_scope`
- changing `code_strategy`
- changing `tabletki_enterprise_code` or `tabletki_branch`
- changing `salesdrive_enterprise_id`

### Future API surface

- `GET /business-stores`
- `POST /business-stores`
- `PUT /business-stores/{id}`
- `GET /business-stores/meta/legacy-scopes`
- `GET /business-stores/meta/business-enterprises`
- `POST /business-stores/{id}/dry-run`
- `POST /business-stores/{id}/generate-missing-codes`

These endpoints are not implemented in this phase.

## 12. What Is Not Implemented Yet

- no store-aware scheduler routing
- no store-aware live stock export
- no store-aware live catalog export
- no store-aware order fetch/import
- no takeover logic that excludes legacy scope from old export
- no admin-panel UI for `business_stores`
- no API endpoints for `business_stores`
- no runtime use of store-level flags

## 13. Risks and Safety Rules

Main risks:

- потерять текущий `mapping_branch.branch -> legacy scope` mapping
- отправить stock/catalog не в тот `tabletki_enterprise_code`
- дважды выгрузить один и тот же legacy scope через old и new contour
- исключить `Lviv` из legacy export до запуска нового live export
- поменять `code_strategy` после генерации кодов
- перепутать `enterprise_settings.enterprise_code` и `business_stores.tabletki_enterprise_code`
- перепутать `salesdrive_enterprise_id` и supplier id
- начать использовать store-level flags до того, как runtime научится их учитывать

Safety rules:

- не менять `dropship_pipeline`, scheduler-ы и current export/order flows на этой стадии
- не удалять и не переписывать `mapping_branch`
- не переименовывать глобально `city`
- не включать takeover только по `migration_status`
- не делать silent fallback между разными identity fields
- не менять code strategy после начала live without explicit migration plan
