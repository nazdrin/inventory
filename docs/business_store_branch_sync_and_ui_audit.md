# Business Store Branch Sync And UI Audit

Дата аудита: 2026-04-23

## Status Update

Следующий шаг после этого аудита уже реализован:

- добавлен backend sync/report/apply layer для `mapping_branch` ↔ `BusinessStore`;
- добавлен CLI:
  - `python -m app.scripts.business_store_branch_sync --dry-run --output-json`
  - `python -m app.scripts.business_store_branch_sync --enterprise-code 223 --dry-run --output-json`
  - `python -m app.scripts.business_store_branch_sync --enterprise-code 223 --apply --output-json`
- добавлен read-only meta endpoint для branch options store form:
  - `GET /developer_panel/business-stores/meta/mapping-branches?enterprise_code=...`
- `tabletki_branch` на `BusinessStoresPage` переведён на select from `mapping_branch`;
- orphan stores в apply mode деактивируются, но не удаляются;
- `legacy_scope_key` по-прежнему не auto-derived из `mapping_branch.store_id`.

Dry-run на текущих локальных данных подтвердил исходный drift:

- all business enterprises:
  - `missing_stores_to_create = 3`
  - `orphan_stores_to_deactivate = 1`
- enterprise `223`:
  - `missing_stores_to_create = 3`
  - `orphan_stores_to_deactivate = 1`
- enterprise `364`:
  - clean, `missing = 0`, `orphan = 0`

## Scope

Цель этого документа:

- зафиксировать текущее состояние модели `BusinessStore` после enterprise catalog migration;
- определить, какие поля ещё остаются store-owned, какие уже стали enterprise-owned, а какие превратились в technical/deprecated;
- проверить текущую связь `mapping_branch` ↔ `BusinessStore`;
- подготовить точное ТЗ для следующего шага:
  - sync магазинов с `mapping_branch`;
  - упрощение store UI;
  - корректное заполнение SalesDrive order payload.

В этом документе:

- исходный аудит остаётся как basis for decisions;
- ниже уже добавлены статусы реализованного sync/UI шага;
- runtime catalog/stock/order/outbound по-прежнему не меняется;
- DB schema не меняется;
- внешние API не вызываются.

Проверенные файлы:

- [app/models.py](/Users/dmitrijnazdrin/inventory_service_1/app/models.py)
- [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- [app/schemas.py](/Users/dmitrijnazdrin/inventory_service_1/app/schemas.py)
- [app/services/business_store_catalog_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_store_catalog_publish_service.py)
- [app/business/business_store_catalog_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_catalog_preview.py)
- [app/business/business_enterprise_catalog_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_enterprise_catalog_preview.py)
- [app/business/business_store_stock_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_stock_preview.py)
- [app/business/business_store_stock_exporter.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_stock_exporter.py)
- [app/services/business_store_stock_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_store_stock_publish_service.py)
- [app/business/business_store_order_mapper.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_order_mapper.py)
- [app/business/order_sender.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/order_sender.py)
- [app/services/order_fetcher.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/order_fetcher.py)
- [app/business/salesdrive_webhook.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/salesdrive_webhook.py)
- [admin-panel/src/pages/BusinessStoresPage.jsx](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/pages/BusinessStoresPage.jsx)
- [admin-panel/src/api/enterpriseApi.js](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/api/enterpriseApi.js)
- [admin-panel/src/api/mappingBranchAPI.js](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/api/mappingBranchAPI.js)
- [docs/business_multistore_architecture.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_multistore_architecture.md)
- [docs/business_stores_ui_enterprise_catalog_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_stores_ui_enterprise_catalog_audit.md)
- [docs/business_enterprise_catalog_identity_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_enterprise_catalog_identity_audit.md)

## Executive Summary

После enterprise catalog migration `BusinessStore` больше не должен выглядеть как владелец каталога. Его текущая реальная роль:

- routing overlay поверх branch;
- scope owner для store stock;
- price overlay owner;
- order/store resolution target по branch;
- rollout/activity metadata.

Главные выводы аудита:

1. Зафиксированное решение `1 mapping_branch.branch = 1 BusinessStore` не обеспечивается БД или API автоматически. Сейчас это только конвенция.
2. `mapping_branch` уже должен стать source of truth по branch-списку предприятия, но текущий UI и backend не синхронизируют stores с этим списком.
3. На данных уже есть drift:
   - `business_364` согласован с `mapping_branch`;
   - `business_223` не согласован: store branch `30421` не существует в `mapping_branch`, а в `mapping_branch` для enterprise `223` есть три branch без stores.
4. `migration_status` слишком заметен в UI, хотя его runtime-роль уже узкая и partly stale:
   - `stock_live` разрешён схемой, но не является publish-ready state ни для catalog, ни для stock.
5. `tabletki_enterprise_code` фактически дублирует `enterprise_code`:
   - в локальных данных mismatches не найдено;
   - поле лучше сделать read-only или hidden compatibility.
6. `tabletki_branch` должен перестать вводиться вручную:
   - его нужно выбирать из branch-ов `mapping_branch` для выбранного enterprise.
7. `salesdrive_enterprise_id` остаётся store-level полем:
   - теперь оно используется как источник `organizationId` для Business SalesDrive order payload;
   - если поле не заполнено, runtime временно использует compatibility fallback `"1"` и логирует warning.
8. `payment_method` в Business order payload теперь фиксирован:
   - для Business orders используется `"Післяплата"`.
9. `takes_over_legacy_scope` сейчас больше похоже на technical migration marker, чем на обычный operator toggle.

## 1. Current Store Model

### 1.1 Поля, которые реально остаются store-owned

Эти поля по текущей архитектуре действительно относятся к магазину:

- `store_code`
- `store_name`
- `legal_entity_name`
- `tax_identifier`
- `is_active`
- `legacy_scope_key`
- `tabletki_branch`
- `stock_enabled`
- `orders_enabled`
- `salesdrive_enterprise_id`
- `extra_markup_enabled`
- `extra_markup_mode`
- `extra_markup_min`
- `extra_markup_max`
- `extra_markup_strategy`

Практический смысл:

- `tabletki_branch` нужен для stock routing и order/outbound store resolution;
- `legacy_scope_key` нужен для stock scope;
- extra markup и `BusinessStoreProductPriceAdjustment` остаются store-level;
- `salesdrive_enterprise_id` подходит как store-level routing field для будущего `organizationId`.

### 1.2 Поля, которые уже enterprise-owned по смыслу, но ещё торчат в store model/UI

Эти поля больше не должны быть primary operator-owned на store уровне:

- `catalog_enabled`
- `catalog_only_in_stock`
- `tabletki_enterprise_code`
- `code_strategy`
- `name_strategy`

Причины:

- catalog gate уже enterprise-level через `EnterpriseSettings.catalog_enabled`;
- catalog branch уже enterprise-level через `EnterpriseSettings.branch_id`;
- catalog codes/names уже enterprise-level через `BusinessEnterpriseProductCode` / `BusinessEnterpriseProductName`;
- `catalog_only_in_stock` физически всё ещё лежит в `BusinessStore`, но в enterprise catalog mode трактуется как настройка главного catalog scope store, а не выбранного оператором store.

### 1.3 Поля, которые стали technical / deprecated / advanced

Эти поля ещё живут в storage и частично в rollback/runtime, но их не стоит держать в основной operator form:

- `migration_status`
- `takes_over_legacy_scope`
- `is_legacy_default`
- `code_strategy`
- `code_prefix`
- `name_strategy`
- `tabletki_enterprise_code`
- `salesdrive_enterprise_code`
- `salesdrive_store_name`

Отдельно:

- `orders_enabled` по смыслу operator-facing, но его runtime-роль пока не доведена до полноценного store gate;
- `migration_status` нужен backend-у для publish eligibility, но как UI control уже перегружен и partly stale.

## 2. MappingBranch ↔ BusinessStore Audit

### 2.1 Как связаны сейчас

Сейчас между `mapping_branch` и `business_stores` нет прямой DB-связи:

- нет FK;
- нет backend validation, что `BusinessStore.tabletki_branch` обязан существовать в `mapping_branch`;
- нет uniqueness rule вида `enterprise_code + mapping_branch.branch -> exactly one BusinessStore`;
- нет sync job, который создаёт missing stores или деактивирует orphan stores.

Фактическая связь сейчас только логическая:

- store-aware order inbound/outbound paths предполагают, что `mapping_branch.branch` синхронизирован с `BusinessStore.tabletki_branch`;
- enterprise catalog main scope store тоже ищется через `BusinessStore.tabletki_branch == EnterpriseSettings.branch_id`.

### 2.2 Что проверяется кодом, а что нет

Проверяется:

- `BusinessStore.tabletki_branch` используется для:
  - stock target branch;
  - inbound order store resolution;
  - outbound status store resolution;
  - enterprise catalog scope store resolution.

Не проверяется:

- что для каждого `mapping_branch.branch` существует store;
- что каждый active store branch действительно присутствует в `mapping_branch`;
- что у enterprise нет двух stores для одного branch;
- что `tabletki_branch` выбирается только из branch-ов `mapping_branch`.

### 2.3 Что показывают текущие данные

Read-only SQL audit по локальной БД дал такую картину.

Текущие stores:

- `business_364`
  - `enterprise_code=364`
  - `tabletki_branch=30630`
  - `legacy_scope_key=Kyiv`
  - `migration_status=catalog_stock_live`
  - `mapping_branch.branch=30630`
  - `mapping_branch.store_id=Kyiv`
  - alignment clean
- `business_223`
  - `enterprise_code=223`
  - `tabletki_branch=30421`
  - `legacy_scope_key=NULL`
  - `migration_status=draft`
  - matching `mapping_branch` row not found
  - orphan relative to current mapping list

`mapping_branch` rows for Business enterprises:

- enterprise `223`:
  - `30422 -> Ivano-Frankivsk`
  - `30423 -> Kyiv`
  - `30491 -> Kremenchuk`
- enterprise `364`:
  - `30630 -> Kyiv`

Derived facts:

- for `364`: `1 branch = 1 store` is already true;
- for `223`: current `BusinessStore` does not match any live branch from `mapping_branch`, while three mapping branches do not yet have stores.

### 2.4 Критичное наблюдение про `mapping_branch.store_id`

`mapping_branch.store_id` нельзя автоматически трактовать как универсальный `legacy_scope_key`.

На локальных данных он уже выглядит по-разному:

- human scope strings:
  - `Kyiv`
  - `Ivano-Frankivsk`
  - `Kremenchuk`
- numeric-like values:
  - `9`
  - `333`
  - `30472`
- GUID-like values
- composite strings:
  - `4 , 24`

Это значит:

- `mapping_branch` действительно может быть source of truth по branch-списку;
- но `mapping_branch.store_id` нельзя безусловно копировать в `BusinessStore.legacy_scope_key` во всех enterprise.

### 2.5 Recommendation

С учётом зафиксированного бизнес-решения безопасная модель такая:

- `mapping_branch` = source of truth по branch-списку внутри enterprise;
- `BusinessStore` = overlay-настройки поверх branch;
- sync должен работать по branch presence, а не по произвольной интерпретации `mapping_branch.store_id`.

Правила sync:

1. Для каждого `mapping_branch.branch` выбранного Business enterprise должен существовать один `BusinessStore`.
2. Missing stores нужно создавать.
3. Orphan stores не удалять:
   - рекомендовать деактивацию;
   - либо деактивировать controlled action-ом;
   - silent delete не делать.
4. `legacy_scope_key` не автозаполнять слепо из `mapping_branch.store_id`.
5. Generic sync должен синхронизировать минимум:
   - `enterprise_code`
   - `tabletki_enterprise_code`
   - `tabletki_branch`
   - existence / active-state.

## 3. migration_status Audit

### 3.1 Все известные значения

По модели и схемам допустимы:

- `draft`
- `dry_run`
- `stock_live`
- `catalog_stock_live`
- `orders_live`
- `disabled`

### 3.2 Где используется реально

Catalog eligibility:

- [app/services/business_store_catalog_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_store_catalog_publish_service.py)
- publish-ready states:
  - `dry_run`
  - `catalog_stock_live`
  - `orders_live`

Stock eligibility:

- [app/services/business_store_stock_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_store_stock_publish_service.py)
- stock-ready states:
  - `dry_run`
  - `catalog_stock_live`
  - `orders_live`

Direct order/outbound runtime gating:

- явного использования `migration_status` в inbound order mapper, order fetcher или outbound webhook mapping не обнаружено.

### 3.3 Что реально есть в данных

В локальной БД сейчас встречаются только:

- `draft` = 1 store
- `catalog_stock_live` = 1 store

### 3.4 Ключевой вывод

`stock_live` сейчас выглядит stale:

- он разрешён model/schema/UI;
- но не является publish-ready state ни для catalog, ни для stock.

Следствие:

- `migration_status` уже не стоит держать как один из главных operator controls;
- безопаснее перенести его в advanced/technical block;
- отдельно стоит решить, нужен ли `stock_live` вообще после отделения enterprise catalog от store stock/orders.

### 3.5 Recommendation for UI

Оператору в основной форме достаточно видеть:

- active / inactive
- stock enabled
- orders enabled
- maybe rollout badge read-only

`migration_status`:

- либо read-only badge;
- либо advanced dropdown;
- но не основной акцент store form.

## 4. tabletki_enterprise_code Audit

### 4.1 Где используется

Поле всё ещё участвует в:

- catalog/stock report metadata;
- rollback eligibility checks;
- stock export target identity;
- store resolution helper by full Tabletki identity:
  - `get_store_by_tabletki_identity(...)`.

### 4.2 Совпадает ли с enterprise_code

На локальных данных mismatches не найдено:

- `tabletki_enterprise_code != enterprise_code` count = `0`.

Текущие stores:

- `business_223`: `223 == 223`
- `business_364`: `364 == 364`

### 4.3 Вывод

После enterprise catalog migration это поле выглядит compatibility-слоем, а не операторской настройкой.

Recommendation:

- сделать read-only в UI;
- либо скрыть из основной формы;
- при создании/синке store автоматически заполнять `tabletki_enterprise_code = enterprise_code`;
- пока не удалять из БД и runtime, потому что stock/export/report paths ещё на него смотрят.

## 5. tabletki_branch Audit

### 5.1 Где используется

`BusinessStore.tabletki_branch` сейчас критичен для:

- stock target branch;
- inbound order store resolution;
- outbound status store resolution;
- enterprise catalog scope store resolution;
- rollback catalog target branch.

### 5.2 Должен ли он выбираться только из mapping_branch

Да, по зафиксированному бизнес-решению должен.

Причины:

- `Store = Branch`;
- `mapping_branch` становится source of truth по branch-списку;
- текущий free-text input позволяет сохранить arbitrary branch и создать drift.

### 5.3 Safest implementation path

Самый безопасный путь без ломки текущего API и данных:

1. Сначала добавить read-only sync/audit layer:
   - report missing stores;
   - report orphan stores;
   - report branch mismatches.
2. Потом добавить branch options API для выбранного enterprise:
   - либо новый meta endpoint;
   - либо фильтрованный read-only endpoint поверх `mapping_branch/view`;
   - не опираться на `admin-panel/src/api/enterpriseApi.js:getMappingBranches()`, потому что соответствующего route в `app/routes.py` сейчас нет.
3. В UI заменить ручной input `tabletki_branch` на select из branch-ов `mapping_branch`.
4. В backend update/create добавить validation:
   - для active store branch должен существовать в `mapping_branch` того же enterprise;
   - существующие orphan stores не удалять, а сохранять как inactive/legacy until fixed.

### 5.4 Дополнительная рекомендация

Если sync будет создавать store автоматически, safest seed defaults такие:

- `store_code = business_{enterprise_code}_{branch}`
- `store_name = <enterprise_name> / <branch>`
- `enterprise_code = enterprise.enterprise_code`
- `tabletki_enterprise_code = enterprise.enterprise_code`
- `tabletki_branch = mapping_branch.branch`
- `is_active = true`
- `stock_enabled = false`
- `orders_enabled = false`
- `migration_status = draft`

Но:

- `legacy_scope_key` не автозаполнять generic rule-ом из `mapping_branch.store_id`.

## 6. SalesDrive ID / Order Payload Audit

### 6.1 Где строится payload

Business order payload для SalesDrive строится в:

- [app/business/order_sender.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/order_sender.py)
- функция:
  - `build_salesdrive_payload(...)`

### 6.2 Что сейчас происходит

Текущее состояние payload:

- `"shipping_method"` берётся из `DeliveryServiceName`
- `"payment_method"` сейчас пустой:
  - `""`
- `"organizationId"` сейчас захардкожен:
  - `"1"`

Это видно прямо в payload builder:

- `"shipping_method": d.get("DeliveryServiceName", "")`
- `"payment_method": ""`
- `"organizationId": "1"`

### 6.3 Где сейчас живёт SalesDrive ID

В store model уже есть:

- `salesdrive_enterprise_id`
- `salesdrive_enterprise_code`
- `salesdrive_store_name`

Использование сейчас:

- field хранится через create/update BusinessStore API;
- есть resolver helper:
  - `get_store_by_salesdrive_enterprise_id(...)`
- но текущий Business order payload builder его не использует.

На локальных данных:

- `business_223`: `salesdrive_enterprise_id = 1`
- `business_364`: `salesdrive_enterprise_id = NULL`

### 6.4 Безопасный путь перевода organizationId

Самый безопасный путь:

1. Оставить `salesdrive_enterprise_id` store-level полем.
2. В Business order send path резолвить store по branch:
   - либо через existing store-aware order normalization context;
   - либо отдельным lightweight resolver-ом по `tabletki_branch`.
3. Если store найден и `salesdrive_enterprise_id` заполнен:
   - `organizationId = str(store.salesdrive_enterprise_id)`
4. Если store не найден или поле пустое:
   - fallback на текущий `"1"` до завершения миграции данных.

Почему это безопасно:

- branch уже приходит в runtime path;
- store-aware order flow уже умеет резолвить store по branch;
- radius of impact ограничивается Business order payload builder, без изменения catalog/stock/webhook flows.

### 6.5 Куда вставить payment_method

Следующий шаг должен просто выставлять:

- `"payment_method": "Післяплата"`

Рекомендация:

- добавить это в тот же payload builder `build_salesdrive_payload(...)`;
- ставить сразу рядом с `shipping_method`, потому что это тот же уровень SalesDrive delivery/payment metadata;
- не завязывать на тип доставки, пока бизнес-правило жёстко фиксировано.

### 6.6 Radius of impact

Изменение затронет в основном:

- [app/business/order_sender.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/order_sender.py)

Опционально:

- lightweight helper/resolver import для поиска store;
- regression tests / simulator for Business order payload.

Не нужно трогать:

- catalog runtime;
- stock runtime;
- inbound reverse mapping;
- outbound webhook mapping;
- SalesDriveSimple.

## 7. takes_over_legacy_scope Audit

### 7.1 Где используется

Поле реально используется в:

- [app/business/business_store_resolver.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_resolver.py)

Там оно влияет на ordering/selection для legacy scope resolution:

- `get_stores_taking_over_legacy_scope(...)`
- `get_active_store_by_legacy_scope_key(...)`

### 7.2 Что это значит practically

Поле не выглядит как everyday operator control.

Это скорее:

- migration intent marker;
- explicit high-risk routing hint;
- advanced flag для controlled cutover.

### 7.3 Recommendation for UI

`takes_over_legacy_scope` стоит оставить:

- только в advanced / deprecated / migration block;
- с явным warning;
- вне основной operator form.

Обычному оператору не нужно видеть это как один из главных store toggles.

## 8. Recommended Target Model

Целевая модель для следующего шага:

- `mapping_branch` = source of truth по branch-списку предприятия
- `BusinessStore` = overlay поверх branch
- `1 branch = 1 store`
- branch выбирается только из `mapping_branch`
- orphan stores не удаляются, а деактивируются
- catalog убран из store ownership
- stock / orders / pricing остаются store-level
- `SalesDrive ID` пока остаётся store-level
- `payment_method` для business orders пока always `"Післяплата"`

### 8.1 Что должно остаться в основной store form

- `store_code`
- `store_name`
- `legal_entity_name`
- `tax_identifier`
- `is_active`
- `tabletki_branch` as selected branch from mapping list
- `legacy_scope_key`
- `stock_enabled`
- `orders_enabled`
- `salesdrive_enterprise_id`
- extra markup block

### 8.2 Что должно уйти в advanced / deprecated

- `migration_status`
- `takes_over_legacy_scope`
- `tabletki_enterprise_code`
- `salesdrive_enterprise_code`
- `salesdrive_store_name`
- `is_legacy_default`
- `code_strategy`
- `code_prefix`
- `name_strategy`

### 8.3 Что должно управляться на уровне enterprise

- `catalog_enabled`
- catalog branch (`EnterpriseSettings.branch_id`)
- enterprise catalog identity status
- catalog assortment policy through main catalog scope store

## 9. Safe Rollout Plan

### Phase 1. Audit only

Сделано этим документом:

- зафиксированы runtime dependencies;
- найден текущий drift по `mapping_branch` vs `BusinessStore`;
- подтверждено, что `organizationId` и `payment_method` ещё не доведены до новой модели.

### Phase 2. Backend sync / validation

Сделать отдельный следующий шаг:

- read-only sync report for one enterprise:
  - branches in mapping without store
  - stores without mapping branch
  - duplicate stores per branch
- apply mode:
  - create missing stores
  - deactivate orphan stores
- не удалять orphan stores
- не копировать `mapping_branch.store_id` blindly into `legacy_scope_key`

### Phase 3. UI refactor

- branch input заменить на select из `mapping_branch`
- `tabletki_enterprise_code` сделать read-only/hidden
- `migration_status` и `takes_over_legacy_scope` перенести в advanced
- store block упростить до:
  - branch
  - scope
  - stock/orders
  - SalesDrive routing
  - markup

### Phase 4. Payload update

- в Business order payload:
  - брать `organizationId` из `store.salesdrive_enterprise_id` with fallback
  - always set `"payment_method": "Післяплата"`

### Phase 5. Regression tests

- sync audit report for enterprise `223` and `364`
- store create/update with branch select
- orphan deactivation path
- Business order payload smoke-test:
  - `organizationId`
  - `payment_method`
- no regressions in stock/order/outbound mapping

## Final Recommendation

Следующий implementation step должен идти в таком порядке:

1. backend sync/report layer for `mapping_branch` ↔ `BusinessStore`
2. store UI simplification with branch select from `mapping_branch`
3. Business order payload update:
   - `organizationId <- store.salesdrive_enterprise_id`
   - `payment_method = "Післяплата"`

Главный риск, который нельзя игнорировать:

- `mapping_branch.store_id` перегружен и не должен использоваться как generic source для `legacy_scope_key`.

Самое точное ТЗ для следующего шага:

- синхронизировать stores по branch presence из `mapping_branch`;
- branch сделать operator-select only from mapping list;
- убрать из основной form всё, что больше не является real store ownership;
- перевести Business SalesDrive payload на store `organizationId` и фиксированный `payment_method`.
