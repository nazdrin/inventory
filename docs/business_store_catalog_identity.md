# Business Store Catalog Identity

Status note:

- this document describes the current and previously planned store-level catalog identity model;
- a newer migration audit now defines the next target architecture where catalog identity becomes enterprise-level;
- use it as the primary reference before changing schema or runtime readers:
  - [docs/business_enterprise_catalog_identity_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_enterprise_catalog_identity_audit.md)

## 1. Scope

Этот документ фиксирует target architecture для следующего слоя Business multistore:

- store-level assortment;
- store-level product codes;
- store-level product names;
- store-level extra price markup;
- dry-run/UI contracts для этих настроек.

Ограничения этого шага:

- runtime не меняется;
- scheduler-ы не меняются;
- `app/business/dropship_pipeline.py` не меняется;
- `app/business/master_catalog_orchestrator.py` не меняется;
- `app/business/tabletki_master_catalog_exporter.py` не меняется;
- DB schema не меняется;
- UI не меняется.

## 1.1 Current implementation status

Foundation for this layer is now implemented in code for:

- `BusinessStore.name_strategy`
- `BusinessStore.extra_markup_*`
- `business_store_product_names`
- `business_store_product_price_adjustments`
- dry-run preview for names and extra markup
- store-aware catalog payload preview
- UI/API actions for generate/cleanup preparation flows

Still explicitly not implemented:

- live catalog export changes
- live stock export changes
- runtime pricing changes
- scheduler changes
- order runtime changes

## 1.2 Catalog payload preview

Read-only catalog payload preview is now implemented as a separate preparation layer:

- builder: `app/business/business_store_catalog_preview.py`
- endpoint: `POST /developer_panel/business-stores/{store_id}/catalog-preview`
- UI action: `Catalog preview`

This layer:

- reads `MasterCatalog` plus existing `BusinessStoreProductCode` and `BusinessStoreProductName`;
- applies `catalog_only_in_stock`, `code_strategy`, and `name_strategy`;
- marks rows as `exportable` or `not exportable`;
- never creates missing mappings;
- never writes files;
- never calls Tabletki API;
- does not modify `tabletki_master_catalog_exporter`.

Read-only stock payload preview is also now implemented:

- builder: `app/business/business_store_stock_preview.py`
- endpoint: `POST /developer_panel/business-stores/{store_id}/stock-preview`
- UI action: `Stock preview`

This layer:

- reads `Offer` plus existing `BusinessStoreProductCode` and `BusinessStoreProductPriceAdjustment`;
- uses local best-offer approximation for preview only;
- applies store-level markup only to previewed price output;
- never creates missing mappings or price adjustments;
- never changes `offers.price`;
- never calls Tabletki API;
- does not modify `dropship_pipeline` or stock scheduler runtime.

Manual store-aware catalog export is now also implemented as a separate operator-only path:

- builder/exporter: `app/business/business_store_catalog_exporter.py`
- CLI: `python -m app.scripts.business_store_catalog_export`

Rules:

- source for export is `build_store_catalog_payload_preview(...)`;
- only rows with `exportable=true` are sent;
- missing mappings are not created during export;
- default mode is dry-run;
- live send requires explicit `--send --confirm`;
- scheduler integration is not added;
- current master publish path is not modified.

Separate legacy stock audit and target manual stock exporter plan:

- [docs/business_store_stock_export_audit.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_stock_export_audit.md)

## 2. Что проверено

Проверены:

- `app/models.py`
- `app/routes.py`
- `app/services/master_catalog_scheduler_service.py`
- `app/services/business_stock_scheduler_service.py`
- `app/business/master_catalog_orchestrator.py`
- `app/business/tabletki_master_catalog_exporter.py`
- `app/business/tabletki_master_catalog_loader.py`
- `app/business/import_catalog.py`
- `app/business/master_content_select.py`
- `app/business/business_store_code_generator.py`
- `app/business/business_store_resolver.py`
- `app/business/business_store_export_dry_run.py`
- `app/business/order_sender.py`
- `admin-panel/src/pages/BusinessStoresPage.jsx`

## 3. Краткий вывод

- Базовый master publish сейчас жёстко работает от одного `enterprise_code` и одного `EnterpriseSettings.branch_id`; точка daily publish проходит через `business_settings` -> `master_catalog_orchestrator` -> `tabletki_master_catalog_exporter`.
- Store-level assortment и store-level catalog identity логично добавлять отдельным будущим export-layer поверх `master_catalog`, не вмешиваясь в текущий exporter.
- Для store-level names лучший primary source сейчас `catalog_supplier_mapping.supplier_product_name_raw`, а не legacy `catalog_mapping.Name_D*`.
- Для стабильных store-level names нужна отдельная mapping table по аналогии с `business_store_product_codes`; names нельзя перегенерировать на каждом dry-run/export.
- Store-level extra markup надо трактовать как overlay поверх уже рассчитанной базовой цены и применять только в будущем store-aware export layer, а не внутри текущего pricing pipeline.

## 4. Текущий runtime поток

### 4.1 Где выбирается enterprise для daily publish

- `app/services/master_catalog_scheduler_service.py`
- `app/services/master_business_settings_resolver.py`
- `app/business/master_catalog_orchestrator.py`

Факт:

- daily publish enterprise резолвится через `business_settings`;
- `master_catalog_orchestrator._require_enterprise(..., purpose="publish")` вызывает `settings.resolve_publish_enterprise()`;
- дальше выбранный `enterprise_code` передаётся в `export_master_catalog_to_tabletki(...)`.

Следствие:

- текущая daily publish модель single-enterprise;
- store-level live export надо делать отдельным маршрутом позже, без подмены текущего publish path.
- staged scheduler connection for that future route is described in `docs/business_store_catalog_scheduler_audit.md`.

### 4.2 Где берётся master catalog

Факт:

- `tabletki_master_catalog_loader.py` синхронизирует данные в `master_catalog`;
- `tabletki_master_catalog_exporter._load_master_catalog_rows()` читает `MasterCatalog` c `is_archived = false`;
- `business_store_export_dry_run.build_store_catalog_dry_run()` тоже читает `MasterCatalog`.

Следствие:

- `master_catalog` уже является правильным базовым source of truth для catalog assortment;
- store-aware export должен фильтровать и переименовывать строки поверх `MasterCatalog`, а не менять сам `master_catalog`.

### 4.3 Где формируется код товара

Базовый publish:

- `tabletki_master_catalog_exporter._build_offer_payload()`
- `"Code": item.sku`

Store dry-run:

- `business_store_export_dry_run.build_store_catalog_dry_run()`
- `business_store_code_generator.ensure_store_product_code()`

Следствие:

- current master export всегда использует внутренний `MasterCatalog.sku`;
- store-aware codes уже имеют стабильную модель через `business_store_product_codes`;
- базовый export не должен переключаться на store codes.

### 4.4 Где формируется название товара

Базовый publish:

- `tabletki_master_catalog_exporter._resolve_offer_name()`
- порядок: `name_ua` -> `name_ru` -> `sku`

Следствие:

- store-aware names должны внедряться в новом слое до формирования payload offer;
- базовый exporter не должен менять свою логику.

### 4.5 Где используется branch_id

- `tabletki_master_catalog_exporter._get_export_settings()`
- `endpoint = ... /Import/Ref/{enterprise_settings.branch_id}`

Следствие:

- базовый publish жёстко привязан к `EnterpriseSettings.branch_id`;
- `BusinessStore.tabletki_branch` пока не участвует в live catalog publish и должен оставаться future-routing полем.

## 5. Store-Level Assortment

### 5.1 Что уже есть

В `BusinessStore` уже есть `catalog_only_in_stock`.

Факт:

- `build_store_catalog_dry_run()` при `catalog_only_in_stock=true` ограничивает catalog списком SKU, для которых есть positive stock в `Offer.city == legacy_scope_key`.

Следствие:

- с точки зрения модели ассортимент уже почти покрыт;
- для будущего UI и API не нужен второй взаимоисключающий boolean вроде `catalog_all_products`.
- preview layer теперь показывает итоговый candidate/exportable catalog payload без live publish.

### 5.2 Рекомендация по полю

Оставить существующее поле:

- `catalog_only_in_stock: bool`

Причины:

- поле уже есть в модели, API и dry-run;
- semantics понятная для runtime;
- `catalog_all_products` дублировал бы ту же логику через инверсию.

UI abstraction допустима:

- `Все товары`
- `Только товары с остатком`

Но storage/model лучше оставить через `catalog_only_in_stock`.

## 6. Store-Level Product Codes

### 6.1 Что уже есть

- `BusinessStore.code_strategy`
- `BusinessStore.is_legacy_default`
- `BusinessStore.code_prefix`
- `BusinessStoreProductCode`

Факт:

- `business_store_product_codes` уже хранит стабильный mapping `store_id + internal_product_code -> external_product_code`;
- generator не перезаписывает существующую запись;
- dry-run умеет показывать missing mappings и генерировать отсутствующие codes.
- catalog payload preview читает эти mappings и помечает товар как `not exportable`, если mapping отсутствует.

### 6.2 Рекомендация

Data model не менять концептуально.

Для UI допустима абстракция:

- `base_codes_enabled`

Но storage стоит оставить как есть:

- `code_strategy`
- `is_legacy_default`

Причина:

- эти поля уже поддерживают базовый кейс `legacy_same` и уникальный per-store mapping;
- дополнительный boolean ухудшит консистентность модели, если оставить и strategy, и boolean одновременно.

## 7. Store-Level Product Names

### 7.1 Что есть в master_catalog

В `MasterCatalog` есть только базовые master names:

- `name_ua`
- `name_ru`

Этого достаточно для base strategy, но недостаточно для store-level supplier-random names.

### 7.2 Что есть в supplier mappings

В `CatalogSupplierMapping` есть:

- `sku`
- `supplier_id`
- `supplier_code`
- `supplier_product_id`
- `supplier_product_name_raw`
- `barcode`

Это лучший текущий нормализованный источник supplier names, потому что:

- поле уже связано с внутренним `sku`;
- mapping уже проходит через barcode/supplier sync;
- source уже дедуплицирован до пары `supplier_id + supplier_code`.

### 7.3 Что есть в legacy catalog_mapping

В `catalog_mapping` есть:

- `Name_D1..Name_D20`
- `Code_D1..Code_D20`

Но это legacy-слой с колонками на поставщика и перегруженной структурой.

Рекомендация:

- не брать `Name_D*` как primary source для новой модели;
- использовать их только как fallback-of-last-resort, если для SKU нет данных в `CatalogSupplierMapping`.

Причины:

- нет нормальной мета-информации по source;
- нет общего supplier_id;
- модель плохо расширяется;
- часть новых master flows уже идёт через `CatalogSupplierMapping`.

### 7.4 Лучший источник supplier names

Приоритет источников для будущего generator:

1. `CatalogSupplierMapping.supplier_product_name_raw`
2. при необходимости fallback в `RawSupplierFeedProduct.name_raw` через связку `supplier_id + supplier_code`
3. только как legacy fallback `catalog_mapping.Name_D*`
4. если ничего нет, оставлять базовое `MasterCatalog.name_ua/name_ru`

### 7.5 Требуемая стратегия

Для store-level names рекомендуется новое поле:

- `name_strategy`

Значения:

- `base`
- `supplier_random`

Альтернативный boolean `use_base_product_names` хуже, потому что:

- не расширяется, если позже появятся `supplier_fixed`, `manual`, `brand_safe` и т.д.;
- плохо читается рядом с `code_strategy`.

## 8. Новая mapping table для names

### 8.1 Нужна ли отдельная таблица

Да, нужна отдельная таблица `business_store_product_names`.

Причины:

- generated/selected supplier name должен быть stable per `store + internal_product_code`;
- existing name нельзя перезаписывать на следующих export/dry-run;
- нужен отдельный слой, независимый от `master_catalog` и supplier rematch;
- store-level names семантически эквивалентны store-level codes: это external identity overlay.

### 8.2 Рекомендуемые поля

- `id`
- `store_id`
- `internal_product_code`
- `external_product_name`
- `name_source`
- `source_supplier_id`
- `source_supplier_code`
- `source_supplier_product_id`
- `source_supplier_product_name_raw`
- `is_active`
- `created_at`
- `updated_at`

### 8.3 Constraints and indexes

- `unique(store_id, internal_product_code)`
- `index(store_id)`
- `index(internal_product_code)`
- optional `index(source_supplier_id, source_supplier_code)`

### 8.4 Что хранить дополнительно

Рекомендовано хранить:

- `source_supplier_product_name_raw`
  - чтобы не терять original chosen text;
- `name_source`
  - например `base`, `catalog_supplier_mapping`, `raw_supplier_feed`, `catalog_mapping_legacy`, `manual`;

Не обязательно хранить на первом этапе:

- `language`
  - в текущем use case экспорт идёт одним display name, а supplier raw names часто monolingual/mixed;
- отдельный `generated_flag`
  - его роль покрывает `name_source`;
- `source_priority`
  - при фиксированном выборе приоритет уже не нужен.

### 8.5 Поведение генератора

Правило генерации missing names:

- generator смотрит только товары без записи в `business_store_product_names`;
- существующая запись никогда не перезаписывается автоматически;
- при новых SKU генератор создаёт только missing rows;
- `supplier_random` означает random selection только в момент первичного заполнения, а не на каждый export.

## 9. Как выбирать supplier name

### 9.1 Candidate pool

Для одного `internal_product_code` candidate pool строится по:

- всем active `CatalogSupplierMapping` rows для данного `sku`;
- из них берётся непустой `supplier_product_name_raw`.

Допустимая нормализация:

- trim;
- collapse repeated whitespace;
- убрать полностью пустые значения;
- deduplicate identical names case-insensitively.

### 9.2 Random policy

Рекомендуемая стратегия:

- random only once at mapping creation time;
- сохранять выбранный value в `business_store_product_names`;
- дальнейшие dry-run/export используют только mapping table.

Не рекомендовано:

- runtime-random name на каждый запуск;

Причины:

- один и тот же SKU будет "прыгать" между именами;
- появится риск идентификации как нестабильного или дублирующего источника;
- dry-run перестанет быть воспроизводимым;
- orders/catalog diff станут шумными.

## 10. Store-Level Price Markup

### 10.1 Где появляется базовая финальная цена сейчас

Текущая цена собирается в `app/business/dropship_pipeline.py`:

- `_compute_price_for_item_with_source(...)`
- `compute_price_for_item(...)`
- далее применяется price rounding / jitter внутри pipeline offers.

Scheduler `app/services/business_stock_scheduler_service.py` просто вызывает `run_pipeline(enterprise_code, "stock")`.

Следствие:

- текущая базовая цена является runtime-owned результатом dropship pipeline;
- store-level markup не должен встраиваться внутрь этого pipeline на текущем этапе.

### 10.2 Где лучше применять store markup позже

Лучшее место:

- будущий store-aware stock export layer;
- тот же слой должен обслуживать dry-run preview.

Не рекомендовано:

- добавлять markup в `dropship_pipeline`;
- добавлять markup в `business_stock_scheduler_service`.

Причины:

- это изменит базовую рыночную цену для всех current flows;
- затронет competitor logic, balancer и runtime offers;
- нарушит требование "основной магазин должен остаться с базовой ценой".

### 10.3 Для каких stores применять

Рекомендуемое правило:

- base legacy store использует базовую цену без store markup;
- extra markup включается только у stores, где явно включён store-level overlay.

Практически это означает:

- основной runtime enterprise и его legacy publish не меняются;
- future additional stores могут иметь свой markup policy.

### 10.4 Percent или UAH

Рекомендуемый primary mode:

- `percent`

Причины:

- scale-aware для дешёвых и дорогих товаров;
- предсказуемо в сравнении между stores;
- согласуется с существующей проектной моделью `retail_markup/profit_percent`, где markups в основном процентные.

Допустимо поддержать и `uah`, но как secondary mode:

- для отдельных витрин с фиксированным чеком;
- только если бизнес реально подтвердит такой сценарий.

### 10.5 Stable или random_each_run

Рекомендуемая стратегия:

- `stable_per_product`

Не рекомендовано как default:

- `random_each_run`

Причины:

- current pricing уже может использовать `PRICE_JITTER`;
- второй случайный слой усложнит анализ конкурентности;
- diff stock export будет шумным;
- balancer/competitor объяснимость ухудшится.

Если очень нужен random:

- он должен быть deterministic by seed (`store_id + sku`);
- фактически это уже stable-per-product random, а не runtime-random each run.

### 10.6 Округление

Рекомендуемый future порядок:

1. взять базовую итоговую цену после current pricing pipeline;
2. применить store extra markup;
3. выполнить финальное денежное округление тем же экспортным правилом, что и для store-aware price payload.

Не рекомендовано:

- вставлять markup до current pricing round/jitter.

## 11. Рекомендуемые поля в BusinessStore

### 11.1 Assortment

Оставить:

- `catalog_only_in_stock: bool`

### 11.2 Codes

Оставить:

- `code_strategy: str`
- `is_legacy_default: bool`
- `code_prefix: str | null`

UI может поверх этого показывать более простой label:

- `Базовые коды`
- `Уникальные коды магазина`

### 11.3 Names

Добавить позже:

- `name_strategy: String(64), not null, default 'base'`

Constraint:

- `name_strategy IN ('base', 'supplier_random')`

### 11.4 Price markup

Добавить позже:

- `extra_markup_enabled: Boolean, not null, default false`
- `extra_markup_mode: String(32), not null, default 'percent'`
- `extra_markup_min: Numeric(12, 2), nullable true`
- `extra_markup_max: Numeric(12, 2), nullable true`
- `extra_markup_strategy: String(32), not null, default 'stable_per_product'`

Constraints:

- `extra_markup_mode IN ('percent', 'uah')`
- `extra_markup_strategy IN ('stable_per_product', 'random_each_run')`
- `extra_markup_max >= extra_markup_min`
- для `percent` разумный validation cap должен задаваться отдельно в schema/UI.

## 12. Dry-Run Expansion

### 12.1 Что добавить в catalog dry-run

- `catalog_source`
  - `all_products` / `stock_limited`
- `code_strategy`
- `name_strategy`
- `products_with_name_mapping`
- `products_missing_name_mapping`
- `missing_name_samples`
- `price_markup_preview`

### 12.2 Что показывать в sample items

Для catalog:

- `internal_product_code`
- `external_product_code`
- `external_product_name`
- `base_name`
- `name_source`
- `mapping_generated`

Для stock:

- `internal_product_code`
- `external_product_code`
- `external_product_name`
- `base_price`
- `store_markup_applied`
- `final_store_price_preview`

### 12.3 Что должна уметь отдельная action later

По аналогии с codes:

- `generate-missing-names`

Но только после появления mapping table.

### 12.4 Что показывает catalog payload preview

Preview now returns:

- target routing fields:
  - `tabletki_enterprise_code`
  - `tabletki_branch`
  - `legacy_scope_key`
- summary:
  - `master_catalog_total`
  - `candidate_products`
  - `exportable_products`
  - `not_exportable_products`
  - `missing_code_mapping`
  - `missing_name_mapping`
  - `catalog_source`
- preview rows:
  - `internal_product_code`
  - `external_product_code`
  - `base_name`
  - `external_product_name`
  - `barcode`
  - `manufacturer`
  - `brand`
  - `exportable`
  - `reasons`

Rules:

- preview is read-only and does not create missing mappings;
- missing code mapping marks the row as `not exportable`;
- missing name mapping at `supplier_random` marks the row as `not exportable`;
- preview is outside the current master publish path and does not call Tabletki.

### 12.5 Что показывает stock payload preview

Preview now returns:

- target routing fields:
  - `tabletki_enterprise_code`
  - `tabletki_branch`
  - `legacy_scope_key`
- summary:
  - `offer_rows_total`
  - `candidate_products`
  - `exportable_products`
  - `not_exportable_products`
  - `missing_code_mapping`
  - `missing_price_adjustment`
  - `markup_applied_products`
  - `stock_source`
- preview rows:
  - `internal_product_code`
  - `external_product_code`
  - `supplier_code`
  - `qty`
  - `base_price`
  - `markup_percent`
  - `final_store_price_preview`
  - `tabletki_enterprise_code`
  - `tabletki_branch`
  - `exportable`
  - `reasons`

Rules:

- preview is read-only and does not create code mappings or price adjustments;
- missing code mapping marks the row as `not exportable`;
- missing price adjustment at enabled markup marks the row as `not exportable`;
- markup is applied only to preview output;
- `final_store_price_preview` is rounded to a whole number with standard half-up rounding;
- `offers.price` is not modified;
- preview is outside the current stock runtime path and does not call Tabletki.

## 13. UI Impact

### 13.1 Новый блок "Каталог"

- `Все товары`
- `Только товары с остатком`

Storage:

- `catalog_only_in_stock`

### 13.2 Новый блок "Коды"

- `Базовые`
- `Уникальные`

Storage:

- существующие `code_strategy` / `is_legacy_default`

### 13.3 Новый блок "Названия"

- `Базовые из master catalog`
- `Уникальные из supplier names`
- action `Generate missing names`

Storage:

- `name_strategy`

### 13.4 Новый блок "Цены"

- `Доп. наценка включена`
- `Mode: percent / uah`
- `Min`
- `Max`
- `Strategy: stable / random`

Storage:

- `extra_markup_enabled`
- `extra_markup_mode`
- `extra_markup_min`
- `extra_markup_max`
- `extra_markup_strategy`

## 14. Migration / Rollout Plan

1. Зафиксировать target model в docs.
2. Добавить schema поля в `BusinessStore` и новую table `business_store_product_names`.
3. Добавить generator для missing names без live runtime интеграции.
4. Расширить dry-run отчёт по names/markup preview.
5. Расширить UI блоками assortment/codes/names/prices.
6. Проверить основной store в режиме base codes + base names + no extra markup.
7. Завести второй Business enterprise/store overlay и настроить store-level identity.
8. Проверить dry-run для catalog/stock без live export.
9. Только потом внедрять отдельный store-aware catalog export layer.
10. Потом внедрять store-aware stock export layer.
11. Только после стабилизации identity подключать orders/reverse mapping.

Current codebase status after this step:

1. DB foundation added.
2. Dry-run foundation added.
3. UI/API preparation flows added.
4. Live export/runtime still intentionally unchanged.

## 15. Радиус влияния будущей реализации

- `API`
  - `app/routes.py`
  - `app/schemas.py`
- `DB schema`
  - `app/models.py`
  - `alembic/versions/*`
- `scheduler`
  - без изменений на первом шаге rollout
- `pricing`
  - future export-layer only; не трогать базовый `dropship_pipeline`
- `master catalog`
  - использовать как source, не менять базовый publish path
- `integrations`
  - future Tabletki/SalesDrive store-aware export contracts
- `admin-panel`
  - `admin-panel/src/pages/BusinessStoresPage.jsx`

## 16. Risks

- supplier names могут быть низкого качества, с мусором, капсом, дублями и inconsistent language;
- generated store names нельзя автоматически перезаписывать после первого выбора;
- базовые коды и названия основного enterprise нельзя менять;
- store markup может ухудшить конкурентность;
- runtime-random markup может конфликтовать с текущим `PRICE_JITTER` и объяснимостью pricing;
- разные stores могут иметь разные внешние names/codes, но внутренний `internal_product_code` должен оставаться единым;
- нужно избегать catalog identity, по которой разные stores выглядят как один и тот же источник;
- нельзя ломать текущий single-enterprise master publish.
