# Business Stores UI Audit For Enterprise Catalog Model

Дата аудита: 2026-04-22

## Status Update

После этого аудита UI был частично приведён к модели `store = branch overlay` без изменения DB schema:

- `tabletki_branch` больше не является свободным text input;
- для выбранного enterprise он выбирается только из `mapping_branch` через read-only meta endpoint;
- если текущее значение store branch отсутствует в `mapping_branch`, UI явно показывает warning и временно сохраняет orphan value до явного сохранения;
- основная store form теперь сфокусирована на:
  - store identity
  - stock routing
  - stock scope
  - orders routing
  - pricing
- technical/deprecated fields вынесены из основной формы в `Advanced / deprecated технические поля`.

Что осталось актуальным из этого аудита:

- catalog controls должны оставаться enterprise-level;
- `catalog_only_in_stock` operator-facing уже не должен возвращаться в обычный store block;
- `migration_status`, `takes_over_legacy_scope`, `tabletki_enterprise_code`, `code/name strategy` остаются technical/deprecated until later cleanup.

## Scope

Аудит выполнен после Stage 1-6 enterprise-level catalog identity migration.

Цель:

- не менять UI;
- зафиксировать, какие текущие поля и блоки страницы `BusinessStoresPage` стали misleading;
- определить, что можно перенести на enterprise-level;
- определить, что нельзя скрывать без backend compatibility step.

Анализированные файлы:

- [admin-panel/src/pages/BusinessStoresPage.jsx](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/pages/BusinessStoresPage.jsx)
- [admin-panel/src/pages/BusinessSettingsPage.jsx](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/pages/BusinessSettingsPage.jsx)
- [admin-panel/src/pages/EnterprisePanel.js](/Users/dmitrijnazdrin/inventory_service_1/admin-panel/src/pages/EnterprisePanel.js)
- [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- [app/schemas.py](/Users/dmitrijnazdrin/inventory_service_1/app/schemas.py)
- [app/models.py](/Users/dmitrijnazdrin/inventory_service_1/app/models.py)
- [app/services/business_store_catalog_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_store_catalog_publish_service.py)
- [app/services/business_store_stock_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_store_stock_publish_service.py)
- [app/business/business_store_catalog_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_catalog_preview.py)
- [app/business/business_store_stock_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_stock_preview.py)
- [app/business/business_store_order_mapper.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_order_mapper.py)
- [app/business/business_store_tabletki_outbound_mapper.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_tabletki_outbound_mapper.py)

## Executive Summary

После Stage 1-6 основная архитектурная проблема UI такая:

- страница `BusinessStoresPage` уже содержит enterprise block и store block;
- но store block всё ещё выглядит как владелец catalog identity;
- при этом runtime catalog identity уже может быть enterprise-level;
- а часть store-level полей до сих пор нужна только из-за rollback compatibility.

Ключевые выводы:

1. Каталог больше нельзя визуально объяснять как store-owned feature.
2. `enterprise_settings.branch_id` должен быть явно показан как branch каталога.
3. `business_stores.tabletki_branch` должен быть показан как branch магазина для stock/orders, а не как branch каталога.
4. `business_stores.catalog_enabled` нельзя было просто удалить из UI, пока backend eligibility зависел от него.
5. `catalog_only_in_stock` пока нельзя считать чисто enterprise-level настройкой, потому что current enterprise catalog mode использует store-compatible assortment и читает этот флаг из `BusinessStore`.
6. concern про отдельную create-кнопку уже фактически закрыт: текущая страница строит existing form или unsaved draft автоматически.

## 1. Current UI Structure

Текущая страница уже организована как:

1. `1. Предприятие`
2. `2. Основные настройки предприятия`
3. `3. Разрешения предприятия`
4. `4. Новый магазин Business-контура` / `4. Магазин Business-контура`
5. `Список магазинов выбранного предприятия`
6. store actions and preview blocks

### Что уже хорошо

- отдельная верхняя create-кнопка убрана;
- при выборе enterprise форма магазина показывается сразу;
- если store нет, строится unsaved draft;
- enterprise flags и store flags уже разделены лучше, чем раньше;
- UI уже русифицирован лучше, чем исходная версия.

### Что сейчас misleading

- store section всё ещё содержит:
  - `catalog_enabled`
  - `code_strategy`
  - `code_prefix`
  - `name_strategy`
  - `Preview каталога`
  - `Сгенерировать коды`
  - `Сгенерировать названия`
- это создаёт ощущение, что catalog identity всё ещё принадлежит магазину.

После Stage 3 это уже не соответствует целевой модели:

- catalog branch берётся из `enterprise_settings.branch_id`;
- catalog code/name identity может идти через `BusinessEnterpriseProductCode` / `BusinessEnterpriseProductName`.

## 2. Backend Dependency Matrix

### `enterprise_settings.catalog_enabled`

| Aspect | Status |
| --- | --- |
| Где хранится | `enterprise_settings.catalog_enabled` |
| Где используется | catalog scheduler, catalog publish eligibility, enterprise catalog preview |
| Можно ли убрать из UI | Нет |
| Можно ли скрыть | Нет |
| Рекомендация | Оставить как основной enterprise-level catalog gate |

Фактические runtime points:

- `app/services/business_store_catalog_publish_service.py`
- `app/business/business_enterprise_catalog_preview.py`
- `app/services/catalog_scheduler_service.py`

### `enterprise_settings.branch_id`

| Aspect | Status |
| --- | --- |
| Где хранится | `enterprise_settings.branch_id` |
| Где используется | enterprise catalog preview/export, master catalog export |
| Можно ли убрать из UI | Нет |
| Можно ли скрыть | Нет |
| Рекомендация | Показывать как `Branch каталога / основной branch предприятия` |

Фактические runtime points:

- `app/business/business_enterprise_catalog_preview.py`
- `app/business/business_enterprise_catalog_exporter.py`
- `app/services/business_store_catalog_publish_service.py`
- `app/business/tabletki_master_catalog_exporter.py`

### `business_stores.catalog_enabled`

| Aspect | Status |
| --- | --- |
| Где хранится | `business_stores.catalog_enabled` |
| Где используется | deprecated compatibility field for catalog eligibility / storage |
| Можно ли убрать из UI | Да, как primary operator control |
| Можно ли скрыть | Да |
| Нужен ли backend cleanup | Уже выполнен для enterprise-level mode |

Статус после compatibility cleanup:

- operator-facing catalog gate = `EnterpriseSettings.catalog_enabled`;
- `BusinessStore.catalog_enabled` больше не должен блокировать enterprise-level catalog eligibility;
- field остаётся в storage как deprecated compatibility field.

### `business_stores.code_strategy`

| Aspect | Status |
| --- | --- |
| Где хранится | `business_stores.code_strategy` |
| Где используется | rollback catalog preview, stock preview passthrough behavior, code generator, outbound passthrough rule |
| Можно ли убрать из UI | Не сразу |
| Можно ли скрыть | Да, но только как deprecated/advanced |
| Нужен ли backend cleanup | Да, если цель полностью убрать store-owned catalog identity |

Фактические runtime points:

- `app/business/business_store_catalog_preview.py`
- `app/business/business_store_stock_preview.py`
- `app/business/business_store_code_generator.py`
- `app/business/business_store_tabletki_outbound_mapper.py`

Примечание:

- даже после Stage 6 store `code_strategy` всё ещё влияет на rollback path и на `legacy_same` passthrough semantics.

### `business_stores.name_strategy`

| Aspect | Status |
| --- | --- |
| Где хранится | `business_stores.name_strategy` |
| Где используется | rollback catalog preview and name generation |
| Можно ли убрать из UI | Не сразу |
| Можно ли скрыть | Да, как deprecated/advanced |
| Нужен ли backend cleanup | Да, если strategy переносится на enterprise-level полностью |

Фактические runtime points:

- `app/business/business_store_catalog_preview.py`
- `app/business/business_store_name_generator.py`
- old dry-run endpoints in `app/routes.py`

### `business_stores.catalog_only_in_stock`

| Aspect | Status |
| --- | --- |
| Где хранится | `business_stores.catalog_only_in_stock` |
| Где используется | current assortment scope both in rollback catalog preview and in enterprise catalog `store_compatible` mode |
| Можно ли убрать из UI | Нет |
| Можно ли скрыть | Не рекомендуется |
| Нужен ли backend redesign | Да, если assortment ownership будет отцепляться от store |

Критичный факт:

- `app/business/business_store_catalog_preview.py::resolve_store_catalog_candidate_scope`
  использует `store.catalog_only_in_stock`
- `build_effective_business_store_catalog_payload_preview(...)`
  в enterprise mode всё равно вызывает enterprise preview с `assortment_mode="store_compatible"` и `store_id`

Это значит:

- catalog identity уже может быть enterprise-level;
- но assortment restriction всё ещё store-owned.

Рекомендация:

- не убирать поле;
- operator-facing control перенести на уровень enterprise/catalog block;
- physical storage оставить в `BusinessStore` как compatibility field главного магазина каталога;
- главным store считать active магазин, чей `tabletki_branch` совпадает с `EnterpriseSettings.branch_id`.

### `business_stores.tabletki_branch`

| Aspect | Status |
| --- | --- |
| Где хранится | `business_stores.tabletki_branch` |
| Где используется | stock target branch, inbound store resolution, outbound store resolution, current catalog compatibility checks |
| Можно ли убрать из UI | Нет |
| Можно ли скрыть | Нет |
| Рекомендация | Оставить store-level и переименовать как branch магазина для stock/orders |

Фактические runtime points:

- `app/services/business_store_stock_publish_service.py`
- `app/business/business_store_order_mapper.py`
- `app/business/business_store_tabletki_outbound_mapper.py`
- `app/services/business_store_catalog_publish_service.py` still checks it in eligibility

Важно:

- для catalog в новой модели это уже не primary target branch;
- но для stock/orders это остаётся core routing field.

### `enterprise_settings.order_fetcher`

| Aspect | Status |
| --- | --- |
| Где хранится | `enterprise_settings.order_fetcher` |
| Где используется | global intake gate for order scheduler |
| Можно ли убрать из UI | Нет |
| Рекомендация | Оставить только на enterprise-level |

Фактические runtime points:

- `app/services/order_scheduler_service.py`
- `app/services/order_fetcher.py`

### `business_stores.orders_enabled`

| Aspect | Status |
| --- | --- |
| Где хранится | `business_stores.orders_enabled` |
| Где используется | API/storage/UI mostly; active runtime usage currently minimal |
| Можно ли убрать из UI | Не рекомендуется, но нужно честно пометить как evolving field |
| Нужен ли backend cleanup | Да, если поле должно стать реальным store-level runtime gate |

По grep-аудиту:

- поле сохраняется через `routes.py`;
- но прямой активной runtime gating в `order_fetcher.py` сейчас не обнаружено.

Это значит:

- UI показывает `orders_enabled` как runtime control;
- backend пока не использует его как основной gate так же явно, как `stock_enabled` или `catalog_enabled`.

## 3. Target UI Structure

### A. Блок 1. Выбор Business-предприятия

Оставить.

Требование:

- выбор enterprise;
- сразу below показывать enterprise summary;
- current auto-draft/new-store behavior сохранить.

### B. Блок 2. Основные настройки предприятия

Этот блок должен стать явным owner-ом catalog runtime.

Показывать:

- `enterprise_name`
- `enterprise_code` read-only
- `branch_id` как `Branch каталога / основной branch предприятия`
- `catalog_enabled` как `Каталог предприятия включён`
- `stock_enabled` как `Остатки предприятия разрешены`
- `order_fetcher` как `Получение заказов разрешено`
- credentials/integration fields, если страница остаётся владельцем этих полей

Текст:

- каталог теперь enterprise-level;
- target branch для catalog publish берётся из `enterprise_settings.branch_id`.
- ограничение ассортимента каталога по остаткам тоже должно показываться здесь;
- оно управляет `catalog_only_in_stock` главного магазина каталога.

### C. Блок 3. Каталог предприятия / Catalog identity

Лучше выделить отдельный block, а не прятать всё в store block.

Должен включать:

- current identity status
- пояснение, что codes/names enterprise-level
- future actions:
  - preview
  - publish
  - generate/check enterprise product codes
  - generate/check enterprise product names

Если API под enterprise actions пока нет:

- block можно сделать informational/read-only на первом UI этапе.

Для текущей совместимой реализации:

- `catalog_only_in_stock` редактируется из enterprise-level block;
- сохраняется через existing store update route для главного магазина каталога;
- UI должен явно показывать:
  - `Главный магазин каталога: {store_code} · Branch {tabletki_branch} · Scope {legacy_scope_key}`
  - либо explicit warning про missing/ambiguous main catalog store.

### D. Блок 4. Магазин / stock-pricing-orders overlay

Этот блок должен перестать быть catalog-identity owner.

Оставить тут:

- `store_code`
- `store_name`
- `legal_entity_name`
- `tax_identifier`
- `is_active`
- `migration_status`
- `tabletki_branch` как `Branch магазина для остатков/заказов`
- `legacy_scope_key` как `Scope остатков`
- `stock_enabled`
- `orders_enabled`
- `extra_markup_*`
- `salesdrive_enterprise_id`
- `takes_over_legacy_scope`
- `catalog_only_in_stock` как advanced assortment constraint

## 4. What To Remove / Hide From Store-Level

### Убрать из основного operator-facing store block

- `catalog_enabled` toggle
- `code_strategy`
- `code_prefix`
- `name_strategy`
- catalog preview/publish actions как primary store actions
- catalog code/name generation actions как primary store actions

### Что делать вместо жёсткого удаления

- `catalog_enabled`:
  - не показывать как обычный operator toggle;
  - либо hidden preserved field;
  - либо deprecated advanced read-only field;
  - либо backend cleanup first.
- `code_strategy`, `code_prefix`, `name_strategy`:
  - вынести в deprecated/advanced section до полного backend cleanup;
  - не держать в основной операционной зоне страницы.

## 5. Safe Migration UI Plan

### Phase UI-1

- не менять payload;
- не менять backend;
- переименовать labels;
- визуально отделить enterprise catalog controls от store stock/orders controls;
- убрать `store.catalog_enabled` из основной operator-visible зоны;
- показать его только как deprecated/advanced if needed.

### Phase UI-2

- вынести catalog preview/generation controls в enterprise-level block;
- store block оставить только про stock/pricing/orders/routing;
- `catalog_only_in_stock` оставить в advanced subsection с объяснением, что это store-compatible assortment control.

### Phase UI-3

- backend cleanup:
  - убрать `business_stores.catalog_enabled` из catalog eligibility;
  - решить судьбу `code_strategy` / `name_strategy` / store-level catalog generators;
  - при необходимости ввести enterprise-level UI actions и enterprise-level schemas.

## 6. Risks

### 1. Hiding `store.catalog_enabled` too early

Если скрыть store catalog flag без backend compatibility handling:

- migrated store может внезапно стать `store_catalog_disabled`;
- оператор не увидит причину;
- catalog publish eligibility будет непредсказуемой.

### 2. Confusing branch ownership

Если UI продолжит показывать `store.tabletki_branch` как branch каталога:

- оператор может считать, что catalog publish идёт в store branch;
- но Stage 3 runtime already sends catalog to `enterprise_settings.branch_id`.

### 3. Catalog actions in wrong block

Если оставить preview/generation buttons внутри store block:

- UI будет закреплять старую mental model;
- оператор будет думать, что каталог всё ещё принадлежит магазину.

### 4. `orders_enabled` overstatement

Сейчас UI показывает store `orders_enabled` как полноценный runtime switch, но backend dependency пока слабее и не так очевидна, как у `enterprise.order_fetcher`.

### 5. `catalog_only_in_stock` ownership confusion

Если рано перенести это поле на enterprise-level:

- можно потерять текущую store-compatible assortment semantics;
- comparison between old and new catalog scope станет менее прозрачным.

## 7. Final Recommendation

### Можно ли менять UI сразу

`Да, но только в совместимом режиме`

Можно сразу делать visual refactor страницы под новую модель, если:

- payload к backend не меняется;
- `business_stores.catalog_enabled` не теряется при save;
- deprecated store catalog fields либо скрыты безопасно, либо сохранены в advanced/read-only section.

### Нужен ли backend cleanup до полного UI refactor

`Нет, для enterprise-level catalog gate уже не нужен`

Минимальный backend cleanup для `catalog_enabled` выполнен:

- enterprise-level catalog eligibility управляется через `EnterpriseSettings.catalog_enabled`;
- `BusinessStore.catalog_enabled` остаётся только как compatibility field.

### Exact next implementation prompt

Рекомендуемый следующий prompt:

> Измени `admin-panel/src/pages/BusinessStoresPage.jsx` под enterprise-level catalog model без изменения backend/API.  
>  
> 1. Вынеси catalog-related controls из store block в отдельный enterprise-level block `Каталог предприятия`.  
> 2. В store block оставь только stock/pricing/orders/routing fields.  
> 3. `business_stores.catalog_enabled` не удаляй из payload, но убери из основной operator-visible формы: либо hidden preserved field, либо deprecated advanced read-only section.  
> 4. `code_strategy`, `code_prefix`, `name_strategy` убери из основной формы магазина и перенеси в deprecated advanced subsection.  
> 5. `catalog_only_in_stock` оставь в store block как advanced assortment control с пояснением, что он влияет на store-compatible catalog scope.  
> 6. `enterprise_settings.branch_id` покажи как `Branch каталога / основной branch предприятия`; `business_stores.tabletki_branch` покажи как `Branch магазина для остатков/заказов`.  
> 7. Не меняй backend, API, schema и business logic. Только UI layout/text/visibility.

## Practical Bottom Line

Текущее состояние:

- backend migration stages 1-6 выглядят стабильно;
- UI уже может показывать catalog assortment control на уровне enterprise;
- но делать это нужно как compatibility-preserving refactor, а не как мгновенное удаление store catalog semantics из payload/runtime.
