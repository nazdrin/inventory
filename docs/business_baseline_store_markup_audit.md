# Business Baseline Store Markup Audit

## 1. Вопрос аудита

Нужно установить, участвуют ли store-level extra markup настройки в runtime для Business enterprise в режиме `baseline`, и если нет — как безопасно расширить систему, чтобы baseline enterprise могли использовать store markup без перевода в `custom`.

Под extra markup в этом аудите понимаются:

- `extra_markup_enabled`
- `extra_markup_mode`
- `extra_markup_min`
- `extra_markup_max`
- `extra_markup_strategy`
- связанные `BusinessStoreProductPriceAdjustment` rows

## 2. Текущее фактическое поведение

### 2.1 Baseline stock

Для enterprise в режиме `baseline` store extra markup в runtime stock **не работает**.

Фактическое поведение:

- baseline stock path идёт через legacy stock contour;
- routing строится через `mapping_branch`;
- payload строится без участия `BusinessStore`;
- store-level price overlay и `BusinessStoreProductPriceAdjustment` не используются.

Итог:

- `extra_markup_enabled` и связанные store pricing fields для baseline stock сейчас **не runtime-driving**;
- они не влияют на live stock payload;
- они не влияют и на baseline preview.

### 2.2 Baseline catalog

Для enterprise в режиме `baseline` store extra markup в catalog **не работает**.

Фактическое поведение:

- baseline catalog preview/export использует `MasterCatalog`;
- branch берётся из `EnterpriseSettings.branch_id`;
- payload каталога вообще не содержит price logic;
- `BusinessStore` pricing fields не участвуют.

Итог:

- для baseline catalog extra markup **не применяется вообще**;
- не частично, а полностью отсутствует как runtime concept.

### 2.3 Custom stock

Для enterprise в режиме `custom` store extra markup **работает частично и только в stock contour**.

Фактическое поведение:

- store-aware stock preview/export загружает `BusinessStore`;
- загружает `BusinessStoreProductPriceAdjustment`;
- применяет `apply_extra_markup(...)`;
- в payload использует `final_store_price_preview`.

Но важно:

- raw поля `extra_markup_min` / `extra_markup_max` сами по себе не считаются в момент publish;
- runtime использует уже подготовленные `BusinessStoreProductPriceAdjustment` rows;
- если markup включён, но adjustment rows не созданы, store-aware stock становится неполным или неэкспортируемым.

### 2.4 Custom catalog

Для enterprise в режиме `custom` store extra markup тоже **не работает**, потому что catalog contour не содержит price layer.

Итог по extra markup:

- baseline stock: **нет**
- baseline catalog: **нет**
- custom stock: **да**
- custom catalog: **нет**

## 3. Где это определяется в коде

### 3.1 Mode resolver

Режим предприятия определяется в:

- [app/services/business_runtime_mode_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_runtime_mode_service.py)

Ключевые функции:

- `resolve_business_runtime_mode_from_db(...)`
- `derive_stock_runtime_path(...)`
- `derive_catalog_runtime_path(...)`

Семантика:

- `baseline` -> `baseline_legacy` для stock и catalog
- `custom` -> `store_aware` stock и `enterprise_identity` catalog

### 3.2 Точка отсечения для stock

Главная развилка находится в:

- [app/services/business_stock_publish_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_stock_publish_service.py)

Факт:

- если `business_runtime_mode == baseline`, используется:
  - dry-run -> `build_business_baseline_stock_preview(...)`
  - live -> `run_pipeline(..., "stock")`
- если `business_runtime_mode == custom`, используется:
  - `publish_enabled_business_store_stocks(...)`

Это и есть главная архитектурная точка, где baseline contour уходит мимо `BusinessStore`.

### 3.3 Baseline stock path не зависит от BusinessStore

Baseline preview реализован в:

- [app/services/business_baseline_stock_preview_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_baseline_stock_preview_service.py)

Ключевой факт:

- preview вызывает `build_stock_payload(session, enterprise_code)`;
- в результате явно возвращает:
  - `depends_on_business_stores = False`
  - `price_source = legacy_algorithm`

Сам legacy builder находится в:

- [app/business/dropship_pipeline.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/dropship_pipeline.py)

Ключевой код:

- `build_stock_payload(...)`
- `build_best_offers_by_city(...)`
- `_load_branch_mapping(...)`

Этот путь использует:

- `offers`
- legacy pricing snapshot / competitor logic / jitter
- `mapping_branch`

Он не загружает:

- `BusinessStore`
- `BusinessStoreProductPriceAdjustment`
- `extra_markup_*`

### 3.4 Custom stock path зависит от BusinessStore pricing overlay

Store-aware preview реализован в:

- [app/business/business_store_stock_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_stock_preview.py)

Ключевые места:

- `_load_store_product_price_adjustment_map(...)`
- `apply_extra_markup(...)`
- `final_store_price_preview`

Store-aware export реализован в:

- [app/business/business_store_stock_exporter.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_stock_exporter.py)

Ключевой факт:

- exporter берёт `final_store_price_preview` из preview rows;
- именно это значение уходит в:
  - `Price`
  - `PriceReserve`

То есть price overlay реально участвует только в custom stock contour.

### 3.5 Baseline catalog path не зависит от BusinessStore pricing

Baseline catalog preview:

- [app/business/business_baseline_catalog_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_baseline_catalog_preview.py)

Baseline catalog export:

- [app/business/business_baseline_catalog_exporter.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_baseline_catalog_exporter.py)

Они используют:

- `MasterCatalog`
- `EnterpriseSettings.branch_id`

И не используют:

- `BusinessStore`
- `extra_markup_*`
- `BusinessStoreProductPriceAdjustment`

### 3.6 Effective catalog branch при baseline тоже уходит мимо store pricing

В:

- [app/business/business_store_catalog_preview.py](/Users/dmitrijnazdrin/inventory_service_1/app/business/business_store_catalog_preview.py)

функция `build_effective_business_store_catalog_payload_preview(...)` при `baseline` просто делегирует в baseline catalog preview.

То есть даже если оператор открывает store-oriented screen, effective baseline catalog path всё равно не использует store pricing.

## 4. Фактический вывод

Текущий вывод по коду:

- для enterprise в режиме `baseline` store extra markup сейчас **не применяется** ни в runtime stock, ни в runtime catalog;
- baseline stock полностью идёт по legacy enterprise-level path;
- baseline catalog полностью идёт по baseline enterprise-level path;
- store pricing fields для baseline сейчас существуют как:
  - DB/UI-level configuration
  - потенциальная будущая настройка

Но не как runtime-driving layer.

Дополнительный UX-факт:

- в `BusinessStoresPage` extra markup поля сейчас видимы и редактируемы даже для baseline;
- при этом branch/scope уже disabled в baseline, а extra markup — нет;
- это создаёт риск ложного ожидания, будто baseline enterprise уже использует store price overlay.

## 5. Риски внедрения поддержки baseline store markup

### 5.1 Риск сломать legacy baseline stock contour

Baseline stock path сейчас опирается на существующий `dropship_pipeline`. Любое прямое вмешательство в legacy builder может неожиданно изменить цены сразу для всех baseline enterprises.

### 5.2 Риск неявного изменения цен

Если markup silently включить для baseline без явной диагностики и rollout guard, оператор может увидеть смену цен без понимания, какая часть системы её внесла:

- legacy pricing
- jitter
- store markup overlay

### 5.3 Риск смешать два слоя routing

Baseline routing сейчас:

- city -> branch через `mapping_branch`

Store markup живёт в:

- `BusinessStore`

Чтобы смешать их, нужен явный слой branch -> store resolution. Иначе появится неочевидное поведение:

- какой store выбран для branch;
- что делать при duplicate stores;
- что делать если branch есть в `mapping_branch`, но нет корректного active store.

### 5.4 Риск partially-configured mode

Если разрешить baseline markup без guard/diagnostics, enterprise может оказаться в полусостоянии:

- часть branch получила store overlay
- часть branch осталась на чистом legacy price

Это опасно для продового pricing behavior.

### 5.5 Риск смешать задачу с catalog migration

Store markup для baseline — это stock pricing extension. Её не нужно смешивать:

- с catalog identity
- с baseline/custom catalog semantics
- с order contour

## 6. Варианты доработки

### Вариант A — минимально безопасный

Суть:

- baseline stock routing остаётся legacy;
- `dropship_pipeline` не меняется как источник offers, competitor logic и jitter;
- после сборки baseline stock rows добавляется отдельный post-processing overlay:
  - `branch -> BusinessStore`
  - apply store markup to final `price` / `price_reserve`
- catalog path не трогать.

Практически это означает:

- baseline payload строится как сейчас;
- потом отдельный adapter enriches rows per branch;
- adapter использует store lookup только для pricing overlay.

Плюсы:

- минимальный радиус изменения;
- не надо переписывать baseline routing;
- catalog не затрагивается;
- orders не затрагиваются;
- rollout можно делать через allowlist/flag.

Минусы:

- появляется hybrid path;
- baseline runtime перестаёт быть полностью pure-legacy;
- нужен аккуратный branch -> store resolver и понятные warnings.

Радиус влияния:

- API: минимальный
- DB schema: не нужен
- scheduler: минимальный
- pricing: да
- catalog: нет
- stock: да
- orders: нет
- admin-panel: да, только clarifying UI
- integrations: только stock export payload

### Вариант B — архитектурно чище

Суть:

- ввести отдельный hybrid runtime layer:
  - baseline routing
  - baseline catalog semantics
  - baseline order semantics
  - store pricing overlay as explicit runtime capability

То есть не post-processing patch, а отдельный declared path:

- `baseline_with_store_markup`
или equivalent capability layer inside baseline stock runtime.

Плюсы:

- чище архитектурно;
- capability явно описана;
- лучше долгосрочная читаемость runtime behavior.

Минусы:

- выше объём изменений;
- выше риск accidentally расползтись в новый runtime mode;
- больше touches в scheduler/reporting/docs/UI.

Радиус влияния:

- API: средний
- DB schema: возможно не нужен, но control-plane complexity выше
- scheduler: средний
- pricing: да
- catalog: косвенно docs/UI
- stock: да
- orders: нет
- admin-panel: да
- integrations: stock only

## 7. Рекомендуемый путь

Рекомендую `Вариант A`.

Причина:

- он safest;
- не требует ломать legacy baseline routing;
- не требует трогать catalog;
- не требует нового enterprise runtime mode;
- позволяет ввести baseline store markup как локальный stock-only overlay;
- проще dry-run compare against current baseline.

Ключевой принцип:

- baseline enterprise остаётся baseline;
- baseline stock algorithm остаётся legacy;
- store markup добавляется как поздний, явно диагностируемый overlay только на финальном stock payload stage.

## 8. Пошаговый план внедрения

### Phase 1. Read-only diagnostics

Добавить read-only report для baseline stock markup readiness:

- branch list from `mapping_branch`
- matching active store by `(enterprise_code, tabletki_branch)`
- duplicate stores per branch
- stores without markup config
- stores with markup enabled but without generated adjustments

Цель:

- понять, можно ли безопасно применять overlay branch-by-branch.

### Phase 2. Branch -> store resolver

Добавить отдельный read-only resolver для baseline pricing overlay:

- input: `enterprise_code`, `branch`
- output: one active `BusinessStore` or explicit reason:
  - missing_store_for_branch
  - ambiguous_store_for_branch
  - markup_disabled

Без silent fallback.

### Phase 3. Overlay adapter only for stock payload

После `build_stock_payload(...)` добавить optional adapter:

- читает final baseline stock rows;
- находит store per branch;
- применяет markup only to `price` / `price_reserve`;
- не меняет qty, routing, code mapping, supplier logic, jitter logic upstream.

Важно:

- catalog не трогать;
- `dropship_pipeline` pricing formula не переписывать;
- просто enrich final rows.

### Phase 4. Dry-run compare

Нужен compare report:

- baseline price before overlay
- price after overlay
- affected branches count
- affected rows count
- skipped rows / skipped branches
- reasons

Цель:

- безопасно оценить эффект до live send.

### Phase 5. Scoped rollout

Включать не глобально, а через scoped control:

- enterprise allowlist
- или enterprise-level feature flag/capability

Сначала для одного baseline enterprise.

### Phase 6. UI clarification

После backend dry-run readiness:

- в `BusinessStoresPage` для baseline показать, что extra markup теперь относится только к stock pricing overlay;
- если baseline overlay ещё не enabled, явно показать read-only explanation;
- не создавать впечатление, что catalog начал зависеть от store markup.

## 9. Что менять не нужно

В первой фазе не нужно:

- трогать catalog identity migration;
- трогать custom stock contour;
- трогать order runtime;
- трогать outbound/status runtime;
- переписывать `dropship_pipeline` pricing core;
- менять DB schema;
- делать большой page redesign;
- смешивать baseline store markup с catalog store overlays.

Отдельно важно:

- не надо пытаться “сделать baseline enterprise почти custom”;
- задача узкая: добавить store pricing overlay к baseline stock, а не переделать baseline runtime целиком.
