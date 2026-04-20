# Business Stores UI Handoff

## Статус после архитектурного аудита

Актуальная целевая модель зафиксирована в [docs/business_multistore_architecture.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_multistore_architecture.md).

Следующий слой store-level catalog identity и extra markup зафиксирован в [docs/business_store_catalog_identity.md](/Users/dmitrijnazdrin/inventory_service_1/docs/business_store_catalog_identity.md).

Текущий статус после foundation-реализации:

- `Business Stores` уже хранит `name_strategy` и `extra_markup_*`;
- UI уже умеет сохранять эти поля;
- dry-run уже показывает missing names и stable markup preview;
- live export/runtime по-прежнему не подключён.

Этот handoff теперь трактует страницу `Business-продавцы` только как UI для store-level overlay.

После текущего UI-этапа страница также редактирует enterprise-owned поля выбранного Business enterprise, но ownership данных от этого не меняется:

- enterprise-owned блоки сохраняются в `enterprise_settings`;
- store-owned блоки сохраняются в `business_stores`.

## Базовый принцип

- Business enterprise создаётся и живёт в `enterprise_settings`;
- `business_stores` не создаёт новое master-предприятие;
- `BusinessStore` — overlay поверх существующего `EnterpriseSettings` с `data_format='Business'`;
- live runtime на этой странице не включается.

## Что уже реализовано

- foundation-модели `BusinessStore` и `BusinessStoreProductCode`;
- backend API для `business-stores`;
- dry-run и `generate-missing-codes`;
- UI-страница `Business-продавцы`.

## Роль страницы Business Stores

На странице должны жить только store-owned поля:

- store identity/display;
- legal/tax metadata;
- future external Tabletki/SalesDrive routing;
- future store-level flags;
- legacy scope linkage;
- code strategy;
- migration state;
- dry-run and missing-code actions.

На этой странице не должны редактироваться enterprise-owned runtime fields:

- `branch_id`
- `tabletki_login`
- `tabletki_password`
- `token`
- `catalog_enabled` / `stock_enabled` / `order_fetcher` как global enterprise gates
- `auto_confirm`
- scheduler frequencies

Уточнение после текущего UI-шага:

- enterprise-owned поля временно редактируются на этой же странице для удобства оператора;
- они всё равно должны сохраняться в `enterprise_settings`;
- Enterprise Settings page остаётся валидным местом просмотра и редактирования той же строки.

## Как должен работать UI

- сверху выбирается существующее Business-предприятие из `enterprise_settings where data_format='Business'`;
- если для него нет overlay, UI предлагает создать overlay;
- если overlay уже есть, открывается существующая store-level настройка;
- страница должна явно сообщать, что это overlay, а не создание нового Business enterprise.

Рекомендуемая формулировка кнопки:

- `Создать overlay для выбранного Business-предприятия`

Допустимый укороченный вариант:

- `Создать overlay для выбранного предприятия`

## Что подтягивается по умолчанию из EnterpriseSettings

При создании overlay:

- `business_stores.enterprise_code` <- выбранный `enterprise_code`
- `store_code` <- suggested value, например `business_223`
- `store_name` <- `enterprise_name`
- `tabletki_enterprise_code` <- `enterprise_code`
- `tabletki_branch` <- `branch_id`

Это только defaults. Ownership этих полей не переходит в `enterprise_settings` или обратно.

## Что показывать как read-only summary

На странице нужно показывать только summary базового enterprise profile:

- выбранное Business enterprise;
- global enterprise flags:
  - `catalog_enabled`
  - `stock_enabled`
  - `order_fetcher`
- пояснение:
  - enterprise flags gate the whole runtime;
  - store flags below do not affect current runtime yet.

## Какие enterprise-owned поля теперь показываются и редактируются на странице

Источник данных: `enterprise_settings`.

- `enterprise_code`
  - read-only
- `enterprise_name`
- `branch_id`
- `data_format`
  - read-only / disabled
- `stock_upload_frequency`
- `catalog_upload_frequency`
- `tabletki_login`
- `tabletki_password`
- `token`
- `catalog_enabled`
- `stock_enabled`
- `order_fetcher`
- `auto_confirm`
- `stock_correction`

Для этих полей на странице нужна отдельная кнопка:

- `Сохранить предприятие`

Это отдельный save flow от overlay.

## Какие поля editable в Business Stores

- `store_code`
  - только при первичном создании
- `store_name`
- `legal_entity_name`
- `tax_identifier`
- `legacy_scope_key`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `salesdrive_enterprise_id`
- `code_strategy`
- `code_prefix`
- `is_legacy_default`
- `catalog_enabled`
- `stock_enabled`
- `orders_enabled`
- `catalog_only_in_stock`
- `migration_status`
- `is_active`

Для этих полей на странице нужна отдельная кнопка:

- `Сохранить настройки магазина`

## Следующий UI-слой для multistore identity

Не реализовано в этом шаге, но будущий UI для `Business Stores` должен добавить ещё четыре понятных блока поверх текущей overlay-модели.

### 1. Каталог

UI labels:

- `Все товары`
- `Только товары с остатком`

Storage recommendation:

- оставить существующее `catalog_only_in_stock`

### 2. Коды

UI labels:

- `Базовые коды`
- `Уникальные коды магазина`

Storage recommendation:

- не добавлять новый boolean как primary source of truth;
- оставить `code_strategy` и `is_legacy_default`;
- UI может поверх этого давать более простой label.

### 3. Названия

UI labels:

- `Базовые названия из master catalog`
- `Уникальные названия из supplier names`
- action `Generate missing names`

Storage recommendation:

- будущий `name_strategy`
  - `base`
  - `supplier_random`
- отдельная table `business_store_product_names`

UI rule:

- generated names должны быть стабильными;
- existing mapping нельзя молча перегенерировать;
- action должна обогащать только missing names.

### 4. Цены

UI labels:

- `Доп. наценка включена`
- `Mode: percent / uah`
- `Min`
- `Max`
- `Strategy: stable / random`

Storage recommendation:

- `extra_markup_enabled`
- `extra_markup_mode`
- `extra_markup_min`
- `extra_markup_max`
- `extra_markup_strategy`

UI rule:

- это future store-aware export overlay;
- текущий runtime и базовая цена enterprise от этого не меняются.

## Что должен показывать future dry-run

Дополнительно к текущему dry-run:

- `catalog source`
  - `all products` / `stock-limited`
- `code strategy`
- `name strategy`
- `products with mapped names`
- `missing product names`
- `price markup preview`

В sample rows позже стоит показывать:

- `internal_product_code`
- `external_product_code`
- `external_product_name`
- `base_name`
- `base_price`
- `store_price_preview`
- `name_source`

## Какие поля high-risk

Нужны подсказки/guardrails:

- `legacy_scope_key`
- `tabletki_enterprise_code`
- `tabletki_branch`
- `salesdrive_enterprise_id`
- `code_strategy`
- `code_prefix`
- `takes_over_legacy_scope`

`takes_over_legacy_scope` нельзя позиционировать как обычный toggle. До появления live store-aware runtime он должен оставаться только migration intent flag.

## Какие поля скрыть или вынести в legacy / advanced

- `salesdrive_enterprise_code`
- `salesdrive_store_name`
- позже `code_prefix`, если `code_strategy != 'prefix_mapping'`

## Почему могут “не сохраняться” `legal_entity_name` / `tax_identifier`

По текущему коду это не похоже на schema/backend ownership issue:

- поля есть в модели;
- поля есть в create/update schemas;
- backend create/update routes их записывают;
- response schema их возвращает.

Наиболее вероятная текущая причина — frontend state issue:

- страница после reload опирается на первый store в `storesForSelectedEnterprise[0]`;
- при нескольких overlay для одного enterprise форма может визуально перескочить на другой store;
- пользователю кажется, что поля не сохранились.

Для следующего UI/API шага надо проверить и исправить:

- сохранение выбранного `selectedStoreId` после reload;
- отказ от автосброса формы к “первому overlay”;
- более явное отображение `legal_entity_name` / `tax_identifier` в UI после save.

## Что не менялось

- runtime не менялся;
- live export не подключался;
- scheduler-ы не менялись;
- `dropship_pipeline.py` не менялся;
- `mapping_branch` runtime не менялся;
- order fetch/import/order sender runtime не менялся.

## Следующий этап

Следующий prompt должен быть про UI/API cleanup без runtime rewrites:

- вычистить ownership boundaries;
- убрать enterprise-owned поля из Business Settings page;
- сделать Business Stores page overlay-only;
- поправить save/selection behavior;
- добавить правильные read-only summaries и подсказки.

Отдельный следующий prompt после этого может быть про non-runtime UI/API expansion для:

- `name_strategy`
- `business_store_product_names`
- `generate-missing-names`
- extra markup fields
- dry-run preview expansion
