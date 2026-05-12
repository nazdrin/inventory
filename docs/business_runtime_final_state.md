# Business Runtime Final State

## 1. Общая модель

Business-контур теперь работает в двух enterprise-level режимах:

- `baseline`
- `custom`

Режим задаётся на уровне `enterprise_settings.business_runtime_mode` и определяет сразу весь runtime-контур предприятия:

- catalog
- stock
- orders

Это именно per-enterprise модель. Один enterprise не должен одновременно частично жить в baseline catalog и custom stock без отдельной явной архитектуры.

## 2. Что означает `baseline`

Для enterprise в режиме `baseline`:

- catalog работает по baseline legacy path
- stock работает по baseline legacy path
- orders идут по baseline passthrough path

В этом режиме `BusinessStore` не является источником runtime для catalog/stock.

Важно:

- store overlays могут существовать для admin visibility и branch overlay
- `tabletki_branch` и `legacy_scope_key` магазина не управляют baseline catalog runtime
- `tabletki_branch` и `legacy_scope_key` магазина не управляют baseline stock runtime

## 3. Что означает `custom`

Для enterprise в режиме `custom`:

- catalog работает по enterprise identity path
- stock работает по store-aware path
- orders работают по custom/store-aware business contour

В этом режиме:

- catalog identity enterprise-level
- stock routing и stock overlay store-level
- order mapping использует custom business contour

## 4. Как работает `223`

`223` зафиксирован как `baseline`.

Для `223`:

- catalog идёт по baseline path
- stock идёт по baseline path
- orders идут по baseline passthrough path
- store настройки не требуются для baseline catalog/stock runtime
- fallback `organizationId = "1"` для baseline order path допустим

## 5. Как работает `364`

`364` зафиксирован как `custom`.

Для `364`:

- catalog идёт по enterprise identity path
- stock идёт по store-aware path
- orders идут по custom contour
- `organizationId` берётся из `BusinessStore.salesdrive_enterprise_id`
- `payment_method` фиксирован как `Післяплата`

## 6. Enterprise-level настройки

На уровне enterprise управляются:

- `business_runtime_mode`
- `catalog_enabled`
- `branch_id` как основной branch каталога
- ограничение ассортимента каталога
- `code_strategy`
- `name_strategy`
- `code_prefix` при relevant strategy

## 7. Store-level настройки

На уровне store управляются:

- `tabletki_branch` магазина
- `legacy_scope_key`
- `stock_enabled`
- `orders_enabled`
- `salesdrive_enterprise_id`
- store-level extra markup

Store block является operational overlay для custom contour, а не владельцем enterprise catalog runtime.

## 8. Enforced ограничения

Сейчас enforced следующие guards:

- запрещён переход `custom -> baseline`, если для enterprise уже существуют `BusinessEnterpriseProductCode` или `BusinessEnterpriseProductName`
- для baseline enterprise запрещено менять `tabletki_branch` и `legacy_scope_key` через store update
- UI и backend validation согласованы по этим ограничениям

## 9. Что убрано из operator UI

Из обычного operator-facing UI убраны:

- technical / deprecated / debug blocks
- migration/internal fields
- manual dry-run / preview / generate actions
- store-level catalog identity internals
- compatibility helper texts, не нужные оператору

`BusinessStoresPage` теперь отражает реальную рабочую модель:

- enterprise block управляет enterprise runtime
- store block управляет store operations
