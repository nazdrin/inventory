# Business Production Checklist

## 1. Pre-deploy checks

- Убедиться, что `alembic upgrade head` уже применён.
- Проверить `enterprise_settings.business_runtime_mode`.
- Проверить, что `223 = baseline`.
- Проверить, что `364 = custom`.
- Проверить, что для `364` заполнен `BusinessStore.salesdrive_enterprise_id`.
- Проверить, что enterprise-level catalog settings заполнены корректно.
- Проверить нужные env и scheduler configs.

## 2. UI / config checks

### `223`

- В `BusinessStoresPage` показан режим `Базовый`.
- Catalog identity controls disabled.
- Store `Branch` и `Scope` disabled/read-only.

### `364`

- В `BusinessStoresPage` показан режим `Настраиваемый`.
- Если enterprise mappings уже существуют, переключение режима заблокировано.
- Store controls доступны для operational настройки.

## 3. Runtime checks after deploy

- Проверить catalog publish для `223`.
- Проверить stock publish для `223`.
- Проверить catalog publish для `364`.
- Проверить stock publish для `364`.
- Проверить order flow для `364`.
- Проверить отправку status 2 обратно.
- Проверить webhook status 4 / TTN path.

## 4. Logs to inspect

- `Baseline enterprise skipped from custom business order contour`
- `Baseline enterprise order passthrough path`
- `Custom enterprise order path`
- stock scheduler runtime mode resolution
- catalog publish summary
- `SalesDrive payload built`
- `status_2 sent`
- TTN send/update logs

## 5. Failure checklist

Если что-то пошло не так, сначала проверить:

- `enterprise_settings.business_runtime_mode`
- наличие enterprise catalog mappings
- `BusinessStore.salesdrive_enterprise_id`
- store branch/scope/config
- webhook payload
- `ord_delivery_data`

Наиболее вероятные точки ошибки:

- неверный runtime mode у enterprise
- попытка baseline/custom path не для того enterprise
- не заполнен `salesdrive_enterprise_id`
- неправильный branch/scope у custom store
- некорректный webhook payload

## 6. Minimal rollback notes

Config-only reversible:

- переключение enterprise runtime mode там, где это не заблокировано guards
- enterprise/store operational settings
- scheduler/env configuration

Требует отдельного осторожного rollback:

- DB migration rollback
- удаление или откат enterprise catalog mappings

Быстро отключаемые вещи:

- enterprise-level operational flags
- scheduler/runtime config
- enterprise mode only там, где это не конфликтует с existing mappings
