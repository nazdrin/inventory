# Configuration Reference

## Purpose

Описать текущую конфигурационную модель `inventory_service`: что управляется через `.env`, что через БД, где действует DB-first логика и где остаётся fallback.

## Scope

Документ покрывает:

- env vs DB configuration split;
- роль `enterprise_settings` и `business_settings`;
- fallback logic для business/master/biotus/pricing контуров;
- restart impact.

Документ не покрывает:

- реальные значения секретов;
- полный список всех supplier-specific env переменных;
- полный data dictionary всех таблиц.

## High-level Overview

В проекте одновременно используются два основных слоя конфигурации:

- `.env` и process environment
- DB-backed settings в PostgreSQL

Не все контуры используют их одинаково.

Текущая подтверждённая модель:

- большинство общих integration/runtime параметров остаются env-driven;
- `enterprise_settings` управляет catalog/stock/order routing по enterprise;
- `business_settings` становится DB-first control plane для business stock, pricing, master catalog и части Biotus behavior;
- при отсутствии строки `business_settings` несколько контуров fallback-ят к env.

## Quick Mental Model

- env = инфраструктура, секреты, внешние endpoints и process-level switches
- `enterprise_settings` = routing и per-enterprise operational control
- `business_settings` = business-логика и DB-first control plane для Business contour

## Configuration Priority

1. `business_settings`
2. `enterprise_settings`
3. env fallback

Практический смысл:

- если contour поддерживает `business_settings`, именно DB row должна определять effective behavior;
- `enterprise_settings` управляет routing и enterprise-level execution, но не должен переопределять DB-first business control plane;
- env нужен для инфраструктуры, секретов и transitional fallback paths;
- env не должен рассматриваться как override для уже существующих DB settings;
- fallback через env остаётся transitional behavior и не должен считаться основной steady-state моделью.

## DB Configuration

### `enterprise_settings`

Operational role:

- список enterprise-контуров;
- `data_format` для adapter dispatch;
- частоты catalog/stock;
- включение отдельных pipeline branches;
- часть credentials/operational fields per enterprise.

Где используется:

- catalog scheduler
- stock scheduler
- order scheduler
- admin/developer panel
- часть order/webhook/export flows

Практический смысл:

- это основной control plane для non-Business enterprise routing.

### `business_settings`

Operational role:

- singleton-like DB control plane для Business contour;
- target enterprise selection for master/biotus/business stock;
- business stock interval and enable flag;
- pricing parameters;
- Biotus fallback policy.

Подтверждённые области управления:

- `business_enterprise_code`
- daily/weekly/biotus enterprise overrides
- master scheduler timing flags
- `business_stock_enabled`
- `business_stock_interval_seconds`
- pricing fields `pricing_*`
- Biotus fallback fields

Где используется:

- `app/services/business_stock_scheduler_service.py`
- `app/services/master_business_settings_resolver.py`
- `app/services/business_pricing_settings_resolver.py`
- `app/services/master_catalog_scheduler_service.py`
- `app/services/biotus_check_order_scheduler.py`
- `app/business/dropship_pipeline.py`

## Environment Configuration

Основные env-группы, подтверждённые кодом и `ENV_REFERENCE.md`:

- base runtime: `DATABASE_URL`, `SECRET_KEY`, `TEMP_FILE_PATH`, `LOG_DIR`
- frontend: `REACT_APP_API_BASE_URL`, `REACT_APP_ENV`
- Google Drive and file integrations
- SalesDrive / orders / webhook
- Telegram / notifications
- pricing / dropship
- competitor scheduler
- Biotus / Nova Poshta
- master catalog scheduler
- balancer
- FTP and integration credentials
- backup-related env such as remote copy and GDrive backup folder

Operational rule:

- env остаётся source of truth для большинства secret-bearing и infrastructure-level настроек.

## DB-First And Fallback Logic

### Business pricing

Подтверждённое поведение:

- resolver сначала пытается прочитать `business_settings`;
- если строки нет, используется `env-fallback`;
- если DB payload unreadable/invalid, тоже используется `env-fallback`.

Env fallback fields:

- `BASE_THR`
- `PRICE_BAND_LOW_MAX`
- `PRICE_BAND_MID_MAX`
- `THR_MULT_LOW`
- `THR_MULT_MID`
- `THR_MULT_HIGH`
- `NO_COMP_MULT_LOW`
- `NO_COMP_MULT_MID`
- `NO_COMP_MULT_HIGH`
- `COMP_DISCOUNT_SHARE`
- `COMP_DELTA_MIN_UAH`
- `COMP_DELTA_MAX_UAH`
- `PRICE_JITTER_ENABLED`
- `PRICE_JITTER_STEP_UAH`
- `PRICE_JITTER_MIN_UAH`
- `PRICE_JITTER_MAX_UAH`

Transitional note:

- env fallback для pricing - рабочий transitional path, но не целевая steady-state модель.

### Master / Biotus settings

Подтверждённое поведение:

- `load_master_business_settings_snapshot()` читает `business_settings`;
- если строки нет, строится `source="env-fallback"` snapshot;
- если строка есть, resolver дополнительно проверяет existence target enterprise codes в `enterprise_settings`.

Env fallback group:

- `MASTER_CATALOG_ENTERPRISE_CODE`
- `MASTER_DAILY_PUBLISH_ENTERPRISE`
- `MASTER_WEEKLY_SALESDRIVE_ENTERPRISE`
- `MASTER_WEEKLY_*`
- `MASTER_DAILY_PUBLISH_*`
- `MASTER_ARCHIVE_*`
- `BIOTUS_ENTERPRISE_CODE`
- `BIOTUS_ENABLE_UNHANDLED_FALLBACK`
- `BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES`
- `BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS`
- `BIOTUS_DUPLICATE_STATUS_ID`
- `BIOTUS_TIME_*`
- `BIOTUS_TZ`

Known risk:

- DB row may exist but point to missing `enterprise_settings` record; resolver logs this as inconsistency instead of silently hiding it.

### Business stock

Подтверждённое поведение:

- если `business_settings` row существует, scheduler берёт `business_stock_enabled` и `business_stock_interval_seconds` из DB;
- если строки нет, fallback идёт к старой логике через `EnterpriseSettings.stock_enabled` и `stock_upload_frequency`.

Transitional note:

- fallback path сохранён для совместимости, но не должен документироваться как основной control-plane design.

### Old Business catalog scheduler

Подтверждённое поведение:

- общий catalog scheduler всё ещё умеет запускать `Business` через `import_catalog`;
- env `DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER` позволяет отключить этот legacy path.

Operational interpretation:

- это transitional compatibility switch, а не preferred behavior.

## Restart Impact

Подтверждённые restart dependencies:

- `MASTER_*` env changes -> restart master catalog scheduler
- `BIOTUS_*` env changes -> restart Biotus scheduler
- `TABLETKI_*` env changes -> restart Tabletki cancel retry service
- stock/catalog env changes -> restart соответствующий scheduler
- API-level env changes -> restart backend app
- frontend env changes -> rebuild frontend before redeploy

DB-driven notes:

- изменения в `business_settings` и `enterprise_settings` могут менять runtime behavior без изменения `.env`;
- но long-running processes often pick up new DB values only on next polling cycle or next resolver call, not atomically across all contours.

## Practical Configuration Model

Использовать как рабочее правило:

- env для secrets, external endpoints, infra wiring, scheduler windows, filesystem paths
- `enterprise_settings` для per-enterprise routing and enablement
- `business_settings` для Business control plane

Не использовать как основной narrative:

- legacy `Blank` behavior
- env-only business pricing as target architecture
- old Business catalog scheduler as primary path

## Source Of Truth

- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md)
- [app/models.py](/Users/dmitrijnazdrin/inventory_service_1/app/models.py)
- [app/routes.py](/Users/dmitrijnazdrin/inventory_service_1/app/routes.py)
- [app/services/master_business_settings_resolver.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/master_business_settings_resolver.py)
- [app/services/business_pricing_settings_resolver.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_pricing_settings_resolver.py)
- [app/services/business_stock_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/business_stock_scheduler_service.py)
- [app/services/catalog_scheduler_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/services/catalog_scheduler_service.py)

## Operational Notes

- Документ намеренно не дублирует все env names один-в-один; для полного перечня остаётся `ENV_REFERENCE.md`.
- При диагностике сначала нужно установить, относится ли проблема к env layer или DB settings layer.
- Отсутствие `business_settings` row не всегда ошибка, но это сильный сигнал, что контур работает в transitional fallback mode.

## Known Limitations / Risks

- Repo не содержит production `.env`, поэтому все statements о фактических значениях невозможны.
- Часть supplier-specific env variables intentionally не включена в первую волну, чтобы не смешивать core configuration map и edge-case integrations.
- Если production использует дополнительные secrets managers или systemd environment overrides, этот документ их не видит.
