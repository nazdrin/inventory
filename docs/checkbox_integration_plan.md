# Checkbox Fiscalization Integration Plan

## Цель

Реализовать собственную интеграцию с Checkbox API для фискализации заявок SalesDrive, не ломая текущий production flow, где Checkbox подключен через SalesDrive.

Новый контур должен включаться только через feature flag / allowlist предприятий и работать идемпотентно:

- SalesDrive status `4` / type `1` / "Обработано": создать локальный draft предварительного чека.
- SalesDrive status `5` / type `2` / "Продажа": создать фискальный чек в Checkbox.
- После успешной фискализации получить ссылку на чек.
- Сохранить ссылку в БД.
- Записать ссылку в поле `check` заявки SalesDrive.
- Тестировать на тестовых данных Checkbox.

## Анализ текущего flow

### 1. Где принимается SalesDrive webhook

Основной webhook находится в `app/routes.py`:

- endpoint: `POST /webhooks/salesdrive`
- функция: `salesdrive_webhook`
- payload принимается как raw `dict`
- request headers и summary логируются в logger `salesdrive`
- обработка запускается фоном: `background.add_task(process_salesdrive_webhook, payload)`

Рядом есть отдельный endpoint `POST /webhooks/salesdrive-simple/{branch}`, но это другой контур.

### 2. Где определяется статус заказа из webhook payload

Основная бизнес-логика webhook находится в `app/business/salesdrive_webhook.py`.

Ключевые места:

- `_extract_data_items(payload)` нормализует `payload.data` в список dict-ов.
- В `process_salesdrive_webhook(payload)` для каждого `data` читается:
  - `status_in = data.get("statusId")`
  - `external_id = str(data.get("externalId") or "")`
  - `order_id = str(data.get("id") or "")`
  - `branch_value = data.get("branch")`, fallback на `data.get("utmSource")`
- `enterprise_code` определяется через `MappingBranch.branch == branch_value`.
- Сейчас статус дополнительно мапится в `STATUS_MAP` для отправки в Tabletki:
  - `STATUS_MAP = {2: 4, 3: 4, 4: 4, 5: 6, 6: 7, 10: 4, 16: 4}`

Важно: новый Checkbox flow должен использовать исходный `status_in`, а не `mapped_status`, потому что `mapped_status` предназначен для Tabletki.

### 3. Где сейчас обновляются данные заказа в SalesDrive

В основном webhook `app/business/salesdrive_webhook.py` обратного обновления SalesDrive сейчас нет. Он:

- sync-ит заказ в reporting через `safe_upsert_salesdrive_order`;
- отправляет статусы в Tabletki;
- отправляет ТТН через `send_ttn`;
- обрабатывает call-request уведомления.

Существующие примеры обратного обновления SalesDrive:

- `app/business/order_sender.py`
  - `_salesdrive_update_order(update_url, api_key, payload)`
  - используется для cancelled-orders flow;
  - endpoint: `/api/order/update/`
  - headers: `X-Api-Key`
  - payload pattern: `{"externalId": ext_id, "data": {...}}`
- `app/business/biotus_check_order.py`
  - `_update_status(...)`
  - `_update_obrabotano_only(...)`
  - endpoint: `/api/order/update/`
  - payload pattern: `{"id": order_id, "data": {...}}`
- `app/salesdrive_simple/salesdrive_simple_sender.py`
  - отправляет новые заказы в SalesDriveSimple через `/handler/`, не подходит напрямую для `check` update.

Рекомендация: для Checkbox-ссылки сделать отдельный маленький helper/client `app/integrations/salesdrive/client.py` или локальный модуль внутри Checkbox-интеграции, чтобы не расширять `app/business/order_sender.py`.

### 4. Есть ли таблица заказов, куда можно добавить поля

Есть reporting-таблица `report_orders` и ORM-модель `ReportOrder` в `app/models.py`.

Поля уже есть:

- `source`
- `enterprise_code`
- `external_order_id`
- `salesdrive_order_id`
- `tabletki_order_id`
- `status_id`
- `raw_json`
- финансовые поля отчётности

Технически туда можно добавить:

- `check_url`
- `checkbox_receipt_id`
- `checkbox_status`

Но для фискализации лучше не делать `report_orders` главным источником состояния Checkbox. Причины:

- `report_orders` является агрегированной отчётной моделью, а не интеграционным outbox/state machine.
- Нужно хранить `payload_json`, `response_json`, ошибки, retry-состояния, shift id, receipt id.
- Нужны уникальные ограничения для идемпотентности.
- Чек может жить дольше одного webhook и проходить состояния `draft`, `pending`, `fiscalized`, `failed`.

Рекомендация:

- создать отдельные таблицы `checkbox_receipts` и `checkbox_shifts`;
- опционально добавить read-optimized поля в `report_orders` позже, если UI/отчёты реально будут читать чек из reporting.

### 5. Существующий Checkbox-код

Сейчас `app/checkbox_data_service` не занимается фискализацией. Он импортирует catalog/stock из Checkbox goods API:

- `checkbox_common.py`
  - signin cashier;
  - sync HTTP через `requests`;
  - retry;
  - fetch `/api/v1/goods`;
  - сохранение JSON во временный файл;
- `checkbox_catalog_conv.py`
  - transform goods в catalog;
- `checkbox_stock_conv.py`
  - transform goods в stock.

Переиспользовать можно идеи:

- logger namespace `checkbox.*`;
- auth flow;
- retry/backoff;
- `CHECKBOX_AUTH_URL` как текущий env precedent.

Не стоит переиспользовать напрямую:

- sync `requests` внутри async webhook flow;
- `EnterpriseSettings.token` как хранилище Checkbox credentials для фискализации;
- goods-specific URL/mapper.

### 6. Как в проекте делают HTTP clients

В проекте нет единого универсального HTTP client слоя.

Используемые паттерны:

- `aiohttp` в `app/services/order_sender.py` и `app/services/order_fetcher.py` для Tabletki.
- `httpx.AsyncClient` в `app/business/order_sender.py`, `app/business/biotus_check_order.py`, `app/salesdrive_simple/salesdrive_simple_sender.py`.
- `requests` в `app/checkbox_data_service/checkbox_common.py` и `app/services/notification_service.py`.

Для новой Checkbox фискализации лучше использовать `httpx.AsyncClient`, потому что:

- webhook handler async;
- polling receipt status async;
- проще единый timeout/retry;
- уже есть похожий SalesDrive code style.

### 7. Alembic migrations

Alembic настроен в `alembic/env.py`:

- берёт `DATABASE_URL` из env;
- заменяет `asyncpg` на `psycopg2`;
- `target_metadata = Base.metadata`;
- включены `compare_type=True` и `compare_server_default=True`.

Миграции лежат в `alembic/versions/*`. В проекте есть ручные миграции с:

- `sa.Column(...)`;
- `postgresql.JSONB(...)`;
- indexes;
- unique constraints;
- check constraints;
- downgrade.

Локально команда `alembic` не была доступна как shell command, поэтому проверку heads нужно выполнять из проектного окружения:

```bash
python -m alembic heads
python -m alembic upgrade head
```

### 8. Systemd / scheduler процессы

Production unit templates лежат в `deploy/systemd/`.

Примеры:

- `deploy/systemd/fastapi.service`
- `deploy/systemd/order_scheduler.service`
- `deploy/systemd/tabletki-cancel-retry.service`
- `deploy/systemd/telegram_bot.service`

Типовой scheduler unit:

- `WorkingDirectory=/root/inventory/app/services`
- `Environment="PYTHONPATH=/root/inventory"`
- `EnvironmentFile=/root/inventory/.env`
- `ExecStart=/root/inventory/.venv/bin/python -m app.services.<service_name>`
- `Restart=always`
- `RestartSec=5`

Для Checkbox нужны отдельные unit-файлы, если будут самостоятельные фоновые процессы:

- `checkbox-shift-scheduler.service`
- `checkbox-receipt-retry.service`

## Предлагаемая архитектура

Новые файлы:

- `app/integrations/checkbox/__init__.py`
- `app/integrations/checkbox/config.py`
- `app/integrations/checkbox/client.py`
- `app/integrations/checkbox/schemas.py`
- `app/integrations/checkbox/mapper.py`
- `app/integrations/checkbox/repository.py`
- `app/integrations/checkbox/service.py`
- `app/integrations/checkbox/shift_service.py`
- `app/integrations/salesdrive/client.py`
- `app/services/checkbox_shift_scheduler_service.py`
- `app/services/checkbox_receipt_retry_service.py`

Минимальные изменения существующих файлов:

- `app/models.py`
  - добавить ORM модели `CheckboxReceipt`, `CheckboxShift`, опционально `CheckboxCashRegister`.
- `app/business/salesdrive_webhook.py`
  - после `safe_upsert_salesdrive_order(...)` вызвать новый Checkbox service, если включен feature flag.
- `app/schemas.py`
  - только если нужны admin/debug endpoints.
- `ENV_REFERENCE.md`
  - описать новые env.
- `deploy/systemd/*.service`
  - добавить templates для новых scheduler-ов.
- `alembic/versions/<revision>_add_checkbox_fiscalization_tables.py`
  - создать таблицы.

## DB design

### checkbox_receipts

Назначение: state machine и идемпотентность по фискализации SalesDrive заказа.

Поля:

- `id`: bigint PK
- `salesdrive_order_id`: string, `data.id`
- `salesdrive_external_id`: string, `data.externalId`, полезно для `/api/order/update/`
- `enterprise_code`: string FK на `enterprise_settings.enterprise_code`
- `cash_register_code`: string nullable, код локальной кассы
- `salesdrive_status_id`: integer
- `checkbox_receipt_id`: string nullable
- `checkbox_order_id`: string nullable
- `checkbox_shift_id`: string nullable
- `checkbox_status`: string not null default `draft`
- `fiscal_code`: string nullable
- `receipt_url`: string nullable
- `total_amount`: numeric(14, 2) nullable
- `items_count`: integer nullable
- `payload_json`: JSONB nullable
- `response_json`: JSONB nullable
- `error_message`: text nullable
- `retry_count`: integer not null default 0
- `next_retry_at`: timestamptz nullable
- `created_at`: timestamptz
- `updated_at`: timestamptz
- `fiscalized_at`: timestamptz nullable

Constraints / indexes:

- unique `(enterprise_code, salesdrive_order_id)` для идемпотентности фискального чека.
- index `(checkbox_status, next_retry_at)` для retry worker.
- index `(enterprise_code, created_at)`.
- index `checkbox_receipt_id`.
- check `checkbox_status IN ('draft', 'pending', 'fiscalized', 'failed', 'cancelled', 'skipped')`.

Важно: если один SalesDrive заказ может иметь refund/return чек, для будущего лучше добавить `receipt_type` и unique `(enterprise_code, salesdrive_order_id, receipt_type)`. Для первого этапа достаточно продажи.

### checkbox_shifts

Назначение: хранение смен и Telegram summary.

Поля:

- `id`: bigint PK
- `enterprise_code`: string FK
- `cash_register_code`: string nullable
- `checkbox_shift_id`: string nullable
- `status`: string not null
- `opened_at`: timestamptz nullable
- `closed_at`: timestamptz nullable
- `receipts_count`: integer not null default 0
- `receipts_total_amount`: numeric(14, 2) not null default 0
- `response_json`: JSONB nullable
- `error_message`: text nullable
- `created_at`: timestamptz
- `updated_at`: timestamptz

Constraints / indexes:

- unique `(enterprise_code, cash_register_code, checkbox_shift_id)`, nullable-aware caution.
- index `(enterprise_code, status)`.
- index `(cash_register_code, status)`.
- check `status IN ('opening', 'opened', 'closing', 'closed', 'failed')`.

### checkbox_cash_registers

Пользователь отдельно указал, что в production может быть несколько касс и для каждого предприятия/организации своя касса. Поэтому лучше сразу предусмотреть таблицу или config mapping.

Для минимального test-mode можно начать с env mapping, но production-ready вариант:

- `id`: bigint PK
- `enterprise_code`: string FK
- `cash_register_code`: string not null
- `organization_code`: string nullable
- `license_key`: string not null
- `cashier_login`: string nullable
- `cashier_password_secret_ref`: string nullable
- `cashier_pin_secret_ref`: string nullable
- `is_default`: boolean default false
- `is_active`: boolean default true
- `created_at`
- `updated_at`

Если не хотим хранить секреты в БД, то `license_key`, login/password/pin остаются в env, а таблица хранит только routing metadata. На первом этапе безопаснее env-only.

## Env-переменные

Базовые:

- `CHECKBOX_API_BASE_URL`
- `CHECKBOX_CLIENT_NAME`
- `CHECKBOX_CLIENT_VERSION`
- `CHECKBOX_ACCESS_KEY`
- `CHECKBOX_LICENSE_KEY`
- `CHECKBOX_CASHIER_LOGIN`
- `CHECKBOX_CASHIER_PASSWORD`
- `CHECKBOX_CASHIER_PIN`
- `CHECKBOX_TEST_MODE`
- `CHECKBOX_ENABLED_ENTERPRISES`

Shift:

- `CHECKBOX_SHIFT_CLOSE_TIME`
- `CHECKBOX_SHIFT_TIMEZONE`
- `CHECKBOX_SHIFT_OPEN_ON_DEMAND`

Receipt polling / retry:

- `CHECKBOX_RECEIPT_POLL_INTERVAL_SEC`
- `CHECKBOX_RECEIPT_POLL_TIMEOUT_SEC`
- `CHECKBOX_RECEIPT_RETRY_ENABLED`
- `CHECKBOX_RECEIPT_RETRY_INTERVAL_SEC`
- `CHECKBOX_RECEIPT_RETRY_MAX_ATTEMPTS`

SalesDrive update:

- `CHECKBOX_SALESDRIVE_UPDATE_CHECK_ENABLED`
- `CHECKBOX_SALESDRIVE_CHECK_FIELD`

Telegram:

- `CHECKBOX_TELEGRAM_NOTIFICATIONS_ENABLED`
- `CHECKBOX_TELEGRAM_RECEIPT_NOTIFICATIONS_ENABLED`
- `CHECKBOX_TELEGRAM_SHIFT_NOTIFICATIONS_ENABLED`
- `CHECKBOX_TELEGRAM_BOT_TOKEN`
- `CHECKBOX_TELEGRAM_CHAT_IDS`

Multi-cash-register:

- `CHECKBOX_CASH_REGISTER_MAP_JSON`
- `CHECKBOX_DEFAULT_CASH_REGISTER_CODE`

`CHECKBOX_CASH_REGISTER_MAP_JSON` может быть JSON вида:

```json
{
  "Business": {
    "default": {
      "license_key_env": "CHECKBOX_BUSINESS_LICENSE_KEY",
      "cashier_login_env": "CHECKBOX_BUSINESS_CASHIER_LOGIN",
      "cashier_password_env": "CHECKBOX_BUSINESS_CASHIER_PASSWORD",
      "cashier_pin_env": "CHECKBOX_BUSINESS_CASHIER_PIN"
    }
  }
}
```

Так реальные секреты остаются в `.env`, а map хранит только имена env-переменных.

## Receipt flow

### Status 4: draft

1. Webhook получает `statusId == 4`.
2. Если `enterprise_code` не в `CHECKBOX_ENABLED_ENTERPRISES`, ничего не делать.
3. Создать или обновить `checkbox_receipts` со статусом `draft`.
4. Сохранить нормализованный payload, total, items count.
5. Не отправлять фискальный чек в Checkbox.

Назначение draft:

- зафиксировать будущий чек;
- увидеть проблемы маппинга до продажи;
- иметь локальное состояние для idempotency.

### Status 5: fiscalize

1. Webhook получает `statusId == 5`.
2. Найти `checkbox_receipts` по `(enterprise_code, salesdrive_order_id)`.
3. Если `checkbox_status == 'fiscalized'` и есть `receipt_url`, не создавать новый чек; при необходимости повторить только update SalesDrive `check`.
4. Если есть `pending`, проверить статус через Checkbox или передать в retry worker.
5. Если нет записи, создать запись из webhook payload.
6. Убедиться, что смена открыта:
   - либо открыть on-demand;
   - либо использовать текущую открытую смену из `checkbox_shifts`.
7. Построить payload через `mapper.py`.
8. Отправить чек в Checkbox.
9. Poll до финального статуса или timeout.
10. При успехе:
    - записать `checkbox_receipt_id`, `fiscal_code`, `receipt_url`, `fiscalized_at`, `response_json`;
    - обновить `checkbox_status='fiscalized'`;
    - обновить SalesDrive поле `check`;
    - отправить test-mode Telegram уведомление о чеке, если включено.
11. При временной ошибке:
    - `checkbox_status='pending'` или `failed`;
    - `next_retry_at`;
    - `retry_count += 1`;
    - retry worker попробует снова.

## Idempotency

Обязательные правила:

- Не делать второй fiscal receipt для того же `(enterprise_code, salesdrive_order_id)`.
- DB unique constraint является последней линией защиты.
- Сервис должен делать select before create и корректно обрабатывать race через IntegrityError.
- Если запись уже `fiscalized`, повторный webhook только проверяет/повторяет запись ссылки в SalesDrive.
- `checkbox_order_id` / external id в Checkbox payload должен быть детерминированным, например `salesdrive:{enterprise_code}:{salesdrive_order_id}`.
- Retry worker не создаёт новый чек, если в записи уже есть `checkbox_receipt_id`; он poll-ит или дозавершает существующий.

## Telegram notifications

Сейчас есть два механизма:

- `app/services/notification_service.py`: sync Telegram send через `TELEGRAM_DEVELOP` / `TELEGRAM_BOT_TOKEN`.
- `app/services/telegram_bot.py`: aiogram bot и branch-specific уведомления.

Для Checkbox лучше сделать отдельный модуль `app/integrations/checkbox/notifications.py`, который:

- использует `CHECKBOX_TELEGRAM_BOT_TOKEN`, fallback на `TELEGRAM_DEVELOP`, затем `TELEGRAM_BOT_TOKEN`;
- использует `CHECKBOX_TELEGRAM_CHAT_IDS`, fallback на `TELEGRAM_CHAT_IDS`;
- не зависит от branch registration;
- не ломает текущий Telegram bot polling.

Уведомления:

- shift opened:
  - enterprise;
  - cash register code;
  - Checkbox shift id;
  - opened_at;
  - test/prod mode.
- shift closed:
  - enterprise;
  - cash register code;
  - Checkbox shift id;
  - opened_at / closed_at;
  - receipts count;
  - total amount.
- receipt fiscalized test-mode:
  - SalesDrive order id;
  - externalId;
  - enterprise;
  - cash register code;
  - amount;
  - items count;
  - receipt URL.

Feature flags:

- `CHECKBOX_TELEGRAM_SHIFT_NOTIFICATIONS_ENABLED=true`
- `CHECKBOX_TELEGRAM_RECEIPT_NOTIFICATIONS_ENABLED=true` на тестовый период
- после стабилизации receipt notifications выключить, shift notifications оставить.

## Shift handling

Открытие/закрытие смены сейчас происходит автоматически в SalesDrive-connected Checkbox flow. В новом контуре это нужно контролировать самим.

Рекомендуемая стратегия:

- `shift_service.py` умеет:
  - получить текущую смену;
  - открыть смену;
  - закрыть смену;
  - сохранить `checkbox_shifts`;
  - посчитать summary по `checkbox_receipts` за смену.
- `checkbox_shift_scheduler_service.py`:
  - запускается через systemd;
  - проверяет open shift по каждой enabled кассе;
  - закрывает смену в `CHECKBOX_SHIFT_CLOSE_TIME`;
  - отправляет Telegram summary;
  - при необходимости открывает новую смену по расписанию или оставляет open-on-demand.

Для первого этапа test-mode:

- разрешить `CHECKBOX_SHIFT_OPEN_ON_DEMAND=true`;
- закрытие смены можно делать вручную endpoint/script или scheduler-ом;
- Telegram shift summary всё равно реализовать, чтобы проверить формат.

## Mapper

`app/integrations/checkbox/mapper.py` должен преобразовать SalesDrive payload в Checkbox receipt payload.

Источники из webhook:

- order id: `data.id`
- external id: `data.externalId`
- products: `data.products`
- quantity: `amount`
- price: `price` или `costPerItem`, нужно подтвердить формат SalesDrive payload
- product name: `name` или `documentName`
- SKU/code: `sku`, `id`, `parameter`
- payment amount: `paymentAmount`
- payment type: поля SalesDrive нужно уточнить на реальном webhook

Открытые вопросы для маппинга:

- какие ставки налогов нужны по товарам;
- какой payment method отправлять в Checkbox для SalesDrive заявок;
- как обрабатывать доставки/скидки;
- нужны ли rounding rules;
- как обрабатывать частичные продажи / возвраты.

## Retry worker

`app/services/checkbox_receipt_retry_service.py` нужен, если:

- Checkbox receipt fiscalization асинхронная;
- webhook может завершиться до финального статуса;
- есть сетевые ошибки;
- SalesDrive update `check` может временно упасть.

Worker:

- выбирает `checkbox_receipts` where:
  - `checkbox_status IN ('pending', 'failed')`
  - `next_retry_at <= now`
  - `retry_count < CHECKBOX_RECEIPT_RETRY_MAX_ATTEMPTS`
- если есть `checkbox_receipt_id`, poll-ит статус;
- если нет `checkbox_receipt_id`, создаёт чек только если запись ещё не fiscalized и idempotency key тот же;
- при успехе обновляет SalesDrive `check`;
- при exhausted retries отправляет error notification.

## Предложение DB migration

Новая миграция:

`alembic/versions/<revision>_add_checkbox_fiscalization_tables.py`

Содержимое:

- create table `checkbox_receipts`;
- create table `checkbox_shifts`;
- опционально create table `checkbox_cash_registers`;
- indexes и unique constraints;
- JSONB поля через `postgresql.JSONB(astext_type=sa.Text())`;
- downgrade drop indexes/tables в обратном порядке.

Минимальный DDL sketch:

```python
op.create_table(
    "checkbox_receipts",
    sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column("salesdrive_order_id", sa.String(length=255), nullable=False),
    sa.Column("salesdrive_external_id", sa.String(length=255), nullable=True),
    sa.Column("enterprise_code", sa.String(), nullable=False),
    sa.Column("cash_register_code", sa.String(length=255), nullable=True),
    sa.Column("salesdrive_status_id", sa.Integer(), nullable=True),
    sa.Column("checkbox_receipt_id", sa.String(length=255), nullable=True),
    sa.Column("checkbox_order_id", sa.String(length=255), nullable=True),
    sa.Column("checkbox_shift_id", sa.String(length=255), nullable=True),
    sa.Column("checkbox_status", sa.String(length=32), server_default=sa.text("'draft'"), nullable=False),
    sa.Column("fiscal_code", sa.String(length=255), nullable=True),
    sa.Column("receipt_url", sa.String(length=1000), nullable=True),
    sa.Column("total_amount", sa.Numeric(14, 2), nullable=True),
    sa.Column("items_count", sa.Integer(), nullable=True),
    sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column("response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column("error_message", sa.Text(), nullable=True),
    sa.Column("retry_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
    sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
    sa.Column("fiscalized_at", sa.DateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("enterprise_code", "salesdrive_order_id", name="uq_checkbox_receipts_enterprise_salesdrive_order"),
    sa.CheckConstraint(
        "checkbox_status IN ('draft', 'pending', 'fiscalized', 'failed', 'cancelled', 'skipped')",
        name="ck_checkbox_receipts_status",
    ),
)
```

## Пошаговый план реализации

### Этап 1. Foundation

1. Добавить модели `CheckboxReceipt`, `CheckboxShift`.
2. Добавить Alembic migration.
3. Добавить `ENV_REFERENCE.md` секцию Checkbox fiscalization.
4. Добавить `app/integrations/checkbox/config.py`:
   - env parsing;
   - enterprise allowlist;
   - test mode;
   - cash register resolution.

### Этап 2. Checkbox client

1. Реализовать `client.py` на `httpx.AsyncClient`.
2. Методы:
   - `signin`;
   - `open_shift`;
   - `close_shift`;
   - `get_shift`;
   - `create_receipt`;
   - `get_receipt`;
3. Добавить retry на 429/5xx/network.
4. Не логировать секреты.

### Этап 3. Repository/service

1. `repository.py`:
   - get/create draft by SalesDrive order;
   - mark pending/fiscalized/failed;
   - get due retries;
   - update shift summary.
2. `mapper.py`:
   - SalesDrive webhook payload -> Checkbox receipt payload.
3. `service.py`:
   - `handle_salesdrive_status(session, data, enterprise_code)`;
   - draft на status `4`;
   - fiscalize на status `5`;
   - idempotency guard.

### Этап 4. SalesDrive update

1. Добавить helper для `/api/order/update/`.
2. API key брать по `EnterpriseSettings.token`, как текущий SalesDrive code.
3. Base URL брать из `SALESDRIVE_BASE_URL` или enterprise-specific config, если понадобится.
4. Обновлять только `data.check = receipt_url`.
5. Не менять `statusId`.

### Этап 5. Webhook integration

1. В `process_salesdrive_webhook` после определения `enterprise_code` и `safe_upsert_salesdrive_order` вызвать Checkbox service.
2. Вызов обернуть в `try/except`, чтобы ошибка Checkbox не ломала текущие Tabletki/TTN действия.
3. Проверять:
   - `CHECKBOX_TEST_MODE`;
   - `CHECKBOX_ENABLED_ENTERPRISES`;
   - статус `4` или `5`.

### Этап 6. Shift scheduler

1. Реализовать `checkbox_shift_scheduler_service.py`.
2. Добавить systemd template.
3. Реализовать Telegram notification на open/close.
4. Summary на close считать из `checkbox_receipts` по `checkbox_shift_id`.

### Этап 7. Retry worker

1. Реализовать `checkbox_receipt_retry_service.py`.
2. Добавить systemd template.
3. Обрабатывать:
   - pending receipt status polling;
   - повтор SalesDrive `check` update;
   - exhausted retries notification.

### Этап 8. Test-mode rollout

1. Включить `CHECKBOX_TEST_MODE=true`.
2. Включить только одно тестовое предприятие в `CHECKBOX_ENABLED_ENTERPRISES`.
3. Включить receipt Telegram notifications.
4. Прогнать:
   - status 4 webhook;
   - повтор status 4 webhook;
   - status 5 webhook;
   - повтор status 5 webhook;
   - network failure retry;
   - SalesDrive update failure retry;
   - shift close summary.

## Риски

- Неверный payload Checkbox receipt: нужны точные требования по taxes/payment methods/rounding.
- Повторные SalesDrive webhooks: без DB unique можно создать дубль фискального чека.
- Status `5` может прийти без предшествующего status `4`; service должен создавать запись сразу.
- SalesDrive `check` field может иметь другой API key/name/format. Нужно проверить на test account.
- В текущем `EnterpriseSettings.token` уже используются разные смыслы токена в разных интеграциях; не стоит добавлять туда Checkbox secrets.
- В production несколько касс: env-only single license key подходит только для тестового этапа.
- Смена может быть закрыта/не открыта в момент продажи.
- Telegram receipt notifications могут быть шумными; нужен отдельный флаг и выключение после теста.
- Нельзя отключать SalesDrive Checkbox production flow до полного теста нового контура.

## Точки тестирования

- Unit tests:
  - config env parsing;
  - enterprise allowlist;
  - mapper totals/items;
  - idempotency repository logic.
- Integration tests with mocked Checkbox:
  - signin;
  - create receipt;
  - poll receipt;
  - open/close shift.
- Webhook tests:
  - status `4` creates draft only;
  - status `5` fiscalizes;
  - repeated status `5` does not create second receipt;
  - failure in Checkbox does not block Tabletki/TTN flow.
- DB tests:
  - unique `(enterprise_code, salesdrive_order_id)`;
  - JSONB payload saved;
  - retry selection by `next_retry_at`.
- Manual test-mode:
  - receipt URL opens;
  - SalesDrive `check` field updated;
  - Telegram receipt notification received;
  - shift close notification shows count and total.

## Minimal patch после согласования

Минимальная test-mode реализация должна включать:

- модели + migration для `checkbox_receipts` и `checkbox_shifts`;
- async Checkbox client с test env;
- mapper только для простого sale receipt;
- service с status `4` draft и status `5` fiscalize;
- idempotency через DB unique;
- SalesDrive update `check`;
- Telegram notifications behind flags;
- webhook hook behind `CHECKBOX_ENABLED_ENTERPRISES`;
- retry worker только если Checkbox API требует async polling или если хотим устойчивость к SalesDrive update failures.

Не включать в минимальный patch:

- admin-panel UI;
- перенос production flow с SalesDrive на новый Checkbox;
- рефакторинг существующего order flow;
- хранение реальных ключей в репозитории.
