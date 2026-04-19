# ENV_REFERENCE

## Назначение

Этот файл описывает назначение ключевых переменных окружения без значений. Источник истины по фактическим значениям - локальный `.env` и окружение сервера.

## Когда читать

- когда нужно понять, какая env-переменная на что влияет;
- когда изменения затрагивают scheduler, интеграции, pricing или order flow;
- когда нужно найти вероятные runtime-факторы поведения;
- когда нужно документировать новые env-переменные без раскрытия секретов.

## Связанные документы

- [README.md](/Users/dmitrijnazdrin/inventory_service_1/README.md)
- [AGENTS.md](/Users/dmitrijnazdrin/inventory_service_1/AGENTS.md)
- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md)
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md)
- [CODEX_RULES.md](/Users/dmitrijnazdrin/inventory_service_1/CODEX_RULES.md)

## Принцип

- Этот файл не должен содержать реальные значения.
- Источник истины по значениям - локальный `.env` и продовое окружение.
- Здесь фиксируется только смысл переменных и области их влияния.

## Базовые переменные

- `DATABASE_URL` - основной async DSN для FastAPI и фоновых сервисов.
- `SECRET_KEY` - ключ подписи токенов авторизации.
- `TEMP_FILE_PATH` - рабочая директория для временных файлов импорта.
- `REACT_APP_API_BASE_URL` - URL backend для `admin-panel`.
- `REACT_APP_ENV` - режим frontend-окружения.
- `LOG_DIR` - директория логов для части HTTP/webhook-обработчиков.

## Google Drive и внешние файлы

- `GOOGLE_DRIVE_CREDENTIALS_PATH` - путь к credentials для Google APIs.
- `GOOGLE_DRIVE_FOLDER_ID` - базовая папка Google Drive.
- `COMPETITOR_GDRIVE_FOLDER_ID` - папка с данными конкурентов.
- `D3_CATALOG_FOLDER_ID` - папка каталога для D3-related flow.
- `DOBAVKI_GDRIVE_FOLDER_ID` - папка для поставщика Dobavki.
- `MASTER_ARCHIVE_FOLDER_ID` - архив master catalog файлов.
- `COMPETITOR_DELIVERY_JSON_NAME` - имя JSON-файла с delivery/competitor данными.

## SalesDrive / заказы / webhook-и

- `SALESDRIVE_BASE_URL` - базовый URL SalesDrive API.
- `SALESDRIVE_API_KEY` - API ключ SalesDrive.
- `SALESDRIVE_PRODUCT_HANDLER_URL` - URL обработчика товаров.
- `SALESDRIVE_CATEGORY_HANDLER_URL` - URL обработчика категорий.
- `ENABLE_CALL_REQUEST_NOTIFY` - включает уведомления по call request из webhook logic.
- `ORDER_FETCHER_LOG_LEVEL` - уровень логирования order fetcher.
- `ORDER_FETCHER_VERBOSE_ORDER_LOGS` - расширенные логи по заказам.
- `ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS` - уведомления при новых заказах.
- `ORDER_SENDER_LOG_LEVEL` - уровень логирования order sender.
- `ORDER_SENDER_VERBOSE_SALESDRIVE_LOGS` - детальные логи обмена с SalesDrive.

## Telegram / уведомления

- `TELEGRAM_BOT_TOKEN` - токен Telegram bot.
- `TELEGRAM_DEVELOP` - чат/канал для developer notifications.
- `CALL_DELAY_SECONDS` - задержка перед частью уведомлений.
- `TELEGRAM_CALL_DELAY_SECONDS` - отдельная задержка для Telegram bot flow.

## Pricing / dropship

- `BASE_THR` - базовый порог ценообразования.
- `PRICE_BAND_LOW_MAX` - верхняя граница low band.
- `PRICE_BAND_MID_MAX` - верхняя граница middle band.
- `THR_MULT_LOW` - множитель порога для low band.
- `THR_MULT_MID` - множитель порога для middle band.
- `THR_MULT_HIGH` - множитель порога для high band.
- `NO_COMP_MULT_LOW` - коэффициент при отсутствии конкурентов для low band.
- `NO_COMP_MULT_MID` - коэффициент при отсутствии конкурентов для middle band.
- `NO_COMP_MULT_HIGH` - коэффициент при отсутствии конкурентов для high band.
- `COMP_DELTA_MIN_UAH` - минимальный шаг undercut в гривне.
- `COMP_DELTA_MAX_UAH` - максимальный шаг undercut в гривне.
- `COMP_DISCOUNT_SHARE` - доля скидки относительно конкурента.
- `PRICE_MIN_UAH` - минимально допустимая цена.
- `PRICE_JITTER_ENABLED` - включает jitter для цены.
- `PRICE_JITTER_STEP_UAH` - шаг jitter.
- `PRICE_JITTER_MIN_UAH` - минимальный jitter.
- `PRICE_JITTER_MAX_UAH` - максимальный jitter.
- `SUPPLIER_SCHEDULE_ENABLED` - включает ограничения по расписанию поставщиков.
- `ALLOWED_SUPPLIERS` - список допустимых поставщиков.
- `USE_MASTER_MAPPING_FOR_STOCK` - использовать master mapping в stock/order flows.
- `DROPSHIP_LOG_LEVEL` - уровень логирования dropship pipeline.
- `DROPSHIP_VERBOSE_ITEM_LOGS` - расширенные item-level логи.

## Competitor scheduler

- `COMPETITOR_SCHEDULER_WINDOW_START` - начало окна запусков.
- `COMPETITOR_SCHEDULER_WINDOW_END` - конец окна запусков.
- `COMPETITOR_SCHEDULER_INTERVAL_MINUTES` - интервал запусков.
- `COMPETITOR_SCHEDULER_NOTIFY_SUCCESS` - отправлять success-notifications.
- `COMPETITORS_ROOT_DIR` - корневая директория конкурентных данных.
- `COMPETITOR_CITIES` - список городов для расчётов/загрузки.

## Biotus / доставка / Нова Пошта

- `BIOTUS_BASE_URL` - базовый URL Biotus.
- `BIOTUS_LOGIN` - логин Biotus.
- `BIOTUS_PASSWORD` - пароль Biotus.
- `BIOTUS_AFTER_LOGIN_URL` - URL после логина.
- `BIOTUS_TEST_SKU` - SKU для тестовых проверок.
- `BIOTUS_SCHEDULER_INTERVAL_SECONDS` - интервал проверки Biotus scheduler.
- `BIOTUS_ENTERPRISE_CODE` - enterprise code для Biotus сценария.
- `BIOTUS_TZ` - таймзона Biotus-процесса.
- `BIOTUS_TIME_DEFAULT_MINUTES` - базовое время обработки.
- `BIOTUS_TIME_SWITCH_HOUR` - час переключения режима.
- `BIOTUS_TIME_SWITCH_END_HOUR` - конец окна переключения.
- `BIOTUS_TIME_AFTER_SWITCH_MINUTES` - время после переключения.
- `BIOTUS_NIGHT_START_HOUR` - начало ночного режима.
- `BIOTUS_NIGHT_END_HOUR` - конец ночного режима.
- `BIOTUS_NIGHT_MODE` - включает ночной режим.
- `BIOTUS_NIGHT_INTERVAL_SECONDS` - интервал в ночном режиме.
- `BIOTUS_ENABLE_UNHANDLED_FALLBACK` - fallback для необработанных заказов.
- `BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES` - timeout необработанного заказа.
- `BIOTUS_UNHANDLED_ORDER_NOTE` - служебная заметка для такого сценария.
- `NP_API_KEY` - API ключ Новой Почты.
- `NP_SENDER_REF` - ref отправителя.
- `NP_CONTACT_SENDER_REF` - ref контакта отправителя.
- `NP_SENDER_ADDRESS_REF` - ref адреса отправителя.
- `NP_CITY_SENDER_REF` - ref города отправителя.
- `NP_DEFAULT_SEATS` - число мест по умолчанию.
- `NP_DEFAULT_WEIGHT_KG` - вес по умолчанию.
- `NP_DEFAULT_VOLUME_M3` - объём по умолчанию.
- `NP_DEFAULT_DESCRIPTION` - описание отправления по умолчанию.

## Tabletki / cancel / retry

- `TABLETKI_CANCEL_REASON_DEFAULT` - причина отмены по умолчанию.
- `TABLETKI_CANCEL_RETRY_POLL_INTERVAL_SEC` - интервал ретраев отмен.
- `TABLETKI_CANCEL_WARNING_RETRY_DELAY_MINUTES` - задержка retry warning.
- `TABLETKI_CANCEL_WARNING_RETRY_MAX` - максимум retry warning.
- `TABLETKI_ORDER_RETRY_ATTEMPTS` - число повторов order request.
- `TABLETKI_ORDER_RETRY_DELAY_SEC` - задержка между повторами.
- `FALLBACK_ADDITIONAL_STATUS_IDS` - доп. статусы для fallback order logic.

## Business stores foundation

- `BUSINESS_STORES_ENABLED` - feature flag для будущего подключения нового store-layer в runtime.
- `BUSINESS_STORE_DRY_RUN` - dry-run флаг для будущих store-aware export/import сценариев.
- `BUSINESS_STORE_CODE_SALT` - соль для детерминированной генерации внешних кодов товаров per store.
- `BUSINESS_STORE_CODE_LENGTH` - длина opaque-части внешнего кода товара.
- `BUSINESS_STORE_FAIL_ON_MISSING_CODE` - будущий runtime-флаг строгого поведения при отсутствии mapping-а кода.
- `enterprise_settings` остаётся текущим runtime/control-plane профилем Business-контура.
- `business_stores` является store-level overlay поверх `enterprise_settings`, а не заменой старого runtime контура.
- `takes_over_legacy_scope` позже будет использоваться для поэтапного выключения legacy export по конкретному `legacy_scope_key`, без глобального переключения.
- `migration_status` пока информационный и сам по себе не должен менять runtime поведение.
- `salesdrive_enterprise_id` - числовой ID предприятия в SalesDrive для будущего store-aware order/export routing.
- `salesdrive_enterprise_code` не удаляется и остаётся legacy/string identity полем для совместимости.

## Master catalog scheduler

- `MASTER_CATALOG_ENTERPRISE_CODE` - enterprise для master catalog сценариев.
- `MASTER_SCHEDULER_ENABLED` - общий флаг включения master scheduler.
- `MASTER_SCHEDULER_TIMEZONE` - таймзона master scheduler.
- `MASTER_SCHEDULER_FIRE_WINDOW_SEC` - окно срабатывания jobs.
- `MASTER_SCHEDULER_POLL_INTERVAL_SEC` - интервал poll.
- `MASTER_WEEKLY_ENABLED` - включает weekly enrichment.
- `MASTER_WEEKLY_DAY` - день запуска weekly job.
- `MASTER_WEEKLY_HOUR` - час weekly job.
- `MASTER_WEEKLY_MINUTE` - минута weekly job.
- `MASTER_WEEKLY_SALESDRIVE_ENTERPRISE` - enterprise для weekly SalesDrive export.
- `MASTER_WEEKLY_SALESDRIVE_BATCH_SIZE` - batch size weekly export.
- `MASTER_DAILY_PUBLISH_ENABLED` - включает daily publish.
- `MASTER_DAILY_PUBLISH_HOUR` - час daily publish.
- `MASTER_DAILY_PUBLISH_MINUTE` - минута daily publish.
- `MASTER_DAILY_PUBLISH_ENTERPRISE` - enterprise для daily publish.
- `MASTER_DAILY_PUBLISH_LIMIT` - лимит публикуемых записей.
- `MASTER_ARCHIVE_ENABLED` - включает периодический archive import.
- `MASTER_ARCHIVE_EVERY_MINUTES` - интервал archive import.
- `DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER` - отключает legacy Business catalog scheduler.

## Balancer

- `BALANCER_RUN_MODE` - режим pipeline (`TEST`/`LIVE`).
- `BALANCER_RUN_BOTH` - прогонять оба режима подряд.
- `BALANCER_TZ` - таймзона сегментов.
- `BALANCER_FIRE_WINDOW_SEC` - окно старта после границы сегмента.
- `BALANCER_SCHEDULER_STATE_FILE` - файл состояния последней границы.
- `BALANCER_COLLECT_SEGMENT_END_UTC` - служебная переменная для текущего segment end.
- `BALANCER_TTL_KEEP_DAYS` - срок хранения state/результатов.
- `BALANCER_DEBUG` - расширенный debug mode.

## FTP и интеграционные переменные

- `FTP_HOST` - FTP host.
- `FTP_PORT` - FTP port.
- `FTP_USER` - FTP user.
- `FTP_PASS` - FTP password.
- `FTP_DIR` - каталог на FTP.
- `FTP_USER_1` - дополнительный пользователь для multi FTP.
- `FTP_PASS_1` - дополнительный пароль для multi FTP.
- `CHECKBOX_AUTH_URL` - auth endpoint Checkbox.
- `ZOOHUB_PRICE_URL` - URL price feed для Zoohub.

## Переменные, которые стоит документировать отдельно при расширении

- supplier-specific `*_STATE_DIR`, `*_CACHE_*`, `*_URL`, `*_TIMEOUT_*`;
- DB alias-переменные, используемые в `notification_service`;
- временные feature flags, если они влияют на scheduler behavior.

## Что не хранить в документации

- реальные токены;
- реальные пароли;
- содержимое `.env`;
- приватные URL, если они содержат ключи в query string.
