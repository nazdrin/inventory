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

## Payment reporting / SalesDrive payments

- `SALESDRIVE_PAYMENTS_BASE_URL` - базовый URL SalesDrive для импорта платежей.
- `SALESDRIVE_PAYMENTS_API_KEY` - API ключ SalesDrive для `GET /api/payment/list/`.
- `SALESDRIVE_PAYMENTS_TIMEOUT_SECONDS` - timeout запроса платежей, дефолт `30`.
- `SALESDRIVE_PAYMENTS_PAGE_LIMIT` - размер страницы платежей, максимум `100`.
- `SALESDRIVE_PAYMENTS_RATE_LIMIT_RETRY_SECONDS` - пауза перед retry при rate limit, дефолт `65`.
- `SALESDRIVE_PAYMENTS_RATE_LIMIT_MAX_RETRIES` - максимальное количество retry при rate limit/transport error, дефолт `2`.
- `PAYMENT_REPORTING_STRICT_INCOMING_CLASSIFICATION` - строгая классификация входящих платежей: неизвестные входящие остаются `unknown_incoming`.
- `PAYMENT_REPORTING_SCHEDULER_ENABLED` - включает отдельный daily scheduler платежной отчетности.
- `PAYMENT_REPORTING_DAILY_IMPORT_HOUR` - час ежедневного импорта SalesDrive payments, дефолт `2`.
- `PAYMENT_REPORTING_DAILY_IMPORT_MINUTE` - минута ежедневного импорта SalesDrive payments, дефолт `0`.

## Checkbox fiscalization

- `CHECKBOX_API_BASE_URL` - базовый URL Checkbox API, дефолт `https://api.checkbox.ua`.
- `CHECKBOX_CLIENT_NAME` - имя интеграции для заголовка `X-Client-Name`.
- `CHECKBOX_CLIENT_VERSION` - версия интеграции для заголовка `X-Client-Version`.
- `CHECKBOX_ACCESS_KEY` - integration access key Checkbox для заголовка `X-Access-Key`, если нужен.
- `CHECKBOX_LICENSE_KEY` - license key кассы Checkbox; секрет, не коммитить.
- `CHECKBOX_CASHIER_LOGIN` - login кассира Checkbox для login/password auth; секретное значение не документировать.
- `CHECKBOX_CASHIER_PASSWORD` - password кассира Checkbox для login/password auth; секрет, не коммитить.
- `CHECKBOX_CASHIER_PIN` - PIN кассира Checkbox для `signinPinCode`; секрет, не коммитить.
- `CHECKBOX_TEST_MODE` - включает test-mode маркировку логов/уведомлений; не заменяет реальные тестовые ключи.
- `CHECKBOX_ENABLED_ENTERPRISES` - allowlist `enterprise_code` через запятую для нового Checkbox flow; для первого теста используется `223`.
- `CHECKBOX_DEFAULT_CASH_REGISTER_CODE` - локальный код кассы для хранения в БД, дефолт `default`.
- `CHECKBOX_SHIFT_OPEN_ON_DEMAND` - открывать смену автоматически перед фискализацией, дефолт `true`.
- `CHECKBOX_SHIFT_CLOSE_TIME` - локальное время закрытия смены в формате `HH:MM`.
- `CHECKBOX_SHIFT_TIMEZONE` - timezone scheduler-а закрытия смены, дефолт `Europe/Kiev`.
- `CHECKBOX_SHIFT_SCHEDULER_POLL_INTERVAL_SEC` - интервал проверки shift scheduler-а.
- `CHECKBOX_RECEIPT_POLL_INTERVAL_SEC` - интервал polling статуса чека.
- `CHECKBOX_RECEIPT_POLL_TIMEOUT_SEC` - timeout ожидания финального статуса чека в webhook path.
- `CHECKBOX_RECEIPT_RETRY_INTERVAL_SEC` - интервал retry worker-а для незавершённых чеков.
- `CHECKBOX_RECEIPT_RETRY_MAX_ATTEMPTS` - максимум попыток retry worker-а.
- `CHECKBOX_DEFAULT_PAYMENT_METHOD_ID` - fallback SalesDrive payment method id, дефолт `20` (`Післяплата`).
- `CHECKBOX_DEFAULT_TAX_CODE` - tax code Checkbox для товаров; дефолт `8` (`Без ПДВ`), пустое значение отключает передачу tax.
- `CHECKBOX_EXCLUDED_SUPPLIERS` - список поставщиков через запятую, для которых не создавать Checkbox чеки; дефолт `40,D3,ProteinPlus`.
- `CHECKBOX_SALESDRIVE_UPDATE_CHECK_ENABLED` - обновлять поле ссылки на чек в SalesDrive после фискализации, дефолт `true`.
- `CHECKBOX_SALESDRIVE_CHECK_FIELD` - имя поля SalesDrive для ссылки на чек, дефолт `check`.
- `CHECKBOX_TELEGRAM_NOTIFICATIONS_ENABLED` - общий флаг Checkbox Telegram уведомлений.
- `CHECKBOX_TELEGRAM_RECEIPT_NOTIFICATIONS_ENABLED` - test-mode уведомления по каждому фискализированному чеку.
- `CHECKBOX_TELEGRAM_SHIFT_NOTIFICATIONS_ENABLED` - уведомления об открытии/закрытии смены и summary.

## Telegram / уведомления

- `TELEGRAM_BOT_TOKEN` - токен Telegram bot.
- `TELEGRAM_DEVELOP` - токен Telegram bot для developer/info notifications.
- `TELEGRAM_ERROR_BOT_TOKEN` - токен Telegram bot для error notifications; если не задан, ошибки fallback-ятся в обычный info bot.
- `TELEGRAM_CHAT_IDS` - optional список chat_id через запятую для обычных уведомлений; если не задан, используется fallback из `notification_service`.
- `TELEGRAM_ERROR_CHAT_IDS` - optional список chat_id через запятую для ошибок; если не задан, используется `TELEGRAM_CHAT_IDS` или fallback из `notification_service`.
- `CALL_DELAY_SECONDS` - задержка перед частью уведомлений.
- `TELEGRAM_CALL_DELAY_SECONDS` - отдельная задержка для Telegram bot flow.
- `ORDER_REPORT_TELEGRAM_ENABLED` - включает hourly Telegram scheduler для дневной накопительной отчетности по заказам, дефолт `false`.
- `ORDER_REPORT_TELEGRAM_BOT_TOKEN` - отдельный токен Telegram bot для отчетов по заказам; если не задан, используется `TELEGRAM_DEVELOP`, затем `TELEGRAM_BOT_TOKEN`.
- `ORDER_REPORT_TELEGRAM_CHAT_IDS` - optional один или несколько Telegram chat_id через запятую для отчетов по заказам; если не задан, используется `TELEGRAM_CHAT_IDS` или fallback из сервиса.
- `ORDER_REPORT_TELEGRAM_PROFIT_BASIS` - база показателя `Net profit`: `orders` для активных+проданных заказов без отказов/возвратов/удаленных, `sales` только по продажам; дефолт `orders`.
- `ORDER_REPORT_TELEGRAM_SEND_ON_START` - отправить отчет сразу при старте scheduler, дефолт `false`.

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
- Store-aware reverse mapping в `order_fetcher` управляется `EnterpriseSettings.business_runtime_mode` и `BusinessStore.code_strategy`: baseline остаётся legacy passthrough, custom использует `BusinessEnterpriseProductCode`.
- `BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED` - включает отдельную отправку Tabletki status `2` после успешной обработки store-aware normalized order; default `false`; для Tabletki-facing payload восстанавливает `goodsCode` из `originalGoodsCodeExternal`, если поле есть.
- Outbound status code mapping в основном SalesDrive webhook `/webhooks/salesdrive` управляется `BusinessStore.code_strategy`: `legacy_same` отправляет базовые коды без lookup-а, custom-стратегии преобразуют `products[].parameter` и `products[].sku` через `BusinessEnterpriseProductCode`; `mapping_error` блокирует automatic outbound send для конкретного webhook event.
- Enterprise catalog identity управляется `EnterpriseSettings.business_runtime_mode`: baseline отправляет базовые коды/названия, custom берёт code/name lookup через `BusinessEnterpriseProductCode` / `BusinessEnterpriseProductName` по `enterprise_code`, target branch берётся из `EnterpriseSettings.branch_id`, а assortment остаётся `store_compatible`, чтобы не расширять runtime до всего `MasterCatalog`.
- В custom operator-facing catalog gate = `EnterpriseSettings.catalog_enabled`; `BusinessStore.catalog_enabled` больше не должен блокировать enterprise-level catalog eligibility и остаётся как deprecated compatibility field для rollback/storage.
- В custom ограничение каталога по остаткам берётся не из выбранного store, а из главного магазина каталога: active `BusinessStore` с `enterprise_code == EnterpriseSettings.enterprise_code` и `tabletki_branch == EnterpriseSettings.branch_id`. Если такой store не найден или найдено несколько, enterprise catalog preview/export должен отдавать explicit error.
- Store-aware stock preview/export/publish выбирает коды по `BusinessStore.code_strategy`: `legacy_same` отправляет internal product_code, custom-стратегии берут `external_product_code` из `BusinessEnterpriseProductCode` по `enterprise_code`; branch, legacy scope, offers selection, store-level markup и `BusinessStoreProductPriceAdjustment` остаются без изменений.
- `BUSINESS_CUSTOM_STOCK_LIVE_PATH` - runtime selector для live stock publish у Business enterprise в режиме `custom`; допустимые значения: `legacy`, `store_native`; default `legacy`. Используется тем же `business_stock_scheduler.service`, отдельный scheduler для нового контура не нужен.
- `BUSINESS_BASELINE_STOCK_LIVE_PATH` - runtime selector для live stock publish у Business enterprise в режиме `baseline`; допустимые значения: `legacy`, `store_native`; default `legacy`. Позволяет включать новый contour для baseline через тот же `business_stock_scheduler.service`, без смены команды запуска сервиса.
- `BUSINESS_STORE_NATIVE_REFRESH_OFFERS_BEFORE_STOCK` - включает auto-refresh `business_store_offers` перед stock publish для нового `store_native` контура; default `true`. Используется mode-aware stock publish path и обычным `business_stock_scheduler.service`.
- `BUSINESS_STORE_NATIVE_REFRESH_OFFERS_BEFORE_CATALOG` - включает auto-refresh `business_store_offers` перед custom catalog identity refresh/export; default `true`. Позволяет каталогу работать от актуального supplier assortment без ручного rebuild.
- `BUSINESS_BASELINE_STOCK_ENTERPRISE_CODES` - legacy/diagnostic env allowlist для раннего baseline stock mode control. Основной runtime selector теперь читает `EnterpriseSettings.business_stock_mode`; env не является source of truth для operator UI.
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
- `BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED` - включает отдельный post-daily-publish hook для store-aware catalog publish; по умолчанию `false`.
- `BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN` - переводит store-aware catalog scheduler hook в dry-run/live режим; по умолчанию `true`, то есть без внешней отправки.
- `BUSINESS_STORE_STOCK_SCHEDULER_ENABLED` - включает отдельный store-aware stock scheduler; по умолчанию `false`.
- `BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN` - переводит store-aware stock scheduler в dry-run/live режим; по умолчанию `true`, то есть без внешней отправки.
- `BUSINESS_STORE_STOCK_SCHEDULER_INTERVAL_SECONDS` - интервал цикла отдельного store-aware stock scheduler; рекомендуемый default `300`, с защитным minimum `30`.
- `BUSINESS_STORE_STOCK_REFRESH_OFFERS_BEFORE_PUBLISH` - включает optional refresh `offers` перед каждым циклом store-aware stock publish; по умолчанию `false`.
- `BUSINESS_STORE_STOCK_REFRESH_ENTERPRISE_CODE` - optional enterprise selector для refresh-before-publish; если пусто, selector берётся из `BusinessSettings.business_enterprise_code` или fallback-логики refresh service.
- `BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL` - разрешает publish после refresh со статусом `partial`; по умолчанию `false`, то есть partial refresh останавливает publish cycle.

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
