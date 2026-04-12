# AGENTS.md

## Назначение

Этот файл - основная операционная инструкция для AI-агентов и разработчиков в этом репозитории.

Он нужен, чтобы быстро понять:

- как безопасно работать в проекте;
- какие документы читать в первую очередь;
- какие ограничения проекта важны при изменениях;
- где описаны git-сценарии.

Подробные git-правила вынесены в [GIT_WORKFLOW.md](/Users/dmitrijnazdrin/inventory_service_1/GIT_WORKFLOW.md).

## Режим работы

- По умолчанию не выполнять git-действия.
- Для обычных задач: внести изменение, по возможности проверить результат, затем показать изменённые файлы, короткое summary и предложить commit message.
- Коммит, push, переключение ветки, merge и rebase допустимы только по явной команде пользователя.
- Если есть неоднозначность или риск, остановиться и кратко объяснить причину.

## Карта контекста

Перед high-impact изменениями читать по необходимости:

- [PROJECT_OVERVIEW.md](/Users/dmitrijnazdrin/inventory_service_1/PROJECT_OVERVIEW.md): назначение проекта, архитектура, основные пайплайны.
- [FILE_MAP.md](/Users/dmitrijnazdrin/inventory_service_1/FILE_MAP.md): где лежат ключевые модули и критичные точки входа.
- [ENV_REFERENCE.md](/Users/dmitrijnazdrin/inventory_service_1/ENV_REFERENCE.md): назначение важных env-переменных без секретов.
- [CODEX_RULES.md](/Users/dmitrijnazdrin/inventory_service_1/CODEX_RULES.md): стиль архитектурного анализа и требования к ответам.
- [README.md](/Users/dmitrijnazdrin/inventory_service_1/README.md): локальный запуск и базовая разработка.
- [README_PROD.md](/Users/dmitrijnazdrin/inventory_service_1/README_PROD.md): продовый runbook.

## Правила безопасности

- Не коммитить `.env`, секреты, токены, дампы, логи, кэши, временные артефакты и backup-файлы без явного запроса.
- Считать `state_cache/` runtime-кэшем, а не исходным кодом.
- Не возвращать в репозиторий удалённые одноразовые скрипты и вынесенные standalone-flow без подтверждённой необходимости.
- С особой осторожностью менять код, который влияет на БД, шедулеры, pricing, order flow и внешние интеграции.

## Зоны повышенного риска

Изменения в этих местах требуют отдельной оценки влияния и, как правило, локальной проверки:

- `app/models.py`
- `app/routes.py`
- `app/database.py`
- `app/services/*scheduler*`
- `app/services/order_sender.py`
- `app/business/dropship_pipeline.py`
- `app/business/master_catalog_orchestrator.py`
- `app/business/salesdrive_master_catalog_exporter.py`
- `admin-panel/`
- `alembic/versions/*`

При изменениях в этих зонах отдельно проверять:

- влияние на API;
- влияние на схему БД;
- влияние на scheduler/runtime поведение;
- влияние на pricing;
- влияние на интеграции;
- влияние на `admin-panel`.

## Ограничения проекта

### Вынесенные сценарии

- `competitors_min_price.py` вынесен в отдельный проект и не должен возвращаться в этот репозиторий.
- `salesdrive_catalog_uploader.py` вынесен в отдельный проект и больше не является частью этого репозитория.

### SalesDrive master export

- Текущий master catalog export в SalesDrive остаётся в этом репозитории.
- Он зависит от:
  - `SALESDRIVE_PRODUCT_HANDLER_URL`
  - `SALESDRIVE_CATEGORY_HANDLER_URL`
  - `MASTER_WEEKLY_SALESDRIVE_ENTERPRISE`
  - `MASTER_WEEKLY_SALESDRIVE_BATCH_SIZE`

### Runtime и cleanup

- `state_cache/` должен оставаться вне git; при необходимости его нужно убирать из индекса без удаления локального кэша.
- `catalog_mapping_d2_mismatch.xlsx` считается неиспользуемым и должен оставаться удалённым, если не подтверждена новая зависимость.

### Bioteca

- Использовать единый модуль `app/bioteca_data_service/bioteca_conv.py` для `catalog` и `stock`.
- Токен брать только из `EnterpriseSettings.token`.
- `store_id` / `branch` брать только из `MappingBranch`.
- В AINUR API ходить отдельно по каждому `store_id`.
- Для пагинации использовать `offset`.
- В `catalog` дедуплицировать по `code`.
- В `stock` не агрегировать остатки между branch.

### D14

- D14 работает как special-case direct flow в `app/business/feed_fulfillment_salesdrive.py`.
- Парсер читает fulfillment YML из `dropship_enterprises.feed_url`.
- `offer.id` используется как готовый `product_code`.
- Для D14 не нужен mapping через `catalog_mapping` или `catalog_supplier_mapping`.
- `price_retail` считается от `vendorprice` через `profit_percent`; `feed.price` игнорируется.
- Приоритет можно повышать через `STOCK_PRIORITY_SUPPLIERS`.
- В отправке заказов `supplierlist` всё равно идёт через общий identity-layer.
- Root/item `stockId=2` ставится только по D14-специфичным правилам SalesDrive.

### Заказы

- Контур заказов в целом считается рабочим, но после изменений по D14 и SalesDrive всё ещё нужен фактический прогон сценариев:
  - single-item
  - full-D14
  - mixed multi-supplier

### Nova Poshta fulfillment

- В `app/business/biotus_check_order.py` режим fulfillment для ТТН не захардкожен.
- Использовать `NP_FULFILLMENT_SUPPLIER_IDS`.
- Адрес отправителя для таких заказов брать из `NP_FULFILLMENT_ADDRESS_REF` с fallback на `NP_SENDER_ADDRESS_REF`.

## Что делать после обычной задачи

Если пользователь явно не запросил git-действия:

1. Показать, какие файлы изменены.
2. Кратко описать, что сделано.
3. Предложить commit message.
4. Остановиться и ждать следующую команду.

Если пользователь явно просит git-действие, следовать [GIT_WORKFLOW.md](/Users/dmitrijnazdrin/inventory_service_1/GIT_WORKFLOW.md) без отклонений.
