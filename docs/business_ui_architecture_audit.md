# Business UI Architecture Audit

## 1. Текущее состояние

Сейчас business-контур в UI распределён между двумя страницами:

- `Business Settings`
- `Business-магазины`

Фактическая runtime-модель уже разделяет две зоны ответственности:

- enterprise runtime и catalog mode;
- store operations для custom-контура.

Но UI всё ещё выглядит как система с двумя частично пересекающимися control-panels.

Главное текущее пересечение:

- `BusinessSettingsPage` всё ещё показывает и редактирует часть enterprise-level operational полей из `enterprise_settings`:
  - `branch_id`
  - `tabletki_login`
  - `tabletki_password`
  - `order_fetcher`
  - `auto_confirm`
  - `stock_correction`
- `BusinessStoresPage` уже стала фактической enterprise/store operations page и тоже управляет enterprise-level runtime:
  - `business_runtime_mode`
  - `catalog_enabled`
  - `branch_id`
  - catalog assortment restriction
  - catalog identity strategy

В результате оператору неочевидно:

- какая страница является основной для enterprise runtime;
- где нужно настраивать предприятие;
- где заканчиваются enterprise-level настройки и начинаются store overlays;
- какие блоки являются summary, а какие реально меняют runtime.

## 2. Основные UX-проблемы

### 2.1 Дублирование enterprise controls

Сейчас часть enterprise-level полей и смыслов живёт сразу в двух местах:

- `Business Settings` содержит operational enterprise fields;
- `Business-магазины` уже содержит enterprise runtime block.

Это создаёт ложное ощущение двух равноправных страниц настройки одного и того же объекта.

### 2.2 Смешение control-plane и operational UI

`BusinessSettingsPage` смешивает:

- control-plane и scheduler;
- master catalog schedule;
- pricing;
- integration access;
- enterprise operational fields.

Архитектурно это уже не один уровень ответственности.

### 2.3 `Business-магазины` выполняет сразу три роли

Страница одновременно пытается быть:

- выбором предприятия;
- enterprise runtime editor;
- store operations editor.

Это допустимо, но требует очень чёткого визуального порядка. Сейчас логика понятна только после знания backend-модели.

### 2.4 Baseline vs custom отражены недостаточно декларативно

Ограничения baseline/custom уже есть, но пока воспринимаются как набор disabled полей, а не как отдельные сценарии работы:

- baseline enterprise;
- custom enterprise.

Для оператора это должно быть видно в верхней части страницы ещё до взаимодействия со store form.

### 2.5 Избыточный визуальный вес вторичных блоков

На `Business-магазины` много одинаково тяжёлых карточек:

- selector enterprise;
- enterprise summary;
- enterprise controls;
- store form;
- store list.

Из-за одинаковой визуальной плотности неочевидно, что является primary action area.

### 2.6 Нет достаточно жёсткого single source of truth в UI-слое

По факту single source of truth уже такой:

- enterprise runtime -> `BusinessStoresPage`
- control-plane / schedule / pricing -> `BusinessSettingsPage`

Но визуально и текстово это ещё не зафиксировано.

## 3. Предлагаемое разделение ответственности между страницами

### `Business Settings`

Должно остаться только здесь:

- выбор primary business enterprise для control-plane;
- master catalog scheduling;
- business stock scheduler enable/interval;
- global pricing settings;
- integration access / общие credentials, если они действительно относятся к shared runtime contour;
- fallback / additional order processing policies;
- control-plane summary по pipeline.

То есть это должна быть страница:

- scheduling
- pricing
- integration access
- pipeline control-plane

Она не должна быть страницей редактирования enterprise runtime profile.

### `Business-магазины`

Должно остаться только здесь:

- выбор конкретного Business enterprise;
- enterprise runtime profile:
  - `business_runtime_mode`
  - `catalog_enabled`
  - `branch_id`
  - catalog assortment restriction
  - catalog code strategy
  - catalog name strategy
  - `code_prefix` when relevant
- store overlays:
  - store list
  - branch
  - scope
  - stock/orders flags
  - `salesdrive_enterprise_id`
  - extra markup

То есть это должна быть страница:

- enterprise operational runtime
- store operational overlays

### Что должно быть удалено или перенесено

Из `Business Settings` нужно убрать:

- `branch_id`
- `tabletki_login`
- `tabletki_password`, если они уже воспринимаются как enterprise runtime access
- `order_fetcher`
- `auto_confirm`
- `stock_correction`

Из `Business-магазины` не нужно переносить обратно в `Business Settings`:

- `business_runtime_mode`
- `catalog_enabled`
- `branch_id`
- catalog identity controls
- catalog assortment restriction

## 4. Рекомендации по странице `Business-магазины`

Рекомендуемая структура сверху вниз:

### 4.1 Header

- Заголовок страницы
- короткий subtitle:
  - `Сначала настраивается предприятие, затем — конкретные магазины.`
- subtitle должен быть компактным, не warning-block, а тонкой helper-note

Причина:

- это правильная ментальная модель страницы;
- она помогает сразу объяснить иерархию enterprise -> store.

### 4.2 Enterprise selector

- отдельный компактный card-row сразу под header
- selector предприятия
- справа 3-4 summary chips:
  - код
  - runtime mode
  - catalog status
  - stock/orders summary

Информационный блок `Предприятие` лучше действительно поднять выше selector/store form pair и сделать частью header zone.

### 4.3 Enterprise summary

Под selector показывать короткий summary block:

- enterprise code
- enterprise name
- runtime mode
- catalog branch
- catalog enabled
- stock/orders status summary

Это должен быть summary, а не форма.

### 4.4 Enterprise controls

Следующий блок:

- `Режим предприятия`
- `Каталог предприятия включён`
- `Branch каталога / основной branch предприятия`
- `Ограничение ассортимента каталога`
- `Стратегия кодов каталога`
- `Стратегия названий каталога`
- `code_prefix` when relevant

Этот блок должен быть единственным местом enterprise runtime editing.

### 4.5 Store list

После enterprise block логично показывать список магазинов выбранного enterprise:

- это даёт контекст перед открытием конкретного store;
- оператор видит coverage enterprise по stores;
- проще понять, что stores являются overlays одного enterprise.

### 4.6 Store details

Форма магазина должна идти после списка и восприниматься как detail panel выбранного store.

Оставить только:

- store identity
- routing/orders
- pricing
- process participation

Без enterprise catalog смысла.

## 5. Отдельно: baseline vs custom UX

### Для `baseline`

Страница должна выглядеть как:

- enterprise summary visible
- enterprise runtime mode visible
- enterprise controls частично read-only
- catalog identity controls disabled
- store branch/scope disabled
- store block визуально secondary

Рекомендуемая подача:

- компактный info banner в enterprise block:
  - `Для базового режима предприятие использует стандартный контур каталога и остатков.`
- компактный info banner в store block:
  - `Настройки магазинов не влияют на каталог и остатки в базовом режиме.`

Не нужен тяжёлый warning style, потому что это штатный режим, а не ошибка.

### Для `custom`

Страница должна выглядеть как:

- enterprise summary visible
- enterprise controls fully active
- store block fully operational
- store list воспринимается как обязательная часть настройки runtime

Если mappings уже есть:

- runtime mode switch disabled
- short inline warning near selector, not giant warning block

## 6. Варианты архитектуры

### Вариант A — conservative

Минимум изменений:

- `BusinessStoresPage` остаётся enterprise + stores page;
- `BusinessSettingsPage` чистится до control-plane / pricing / schedule / integration;
- `BusinessStoresPage` получает более аккуратную верхнюю композицию:
  - header
  - enterprise selector
  - enterprise summary
  - enterprise controls
  - store list
  - store details

Плюсы:

- минимальный риск;
- соответствует текущей runtime-модели;
- требует в основном layout cleanup и page responsibility cleanup;
- не ломает существующие mental models полностью.

Минусы:

- `BusinessStoresPage` всё ещё останется довольно насыщенной;
- enterprise selector и store editor останутся на одной странице, что требует хорошей визуальной иерархии.

Риск:

- низкий.

### Вариант B — cleaner architecture

Более чистое разделение:

- `Business Settings` = control-plane only
- `Business Enterprise` page = отдельная страница enterprise runtime
- `Business-магазины` = только stores

Тогда текущая `BusinessStoresPage` распадается на:

- enterprise runtime page
- stores overlay page

Плюсы:

- максимально чистая ответственность;
- меньше cognitive load на одной странице;
- легче масштабировать custom enterprise model дальше.

Минусы:

- больше routing/UI refactor;
- больше навигационных изменений;
- больше риск случайно сломать operator flow прямо перед/сразу после прод-стабилизации.

Риск:

- средний.

### Рекомендуемый вариант

Рекомендую `Вариант A`.

Причина:

- он уже совпадает с текущим backend ownership;
- даёт заметную UX-прибавку без page split;
- позволяет быстро зафиксировать single source of truth;
- его можно внедрить поэтапно и безопасно.

## 7. Практические рекомендации по визуальному балансу

### Что уменьшить визуально

- верхний informational block `Предприятие` сделать компактным summary row, а не тяжёлой карточкой;
- helper texts сократить до одной строки;
- baseline/custom messages сделать info-inline, не большими warning blocks;
- store list визуально облегчить: меньше border weight, компактнее row height.

### Где использовать summary cards

- только в верхнем enterprise summary;
- не делать summary cards внутри каждой секции;
- store details лучше оставить обычной form-layout.

### Где использовать helper text

- под selector enterprise:
  - `Сначала настраивается предприятие, затем — конкретные магазины.`
- под runtime mode:
  - короткое объяснение baseline/custom
- под store routing:
  - одно предложение про branch и orders/stock

### Где не нужны крупные warning blocks

- baseline mode itself;
- locked mode switch;
- disabled catalog identity controls в baseline.

Там лучше работают:

- inline helper
- muted info row
- short status chip

### Какие тексты сократить

Сейчас стоит упростить:

- длинные объяснения про compatibility;
- повторяющиеся фразы про catalog enterprise-level;
- повторяющиеся объяснения про store routing.

Оператору нужен не architectural essay, а короткая operational подсказка рядом с action.

## 8. Рекомендованный следующий шаг

Рекомендую следующий Codex-шаг как:

- `layout cleanup + page responsibility refactor`

То есть не только косметика, а именно:

1. убрать enterprise-owned operational fields из `BusinessSettingsPage`;
2. зафиксировать `BusinessSettingsPage` как control-plane / pricing / schedules page;
3. перестроить `BusinessStoresPage` по схеме:
   - header
   - enterprise selector
   - enterprise summary
   - enterprise controls
   - store list
   - store details
4. сократить warning/helper noise;
5. визуально развести baseline и custom сценарии.

Если нужен самый безопасный incremental path, его можно разбить на два шага:

- шаг 1: responsibility cleanup;
- шаг 2: layout cleanup.

Но целевая архитектура уже достаточно понятна и не требует нового backend redesign.
