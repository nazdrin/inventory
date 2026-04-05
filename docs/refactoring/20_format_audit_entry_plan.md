# Format Audit Entry Plan

## 1. Scope

Этот документ фиксирует, с какого seller format / adapter layer логично начинать следующий practical step после стабилизации shared runtime и `database_service`.

В scope:

- seller adapters / data formats, реально подключённые в catalog/stock scheduler-ах;
- их operational profile;
- порядок входа в format-layer.

Вне scope:

- новый framework для adapters;
- DB schema changes;
- новый shared persistence redesign;
- повторный Dntrade-focused этап;
- UI / admin-panel.

## 2. Current project state before moving to formats

Перед переходом к format layer уже сделаны важные инфраструктурные шаги:

- стабилизирован `stock_scheduler` session lifecycle;
- `database_service` получил явные flush boundaries и фазовое логирование;
- error ownership и run outcome стали предсказуемее;
- Dntrade частично локально стабилизирован, но не является лучшим pilot candidate для нового format этапа;
- side-effect ordering сознательно оставлен без изменения.

Следствие:

- shared tail достаточно прозрачен для перехода к seller adapters;
- следующий выигрыш уже должен приходить не из очередного общего refactor, а из локальных format-level fixes.

## 3. Seller format registry

Ниже не полный список всех файлов в `app/`, а practical registry форматов, реально важных как adapter layer.

### API-driven

- `Dntrade`
  - Source: API
  - Scope: catalog + stock
  - Особенности: branch/store mapping, heavy payload, rate-limit/fallback risk
  - Зрелость: проблемный, но уже частично стабилизирован
  - Тип следующей работы: later / postpone

- `Checkbox`
  - Source: API
  - Scope: catalog + stock
  - Особенности: auth handshake через `login,password`, pagination, stock unit scaling
  - Зрелость: локально стабилизирован после hardening pass
  - Тип работы: closed for now / revisit only on incidents

- `KeyCRM`
  - Source: API
  - Scope: catalog + stock
  - Особенности: sync HTTP inside async flow, tiny page size, raw dump side effects, branch mapping requirement for stock
  - Зрелость: средний после первого hardening шага
  - Тип работы: next only if second-step hardening becomes necessary

- `Prom`
  - Source: API
  - Scope: catalog + stock
  - Особенности: single-request assumption `limit=100000`, simple transforms
  - Зрелость: локально причёсан
  - Тип работы: closed for now unless pagination support becomes necessary

- `Vetmanager`
  - Source: API
  - Scope: catalog + stock
  - Особенности: hidden config overload, mixed sync/async flow, very expensive per-good stock fan-out, branch replication semantics
  - Зрелость: medium after config/runtime hardening
  - Тип работы: revisit only if new incident or deeper API-contract change appears

- `Bioteca`
  - Source: API
  - Scope: catalog + stock
  - Особенности: multi-store mapping, async fetch, partial-success semantics
  - Зрелость: хороший после partial-success summaries и paging guards
  - Тип работы: closed for now / revisit only on incidents

### XML / feed-driven

- `DSN`
  - Source: XML/feed
  - Scope: catalog + stock
  - Особенности: unified adapter module, explicit branch validation, conditional debug JSON
  - Зрелость: хороший после unification/hardening
  - Тип работы: closed for now

- `Rozetka`
  - Source: XML/feed
  - Scope: catalog + stock
  - Особенности: feed URL from settings, unified adapter module, simple XML contract
  - Зрелость: локально закрыт
  - Тип работы: closed for now

- `Biotus`
  - Source: XML/feed
  - Scope: catalog + stock plus separate order-related jobs
  - Особенности: extra operational surface outside plain adapter flow
  - Зрелость: хороший после feed/runtime hardening
  - Тип работы: closed for now / revisit only if order-side requirements change

- `HProfit`
  - Source: XML/feed
  - Scope: catalog + stock
  - Особенности: feed adapter, limited current evidence of acute pain
  - Зрелость: хороший после feed/runtime hardening
  - Тип работы: closed for now

- `Salesdrive / ComboKeyCRM`
  - Source: XML/feed
  - Scope: catalog + stock
  - Особенности: mixed source-resolution contracts, enterprise-specific compatibility behavior, naming mismatch
  - Зрелость: medium after config cleanup and prod hotfixes
  - Тип работы: closed for now unless source contract changes again

### FTP / file-driven

- `Ftp`
  - Source: FTP/file
  - Scope: catalog + stock
  - Особенности: destructive cleanup in ingest path, global FTP env, partial-upload risk
  - Зрелость: high-risk
  - Тип работы: runtime risk / cleanup semantics

- `FtpMulti`
  - Source: FTP/file
  - Scope: catalog + stock
  - Особенности: multiple files, same FTP operational class, cleanup/selection semantics
  - Зрелость: high-risk
  - Тип работы: runtime risk / later

- `FtpTabletki`
  - Source: file-driven / uploads dir
  - Scope: catalog + stock
  - Особенности: local file lifecycle, DB-backed catalog validation
  - Зрелость: medium after safer file handling
  - Тип работы: revisit only if validation contract becomes a problem

- `FtpZoomagazin`
  - Source: FTP/file
  - Scope: catalog + stock
  - Особенности: remote cleanup, latest-file semantics
  - Зрелость: high-risk
  - Тип работы: runtime risk / later

- `ExcelFeed`
  - Source: local file / feed
  - Scope: catalog + stock
  - Особенности: file-to-temp conversion pattern
  - Зрелость: средний
  - Тип работы: later

### Google Drive / file

- `GoogleDrive`
  - Source: Google Drive
  - Scope: catalog + stock
  - Особенности: generic file-ingestion framework, broad converter surface
  - Зрелость: medium/good after orchestration summaries and normalization fixes
  - Тип работы: closed for now / touch carefully because radius is wide

- `JetVet`
  - Source: Google Drive
  - Scope: catalog + stock
  - Особенности: file-driven, seller-specific parsing
  - Зрелость: хороший после file/branch/run summaries
  - Тип работы: closed for now

- `TorgsoftGoogle`
  - Source: Google Drive
  - Scope: catalog + stock
  - Особенности: file-driven, converter pattern
  - Зрелость: средний
  - Тип работы: later

- `TorgsoftGoogleMulti`
  - Source: Google Drive
  - Scope: catalog + stock
  - Особенности: multi-file / multi-branch file semantics
  - Зрелость: средний
  - Тип работы: later

### Special / route-driven

- `Unipro`
  - Source: route/file-driven
  - Scope: mostly skipped in schedulers
  - Особенности: not a good representative pilot
  - Зрелость: postpone
  - Тип работы: later

- `Business` / dropship pipeline
  - Source: mixed feeds + mapping + pricing
  - Scope: stock-heavy business pipeline
  - Особенности: pricing complexity, supplier-specific feeds, mapping/offer semantics
  - Зрелость: high-risk
  - Тип работы: postpone as separate domain

- `Blank`
  - Source: none
  - Scope: no-op
  - Тип работы: none

## 4. Format evaluation criteria

Форматы оценивались по сочетанию факторов:

- operational pain прямо сейчас;
- шанс получить быстрый и полезный practical result;
- риск сломать production behavior;
- зависимость от уже стабилизированного shared layer;
- уровень config/mapping chaos;
- runtime complexity;
- насколько format подходит как pilot после infrastructure stabilization;
- насколько findings будут переносимы на другие adapters.

## 5. Candidate comparison

### KeyCRM

- Почему имеет смысл сейчас:
  - есть ясный локальный defect: throttle фактически не работает;
  - adapter относительно простой по domain semantics;
  - shared tail уже подготовлен и даст честные runtime logs после local hardening.
- Expected practical effect:
  - убрать ложное ощущение rate limiting;
  - сократить scheduler unpredictability;
  - получить шаблон для API-driven hardening без тяжелого config redesign.
- Зависимость от shared layer:
  - средняя; сейчас shared layer уже достаточно стабилен.
- Risk:
  - medium

### KeyCRM status after first step

Первый practical step для `KeyCRM` уже выполнен.

Что подтверждено:

- исправлен неработающий throttle;
- добавлены fetch/transform/run summaries для `catalog` и `stock`;
- `non-200` ответы перестали маскироваться под empty dataset;
- `stock` больше не пишет в branch `unknown`, а требует реальный `MappingBranch.branch`.

Инженерный вывод:

- `KeyCRM` больше не является лучшим немедленным следующим target;
- локальный pilot дал ожидаемый результат;
- следующий format можно брать уже вне `KeyCRM`, если не появится новый продовый incident именно в этом adapter-е.

## 6. Current practical status

На текущем этапе safe-pass уже выполнен для большинства реально используемых low/medium adapters:

- `Vetmanager`
- `Salesdrive / ComboKeyCRM`
- `Rozetka`
- `Checkbox`
- `Prom`
- `FTP Tabletki`
- `Bioteca`
- `DSN`
- `HProfit`
- `GoogleDrive`
- `JetVet`
- `Biotus`

Следствие:

- следующий practical выигрыш уже не в повторном проходе по этим форматам;
- дальше логичнее делать doc/status cleanup и repo-wide legacy cleanup;
- high-risk форматы (`Dntrade`, `Ftp*`, `TorgsoftGoogleMulti`) остаются отдельным треком и не должны возвращаться в работу без явной причины.

### Vetmanager

- Почему имеет смысл, но не первым:
  - format очень показательный и важный;
  - но он сочетает overloaded config и expensive stock runtime.
- Expected practical effect:
  - высокий, если удастся локально стабилизировать config/runtime contract.
- Зависимость от shared layer:
  - высокая; он сильно выигрывает от уже сделанной прозрачности `database_service`.
- Risk:
  - high

### Salesdrive / ComboKeyCRM

- Почему имеет смысл:
  - есть один очень чёткий конфигурационный smell: hardcoded URL вместо DB-configured source.
- Expected practical effect:
  - повысить достоверность configuration contract;
  - уменьшить скрытую рассинхронизацию между code и admin settings.
- Зависимость от shared layer:
  - низкая/средняя.
- Risk:
  - medium

### FTP family

- Почему пока не лучшая точка входа:
  - operational risk высокий;
  - cleanup semantics могут затронуть реальные входные артефакты;
  - как первый pilot слишком легко получить destructive regression.
- Expected practical effect:
  - потенциально высокий, но с плохим risk/reward ratio на первом format этапе.
- Risk:
  - high

### Bioteca

- Почему не сейчас:
  - adapter уже относительно зрелый;
  - main issue скорее в partial-success observability, а не в грубом локальном defect.
- Expected practical effect:
  - умеренный.
- Risk:
  - low/medium

### Dntrade

- Почему не сейчас:
  - туда уже был вложен отдельный focused этап;
  - сейчас там открыт отдельный класс вопросов: rate-limit, fallback policy, delta experiment rollback.
- Expected practical effect:
  - возможен, но это уже не лучший “first adapter after infrastructure”.
- Risk:
  - high

## 6. Recommended start order

### 1. KeyCRM

- Почему лучший следующий шаг:
  - есть чёткий локальный bug-level target без нового framework;
  - adapter достаточно простой, чтобы быть pilot;
  - изменения дадут быстрый practical эффект без глубокого domain redesign.
- Expected benefit:
  - рабочий throttle;
  - лучшее page/runtime summary;
  - пилотный шаблон hardening для API adapters.
- Expected risk:
  - medium
- Тип работы:
  - local hardening

### 2. Salesdrive / ComboKeyCRM

- Почему второй:
  - после API-hardening шага логично идти в config-integrity issue;
  - hardcoded source — очень локальный и понятный smell.
- Expected benefit:
  - восстановление доверия к enterprise config;
  - упрощение reasoning по scheduler behavior.
- Expected risk:
  - medium
- Тип работы:
  - config cleanup

### 3. Vetmanager

- Почему третий:
  - format очень ценный как representative “сложного adapter-а”;
  - но брать его первым после infrastructure stabilization слишком рискованно.
- Expected benefit:
  - высокий operational эффект;
  - полезный шаблон для overloaded-config adapters.
- Expected risk:
  - high
- Тип работы:
  - audit-to-local-hardening, потом config cleanup

## 7. Why some formats should wait

- `Dntrade`
  - уже был отдельный focused этап;
  - сейчас там не pilot-class issue, а отдельный operational class around fallback and source behavior.

- `FTP` family
  - destructive cleanup и file lifecycle risk;
  - first pilot на таких форматах имеет слишком высокий blast radius.

- `Business` / dropship pipeline
  - это не просто adapter, а отдельный business domain с pricing/mapping complexity;
  - его лучше брать отдельным треком, а не как seller-format cleanup pilot.

- `Bioteca`
  - сравнительно зрелый;
  - не даёт лучшего quick-win по сравнению с KeyCRM/Salesdrive.

- `Google Drive` family
  - форматы важные, но не дают настолько явного first-step defect, как KeyCRM.

## 8. Recommended first practical step

Рекомендуемый первый format: `KeyCRM`.

### Как заходить

Первый этап делать не как “рефакторинг всего модуля”, а как focused hardening pass.

### С чего начинать

1. Подтвердить текущий runtime path в:
   - [app/key_crm_data_service/key_crm_catalog_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/key_crm_data_service/key_crm_catalog_conv.py)
   - [app/key_crm_data_service/key_crm_stock_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/key_crm_data_service/key_crm_stock_conv.py)
2. Проверить фактическую пагинацию, page size и throttle path.
3. Исправить только:
   - неработающий rate-limit guard;
   - summary logging;
   - при необходимости tiny local retry/timeout hardening.

### Scope первого шага

- audit existing runtime behavior;
- local hardening;
- phase summary for pages/records;
- без общего pagination framework.

### Что не трогать на первом этапе

- не объединять catalog и stock в shared helper prematurely;
- не менять caller contract;
- не менять `database_service`;
- не менять admin/config model;
- не переносить адаптер на новый общий framework.

### Expected result первого шага

- KeyCRM станет более предсказуемым runtime-wise;
- появится clean template для “small API adapter hardening”;
- будет проще решить, заходить ли потом в Salesdrive или Vetmanager.

### Status update

Этот шаг уже завершён.

Следующий рекомендуемый format:

- `Salesdrive / ComboKeyCRM`

Почему именно он следующий:

- у него локальный и очень чёткий config-integrity defect;
- scope по-прежнему маленький;
- риск ниже, чем у `Vetmanager` и `FTP` family;
- он подходит как второй pilot после успешного `KeyCRM` hardening.

## 9. Risks

- Слишком ранний вход в heavy adapters (`Vetmanager`, `FTP` family, `Business`) может снова размыть scope.
- Попытка “стандартизовать все formats сразу” приведёт к новому shared-framework detour.
- Некоторые audit-docs фиксируют состояние до последних shared-layer изменений; это нужно учитывать как контекст, а не как буквальное runtime состояние.

## 10. Open questions

- Intentional ли current runtime behavior `ComboKeyCRM` с hardcoded source URL.
- Насколько часто KeyCRM реально упирается в rate limit на production traffic.
- Нужен ли для Vetmanager сначала отдельный config-contract note, прежде чем делать кодовый шаг.
- Какие из FTP cleanup semantics реально являются бизнес-требованием, а какие просто legacy behavior.
