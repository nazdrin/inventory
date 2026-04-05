# Refactoring Master Plan

## Goal

Подготовить управляемый, пошаговый рефакторинг `inventory_service` без массовой перестройки рабочего монолита. Цель не в "переписывании архитектуры", а в снижении операционного риска, разборе реальных runtime-узких мест и постепенном выносе наиболее хрупких сценариев в более предсказуемые контуры.

## Why This Must Be Incremental

`inventory_service` не является stateless CRUD-сервисом. Это stateful backend-монолит с:

- несколькими scheduler/process loop сценариями;
- env-driven поведением;
- зависимостями от `enterprise_settings`, `mapping_branch`, global developer settings и внешних API;
- побочными эффектами в БД, файлах, Telegram и внешних endpoint-ах.

Массовый рефакторинг здесь опасен по следующим причинам:

- один и тот же pipeline часто зависит и от БД, и от внешней интеграции, и от scheduler runtime;
- часть адаптеров использует общие сервисы записи/экспорта, поэтому локальное изменение может повлиять сразу на несколько поставщиков;
- многие проблемы лежат выше конкретного seller adapter-а, в общих helper-слоях;
- часть логики построена вокруг реального текущего прод-поведения, а не вокруг явно описанных контрактов.

Поэтому безопасный путь: сначала инвентаризация и аудит, затем минимальные локальные улучшения, затем ограниченные этапы переработки.

## Main Workstreams

### 1. Seller Pipelines Audit And Cleanup

Goal:
Понять фактическое устройство catalog/stock adapters и убрать самые рискованные узкие места без ломки scheduler-контуров.

In scope:
- аудит seller adapters и их реальных runtime-зависимостей;
- анализ DB I/O, env-зависимостей, export/save слоёв, retry/timeout логики;
- локальные улучшения внутри конкретных pipeline-ов после аудита.

Out of scope:
- тотальная унификация всех адаптеров за один этап;
- массовое переименование модулей и переносы файлов;
- пересборка всех scheduler-ов в новую архитектуру.

Primary risk:
Локальный фикс в одном pipeline может затронуть общий `database_service`, export layer или поведение других интеграций.

Current status:
`in_progress` — основной safe-pass по format-layer уже выполнен для большинства реально используемых low/medium adapters; дальше логичнее делать doc/status cleanup, repo-wide legacy cleanup и возвращаться к high-risk форматам только по явной необходимости.

### 2. Admin/Developer/Business Settings UI Redesign

Goal:
Снизить стоимость ручной настройки и эксплуатации, разделив developer, business и runtime-sensitive настройки по более понятным сценариям.

In scope:
- аудит текущего admin-panel и backend CRUD surface;
- выявление перегруженных форм и неочевидных настроек;
- подготовка нового IA/UX для enterprise/developer/business settings.

Out of scope:
- полный frontend rewrite без подтверждённого backend-контракта;
- изменение схемы БД без отдельного этапа.

Primary risk:
UI-изменения могут замаскировать реальные backend-проблемы и привести к случайной поломке административных сценариев.

Current status:
`planned`

### 3. Legacy Catalog Cleanup And Master Catalog Review

Goal:
Разделить legacy catalog-потоки, актуальный master catalog и устаревшие промежуточные сценарии, чтобы сократить технический шум.

In scope:
- ревизия catalog adapters, export flows и legacy/import overlap;
- поиск неиспользуемых файлов, старых конвертеров и дублирующей логики;
- обзор связей с master catalog scheduler и export.

Out of scope:
- немедленное удаление всего старого кода без подтверждения runtime-использования;
- переписывание master catalog orchestration в рамках одного этапа.

Primary risk:
Можно удалить "мертвый" код, который на деле still-in-use через scheduler/env.

Current status:
`planned`

### 4. Backup Reliability And Restore Testing

Goal:
Сделать backup/restore процедурно проверяемыми, а не предполагаемыми.

In scope:
- инвентаризация текущих backup-источников и артефактов;
- проверка restore-сценариев;
- фиксация RPO/RTO ожиданий и пробелов.

Out of scope:
- полная DevOps-перестройка инфраструктуры;
- внедрение новой backup-платформы без подтверждения потребности.

Primary risk:
Команда может считать backup "существующим", хотя restore не проверялся в реальных условиях.

Current status:
`planned`

### 5. Security Review

Goal:
Найти фактические security-риски в конфигурации, секретах, auth surface и интеграционных сценариях.

In scope:
- env/secrets handling;
- auth/admin access review;
- логирование чувствительных данных;
- внешние webhook/API точки.

Out of scope:
- формальная compliance-программа;
- полный threat model всего бизнеса за один этап.

Primary risk:
Часть рисков спрятана в operational practice, а не только в коде.

Current status:
`planned`

### 6. Docker/Containerization Feasibility

Goal:
Понять, можно ли безопасно контейнеризовать сервис без слома scheduler/stateful сценариев и неявных filesystem/env зависимостей.

In scope:
- аудит runtime-предпосылок;
- inventory всех stateful точек: temp files, cache, local paths, systemd assumptions;
- оценка разделения API и фоновых воркеров.

Out of scope:
- немедленный production migration в Docker;
- Kubernetes redesign.

Primary risk:
Контейнеризация может скрыть реальные проблемы state management и ещё больше усложнить эксплуатацию.

Current status:
`planned`

## Priorities

1. Seller pipelines audit and cleanup
2. Legacy catalog cleanup and master catalog review
3. Backup reliability and restore testing
4. Security review
5. Admin/developer/business settings UI redesign
6. Docker/containerization feasibility

Такой порядок выбран потому, что seller pipelines и общий catalog/stock runtime создают основной операционный риск прямо сейчас, а containerization и UI имеют смысл только после фиксации реального backend-контекста.

## Execution Order

1. Подтвердить high-impact pipeline-аудиты по конкретным интеграциям, начиная с Dntrade.
2. Для каждого pipeline оформить безопасный backlog мелких улучшений без смены архитектуры.
3. Выделить общие проблемные слои, которые повторяются в нескольких адаптерах.
4. После этого запускать узкие refactoring этапы по shared infrastructure.
5. Отдельным треком провести admin/UI redesign на уже подтверждённой модели настроек.
6. Завершить review по backup/security/containerization и только потом решать вопрос более крупной архитектурной перестройки.

## Implementation Principles

- Сначала фиксируем фактический runtime flow, потом меняем код.
- Никаких массовых rename/move реорганизаций без доказанной пользы.
- Любой refactor должен иметь ограниченный радиус влияния.
- Общие helper-слои меняем только после того, как проблема подтверждена минимум в одном реальном pipeline.
- Для high-impact изменений нужны отдельные проверки scheduler/runtime поведения.
- Документация должна ссылаться на реальные файлы, функции и зависимости проекта.

## Risks

- Hidden coupling через `app/services/database_service.py`, export services и notification layer.
- Неполный контекст по реально используемым env и продовым scheduler-запускам.
- Смешение локальных seller-specific проблем с общими architectural issues.
- Потенциальная деградация производительности при "косметических" изменениях в общих сервисах.
- Риск задокументировать неверный runtime, если часть flow активируется только через окружение или внешние cron/systemd сценарии.

## Status Summary

| Workstream | Status | Next step |
| --- | --- | --- |
| Seller pipelines audit and cleanup | in_progress | Догнать docs/status, закрыть cleanup-хвосты и не трогать high-risk форматы без отдельного повода |
| Admin/developer/business settings UI redesign | planned | Зафиксировать текущий backend/UI scope и pain points |
| Legacy catalog cleanup and master catalog review | planned | Найти реально используемые vs legacy catalog paths |
| Backup reliability and restore testing | planned | Собрать текущие backup/restore процедуры |
| Security review | planned | Провести inventory auth/secrets/logging surface |
| Docker/containerization feasibility | planned | Описать stateful/runtime зависимости процесса |

## Format Status Snapshot

Локальные hardening-проходы уже выполнены для следующих форматов:

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

Практический смысл этого статуса:

- эти adapters больше не являются лучшими немедленными target-ами для следующего refactoring step;
- к ним стоит возвращаться только при production-инцидентах, при изменении внешних API/contracts или если появится отдельный explicit cleanup goal;
- следующий реальный выигрыш, скорее всего, будет уже в ещё не доведённых format-ах, а не в повторном проходе по перечисленным выше.
