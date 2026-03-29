# Google Drive Pipeline Audit

## 1. Scope

Покрывает:
- [app/google_drive/google_drive_service.py](/Users/dmitrijnazdrin/inventory_service_1/app/google_drive/google_drive_service.py)
- [app/google_drive/data_converter.py](/Users/dmitrijnazdrin/inventory_service_1/app/google_drive/data_converter.py)
- зависимости на `validate_data(...)`, `process_database_service(...)`, `EnterpriseSettings`, `DeveloperSettings`

## 2. Related files and dependencies

- Drive auth через `GOOGLE_DRIVE_CREDENTIALS_PATH`
- Folder IDs из `EnterpriseSettings.google_drive_folder_id_ref/rest`
- Конвертер поддерживает Excel/CSV/XML/JSON
- Save path: `process_database_service`

## 3. Current catalog flow

- Scheduler вызывает `extract_catalog_from_google_drive()`
- Читаются enterprise settings и developer settings
- Файлы каталога скачиваются из Google Drive во временную папку
- Для каждого файла вызывается `validate_data(...)`, которая дальше использует `data_converter`
- В `process_data_converter()` данные конвертируются, добавляются `branch_id/branch`, приводятся типы, пишется JSON и вызывается `process_database_service`

## 4. Current stock flow

- Аналогичный drive download flow через `google_drive_folder_id_rest`
- В `data_converter` stock строки нормализуются в `branch/code/price/qty/price_reserve`
- Для single_store может добавляться branch через `store_serial`

## 5. Findings

### DB inefficiencies

- Многоступенчатый flow repeatedly читает enterprise settings и branch metadata.
- Повторяется общий delete/export/save overhead после file validation/conversion.

### Heavy transformations

- Конвертер поддерживает несколько форматов и выполняет универсальную нормализацию ключей/типов.
- XML/Excel/CSV все сводятся к одному generic path, что даёт гибкость, но и лишнюю сложность.

### Config/env issues

- Используется `TEMP_DIR`, а не только `TEMP_FILE_PATH`.
- Runtime зависит от корректности Google service account file и folder IDs.

### Structure/code issues

- Реальный pipeline размазан между drive service, validator и converter.
- `data_converter.py` смешивает parsing, normalization, branch enrichment и persistence.

### Reliability issues

- Все downloaded files обрабатываются подряд; нет explicit latest-file strategy.
- Ошибки части файлов не останавливают весь run uniformly.
- Модуль очень generic, поэтому любая правка имеет широкий радиус для всех GoogleDrive enterprises.

### Logging/observability gaps

- Есть event logs, но трудно собрать единый run trace на один enterprise/file.

## 6. Risk classification

- High: generic multi-format converter всё ещё имеет широкий радиус влияния.
- Medium: scattered flow через несколько слоёв и env/folder dependency остаются.
- Low: orchestration observability и часть data-normalization рисков уже закрыты.

## 7. Current status after safe pass

Уже сделано:

1. В `google_drive_service.py` добавлен unified orchestration helper и enterprise-level run summary.
2. Исправлен file outcome tracking: `validate_data(...) == False` больше не считается `success`.
3. В `data_converter.py` добавлена нормализация:
   - `stock`: `price_reserve` зажимается до `price`, если аномально больше;
   - `catalog`: невалидные и локально дублирующиеся строки отбрасываются до persistence.
4. Продовый прогон подтвердил устранение двух реальных failure modes: `catalog_pre_delete_guard` и `stock_pre_delete_guard` для конкретных GoogleDrive enterprises.

Текущий practical status:

- framework приведён в более безопасное рабочее состояние;
- но из-за широкого generic converter surface его всё равно следует трогать осторожно.

## 8. Notes about differences from Dntrade

- В отличие от Dntrade, это generic file-ingestion framework, а не один seller API adapter.
- Повторяются shared persistence issues, но уникальный риск здесь в чрезмерно универсальном converter path.
