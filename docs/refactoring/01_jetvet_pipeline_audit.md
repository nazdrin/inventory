# JetVet Pipeline Audit

## 1. Scope

Покрывает:
- [app/jetvet_data_service/jetvet_google_drive.py](/Users/dmitrijnazdrin/inventory_service_1/app/jetvet_data_service/jetvet_google_drive.py)
- [app/jetvet_data_service/jetvet_catalog_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/jetvet_data_service/jetvet_catalog_conv.py)
- [app/jetvet_data_service/jetvet_stock_conv.py](/Users/dmitrijnazdrin/inventory_service_1/app/jetvet_data_service/jetvet_stock_conv.py)

## 2. Related files and dependencies

- Google Drive auth via `GOOGLE_DRIVE_CREDENTIALS_PATH`
- Catalog folder: `EnterpriseSettings.google_drive_folder_id_ref`
- Stock folders: `MappingBranch.google_folder_id`
- Stock parsing uses `pandas`; catalog uses `csv.DictReader`

## 3. Current catalog flow

- Catalog files скачиваются из одной Google Drive папки
- Для каждого файла вызывается `process_jetvet_catalog()`
- Конвертер определяет кодировку через `chardet`, сохраняет original CSV copy, читает `;`-separated rows
- Формирует `code/name/barcode/vat/producer` и отправляет в `process_database_service`

## 4. Current stock flow

- Для каждого branch с `google_folder_id` скачиваются все файлы
- `process_jetvet_stock()` читает CSV через `pandas`
- Каждой строке назначается branch из caller-а
- Stock JSON уходит в общий persistence path

## 5. Findings

### DB inefficiencies

- Stock fan-out по branch folders делает много последовательных runs и downstream writes.

### Heavy transformations

- Catalog и stock используют разные parsing stacks для похожих CSV-like inputs.
- Catalog сохраняет копию исходного файла для каждого запуска.

### Config/env issues

- Используется `TEMP_DIR` для download и `TEMP_FILE_PATH` внутри catalog converter, что даёт два temp-domain.

### Structure/code issues

- Drive orchestration и file converters разделены хорошо, но catalog/stock parsing asymmetrical.
- Stock converter получает `single_store/store_serial`, но фактически использует только `branch`.

### Reliability issues

- Все файлы во всех branch folders обрабатываются подряд; нет latest-file/filter strategy.
- Кодировка определяется эвристически; возможны silent parse issues.

### Logging/observability gaps

- Нет единого summary per enterprise/branch/file count.

## 6. Risk classification

- Medium: all-files processing per folder without selection strategy остаётся.
- Medium: dual temp paths, divergent parsing stacks и heuristic encoding detection остаются conscious tradeoff.
- Low: observability gaps уже существенно снижены.

## 7. Current status after safe pass

Уже сделано:

1. Добавлены summary per folder/branch/file/run.
2. Добавлен partial-success accounting по упавшим файлам.
3. Сохранение original CSV в catalog path стало условным через `JETVET_SAVE_ORIGINAL_CSV`.

Текущий practical status:

- adapter закрыт на текущем этапе;
- file selection policy остаётся отдельным optional improvement, но уже не выглядит blocker-ом.

## 8. Notes about differences from Dntrade

- Нет API pagination, но есть branch-folder fan-out и file selection risk.
- Уникальная проблема: catalog и stock используют разные parsing engines для похожего CSV source.
