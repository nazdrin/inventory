# Business Store Branch Sync Apply 223

Дата: 2026-04-23

## Scope

Цель шага:

- применить `branch-sync` только для `enterprise_code=223`;
- не менять runtime catalog/stock/order/outbound;
- не менять DB schema;
- зафиксировать фактический результат apply и post-check.

## Команда apply

```bash
.venv/bin/python -m app.scripts.business_store_branch_sync --enterprise-code 223 --apply --output-json
```

## Raw apply result summary

Фактический результат apply:

- `status = ok`
- `enterprise_code_filter = 223`
- `enterprises_scanned = 1`
- `mapping_branch_rows = 4`
- `stores_found = 1`
- `duplicates = 0`
- `missing_stores_to_create = 3`
- `orphan_stores_to_deactivate = 0`
- `created = 3`
- `deactivated = 0`

Важно:

- исторический dry-run snapshot до этого шага говорил про orphan store `business_223` с branch `30421`;
- но в момент фактического apply в `mapping_branch` для enterprise `223` уже было `4` branch rows, включая `30421`;
- поэтому `business_223` на момент apply уже не был orphan и деактивация не понадобилась.

## Stores Before / After

### Historical before snapshot

Подтверждённый ранее dry-run snapshot для `223`:

- `missing_stores_to_create = 3`
- `orphan_stores_to_deactivate = 1`
- missing branches:
  - `30422`
  - `30423`
  - `30491`
- historical orphan:
  - `business_223` with branch `30421`

### Apply-time before snapshot

Фактическое состояние в момент apply уже отличалось:

- `mapping_branch_rows = 4`
- `stores_found = 1`
- `missing_stores_to_create = 3`
- `orphan_stores_to_deactivate = 0`

Это означает, что `mapping_branch` к моменту apply уже содержал:

- `30421`
- `30422`
- `30423`
- `30491`

### After apply

Read-only post-check после apply:

- `store_count = 4`
- `duplicates = []`
- `tabletki_branches = [30421, 30422, 30423, 30491]`
- `mapping_branches = [30421, 30422, 30423, 30491]`

Вывод:

- `enterprise 223` после apply согласован с текущим `mapping_branch`;
- все branch values из `mapping_branch` теперь представлены в `BusinessStore`;
- duplicate rows по `(enterprise_code, tabletki_branch)` не появились.

## Created Stores

Созданы stores:

1. `business_223_30422`
2. `business_223_30423`
3. `business_223_30491`

Проверенные значения created rows:

| store_code | store_name | enterprise_code | tabletki_enterprise_code | tabletki_branch | is_active | stock_enabled | orders_enabled | migration_status | salesdrive_enterprise_id | legacy_scope_key |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `business_223_30422` | `test-- / 30422` | `223` | `223` | `30422` | `true` | `false` | `false` | `draft` | `null` | `null` |
| `business_223_30423` | `test-- / 30423` | `223` | `223` | `30423` | `true` | `false` | `false` | `draft` | `null` | `null` |
| `business_223_30491` | `test-- / 30491` | `223` | `223` | `30491` | `true` | `false` | `false` | `draft` | `null` | `null` |

Отдельно подтверждено:

- `legacy_scope_key` у новых rows не был заполнен автоматически;
- `tabletki_enterprise_code` совпадает с `enterprise_code`;
- safe defaults применились как ожидалось.

## Deactivated Stores

Фактический результат apply:

- `deactivated = 0`

Причина:

- на момент apply `business_223` уже совпадал с существующим branch `30421` в `mapping_branch`;
- therefore no orphan row remained to deactivate.

## Post-check Summary

Использованные проверки:

1. `python3 -m compileall app`
2. `.venv/bin/python -c "import app.services.business_store_branch_sync_service as m; print('branch sync service ok')"`
3. apply CLI for `223`
4. read-only ORM post-check по:
   - `BusinessStore.enterprise_code = '223'`
   - `MappingBranch.enterprise_code = '223'`
   - duplicates by `(enterprise_code, tabletki_branch)`
5. повторный read-only report:
   - `build_business_store_branch_sync_report(session, enterprise_code='223')`

Post-check result:

- `stores_found = 4`
- `missing_stores_to_create = 0`
- `orphan_stores_to_deactivate = 0`
- `duplicates = 0`

## Dry-run After Apply

Повторный post-check report после apply:

- `status = ok`
- `stores_found = 4`
- `mapping_branch_rows = 4`
- `missing_stores_to_create = 0`
- `orphan_stores_to_deactivate = 0`
- `duplicates = 0`

Примечание:

- ранее параллельно запущенный dry-run дал stale snapshot до commit;
- финальным источником истины для after-state считать последовательный post-check report и read-only DB query после apply.

## Remaining Risks

- новые stores созданы с безопасными, но пустыми business-specific overlays:
  - `legacy_scope_key = null`
  - `salesdrive_enterprise_id = null`
  - `stock_enabled = false`
  - `orders_enabled = false`
- эти rows готовы как branch overlays, но не готовы к live stock/orders без дальнейшей операторской настройки;
- `migration_status = draft` сохраняет их вне live rollout;
- runtime catalog/stock/order/outbound этим шагом не менялся.

## Conclusion

Итог:

- `enterprise 223` после apply **aligned with mapping_branch**
- missing stores созданы
- duplicates не появились
- silent delete не было
- orphan deactivation в этом конкретном apply не понадобилась, потому что на момент запуска `30421` уже существовал в `mapping_branch`
