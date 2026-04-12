# Systemd Migration Plan

## Purpose

Зафиксировать список `systemd` service templates проекта `inventory_service`, которые относятся к Python-сервисам и должны использовать единый запуск через:

```bash
python -m app.services.<module_name>
```

Документ является analysis-only артефактом. Он не описывает применение изменений на сервере.

## Scope

Документ покрывает:

- service templates в `deploy/systemd/`;
- текущий `ExecStart` в templates;
- исходный production-style `ExecStart` из inventory;
- целевой `ExecStart` в формате `python -m`;
- приоритетность и риск миграции по сервисам.

Документ не покрывает:

- `fastapi.service`;
- `backup_db.service`;
- `*.timer`;
- применение этих изменений на production.

## Migration Table

| Service | Old ExecStart | New ExecStart | Module name |
| --- | --- | --- | --- |
| `stock_scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/stock_scheduler_service.py` | `/root/inventory/.venv/bin/python -m app.services.stock_scheduler_service` | `app.services.stock_scheduler_service` |
| `catalog_scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/catalog_scheduler_service.py` | `/root/inventory/.venv/bin/python -m app.services.catalog_scheduler_service` | `app.services.catalog_scheduler_service` |
| `order_scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/order_scheduler_service.py` | `/root/inventory/.venv/bin/python -m app.services.order_scheduler_service` | `app.services.order_scheduler_service` |
| `competitor_price_scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/competitor_price_scheduler.py` | `/root/inventory/.venv/bin/python -m app.services.competitor_price_scheduler` | `app.services.competitor_price_scheduler` |
| `biotus_check_order_scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/biotus_check_order_scheduler.py` | `/root/inventory/.venv/bin/python -m app.services.biotus_check_order_scheduler` | `app.services.biotus_check_order_scheduler` |
| `business_stock_scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/business_stock_scheduler_service.py` | `/root/inventory/.venv/bin/python -m app.services.business_stock_scheduler_service` | `app.services.business_stock_scheduler_service` |
| `master-catalog-scheduler.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/master_catalog_scheduler_service.py` | `/root/inventory/.venv/bin/python -m app.services.master_catalog_scheduler_service` | `app.services.master_catalog_scheduler_service` |
| `tabletki-cancel-retry.service` | `/root/inventory/.venv/bin/python -m app.services.tabletki_cancel_retry_service` | `/root/inventory/.venv/bin/python -m app.services.tabletki_cancel_retry_service` | `app.services.tabletki_cancel_retry_service` |
| `telegram_bot.service` | `/root/inventory/.venv/bin/python /root/inventory/app/services/telegram_bot.py` | `/root/inventory/.venv/bin/python -m app.services.telegram_bot` | `app.services.telegram_bot` |

## Service Groups

### Critical

- `order_scheduler.service`
  - directly affects order intake and post-processing;
  - should be treated as highest-risk runtime migration candidate.

### Safe

- `stock_scheduler.service`
- `catalog_scheduler.service`
- `competitor_price_scheduler.service`
- `biotus_check_order_scheduler.service`
- `business_stock_scheduler.service`
- `master-catalog-scheduler.service`

Rationale:

- these are scheduler-like background services;
- they are still important, but their startup normalization is structurally straightforward because each maps cleanly to a single module under `app.services`.

### Requiring Separate Attention

- `telegram_bot.service`
  - service was observed as active but disabled on server;
  - migration of launch style is easy, but runtime role and enablement policy are still operationally ambiguous.

- `tabletki-cancel-retry.service`
  - already uses `python -m`;
  - should be treated as reference implementation rather than migration target.

- `order_scheduler.service`
  - technically migration is straightforward, but operational criticality is high enough to keep it in separate attention alongside the critical label.

## Notes

- `fastapi.service` intentionally stays outside this migration plan because it uses `uvicorn app.main:app`, not a plain `app.services` module launch.
- `backup_db.service` intentionally stays outside this migration plan because it launches a shell script.
- `Old ExecStart` values are taken from the confirmed production inventory in [docs/runtime_systemd_inventory.md](/Users/dmitrijnazdrin/inventory_service_1/docs/runtime_systemd_inventory.md).
- `New ExecStart` values reflect the normalized module-based standard for Python services in repo templates.

## Recommended Next Step

Использовать этот план как checklist при следующем review-шаге:

1. подтвердить, что каждый target module корректно запускается через `python -m`;
2. отдельно проверить high-risk behavior для `order_scheduler.service`;
3. отдельно подтвердить intended status для `telegram_bot.service`;
4. только после этого рассматривать server-side rollout.
