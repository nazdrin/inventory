import asyncio
import time

from app.services.database_service import process_database_service

from app.prom_data_service.prom_common import (
    fetch_enterprise_settings,
    fetch_products,
    get_logger,
    save_to_json,
)


def transform_products(products, branch_id):
    transformed = []
    for product in products.get("products", []):
        quantity = product.get("quantity_in_stock", 0)
        quantity = max(quantity, 0) if quantity is not None else 0
        transformed.append(
            {
                "branch": str(branch_id),
                "code": str(product.get("id")),
                "price": float(product.get("price", 0.0)),
                "qty": quantity,
                "price_reserve": float(product.get("price", 0.0)),
            }
        )
    return transformed


async def run_prom(enterprise_code, file_type):
    logger = get_logger(f"stock.{enterprise_code}")
    run_started_at = time.monotonic()

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings or not enterprise_settings.token:
        logger.error("Prom stock misconfiguration: token not found for enterprise_code=%s", enterprise_code)
        return

    response, fetch_summary = fetch_products(enterprise_settings.token, logger)
    transformed_data = transform_products(response, enterprise_settings.branch_id)
    logger.info(
        "Prom stock transform summary: enterprise_code=%s branch=%s fetched=%s transformed=%s",
        enterprise_code,
        enterprise_settings.branch_id,
        fetch_summary["fetched_records"],
        len(transformed_data),
    )

    json_file_path = save_to_json(transformed_data, enterprise_code, "stock", logger)
    if not json_file_path:
        return

    logger.info(
        "Prom stock run summary: enterprise_code=%s branch=%s fetched=%s transformed=%s elapsed=%.2fs",
        enterprise_code,
        enterprise_settings.branch_id,
        fetch_summary["fetched_records"],
        len(transformed_data),
        time.monotonic() - run_started_at,
    )
    await process_database_service(json_file_path, "stock", enterprise_code)


if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "777"
    asyncio.run(run_prom(TEST_ENTERPRISE_CODE, "stock"))
