import asyncio
import time

from app.services.database_service import process_database_service

from app.prom_data_service.prom_common import (
    fetch_enterprise_settings,
    fetch_products,
    get_logger,
    save_to_json,
)


def transform_products(products):
    transformed = []
    for product in products.get("products", []):
        transformed.append(
            {
                "code": str(product.get("id")),
                "name": product.get("name"),
                "vat": 20,
                "producer": "",
                "morion": "",
                "tabletki": "",
                "barcode": "",
                "badm": "",
                "optima": "",
            }
        )
    return transformed


async def run_prom(enterprise_code, file_type):
    logger = get_logger(f"catalog.{enterprise_code}")
    run_started_at = time.monotonic()

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings or not enterprise_settings.token:
        logger.error("Prom catalog misconfiguration: token not found for enterprise_code=%s", enterprise_code)
        return

    response, fetch_summary = fetch_products(enterprise_settings.token, logger)
    transformed_data = transform_products(response)
    logger.info(
        "Prom catalog transform summary: enterprise_code=%s fetched=%s transformed=%s",
        enterprise_code,
        fetch_summary["fetched_records"],
        len(transformed_data),
    )

    json_file_path = save_to_json(transformed_data, enterprise_code, "catalog", logger)
    if not json_file_path:
        return

    logger.info(
        "Prom catalog run summary: enterprise_code=%s fetched=%s transformed=%s elapsed=%.2fs",
        enterprise_code,
        fetch_summary["fetched_records"],
        len(transformed_data),
        time.monotonic() - run_started_at,
    )
    await process_database_service(json_file_path, "catalog", enterprise_code)


if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "777"
    asyncio.run(run_prom(TEST_ENTERPRISE_CODE, "catalog"))
