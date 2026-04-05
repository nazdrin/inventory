import asyncio
import time

from app.services.database_service import process_database_service

from app.checkbox_data_service.checkbox_common import (
    fetch_all_products,
    get_logger,
    resolve_api_key,
    save_to_json,
)

DEFAULT_VAT = 20


def transform_products(products):
    transformed = []
    for product in products:
        transformed.append(
            {
                "code": product.get("id"),
                "name": product.get("name"),
                "vat": DEFAULT_VAT,
                "producer": "",
                "barcode": product.get("barcode"),
            }
        )
    return transformed


async def run_service(enterprise_code, file_type):
    logger = get_logger(f"catalog.{enterprise_code}")
    run_started_at = time.monotonic()

    api_key = await resolve_api_key(enterprise_code, logger)
    all_products, fetch_summary = fetch_all_products(api_key, logger)
    if not all_products:
        logger.warning("Checkbox catalog returned empty dataset: enterprise_code=%s", enterprise_code)
        return

    transformed_data = transform_products(all_products)
    logger.info(
        "Checkbox catalog transform summary: enterprise_code=%s incoming=%s transformed=%s",
        enterprise_code,
        len(all_products),
        len(transformed_data),
    )

    json_file_path = save_to_json(transformed_data, enterprise_code, "catalog", logger)
    if not json_file_path:
        return

    logger.info(
        "Checkbox catalog run summary: enterprise_code=%s pages=%s fetched=%s transformed=%s elapsed=%.2fs",
        enterprise_code,
        fetch_summary["pages_fetched"],
        fetch_summary["fetched_records"],
        len(transformed_data),
        time.monotonic() - run_started_at,
    )
    await process_database_service(json_file_path, "catalog", enterprise_code)


if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "256"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE, "catalog"))
