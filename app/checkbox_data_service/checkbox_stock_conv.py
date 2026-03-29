import asyncio
import time

from app.services.database_service import process_database_service

from app.checkbox_data_service.checkbox_common import (
    fetch_all_products,
    fetch_branch_by_enterprise,
    get_logger,
    resolve_api_key,
    save_to_json,
)


def transform_stock(products, branch):
    def safe_div(value, divisor):
        return (value or 0) / divisor

    return [
        {
            "branch": branch,
            "code": product.get("id"),
            "price": safe_div(product.get("price"), 100),
            "qty": safe_div(product.get("count"), 1000),
            "price_reserve": safe_div(product.get("price"), 100),
        }
        for product in products
    ]


async def run_service(enterprise_code, file_type):
    logger = get_logger(f"stock.{enterprise_code}")
    run_started_at = time.monotonic()

    api_key = await resolve_api_key(enterprise_code, logger)
    branch = await fetch_branch_by_enterprise(enterprise_code)

    all_products, fetch_summary = fetch_all_products(api_key, logger)
    if not all_products:
        logger.warning("Checkbox stock returned empty dataset: enterprise_code=%s", enterprise_code)
        return

    transformed_data = transform_stock(all_products, branch)
    logger.info(
        "Checkbox stock transform summary: enterprise_code=%s branch=%s incoming=%s transformed=%s",
        enterprise_code,
        branch,
        len(all_products),
        len(transformed_data),
    )

    json_file_path = save_to_json(transformed_data, enterprise_code, "stock", logger)
    if not json_file_path:
        return

    logger.info(
        "Checkbox stock run summary: enterprise_code=%s branch=%s pages=%s fetched=%s transformed=%s elapsed=%.2fs",
        enterprise_code,
        branch,
        fetch_summary["pages_fetched"],
        fetch_summary["fetched_records"],
        len(transformed_data),
        time.monotonic() - run_started_at,
    )
    await process_database_service(json_file_path, "stock", enterprise_code)


if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "256"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE, "stock"))
