import re
import time
import xml.etree.ElementTree as ET

from app.services.database_service import process_database_service

from app.dsn_data_service.dsn_common import (
    download_xml,
    fetch_branch_id,
    fetch_feed_url,
    get_logger,
    maybe_save_debug_json,
    save_to_json,
)


def extract_barcode(description: str) -> str:
    match = re.search(r"Штрихкод:\s*(\d+)", description)
    return match.group(1) if match else ""


def parse_xml_to_catalog(xml_string: str, enterprise_code: str, logger) -> list[dict]:
    root = ET.fromstring(xml_string)
    catalog_data = []

    for offer in root.findall(".//offer"):
        offer_id = offer.attrib.get("id")
        name_el = offer.find("name")
        vendor_el = offer.find("vendor")
        description_el = offer.find("description")

        if not offer_id or name_el is None or vendor_el is None:
            continue

        name_text = name_el.text.strip() if name_el.text else ""
        if name_text.startswith("<![CDATA[") and name_text.endswith("]]>"):
            name_text = name_text[9:-3].strip()

        description_text = description_el.text if description_el is not None and description_el.text else ""
        barcode = extract_barcode(description_text)

        catalog_data.append(
            {
                "code": offer_id,
                "name": name_text,
                "vat": 20,
                "producer": vendor_el.text.strip() if vendor_el.text else "",
                "barcode": barcode,
            }
        )

    maybe_save_debug_json(catalog_data, enterprise_code, "catalog_data", logger)
    return catalog_data


def parse_stock_data(xml_string: str, branch_id: str, enterprise_code: str, logger) -> list[dict]:
    root = ET.fromstring(xml_string)
    stock_data = []

    for offer in root.findall(".//offer"):
        offer_id = offer.attrib.get("id")
        price_text = offer.findtext("price")
        try:
            quantity_in_stock = int(offer.findtext("quantity_in_stock", "0"))
            if quantity_in_stock < 0:
                quantity_in_stock = 0
        except (ValueError, TypeError):
            quantity_in_stock = 0

        if not offer_id or not price_text:
            continue

        try:
            price = float(price_text)
        except ValueError:
            continue

        stock_data.append(
            {
                "branch": branch_id,
                "code": offer_id,
                "price": price,
                "qty": quantity_in_stock,
                "price_reserve": price,
            }
        )

    maybe_save_debug_json(stock_data, enterprise_code, "stock_data", logger)
    return stock_data


async def run_service(enterprise_code, file_type):
    logger = get_logger(f"{file_type}.{enterprise_code}")
    run_started_at = time.monotonic()

    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logger.error("DSN %s misconfiguration: feed URL not found for enterprise_code=%s", file_type, enterprise_code)
        return

    if file_type == "catalog":
        xml_data = download_xml(feed_url, logger)
        parsed_data = parse_xml_to_catalog(xml_data, enterprise_code, logger)
        logger.info("DSN catalog parse summary: enterprise_code=%s records=%s", enterprise_code, len(parsed_data))
        json_file_path = save_to_json(parsed_data, enterprise_code, "catalog", logger)
    elif file_type == "stock":
        branch_id = await fetch_branch_id(enterprise_code)
        if not branch_id:
            logger.error("DSN stock misconfiguration: branch not found for enterprise_code=%s", enterprise_code)
            return

        xml_data = download_xml(feed_url, logger)
        parsed_data = parse_stock_data(xml_data, branch_id, enterprise_code, logger)
        logger.info(
            "DSN stock parse summary: enterprise_code=%s branch=%s records=%s",
            enterprise_code,
            branch_id,
            len(parsed_data),
        )
        json_file_path = save_to_json(parsed_data, enterprise_code, "stock", logger)
    else:
        raise ValueError("file_type must be 'catalog' or 'stock'")

    if not json_file_path:
        return

    logger.info(
        "DSN %s run summary: enterprise_code=%s records=%s elapsed=%.2fs",
        file_type,
        enterprise_code,
        len(parsed_data),
        time.monotonic() - run_started_at,
    )
    await process_database_service(json_file_path, file_type, enterprise_code)
