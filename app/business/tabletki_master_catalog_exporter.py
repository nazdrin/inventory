import argparse
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import func, select

from app.database import get_async_db
from app.models import DeveloperSettings, EnterpriseSettings, MasterCatalog
from app.services.notification_service import send_notification
from app.services.catalog_export_service import SUPPLIER_MAPPING, post_data_to_endpoint


logger = logging.getLogger("tabletki_master_catalog_exporter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

VAT_VALUE = 20.0
EXPORTS_DIR = Path("exports")


@dataclass
class ExportStats:
    enterprise_code: str
    products_selected: int = 0
    products_skipped_archived: int = 0
    offers_count: int = 0
    preview_path: str = ""
    sent: bool = False


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _resolve_offer_name(item: MasterCatalog) -> str:
    return (
        _clean_text(item.name_ua)
        or _clean_text(item.name_ru)
        or _clean_text(item.sku)
    )


def _build_suppliers_block(developer_settings: DeveloperSettings) -> List[Dict[str, str]]:
    suppliers: List[Dict[str, str]] = []
    for supplier_key, supplier_id in SUPPLIER_MAPPING.items():
        suppliers.append(
            {
                "ID": str(supplier_id),
                "Name": supplier_key.capitalize(),
                "Edrpo": _clean_text(getattr(developer_settings, supplier_key, None)),
            }
        )
    return suppliers


def _build_supplier_codes(tabletki_code: str, barcode: str) -> List[Dict[str, str]]:
    supplier_codes: List[Dict[str, str]] = []
    if tabletki_code:
        supplier_codes.append({"ID": str(SUPPLIER_MAPPING["tabletki"]), "Code": tabletki_code})
    if barcode:
        supplier_codes.append({"ID": str(SUPPLIER_MAPPING["barcode"]), "Code": barcode})
    return supplier_codes


def _build_offer_payload(item: MasterCatalog, tabletki_code: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    offer_name = _resolve_offer_name(item)
    producer = _clean_text(item.manufacturer)
    barcode = _clean_text(item.barcode)
    normalized_tabletki_code = _clean_text(tabletki_code)
    supplier_codes = _build_supplier_codes(normalized_tabletki_code, barcode)

    payload_offer = {
        "Code": _clean_text(item.sku),
        "Name": offer_name,
        "Producer": producer,
        "VAT": VAT_VALUE,
        "SupplierCodes": supplier_codes,
    }
    preview_row = {
        "code": _clean_text(item.sku),
        "name": offer_name,
        "producer": producer,
        "vat": VAT_VALUE,
        "barcode": barcode,
        "tabletki_code": normalized_tabletki_code,
        "supplier_codes": supplier_codes,
    }
    return payload_offer, preview_row


def _build_preview_document(
    payload: Dict[str, Any],
    preview_rows: List[Dict[str, Any]],
    stats: ExportStats,
) -> Dict[str, Any]:
    return {
        "enterprise_code": stats.enterprise_code,
        "sent": stats.sent,
        "products_selected": stats.products_selected,
        "products_skipped_archived": stats.products_skipped_archived,
        "offers_count": stats.offers_count,
        "payload": payload,
        "preview_rows": preview_rows,
    }


def _save_preview_file(enterprise_code: str, document: Dict[str, Any]) -> str:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORTS_DIR / f"master_catalog_tabletki_{enterprise_code}.json"
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


async def _get_export_settings(enterprise_code: str) -> tuple[DeveloperSettings, EnterpriseSettings]:
    async with get_async_db() as session:
        developer_settings = (
            await session.execute(select(DeveloperSettings).limit(1))
        ).scalar_one_or_none()
        if not developer_settings:
            raise ValueError("DeveloperSettings not found")

        enterprise_settings = (
            await session.execute(
                select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
        ).scalar_one_or_none()
        if not enterprise_settings:
            raise ValueError(f"EnterpriseSettings not found for enterprise_code={enterprise_code}")

        return developer_settings, enterprise_settings


async def _load_master_catalog_rows(limit: int = 0) -> tuple[List[MasterCatalog], int]:
    async with get_async_db() as session:
        archived_count = int(
            (
                await session.execute(
                    select(func.count()).select_from(MasterCatalog).where(MasterCatalog.is_archived.is_(True))
                )
            ).scalar_one()
            or 0
        )

        stmt = (
            select(MasterCatalog)
            .where(MasterCatalog.is_archived.is_(False))
            .order_by(MasterCatalog.sku.asc())
        )
        if limit and limit > 0:
            stmt = stmt.limit(limit)

        rows = (await session.execute(stmt)).scalars().all()
        return rows, archived_count


async def export_master_catalog_to_tabletki(
    enterprise_code: str,
    limit: int = 0,
    send: bool = False,
) -> Dict[str, Any]:
    logger.info(
        "Starting master catalog export for Tabletki: enterprise=%s send=%s limit=%s",
        enterprise_code,
        send,
        limit,
    )

    stats = ExportStats(enterprise_code=str(enterprise_code))
    master_rows, archived_count = await _load_master_catalog_rows(limit=limit)
    stats.products_selected = len(master_rows)
    stats.products_skipped_archived = archived_count

    developer_settings, enterprise_settings = await _get_export_settings(str(enterprise_code))

    offers: List[Dict[str, Any]] = []
    preview_rows: List[Dict[str, Any]] = []
    for item in master_rows:
        payload_offer, preview_row = _build_offer_payload(
            item,
            _clean_text(item.tabletki_guid),
        )
        offers.append(payload_offer)
        preview_rows.append(preview_row)

    payload = {
        "Suppliers": _build_suppliers_block(developer_settings),
        "Offers": offers,
    }
    stats.offers_count = len(offers)

    preview_document = _build_preview_document(payload, preview_rows, stats)
    stats.preview_path = _save_preview_file(str(enterprise_code), preview_document)

    if send:
        endpoint = f"{developer_settings.endpoint_catalog}/Import/Ref/{enterprise_settings.branch_id}"
        await post_data_to_endpoint(
            endpoint,
            payload,
            enterprise_settings.tabletki_login,
            enterprise_settings.tabletki_password,
            str(enterprise_code),
        )
        stats.sent = True
        preview_document["sent"] = True
        Path(stats.preview_path).write_text(
            json.dumps(preview_document, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        send_notification(
            (
                "🟡 Master catalog успешно отправлен в Tabletki\n"
                f"offers_count={stats.offers_count}\n"
                f"preview_path={stats.preview_path}"
            ),
            str(enterprise_code),
        )

    logger.info(
        "Finished master catalog export for Tabletki: enterprise=%s offers=%s sent=%s preview=%s",
        enterprise_code,
        stats.offers_count,
        stats.sent,
        stats.preview_path,
    )
    return asdict(stats)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry-run/send exporter каталога в Tabletki из master_catalog"
    )
    parser.add_argument("--enterprise", required=True, help="enterprise_code, например 223")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N товаров master_catalog (0 = без лимита)",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="реально отправить payload в Tabletki; без флага выполняется dry-run",
    )
    return parser


async def _main_async(args: argparse.Namespace) -> None:
    result = await export_master_catalog_to_tabletki(
        enterprise_code=str(args.enterprise),
        limit=args.limit,
        send=args.send,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import asyncio

    parser = _build_parser()
    asyncio.run(_main_async(parser.parse_args()))
