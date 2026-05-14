import argparse
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import func, select

from app.database import get_async_db
from app.models import (
    BusinessStore,
    BusinessStoreOffer,
    DeveloperSettings,
    EnterpriseSettings,
    MasterCatalog,
    Offer,
)
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
    catalog_only_in_stock: bool = False
    stock_scope_store_id: int | None = None
    stock_scope_store_code: str = ""
    stock_source: str = "master_all"
    stock_positive_products: int = 0
    warnings: List[str] | None = None
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
        "catalog_only_in_stock": stats.catalog_only_in_stock,
        "stock_scope_store_id": stats.stock_scope_store_id,
        "stock_scope_store_code": stats.stock_scope_store_code,
        "stock_source": stats.stock_source,
        "stock_positive_products": stats.stock_positive_products,
        "warnings": stats.warnings or [],
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


async def _resolve_main_store_stock_filter(
    enterprise_settings: EnterpriseSettings,
) -> tuple[set[str] | None, Dict[str, Any]]:
    enterprise_code = _clean_text(enterprise_settings.enterprise_code)
    branch_id = _clean_text(enterprise_settings.branch_id)
    meta: Dict[str, Any] = {
        "catalog_only_in_stock": False,
        "stock_scope_store_id": None,
        "stock_scope_store_code": "",
        "stock_source": "master_all",
        "stock_positive_products": 0,
        "warnings": [],
    }

    if not enterprise_code or not branch_id:
        meta["warnings"].append(
            "EnterpriseSettings.enterprise_code or branch_id is empty; stock-limited catalog filter is disabled."
        )
        return None, meta

    async with get_async_db() as session:
        stores = (
            await session.execute(
                select(BusinessStore).where(
                    BusinessStore.enterprise_code == enterprise_code,
                    BusinessStore.tabletki_branch == branch_id,
                    BusinessStore.is_active.is_(True),
                )
            )
        ).scalars().all()

        if not stores:
            meta["warnings"].append(
                "No active main BusinessStore found by enterprise_code + branch_id; stock-limited catalog filter is disabled."
            )
            return None, meta
        if len(stores) > 1:
            raise RuntimeError(
                "Ambiguous main BusinessStore for stock-limited Tabletki catalog export: "
                f"enterprise_code={enterprise_code} branch_id={branch_id}"
            )

        store = stores[0]
        meta["catalog_only_in_stock"] = bool(store.catalog_only_in_stock)
        meta["stock_scope_store_id"] = int(store.id)
        meta["stock_scope_store_code"] = _clean_text(store.store_code)
        if not store.catalog_only_in_stock:
            return None, meta

        store_offer_rows_count = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(BusinessStoreOffer)
                    .where(BusinessStoreOffer.store_id == int(store.id))
                )
            ).scalar_one()
            or 0
        )

        if store_offer_rows_count > 0:
            rows = (
                await session.execute(
                    select(BusinessStoreOffer.product_code)
                    .where(
                        BusinessStoreOffer.store_id == int(store.id),
                        func.coalesce(BusinessStoreOffer.stock, 0) > 0,
                    )
                    .distinct()
                )
            ).scalars().all()
            product_codes = {_clean_text(value) for value in rows if _clean_text(value)}
            meta["stock_source"] = "business_store_offers"
        else:
            legacy_scope_key = _clean_text(store.legacy_scope_key)
            if not legacy_scope_key:
                raise RuntimeError(
                    "Main BusinessStore has catalog_only_in_stock=true, but neither "
                    "business_store_offers nor legacy_scope_key is available."
                )
            rows = (
                await session.execute(
                    select(Offer.product_code)
                    .where(
                        Offer.city == legacy_scope_key,
                        func.coalesce(Offer.stock, 0) > 0,
                    )
                    .distinct()
                )
            ).scalars().all()
            product_codes = {_clean_text(value) for value in rows if _clean_text(value)}
            meta["stock_source"] = "legacy_offers"

        meta["stock_positive_products"] = len(product_codes)
        if not product_codes:
            raise RuntimeError(
                "Main BusinessStore has catalog_only_in_stock=true, but no products with positive stock were found; "
                "Tabletki catalog export is stopped to avoid publishing an empty catalog."
            )

        return product_codes, meta


async def _load_master_catalog_rows(
    limit: int = 0,
    product_codes: set[str] | None = None,
) -> tuple[List[MasterCatalog], int]:
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
        if product_codes is not None:
            stmt = stmt.where(MasterCatalog.sku.in_(sorted(product_codes)))
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
    developer_settings, enterprise_settings = await _get_export_settings(str(enterprise_code))
    stock_product_codes, stock_meta = await _resolve_main_store_stock_filter(enterprise_settings)

    stats.catalog_only_in_stock = bool(stock_meta["catalog_only_in_stock"])
    stats.stock_scope_store_id = stock_meta["stock_scope_store_id"]
    stats.stock_scope_store_code = stock_meta["stock_scope_store_code"]
    stats.stock_source = stock_meta["stock_source"]
    stats.stock_positive_products = int(stock_meta["stock_positive_products"])
    stats.warnings = list(stock_meta["warnings"])

    master_rows, archived_count = await _load_master_catalog_rows(
        limit=limit,
        product_codes=stock_product_codes,
    )
    stats.products_selected = len(master_rows)
    stats.products_skipped_archived = archived_count

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
                f"catalog_only_in_stock={stats.catalog_only_in_stock}\n"
                f"stock_source={stats.stock_source}\n"
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
