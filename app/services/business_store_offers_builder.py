from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_price_adjustment_generator import (
    apply_extra_markup,
    generate_stable_markup_percent,
)
from app.business.dropship_pipeline import (
    PARSERS,
    _apply_price_jitter,
    _call_parser_kw,
    _cap_price_not_above_competitor,
    _compute_price_for_item_with_source,
    _round_price_export_for_supplier,
    fetch_active_offer_blocks,
    map_supplier_codes,
    resolve_price_band,
)
from app.models import (
    BusinessStore,
    BusinessStoreOffer,
    BusinessStoreSupplierSettings,
    DropshipEnterprise,
    EnterpriseSettings,
    Offer,
)
from app.services.business_pricing_settings_resolver import (
    BusinessPricingSettingsSnapshot,
    load_business_pricing_settings_snapshot,
)


TWO_PLACES = Decimal("0.01")
ZERO = Decimal("0")
ONE_HUNDRED = Decimal("100")


@dataclass
class _StoreSupplierLink:
    settings: BusinessStoreSupplierSettings
    store: BusinessStore
    enterprise: EnterpriseSettings
    supplier: DropshipEnterprise | None


@dataclass
class _SupplierSourceItem:
    product_code: str
    qty: int
    price_retail: Decimal
    price_opt: Decimal


@dataclass
class _SupplierSourceBundle:
    supplier_code: str
    parser_name: str
    raw_items_count: int
    mapped_items: list[_SupplierSourceItem]
    product_codes: list[str]
    warnings: list[str]
    errors: list[str]


@dataclass(frozen=True)
class _MarketScopeResolution:
    market_scope_key: str | None
    source: str
    warnings: tuple[str, ...] = ()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    normalized = _clean_text(value)
    if not normalized:
        return None
    try:
        return Decimal(normalized)
    except Exception:
        return None


def _round_money(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


def _json_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


def _json_datetime(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return None


def _price_source_label(source: str) -> str:
    normalized = _clean_text(source) or "unknown"
    return f"store_native:{normalized}"


def _resolve_market_scope(link: _StoreSupplierLink) -> _MarketScopeResolution:
    store_scope = _clean_text(link.store.legacy_scope_key)
    if store_scope:
        return _MarketScopeResolution(
            market_scope_key=store_scope,
            source="store_legacy_scope_key_observed_only",
        )

    return _MarketScopeResolution(
        market_scope_key=None,
        source="market_scope_unused",
    )


def _supplier_dumping_mode(link: _StoreSupplierLink) -> bool:
    return bool(getattr(link.supplier, "use_feed_instead_of_gdrive", False))


def _effective_dumping_mode(link: _StoreSupplierLink) -> bool:
    if link.settings.dumping_mode is None:
        return _supplier_dumping_mode(link)
    return bool(link.settings.dumping_mode)


def _supplier_threshold_uah(link: _StoreSupplierLink) -> Decimal:
    supplier_threshold = _decimal_or_none(getattr(link.supplier, "min_markup_threshold", None))
    return supplier_threshold if supplier_threshold is not None else ZERO


def _effective_threshold_uah(link: _StoreSupplierLink) -> Decimal:
    override = _decimal_or_none(link.settings.min_markup_threshold)
    if override is not None:
        return override
    return _supplier_threshold_uah(link)


def _select_band_add_uah(
    competitor_price: Decimal,
    band: str,
    pricing_snapshot: BusinessPricingSettingsSnapshot,
) -> Decimal:
    has_competitor = competitor_price > 0
    if has_competitor:
        if band == "LOW":
            return pricing_snapshot.pricing_thr_add_low_uah
        if band == "MID":
            return pricing_snapshot.pricing_thr_add_mid_uah
        return pricing_snapshot.pricing_thr_add_high_uah
    if band == "LOW":
        return pricing_snapshot.pricing_no_comp_add_low_uah
    if band == "MID":
        return pricing_snapshot.pricing_no_comp_add_mid_uah
    return pricing_snapshot.pricing_no_comp_add_high_uah


def _threshold_effective_share(
    *,
    price_opt: Decimal,
    supplier_add_uah: Decimal,
    band_add_uah: Decimal,
    pricing_snapshot: BusinessPricingSettingsSnapshot,
) -> Decimal:
    if price_opt <= 0:
        return ZERO
    return pricing_snapshot.pricing_base_thr + (band_add_uah + supplier_add_uah) / price_opt


def _store_supplier_markup_percent(
    link: _StoreSupplierLink,
    *,
    product_code: str,
) -> tuple[Decimal | None, str | None]:
    settings = link.settings
    if not bool(settings.extra_markup_enabled):
        return None, "disabled"

    markup_mode = _clean_text(settings.extra_markup_mode) or "percent"
    if markup_mode != "percent":
        return None, "unsupported_markup_mode"

    fixed_percent = _decimal_or_none(settings.extra_markup_value)
    if fixed_percent is not None:
        if fixed_percent < 0:
            return None, "invalid_fixed_markup"
        return fixed_percent, "fixed_percent"

    min_percent = _decimal_or_none(settings.extra_markup_min)
    max_percent = _decimal_or_none(settings.extra_markup_max)
    if min_percent is None or max_percent is None:
        return None, "missing_markup_bounds"
    if min_percent < 0 or max_percent < 0 or max_percent < min_percent:
        return None, "invalid_markup_bounds"

    generated = generate_stable_markup_percent(
        int(link.store.id),
        str(product_code or ""),
        min_percent,
        max_percent,
    )
    return generated, "generated_from_store_supplier_range"


def _finalize_calculated_price(
    *,
    pre_jitter_price: Decimal,
    price_source: str,
    competitor_price: Decimal,
    supplier_code: str,
    pricing_snapshot: BusinessPricingSettingsSnapshot,
) -> tuple[Decimal, Decimal, Decimal]:
    price = _round_price_export_for_supplier(pre_jitter_price, supplier_code)
    jitter_delta = ZERO
    if pricing_snapshot.pricing_jitter_enabled:
        base_price = price
        price, jitter_delta = _apply_price_jitter(price, pricing_snapshot)
        price = _round_price_export_for_supplier(price, supplier_code)
        if (
            competitor_price > 0
            and price > competitor_price
            and price_source in {"under_competitor", "comp_fallback_under_competitor"}
        ):
            price = _cap_price_not_above_competitor(competitor_price, supplier_code)
        if price <= 0:
            price = base_price
            jitter_delta = ZERO
    return _round_money(price) or ZERO, _round_money(jitter_delta) or ZERO, _round_money(pre_jitter_price) or ZERO


def _build_legacy_compare_summary(
    *,
    legacy_offer: Offer | None,
    new_effective_price: Decimal,
    new_stock: int,
    priority_used: int | None,
) -> dict[str, Any] | None:
    if legacy_offer is None:
        return None
    old_price = _round_money(_decimal_or_none(legacy_offer.price))
    old_stock = int(legacy_offer.stock or 0)
    price_delta = None
    if old_price is not None:
        price_delta = _round_money(new_effective_price - old_price)
    return {
        "legacy_price": _json_decimal(old_price),
        "legacy_stock": old_stock,
        "new_price": _json_decimal(new_effective_price),
        "new_stock": new_stock,
        "price_delta": _json_decimal(price_delta),
        "stock_delta": new_stock - old_stock,
        "priority_used": priority_used,
    }


def _supplier_snapshot(link: _StoreSupplierLink) -> dict[str, Any]:
    supplier = link.supplier
    if supplier is None:
        return {"exists": False}
    return {
        "exists": True,
        "code": supplier.code,
        "name": supplier.name,
        "is_active": bool(supplier.is_active),
        "is_rrp": bool(supplier.is_rrp),
        "profit_percent": _json_decimal(_decimal_or_none(supplier.profit_percent)),
        "retail_markup": _json_decimal(_decimal_or_none(supplier.retail_markup)),
        "min_markup_threshold": _json_decimal(_decimal_or_none(supplier.min_markup_threshold)),
        "priority": supplier.priority,
        "dumping_mode": _supplier_dumping_mode(link),
        "city": _clean_text(supplier.city) or None,
    }


def _store_supplier_snapshot(link: _StoreSupplierLink) -> dict[str, Any]:
    settings = link.settings
    return {
        "is_active": bool(settings.is_active),
        "priority_override": settings.priority_override,
        "min_markup_threshold": _json_decimal(_decimal_or_none(settings.min_markup_threshold)),
        "extra_markup_enabled": bool(settings.extra_markup_enabled),
        "extra_markup_mode": _clean_text(settings.extra_markup_mode) or None,
        "extra_markup_value": _json_decimal(_decimal_or_none(settings.extra_markup_value)),
        "extra_markup_min": _json_decimal(_decimal_or_none(settings.extra_markup_min)),
        "extra_markup_max": _json_decimal(_decimal_or_none(settings.extra_markup_max)),
        "dumping_mode": None if settings.dumping_mode is None else bool(settings.dumping_mode),
    }


async def _load_active_store_supplier_links(
    session: AsyncSession,
    *,
    store_id: int | None,
    enterprise_code: str | None,
    supplier_code: str | None,
) -> list[_StoreSupplierLink]:
    stmt = (
        select(
            BusinessStoreSupplierSettings,
            BusinessStore,
            EnterpriseSettings,
            DropshipEnterprise,
        )
        .join(BusinessStore, BusinessStore.id == BusinessStoreSupplierSettings.store_id)
        .join(EnterpriseSettings, EnterpriseSettings.enterprise_code == BusinessStore.enterprise_code)
        .outerjoin(DropshipEnterprise, DropshipEnterprise.code == BusinessStoreSupplierSettings.supplier_code)
        .where(
            BusinessStoreSupplierSettings.is_active.is_(True),
            BusinessStore.is_active.is_(True),
            func.lower(func.coalesce(EnterpriseSettings.data_format, "")) == "business",
        )
        .order_by(
            BusinessStore.enterprise_code.asc(),
            BusinessStore.store_code.asc(),
            BusinessStoreSupplierSettings.supplier_code.asc(),
        )
    )
    if store_id is not None:
        stmt = stmt.where(BusinessStore.id == int(store_id))
    if enterprise_code:
        stmt = stmt.where(BusinessStore.enterprise_code == _clean_text(enterprise_code))
    if supplier_code:
        stmt = stmt.where(BusinessStoreSupplierSettings.supplier_code == _clean_text(supplier_code))

    rows = await session.execute(stmt)
    return [
        _StoreSupplierLink(settings=settings, store=store, enterprise=enterprise, supplier=supplier)
        for settings, store, enterprise, supplier in rows.all()
    ]


async def _load_supplier_source_bundle(
    session: AsyncSession,
    link: _StoreSupplierLink,
) -> _SupplierSourceBundle:
    supplier_code = _clean_text(link.settings.supplier_code)
    supplier = link.supplier
    if supplier is None:
        return _SupplierSourceBundle(
            supplier_code=supplier_code,
            parser_name="missing_supplier",
            raw_items_count=0,
            mapped_items=[],
            product_codes=[],
            warnings=[],
            errors=["Supplier row not found in dropship_enterprises."],
        )

    parser = PARSERS.get(supplier.code)
    parser_name = getattr(parser, "__name__", "parse_feed_stock_to_json_template") if parser else "missing_parser"
    try:
        raw_items = await _call_parser_kw(parser, session, supplier) if parser is not None else []
    except Exception as exc:
        return _SupplierSourceBundle(
            supplier_code=supplier_code,
            parser_name=parser_name,
            raw_items_count=0,
            mapped_items=[],
            product_codes=[],
            warnings=[],
            errors=[f"Supplier parser failed: {exc}"],
        )

    try:
        mapped = await map_supplier_codes(session, supplier.code, raw_items)
    except Exception as exc:
        return _SupplierSourceBundle(
            supplier_code=supplier_code,
            parser_name=parser_name,
            raw_items_count=len(raw_items),
            mapped_items=[],
            product_codes=[],
            warnings=[],
            errors=[f"Supplier code mapping failed: {exc}"],
        )

    blocked_global_codes, blocked_supplier_codes = await fetch_active_offer_blocks(session, supplier.code)
    warnings: list[str] = []
    filtered_items: list[_SupplierSourceItem] = []
    for item in mapped:
        product_code = _clean_text(item.get("product_code"))
        if not product_code:
            continue
        if product_code in blocked_global_codes or product_code in blocked_supplier_codes:
            continue
        filtered_items.append(
            _SupplierSourceItem(
                product_code=product_code,
                qty=int(item.get("qty") or 0),
                price_retail=_round_money(_decimal_or_none(item.get("price_retail"))) or ZERO,
                price_opt=_round_money(_decimal_or_none(item.get("price_opt"))) or ZERO,
            )
        )

    if not filtered_items:
        warnings.append("Supplier parser returned no mapped items after block filtering.")

    return _SupplierSourceBundle(
        supplier_code=supplier_code,
        parser_name=parser_name,
        raw_items_count=len(raw_items),
        mapped_items=filtered_items,
        product_codes=[item.product_code for item in filtered_items],
        warnings=warnings,
        errors=[],
    )


async def _load_competitor_map(
    session: AsyncSession,
    *,
    product_codes: list[str],
)-> tuple[dict[str, Decimal], list[str]]:
    if not product_codes:
        return {}, []
    from app.models import CompetitorPrice

    comp_rows = await session.execute(
        select(CompetitorPrice.code, CompetitorPrice.city, CompetitorPrice.competitor_price).where(
            CompetitorPrice.code.in_(product_codes),
        )
    )
    prices_by_code: dict[str, Decimal] = {}
    distinct_price_counts: dict[str, set[str]] = {}
    sampled_cities: dict[str, list[str]] = {}
    for code, city, price in comp_rows.all():
        normalized_code = _clean_text(code)
        normalized_city = _clean_text(city)
        normalized_price = _round_money(_decimal_or_none(price)) or ZERO
        if not normalized_code:
            continue
        distinct_price_counts.setdefault(normalized_code, set()).add(_json_decimal(normalized_price) or "0")
        if normalized_code not in prices_by_code:
            prices_by_code[normalized_code] = normalized_price
        else:
            prices_by_code[normalized_code] = min(prices_by_code[normalized_code], normalized_price)
        if normalized_city:
            sampled_cities.setdefault(normalized_code, [])
            if normalized_city not in sampled_cities[normalized_code] and len(sampled_cities[normalized_code]) < 3:
                sampled_cities[normalized_code].append(normalized_city)

    warnings: list[str] = []
    inconsistent_codes = [
        code for code, values in distinct_price_counts.items()
        if len(values) > 1
    ]
    if inconsistent_codes:
        sample_codes = inconsistent_codes[:10]
        warnings.append(
            "Competitor prices differ across cities for some product codes; "
            "store-native builder used the lowest competitor price per product. "
            f"sample_codes={sample_codes}"
        )
    return prices_by_code, warnings


async def _load_legacy_offers_map(
    session: AsyncSession,
    *,
    supplier_code: str,
    market_scope_key: str,
    product_codes: list[str],
) -> dict[str, Offer]:
    if not product_codes:
        return {}
    rows = await session.execute(
        select(Offer).where(
            Offer.supplier_code == supplier_code,
            Offer.city == market_scope_key,
            Offer.product_code.in_(product_codes),
        )
    )
    return {
        _clean_text(item.product_code): item
        for item in rows.scalars().all()
        if _clean_text(item.product_code)
    }


async def _upsert_business_store_offers(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0

    chunk_size = 500
    for offset in range(0, len(rows), chunk_size):
        chunk = rows[offset: offset + chunk_size]
        stmt = insert(BusinessStoreOffer).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["store_id", "supplier_code", "product_code"],
            set_={
                "enterprise_code": stmt.excluded.enterprise_code,
                "tabletki_branch": stmt.excluded.tabletki_branch,
                "market_scope_key": stmt.excluded.market_scope_key,
                "base_price": stmt.excluded.base_price,
                "effective_price": stmt.excluded.effective_price,
                "wholesale_price": stmt.excluded.wholesale_price,
                "stock": stmt.excluded.stock,
                "priority_used": stmt.excluded.priority_used,
                "price_source": stmt.excluded.price_source,
                "pricing_context": stmt.excluded.pricing_context,
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    return len(rows)


async def _delete_stale_business_store_offers(
    session: AsyncSession,
    *,
    store_id: int,
    supplier_code: str,
    keep_product_codes: set[str],
) -> int:
    if keep_product_codes:
        stmt = delete(BusinessStoreOffer).where(
            BusinessStoreOffer.store_id == int(store_id),
            BusinessStoreOffer.supplier_code == supplier_code,
            BusinessStoreOffer.product_code.not_in(sorted(keep_product_codes)),
        )
    else:
        stmt = delete(BusinessStoreOffer).where(
            BusinessStoreOffer.store_id == int(store_id),
            BusinessStoreOffer.supplier_code == supplier_code,
        )
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


async def build_business_store_offers(
    session: AsyncSession,
    *,
    dry_run: bool = True,
    store_id: int | None = None,
    enterprise_code: str | None = None,
    supplier_code: str | None = None,
    limit: int | None = None,
    compare_legacy: bool = False,
) -> dict[str, Any]:
    pricing_snapshot = await load_business_pricing_settings_snapshot(session)
    links = await _load_active_store_supplier_links(
        session,
        store_id=store_id,
        enterprise_code=enterprise_code,
        supplier_code=supplier_code,
    )

    global_warnings: list[str] = []
    global_errors: list[str] = []
    link_reports: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    compare_summary = {
        "enabled": bool(compare_legacy),
        "matched": 0,
        "missing_in_legacy": 0,
        "price_changed": 0,
        "stock_changed": 0,
        "sample_differences": [],
    }

    stores_total = len({int(link.store.id) for link in links})
    stores_processed_ids: set[int] = set()
    stores_skipped_ids: set[int] = set()
    supplier_links_processed = 0
    supplier_links_skipped = 0
    candidate_products_total = 0
    upsert_rows_total = 0
    price_sources: set[str] = set()
    stale_rows_deleted = 0

    upsert_payload_rows: list[dict[str, Any]] = []
    keep_products_by_link: dict[tuple[int, str], set[str]] = {}
    source_bundle_cache: dict[str, _SupplierSourceBundle] = {}
    competitor_cache: dict[str, tuple[dict[str, Decimal], list[str]]] = {}
    legacy_offers_cache: dict[tuple[str, str], dict[str, Offer]] = {}
    stale_delete_links: set[tuple[int, str]] = set()

    for link in links:
        scope_resolution = _resolve_market_scope(link)
        normalized_scope = _clean_text(scope_resolution.market_scope_key)
        normalized_supplier_code = _clean_text(link.settings.supplier_code)
        link_warnings: list[str] = []
        link_errors: list[str] = []
        link_warnings.extend(scope_resolution.warnings)

        if link.supplier is None:
            link_errors.append("Supplier row not found in dropship_enterprises.")
        elif not bool(link.supplier.is_active):
            link_errors.append("Supplier is inactive in dropship_enterprises.")

        bundle = source_bundle_cache.get(normalized_supplier_code)
        if bundle is None and not link_errors:
            bundle = await _load_supplier_source_bundle(session, link)
            source_bundle_cache[normalized_supplier_code] = bundle

        if bundle is not None:
            link_warnings.extend(bundle.warnings)
            link_errors.extend(bundle.errors)

        source_items = list(bundle.mapped_items if bundle is not None else [])
        if link_errors:
            source_items = []
        elif limit is not None:
            source_items = source_items[: int(limit)]

        candidate_products = len(source_items)
        candidate_products_total += candidate_products
        built_rows_for_link = 0
        link_sample_rows: list[dict[str, Any]] = []
        compare_differences: list[dict[str, Any]] = []
        link_price_sources: set[str] = set()

        competitor_map: dict[str, Decimal] = {}
        legacy_offers_map: dict[str, Offer] = {}
        policy_context: dict[str, Any] | None = None
        if not link_errors and source_items:
            competitor_map, competitor_warnings = competitor_cache.get(normalized_supplier_code) or ({}, [])
            if normalized_supplier_code not in competitor_cache:
                competitor_map, competitor_warnings = await _load_competitor_map(
                    session,
                    product_codes=[item.product_code for item in source_items],
                )
                competitor_cache[normalized_supplier_code] = (competitor_map, competitor_warnings)
            link_warnings.extend(competitor_warnings)
            if compare_legacy:
                cache_key = (normalized_supplier_code, normalized_scope)
                legacy_offers_map = legacy_offers_cache.get(cache_key) or {}
                if cache_key not in legacy_offers_cache and normalized_scope:
                    legacy_offers_map = await _load_legacy_offers_map(
                        session,
                        supplier_code=normalized_supplier_code,
                        market_scope_key=normalized_scope,
                        product_codes=[item.product_code for item in source_items],
                    )
                    legacy_offers_cache[cache_key] = legacy_offers_map
            policy_context = {
                "status": "disabled_in_store_native_builder",
                "reason": "city-scoped balancer policy lookup is intentionally bypassed in the new stock contour",
            }

        supplier_threshold_uah = _supplier_threshold_uah(link)
        effective_threshold_uah = _effective_threshold_uah(link)
        supplier_dumping = _supplier_dumping_mode(link)
        effective_dumping = _effective_dumping_mode(link)
        priority_used = (
            link.settings.priority_override
            if link.settings.priority_override is not None
            else getattr(link.supplier, "priority", None)
        )

        for source_item in source_items:
            product_code = source_item.product_code
            competitor_price = competitor_map.get(product_code, ZERO)
            band = resolve_price_band(source_item.price_opt, pricing_snapshot)
            band_add_uah = _select_band_add_uah(competitor_price, band, pricing_snapshot)

            supplier_threshold_share = _threshold_effective_share(
                price_opt=source_item.price_opt,
                supplier_add_uah=supplier_threshold_uah,
                band_add_uah=band_add_uah,
                pricing_snapshot=pricing_snapshot,
            )
            effective_threshold_share = _threshold_effective_share(
                price_opt=source_item.price_opt,
                supplier_add_uah=effective_threshold_uah,
                band_add_uah=band_add_uah,
                pricing_snapshot=pricing_snapshot,
            )

            base_pre_jitter, base_source = _compute_price_for_item_with_source(
                pricing_snapshot=pricing_snapshot,
                competitor_price=competitor_price if competitor_price > 0 else None,
                is_rrp=bool(getattr(link.supplier, "is_rrp", False)),
                is_dumping=supplier_dumping,
                retail_markup=getattr(link.supplier, "retail_markup", None),
                price_retail=source_item.price_retail,
                price_opt=source_item.price_opt,
                threshold_percent_effective=supplier_threshold_share,
            )
            base_price = _round_money(base_pre_jitter) or ZERO

            effective_pre_jitter, effective_source = _compute_price_for_item_with_source(
                pricing_snapshot=pricing_snapshot,
                competitor_price=competitor_price if competitor_price > 0 else None,
                is_rrp=bool(getattr(link.supplier, "is_rrp", False)),
                is_dumping=effective_dumping,
                retail_markup=getattr(link.supplier, "retail_markup", None),
                price_retail=source_item.price_retail,
                price_opt=source_item.price_opt,
                threshold_percent_effective=effective_threshold_share,
            )

            markup_percent, markup_source = _store_supplier_markup_percent(
                link,
                product_code=product_code,
            )
            if markup_percent is not None:
                marked_up = apply_extra_markup(effective_pre_jitter, markup_percent)
                if marked_up is not None:
                    effective_pre_jitter = Decimal(marked_up)
            elif bool(link.settings.extra_markup_enabled):
                link_warnings.append(f"extra markup not applied for {product_code}: {markup_source}")

            effective_price, jitter_delta, effective_pre_jitter_rounded = _finalize_calculated_price(
                pre_jitter_price=effective_pre_jitter,
                price_source=effective_source,
                competitor_price=competitor_price,
                supplier_code=normalized_supplier_code,
                pricing_snapshot=pricing_snapshot,
            )
            price_sources.add(_price_source_label(effective_source))
            link_price_sources.add(_price_source_label(effective_source))

            pricing_context = {
                "builder_mode": "store_native_independent",
                "source_pipeline": "parser->mapping->pricing->store_overrides",
                "parser_name": bundle.parser_name if bundle is not None else None,
                "source_market_scope_key": normalized_scope or None,
                "source_market_scope_resolution": scope_resolution.source,
                "competitor_lookup_scope": "global_by_product_any_city",
                "source_item": {
                    "product_code": product_code,
                    "qty": int(source_item.qty),
                    "price_retail": _json_decimal(source_item.price_retail),
                    "price_opt": _json_decimal(source_item.price_opt),
                },
                "business_pricing_snapshot": {
                    "source": pricing_snapshot.source,
                    "pricing_base_thr": _json_decimal(pricing_snapshot.pricing_base_thr),
                    "pricing_price_band_low_max": _json_decimal(pricing_snapshot.pricing_price_band_low_max),
                    "pricing_price_band_mid_max": _json_decimal(pricing_snapshot.pricing_price_band_mid_max),
                    "pricing_thr_add_low_uah": _json_decimal(pricing_snapshot.pricing_thr_add_low_uah),
                    "pricing_thr_add_mid_uah": _json_decimal(pricing_snapshot.pricing_thr_add_mid_uah),
                    "pricing_thr_add_high_uah": _json_decimal(pricing_snapshot.pricing_thr_add_high_uah),
                    "pricing_no_comp_add_low_uah": _json_decimal(pricing_snapshot.pricing_no_comp_add_low_uah),
                    "pricing_no_comp_add_mid_uah": _json_decimal(pricing_snapshot.pricing_no_comp_add_mid_uah),
                    "pricing_no_comp_add_high_uah": _json_decimal(pricing_snapshot.pricing_no_comp_add_high_uah),
                    "pricing_comp_discount_share": _json_decimal(pricing_snapshot.pricing_comp_discount_share),
                    "pricing_comp_delta_min_uah": _json_decimal(pricing_snapshot.pricing_comp_delta_min_uah),
                    "pricing_comp_delta_max_uah": _json_decimal(pricing_snapshot.pricing_comp_delta_max_uah),
                    "pricing_jitter_enabled": bool(pricing_snapshot.pricing_jitter_enabled),
                    "pricing_jitter_step_uah": _json_decimal(pricing_snapshot.pricing_jitter_step_uah),
                    "pricing_jitter_min_uah": _json_decimal(pricing_snapshot.pricing_jitter_min_uah),
                    "pricing_jitter_max_uah": _json_decimal(pricing_snapshot.pricing_jitter_max_uah),
                    "inconsistency": pricing_snapshot.inconsistency,
                },
                "supplier_settings": _supplier_snapshot(link),
                "store_supplier_settings": _store_supplier_snapshot(link),
                "balancer_policy_context": policy_context,
                "price_band": band,
                "competitor_price": _json_decimal(competitor_price if competitor_price > 0 else None),
                "band_add_uah": _json_decimal(band_add_uah),
                "supplier_threshold_uah": _json_decimal(supplier_threshold_uah),
                "effective_threshold_uah": _json_decimal(effective_threshold_uah),
                "supplier_threshold_share": _json_decimal(supplier_threshold_share),
                "effective_threshold_share": _json_decimal(effective_threshold_share),
                "base_price_source": base_source,
                "base_price_pre_jitter": _json_decimal(base_pre_jitter),
                "effective_price_source": effective_source,
                "effective_price_pre_jitter": _json_decimal(effective_pre_jitter_rounded),
                "jitter_delta": _json_decimal(jitter_delta),
                "extra_markup_percent": _json_decimal(markup_percent),
                "extra_markup_source": markup_source,
                "priority_used": priority_used,
                "limitations": [],
            }

            if source_item.price_opt <= 0 and effective_threshold_uah > 0:
                pricing_context["limitations"].append(
                    "effective min_markup_threshold could not affect threshold because price_opt is empty."
                )

            legacy_compare = None
            if compare_legacy:
                legacy_offer = legacy_offers_map.get(product_code)
                if legacy_offer is None:
                    compare_summary["missing_in_legacy"] += 1
                else:
                    compare_summary["matched"] += 1
                legacy_compare = _build_legacy_compare_summary(
                    legacy_offer=legacy_offer,
                    new_effective_price=effective_price,
                    new_stock=int(source_item.qty),
                    priority_used=priority_used,
                )
                pricing_context["legacy_compare"] = legacy_compare
                if legacy_compare is not None:
                    if legacy_compare.get("price_delta") not in {None, "0.00"}:
                        compare_summary["price_changed"] += 1
                        if len(compare_differences) < 10:
                            compare_differences.append(
                                {
                                    "product_code": product_code,
                                    "old_price": legacy_compare.get("legacy_price"),
                                    "new_price": legacy_compare.get("new_price"),
                                    "price_delta": legacy_compare.get("price_delta"),
                                }
                            )
                        if len(compare_summary["sample_differences"]) < 20:
                            compare_summary["sample_differences"].append(
                                {
                                    "store_code": link.store.store_code,
                                    "supplier_code": normalized_supplier_code,
                                    "product_code": product_code,
                                    "old_price": legacy_compare.get("legacy_price"),
                                    "new_price": legacy_compare.get("new_price"),
                                    "price_delta": legacy_compare.get("price_delta"),
                                }
                            )
                    if legacy_compare.get("stock_delta") not in {0, None}:
                        compare_summary["stock_changed"] += 1

            row = {
                "store_id": int(link.store.id),
                "enterprise_code": _clean_text(link.store.enterprise_code),
                "tabletki_branch": _clean_text(link.store.tabletki_branch),
                "supplier_code": normalized_supplier_code,
                "product_code": product_code,
                "market_scope_key": normalized_scope or None,
                "base_price": base_price,
                "effective_price": effective_price,
                "wholesale_price": _round_money(source_item.price_opt),
                "stock": int(source_item.qty),
                "priority_used": priority_used,
                "price_source": _price_source_label(effective_source),
                "pricing_context": pricing_context,
            }
            upsert_payload_rows.append(row)
            built_rows_for_link += 1
            keep_products_by_link.setdefault(
                (int(link.store.id), normalized_supplier_code),
                set(),
            ).add(product_code)

            if len(link_sample_rows) < 10:
                entry = {
                    "product_code": product_code,
                    "base_price": _json_decimal(base_price),
                    "effective_price": _json_decimal(effective_price),
                    "wholesale_price": _json_decimal(source_item.price_opt),
                    "stock": int(source_item.qty),
                    "priority_used": priority_used,
                    "price_source": _price_source_label(effective_source),
                }
                if legacy_compare is not None:
                    entry["legacy_compare"] = legacy_compare
                link_sample_rows.append(entry)

            if len(sample_rows) < 20:
                sample_entry = {
                    "store_id": int(link.store.id),
                    "store_code": link.store.store_code,
                    "supplier_code": normalized_supplier_code,
                    "product_code": product_code,
                    "market_scope_key": normalized_scope or None,
                    "base_price": _json_decimal(base_price),
                    "effective_price": _json_decimal(effective_price),
                    "stock": int(source_item.qty),
                    "price_source": _price_source_label(effective_source),
                }
                if legacy_compare is not None:
                    sample_entry["legacy_compare"] = legacy_compare
                sample_rows.append(sample_entry)

        if link_errors:
            supplier_links_skipped += 1
            stores_skipped_ids.add(int(link.store.id))
            stale_delete_links.add((int(link.store.id), normalized_supplier_code))
            status = "skipped"
        else:
            supplier_links_processed += 1
            stores_processed_ids.add(int(link.store.id))
            status = "ready" if bool(dry_run) else "applied"
            upsert_rows_total += built_rows_for_link

        link_reports.append(
            {
                "status": status,
                "store_id": int(link.store.id),
                "store_code": link.store.store_code,
                "enterprise_code": link.store.enterprise_code,
                "branch": link.store.tabletki_branch,
                "supplier_code": normalized_supplier_code,
                "market_scope_key": normalized_scope or None,
                "candidate_products": candidate_products,
                "upsert_rows": built_rows_for_link,
                "priority_used": priority_used,
                "price_source": sorted(link_price_sources),
                "market_scope_source": scope_resolution.source,
                "parser_name": bundle.parser_name if bundle is not None else None,
                "raw_items_count": bundle.raw_items_count if bundle is not None else 0,
                "warnings": sorted(set(link_warnings)),
                "errors": sorted(set(link_errors)),
                "sample_rows": link_sample_rows,
                "compare_differences": compare_differences,
            }
        )

    if not dry_run:
        if upsert_payload_rows:
            applied_rows = await _upsert_business_store_offers(session, upsert_payload_rows)
            upsert_rows_total = applied_rows
        if limit is None:
            for (link_store_id, link_supplier_code), keep_product_codes in keep_products_by_link.items():
                stale_rows_deleted += await _delete_stale_business_store_offers(
                    session,
                    store_id=link_store_id,
                    supplier_code=link_supplier_code,
                    keep_product_codes=keep_product_codes,
                )
            for link_store_id, link_supplier_code in stale_delete_links:
                stale_rows_deleted += await _delete_stale_business_store_offers(
                    session,
                    store_id=link_store_id,
                    supplier_code=link_supplier_code,
                    keep_product_codes=set(),
                )

    if not links:
        global_warnings.append("No active store-supplier links matched the current filters.")
    if pricing_snapshot.inconsistency:
        global_warnings.append(f"Pricing snapshot fallback used: {pricing_snapshot.inconsistency}")

    status = "ok"
    if global_errors or any(report["errors"] for report in link_reports):
        status = "warning" if link_reports else "error"
    elif global_warnings or any(report["warnings"] for report in link_reports):
        status = "warning"

    return {
        "status": status,
        "dry_run": bool(dry_run),
        "store_id": store_id,
        "enterprise_code": _clean_text(enterprise_code) or None,
        "supplier_code": _clean_text(supplier_code) or None,
        "limit": limit,
        "compare_legacy": bool(compare_legacy),
        "stores_total": stores_total,
        "stores_processed": len(stores_processed_ids),
        "stores_skipped": len(stores_skipped_ids),
        "supplier_links_total": len(links),
        "supplier_links_processed": supplier_links_processed,
        "supplier_links_skipped": supplier_links_skipped,
        "candidate_products": candidate_products_total,
        "upsert_rows": upsert_rows_total,
        "stale_rows_deleted": stale_rows_deleted,
        "price_source": sorted(price_sources) if price_sources else [],
        "sample_rows": sample_rows,
        "links": link_reports,
        "compare_summary": compare_summary,
        "warnings": global_warnings,
        "errors": global_errors,
    }
