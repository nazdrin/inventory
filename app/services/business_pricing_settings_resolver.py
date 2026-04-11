import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_async_db
from app.models import BusinessSettings

BusinessPricingSettingsSource = Literal["db", "env-fallback"]
logger = logging.getLogger("business_pricing_settings_resolver")


@dataclass(frozen=True)
class BusinessPricingFieldSpec:
    key: str
    env_name: str
    db_column: str
    db_type: str
    nullable: bool
    server_default: str
    check_constraint: str
    ui_group: str
    label: str
    help_text: str


@dataclass(frozen=True)
class BusinessPricingGroupSpec:
    key: str
    title: str
    field_keys: tuple[str, ...]


BUSINESS_PRICING_GROUP_SPECS: tuple[BusinessPricingGroupSpec, ...] = (
    BusinessPricingGroupSpec(
        key="base_threshold",
        title="Базовый порог",
        field_keys=("pricing_base_thr",),
    ),
    BusinessPricingGroupSpec(
        key="price_bands",
        title="Диапазоны цен",
        field_keys=(
            "pricing_price_band_low_max",
            "pricing_price_band_mid_max",
        ),
    ),
    BusinessPricingGroupSpec(
        key="competitor_reaction",
        title="Реакция на конкурентов",
        field_keys=(
            "pricing_thr_add_low_uah",
            "pricing_thr_add_mid_uah",
            "pricing_thr_add_high_uah",
            "pricing_comp_discount_share",
            "pricing_comp_delta_min_uah",
            "pricing_comp_delta_max_uah",
        ),
    ),
    BusinessPricingGroupSpec(
        key="without_competitor",
        title="Поведение без конкурентов",
        field_keys=(
            "pricing_no_comp_add_low_uah",
            "pricing_no_comp_add_mid_uah",
            "pricing_no_comp_add_high_uah",
        ),
    ),
    BusinessPricingGroupSpec(
        key="jitter",
        title="Jitter",
        field_keys=(
            "pricing_jitter_enabled",
            "pricing_jitter_step_uah",
            "pricing_jitter_min_uah",
            "pricing_jitter_max_uah",
        ),
    ),
)


BUSINESS_PRICING_FIELD_SPECS: tuple[BusinessPricingFieldSpec, ...] = (
    BusinessPricingFieldSpec(
        key="pricing_base_thr",
        env_name="BASE_THR",
        db_column="pricing_base_thr",
        db_type="Numeric(8, 6)",
        nullable=False,
        server_default="0.08",
        check_constraint="pricing_base_thr >= 0",
        ui_group="base_threshold",
        label="Базовый порог",
        help_text="Доля, которая участвует в расчёте минимального порога. Значение хранится как share: 0.08 = 8%.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_price_band_low_max",
        env_name="PRICE_BAND_LOW_MAX",
        db_column="pricing_price_band_low_max",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="100",
        check_constraint="pricing_price_band_low_max >= 0",
        ui_group="price_bands",
        label="Верхняя граница LOW",
        help_text="Граница диапазона LOW по price_opt. Если price_opt меньше или равен этому значению, товар попадает в LOW.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_price_band_mid_max",
        env_name="PRICE_BAND_MID_MAX",
        db_column="pricing_price_band_mid_max",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="400",
        check_constraint="pricing_price_band_mid_max >= pricing_price_band_low_max",
        ui_group="price_bands",
        label="Верхняя граница MID",
        help_text="Граница диапазона MID по price_opt. Всё, что выше, попадает в HIGH.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_thr_add_low_uah",
        env_name="THR_MULT_LOW",
        db_column="pricing_thr_add_low_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_thr_add_low_uah >= 0",
        ui_group="competitor_reaction",
        label="Надбавка LOW, грн",
        help_text="Фиксированная надбавка в гривне для LOW, когда у товара есть цена конкурента.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_thr_add_mid_uah",
        env_name="THR_MULT_MID",
        db_column="pricing_thr_add_mid_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_thr_add_mid_uah >= 0",
        ui_group="competitor_reaction",
        label="Надбавка MID, грн",
        help_text="Фиксированная надбавка в гривне для MID, когда у товара есть цена конкурента.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_thr_add_high_uah",
        env_name="THR_MULT_HIGH",
        db_column="pricing_thr_add_high_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_thr_add_high_uah >= 0",
        ui_group="competitor_reaction",
        label="Надбавка HIGH, грн",
        help_text="Фиксированная надбавка в гривне для HIGH, когда у товара есть цена конкурента.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_no_comp_add_low_uah",
        env_name="NO_COMP_MULT_LOW",
        db_column="pricing_no_comp_add_low_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_no_comp_add_low_uah >= 0",
        ui_group="without_competitor",
        label="Надбавка LOW без конкурента, грн",
        help_text="Фиксированная надбавка в гривне для LOW, когда цены конкурента нет.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_no_comp_add_mid_uah",
        env_name="NO_COMP_MULT_MID",
        db_column="pricing_no_comp_add_mid_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_no_comp_add_mid_uah >= 0",
        ui_group="without_competitor",
        label="Надбавка MID без конкурента, грн",
        help_text="Фиксированная надбавка в гривне для MID, когда цены конкурента нет.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_no_comp_add_high_uah",
        env_name="NO_COMP_MULT_HIGH",
        db_column="pricing_no_comp_add_high_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_no_comp_add_high_uah >= 0",
        ui_group="without_competitor",
        label="Надбавка HIGH без конкурента, грн",
        help_text="Фиксированная надбавка в гривне для HIGH, когда цены конкурента нет.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_comp_discount_share",
        env_name="COMP_DISCOUNT_SHARE",
        db_column="pricing_comp_discount_share",
        db_type="Numeric(8, 6)",
        nullable=False,
        server_default="0.01",
        check_constraint="pricing_comp_discount_share >= 0 AND pricing_comp_discount_share < 1",
        ui_group="competitor_reaction",
        label="Доля скидки относительно конкурента",
        help_text="Доля для расчёта цены ниже конкурента. Значение хранится как share: 0.01 = 1%.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_comp_delta_min_uah",
        env_name="COMP_DELTA_MIN_UAH",
        db_column="pricing_comp_delta_min_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="2",
        check_constraint="pricing_comp_delta_min_uah >= 0",
        ui_group="competitor_reaction",
        label="Минимальный delta, грн",
        help_text="Минимальное смещение в гривне при попытке поставить цену ниже конкурента.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_comp_delta_max_uah",
        env_name="COMP_DELTA_MAX_UAH",
        db_column="pricing_comp_delta_max_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="15",
        check_constraint="pricing_comp_delta_max_uah >= pricing_comp_delta_min_uah",
        ui_group="competitor_reaction",
        label="Максимальный delta, грн",
        help_text="Максимальное смещение в гривне при попытке поставить цену ниже конкурента.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_jitter_enabled",
        env_name="PRICE_JITTER_ENABLED",
        db_column="pricing_jitter_enabled",
        db_type="Boolean",
        nullable=False,
        server_default="false",
        check_constraint="none",
        ui_group="jitter",
        label="Включить jitter",
        help_text="Включает дополнительное случайное смещение цены после основного расчёта.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_jitter_step_uah",
        env_name="PRICE_JITTER_STEP_UAH",
        db_column="pricing_jitter_step_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="0.5",
        check_constraint="pricing_jitter_step_uah > 0",
        ui_group="jitter",
        label="Шаг jitter, грн",
        help_text="Шаг сетки, по которой выбирается случайное смещение цены.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_jitter_min_uah",
        env_name="PRICE_JITTER_MIN_UAH",
        db_column="pricing_jitter_min_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="-1.0",
        check_constraint="pricing_jitter_min_uah <= pricing_jitter_max_uah",
        ui_group="jitter",
        label="Минимальный jitter, грн",
        help_text="Минимальное возможное смещение цены в гривне. Хранится как явная нижняя граница.",
    ),
    BusinessPricingFieldSpec(
        key="pricing_jitter_max_uah",
        env_name="PRICE_JITTER_MAX_UAH",
        db_column="pricing_jitter_max_uah",
        db_type="Numeric(12, 2)",
        nullable=False,
        server_default="1.0",
        check_constraint="pricing_jitter_max_uah >= pricing_jitter_min_uah",
        ui_group="jitter",
        label="Максимальный jitter, грн",
        help_text="Максимальное возможное смещение цены в гривне. Хранится как явная верхняя граница.",
    ),
)


@dataclass(frozen=True)
class BusinessPricingSettingsSnapshot:
    """Future DB-first pricing snapshot contract.

    Phase 1 freeze only:
    - fields and semantics are fixed here
    - runtime is intentionally not switched to this snapshot yet
    - future resolver implementation should return this structure for both DB and env-fallback paths
    """

    source: BusinessPricingSettingsSource
    business_settings_exists: bool
    pricing_base_thr: Decimal
    pricing_price_band_low_max: Decimal
    pricing_price_band_mid_max: Decimal
    pricing_thr_add_low_uah: Decimal
    pricing_thr_add_mid_uah: Decimal
    pricing_thr_add_high_uah: Decimal
    pricing_no_comp_add_low_uah: Decimal
    pricing_no_comp_add_mid_uah: Decimal
    pricing_no_comp_add_high_uah: Decimal
    pricing_comp_discount_share: Decimal
    pricing_comp_delta_min_uah: Decimal
    pricing_comp_delta_max_uah: Decimal
    pricing_jitter_enabled: bool
    pricing_jitter_step_uah: Decimal
    pricing_jitter_min_uah: Decimal
    pricing_jitter_max_uah: Decimal
    inconsistency: Optional[str] = None


def _env_decimal(name: str, default: str) -> Decimal:
    raw = (os.getenv(name, default) or default).strip()
    try:
        return Decimal(raw)
    except Exception:
        return Decimal(default)


def _env_bool(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_env_jitter_bounds() -> tuple[Decimal, Decimal]:
    jitter_min_raw = os.getenv("PRICE_JITTER_MIN_UAH")
    jitter_max_raw = os.getenv("PRICE_JITTER_MAX_UAH")
    jitter_min = (
        _env_decimal("PRICE_JITTER_MIN_UAH", "0")
        if jitter_min_raw is not None and str(jitter_min_raw).strip() != ""
        else None
    )
    jitter_max = (
        _env_decimal("PRICE_JITTER_MAX_UAH", "0")
        if jitter_max_raw is not None and str(jitter_max_raw).strip() != ""
        else None
    )
    if jitter_min is None or jitter_max is None:
        jitter_range = abs(_env_decimal("PRICE_JITTER_RANGE_UAH", "1.0"))
        return -jitter_range, jitter_range
    if jitter_min <= jitter_max:
        return jitter_min, jitter_max
    logger.warning(
        "Invalid env jitter bounds: PRICE_JITTER_MIN_UAH=%s > PRICE_JITTER_MAX_UAH=%s. Reordering bounds for fallback snapshot.",
        jitter_min,
        jitter_max,
    )
    return jitter_max, jitter_min


def _validate_snapshot(snapshot: BusinessPricingSettingsSnapshot) -> Optional[str]:
    if snapshot.pricing_base_thr < 0:
        return "pricing_base_thr must be >= 0"
    if snapshot.pricing_price_band_low_max < 0:
        return "pricing_price_band_low_max must be >= 0"
    if snapshot.pricing_price_band_mid_max < snapshot.pricing_price_band_low_max:
        return "pricing_price_band_mid_max must be >= pricing_price_band_low_max"
    if snapshot.pricing_thr_add_low_uah < 0:
        return "pricing_thr_add_low_uah must be >= 0"
    if snapshot.pricing_thr_add_mid_uah < 0:
        return "pricing_thr_add_mid_uah must be >= 0"
    if snapshot.pricing_thr_add_high_uah < 0:
        return "pricing_thr_add_high_uah must be >= 0"
    if snapshot.pricing_no_comp_add_low_uah < 0:
        return "pricing_no_comp_add_low_uah must be >= 0"
    if snapshot.pricing_no_comp_add_mid_uah < 0:
        return "pricing_no_comp_add_mid_uah must be >= 0"
    if snapshot.pricing_no_comp_add_high_uah < 0:
        return "pricing_no_comp_add_high_uah must be >= 0"
    if snapshot.pricing_comp_discount_share < 0 or snapshot.pricing_comp_discount_share >= 1:
        return "pricing_comp_discount_share must be >= 0 and < 1"
    if snapshot.pricing_comp_delta_min_uah < 0:
        return "pricing_comp_delta_min_uah must be >= 0"
    if snapshot.pricing_comp_delta_max_uah < snapshot.pricing_comp_delta_min_uah:
        return "pricing_comp_delta_max_uah must be >= pricing_comp_delta_min_uah"
    if snapshot.pricing_jitter_step_uah <= 0:
        return "pricing_jitter_step_uah must be > 0"
    if snapshot.pricing_jitter_max_uah < snapshot.pricing_jitter_min_uah:
        return "pricing_jitter_max_uah must be >= pricing_jitter_min_uah"
    return None


def _snapshot_from_db_row(row: BusinessSettings) -> BusinessPricingSettingsSnapshot:
    return BusinessPricingSettingsSnapshot(
        source="db",
        business_settings_exists=True,
        pricing_base_thr=Decimal(str(row.pricing_base_thr)),
        pricing_price_band_low_max=Decimal(str(row.pricing_price_band_low_max)),
        pricing_price_band_mid_max=Decimal(str(row.pricing_price_band_mid_max)),
        pricing_thr_add_low_uah=Decimal(str(row.pricing_thr_add_low_uah)),
        pricing_thr_add_mid_uah=Decimal(str(row.pricing_thr_add_mid_uah)),
        pricing_thr_add_high_uah=Decimal(str(row.pricing_thr_add_high_uah)),
        pricing_no_comp_add_low_uah=Decimal(str(row.pricing_no_comp_add_low_uah)),
        pricing_no_comp_add_mid_uah=Decimal(str(row.pricing_no_comp_add_mid_uah)),
        pricing_no_comp_add_high_uah=Decimal(str(row.pricing_no_comp_add_high_uah)),
        pricing_comp_discount_share=Decimal(str(row.pricing_comp_discount_share)),
        pricing_comp_delta_min_uah=Decimal(str(row.pricing_comp_delta_min_uah)),
        pricing_comp_delta_max_uah=Decimal(str(row.pricing_comp_delta_max_uah)),
        pricing_jitter_enabled=bool(row.pricing_jitter_enabled),
        pricing_jitter_step_uah=Decimal(str(row.pricing_jitter_step_uah)),
        pricing_jitter_min_uah=Decimal(str(row.pricing_jitter_min_uah)),
        pricing_jitter_max_uah=Decimal(str(row.pricing_jitter_max_uah)),
    )


def build_business_pricing_env_fallback_snapshot() -> BusinessPricingSettingsSnapshot:
    """Build the legacy ENV-backed snapshot in the future DB-first resolver shape.

    This helper intentionally does not query DB in Phase 1.
    Future implementation should add a DB branch and keep this env-fallback path
    only for pre-migration or missing-row scenarios.
    """

    jitter_min, jitter_max = _normalize_env_jitter_bounds()

    return BusinessPricingSettingsSnapshot(
        source="env-fallback",
        business_settings_exists=False,
        pricing_base_thr=_env_decimal("BASE_THR", "0.08"),
        pricing_price_band_low_max=_env_decimal("PRICE_BAND_LOW_MAX", "100"),
        pricing_price_band_mid_max=_env_decimal("PRICE_BAND_MID_MAX", "400"),
        pricing_thr_add_low_uah=_env_decimal("THR_MULT_LOW", "1.0"),
        pricing_thr_add_mid_uah=_env_decimal("THR_MULT_MID", "1.0"),
        pricing_thr_add_high_uah=_env_decimal("THR_MULT_HIGH", "1.0"),
        pricing_no_comp_add_low_uah=_env_decimal("NO_COMP_MULT_LOW", "1.0"),
        pricing_no_comp_add_mid_uah=_env_decimal("NO_COMP_MULT_MID", "1.0"),
        pricing_no_comp_add_high_uah=_env_decimal("NO_COMP_MULT_HIGH", "1.0"),
        pricing_comp_discount_share=_env_decimal("COMP_DISCOUNT_SHARE", "0.01"),
        pricing_comp_delta_min_uah=_env_decimal("COMP_DELTA_MIN_UAH", "2"),
        pricing_comp_delta_max_uah=_env_decimal("COMP_DELTA_MAX_UAH", "15"),
        pricing_jitter_enabled=_env_bool("PRICE_JITTER_ENABLED", "0"),
        pricing_jitter_step_uah=_env_decimal("PRICE_JITTER_STEP_UAH", "0.5"),
        pricing_jitter_min_uah=jitter_min,
        pricing_jitter_max_uah=jitter_max,
    )


async def load_business_pricing_settings_snapshot(
    session: Optional[AsyncSession] = None,
) -> BusinessPricingSettingsSnapshot:
    async def _load(active_session: AsyncSession) -> BusinessPricingSettingsSnapshot:
        row = (
            await active_session.execute(
                select(BusinessSettings)
                .order_by(BusinessSettings.id)
                .limit(1)
            )
        ).scalar_one_or_none()

        if row is None:
            fallback = build_business_pricing_env_fallback_snapshot()
            logger.info(
                "Business pricing settings source=%s reason=business_settings_row_missing",
                fallback.source,
            )
            return fallback

        try:
            snapshot = _snapshot_from_db_row(row)
        except Exception as exc:
            reason = f"business_settings pricing payload unreadable: {exc}"
            logger.warning(
                "Business pricing settings fallback to env: reason=%s",
                reason,
            )
            fallback = build_business_pricing_env_fallback_snapshot()
            return BusinessPricingSettingsSnapshot(
                **{
                    **fallback.__dict__,
                    "business_settings_exists": True,
                    "inconsistency": reason,
                }
            )

        inconsistency = _validate_snapshot(snapshot)
        if inconsistency:
            logger.warning(
                "Business pricing settings fallback to env: reason=%s",
                inconsistency,
            )
            fallback = build_business_pricing_env_fallback_snapshot()
            return BusinessPricingSettingsSnapshot(
                **{
                    **fallback.__dict__,
                    "business_settings_exists": True,
                    "inconsistency": inconsistency,
                }
            )

        logger.info("Business pricing settings source=%s", snapshot.source)
        return snapshot

    if session is not None:
        return await _load(session)

    async with get_async_db(commit_on_exit=False) as managed_session:
        return await _load(managed_session)
