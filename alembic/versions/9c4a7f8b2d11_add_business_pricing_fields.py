"""add business pricing fields

Revision ID: 9c4a7f8b2d11
Revises: 84c1f0e2a9b3
Create Date: 2026-04-11 12:50:00.000000

"""

from decimal import Decimal
from typing import Sequence, Union
import os

from alembic import op
import sqlalchemy as sa


revision: str = "9c4a7f8b2d11"
down_revision: Union[str, None] = "84c1f0e2a9b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _env_decimal(name: str, default: str) -> Decimal:
    raw = (os.getenv(name) or default).strip()
    try:
        return Decimal(raw)
    except Exception:
        return Decimal(default)


def _env_bool(name: str, default: str) -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _resolve_jitter_bounds() -> tuple[Decimal, Decimal]:
    min_raw = (os.getenv("PRICE_JITTER_MIN_UAH") or "").strip()
    max_raw = (os.getenv("PRICE_JITTER_MAX_UAH") or "").strip()

    if min_raw and max_raw:
        try:
            return Decimal(min_raw), Decimal(max_raw)
        except Exception:
            pass

    jitter_range = abs(_env_decimal("PRICE_JITTER_RANGE_UAH", "1.0"))
    return -jitter_range, jitter_range


def upgrade() -> None:
    numeric_8_6 = sa.Numeric(precision=8, scale=6)
    numeric_12_2 = sa.Numeric(precision=12, scale=2)

    op.add_column(
        "business_settings",
        sa.Column("pricing_base_thr", numeric_8_6, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_price_band_low_max", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_price_band_mid_max", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_thr_add_low_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_thr_add_mid_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_thr_add_high_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_no_comp_add_low_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_no_comp_add_mid_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_no_comp_add_high_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_comp_discount_share", numeric_8_6, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_comp_delta_min_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_comp_delta_max_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_jitter_enabled", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_jitter_step_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_jitter_min_uah", numeric_12_2, nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("pricing_jitter_max_uah", numeric_12_2, nullable=True),
    )

    jitter_min, jitter_max = _resolve_jitter_bounds()

    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE business_settings
            SET pricing_base_thr = :pricing_base_thr,
                pricing_price_band_low_max = :pricing_price_band_low_max,
                pricing_price_band_mid_max = :pricing_price_band_mid_max,
                pricing_thr_add_low_uah = :pricing_thr_add_low_uah,
                pricing_thr_add_mid_uah = :pricing_thr_add_mid_uah,
                pricing_thr_add_high_uah = :pricing_thr_add_high_uah,
                pricing_no_comp_add_low_uah = :pricing_no_comp_add_low_uah,
                pricing_no_comp_add_mid_uah = :pricing_no_comp_add_mid_uah,
                pricing_no_comp_add_high_uah = :pricing_no_comp_add_high_uah,
                pricing_comp_discount_share = :pricing_comp_discount_share,
                pricing_comp_delta_min_uah = :pricing_comp_delta_min_uah,
                pricing_comp_delta_max_uah = :pricing_comp_delta_max_uah,
                pricing_jitter_enabled = :pricing_jitter_enabled,
                pricing_jitter_step_uah = :pricing_jitter_step_uah,
                pricing_jitter_min_uah = :pricing_jitter_min_uah,
                pricing_jitter_max_uah = :pricing_jitter_max_uah
            """
        ),
        {
            "pricing_base_thr": _env_decimal("BASE_THR", "0.08"),
            "pricing_price_band_low_max": _env_decimal("PRICE_BAND_LOW_MAX", "100"),
            "pricing_price_band_mid_max": _env_decimal("PRICE_BAND_MID_MAX", "400"),
            "pricing_thr_add_low_uah": max(Decimal("0"), _env_decimal("THR_MULT_LOW", "1.0")),
            "pricing_thr_add_mid_uah": max(Decimal("0"), _env_decimal("THR_MULT_MID", "1.0")),
            "pricing_thr_add_high_uah": max(Decimal("0"), _env_decimal("THR_MULT_HIGH", "1.0")),
            "pricing_no_comp_add_low_uah": max(Decimal("0"), _env_decimal("NO_COMP_MULT_LOW", "1.0")),
            "pricing_no_comp_add_mid_uah": max(Decimal("0"), _env_decimal("NO_COMP_MULT_MID", "1.0")),
            "pricing_no_comp_add_high_uah": max(Decimal("0"), _env_decimal("NO_COMP_MULT_HIGH", "1.0")),
            "pricing_comp_discount_share": min(
                max(Decimal("0"), _env_decimal("COMP_DISCOUNT_SHARE", "0.01")),
                Decimal("0.999999"),
            ),
            "pricing_comp_delta_min_uah": max(Decimal("0"), _env_decimal("COMP_DELTA_MIN_UAH", "2")),
            "pricing_comp_delta_max_uah": max(
                max(Decimal("0"), _env_decimal("COMP_DELTA_MIN_UAH", "2")),
                _env_decimal("COMP_DELTA_MAX_UAH", "15"),
            ),
            "pricing_jitter_enabled": _env_bool("PRICE_JITTER_ENABLED", "0"),
            "pricing_jitter_step_uah": max(Decimal("0.01"), _env_decimal("PRICE_JITTER_STEP_UAH", "0.5")),
            "pricing_jitter_min_uah": min(jitter_min, jitter_max),
            "pricing_jitter_max_uah": max(jitter_min, jitter_max),
        },
    )

    op.alter_column(
        "business_settings",
        "pricing_base_thr",
        existing_type=numeric_8_6,
        server_default=sa.text("0.08"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_price_band_low_max",
        existing_type=numeric_12_2,
        server_default=sa.text("100"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_price_band_mid_max",
        existing_type=numeric_12_2,
        server_default=sa.text("400"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_thr_add_low_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_thr_add_mid_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_thr_add_high_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_no_comp_add_low_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_no_comp_add_mid_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_no_comp_add_high_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_comp_discount_share",
        existing_type=numeric_8_6,
        server_default=sa.text("0.01"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_comp_delta_min_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("2"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_comp_delta_max_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("15"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_jitter_enabled",
        existing_type=sa.Boolean(),
        server_default=sa.text("false"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_jitter_step_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("0.5"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_jitter_min_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("-1.0"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "pricing_jitter_max_uah",
        existing_type=numeric_12_2,
        server_default=sa.text("1.0"),
        nullable=False,
    )

    op.create_check_constraint(
        "ck_business_settings_pricing_base_thr_non_negative",
        "business_settings",
        "pricing_base_thr >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_band_low_non_negative",
        "business_settings",
        "pricing_price_band_low_max >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_band_mid_ge_low",
        "business_settings",
        "pricing_price_band_mid_max >= pricing_price_band_low_max",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_thr_add_low_non_negative",
        "business_settings",
        "pricing_thr_add_low_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_thr_add_mid_non_negative",
        "business_settings",
        "pricing_thr_add_mid_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_thr_add_high_non_negative",
        "business_settings",
        "pricing_thr_add_high_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_no_comp_add_low_non_negative",
        "business_settings",
        "pricing_no_comp_add_low_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_no_comp_add_mid_non_negative",
        "business_settings",
        "pricing_no_comp_add_mid_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_no_comp_add_high_non_negative",
        "business_settings",
        "pricing_no_comp_add_high_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_comp_discount_share_range",
        "business_settings",
        "pricing_comp_discount_share >= 0 AND pricing_comp_discount_share < 1",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_comp_delta_min_non_negative",
        "business_settings",
        "pricing_comp_delta_min_uah >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_comp_delta_max_ge_min",
        "business_settings",
        "pricing_comp_delta_max_uah >= pricing_comp_delta_min_uah",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_jitter_step_positive",
        "business_settings",
        "pricing_jitter_step_uah > 0",
    )
    op.create_check_constraint(
        "ck_business_settings_pricing_jitter_max_ge_min",
        "business_settings",
        "pricing_jitter_max_uah >= pricing_jitter_min_uah",
    )


def downgrade() -> None:
    for constraint_name in (
        "ck_business_settings_pricing_jitter_max_ge_min",
        "ck_business_settings_pricing_jitter_step_positive",
        "ck_business_settings_pricing_comp_delta_max_ge_min",
        "ck_business_settings_pricing_comp_delta_min_non_negative",
        "ck_business_settings_pricing_comp_discount_share_range",
        "ck_business_settings_pricing_no_comp_add_high_non_negative",
        "ck_business_settings_pricing_no_comp_add_mid_non_negative",
        "ck_business_settings_pricing_no_comp_add_low_non_negative",
        "ck_business_settings_pricing_thr_add_high_non_negative",
        "ck_business_settings_pricing_thr_add_mid_non_negative",
        "ck_business_settings_pricing_thr_add_low_non_negative",
        "ck_business_settings_pricing_band_mid_ge_low",
        "ck_business_settings_pricing_band_low_non_negative",
        "ck_business_settings_pricing_base_thr_non_negative",
    ):
        op.drop_constraint(constraint_name, "business_settings", type_="check")

    for column_name in (
        "pricing_jitter_max_uah",
        "pricing_jitter_min_uah",
        "pricing_jitter_step_uah",
        "pricing_jitter_enabled",
        "pricing_comp_delta_max_uah",
        "pricing_comp_delta_min_uah",
        "pricing_comp_discount_share",
        "pricing_no_comp_add_high_uah",
        "pricing_no_comp_add_mid_uah",
        "pricing_no_comp_add_low_uah",
        "pricing_thr_add_high_uah",
        "pricing_thr_add_mid_uah",
        "pricing_thr_add_low_uah",
        "pricing_price_band_mid_max",
        "pricing_price_band_low_max",
        "pricing_base_thr",
    ):
        op.drop_column("business_settings", column_name)
