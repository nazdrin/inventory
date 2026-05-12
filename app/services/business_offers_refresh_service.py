from __future__ import annotations

from sqlalchemy import select

from app.business.dropship_pipeline import refresh_business_offers
from app.models import BusinessSettings, EnterpriseSettings
from app.database import get_async_db


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def _load_business_settings_code(session) -> str | None:
    row = (
        await session.execute(
            select(BusinessSettings)
            .order_by(BusinessSettings.id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return _clean_text(row.business_enterprise_code) or None


async def _load_business_enterprises(session) -> list[EnterpriseSettings]:
    rows = await session.execute(
        select(EnterpriseSettings).order_by(EnterpriseSettings.enterprise_name.asc(), EnterpriseSettings.enterprise_code.asc())
    )
    return [
        enterprise
        for enterprise in rows.scalars().all()
        if _clean_text(enterprise.data_format).lower() == "business"
    ]


async def _validate_explicit_enterprise(session, enterprise_code: str) -> tuple[str | None, str | None]:
    row = (
        await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code).limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        return None, f"EnterpriseSettings not found for enterprise_code={enterprise_code}."
    if _clean_text(row.data_format).lower() != "business":
        return None, f"enterprise_code={enterprise_code} is not a Business enterprise."
    return enterprise_code, None


async def resolve_business_refresh_enterprise_code(
    explicit_enterprise_code: str | None = None,
) -> tuple[str | None, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []

    async with get_async_db(commit_on_exit=False) as session:
        explicit = _clean_text(explicit_enterprise_code) or None
        if explicit:
            resolved, error = await _validate_explicit_enterprise(session, explicit)
            if error:
                errors.append(error)
                return None, warnings, errors
            return resolved, warnings, errors

        business_settings_code = await _load_business_settings_code(session)
        if business_settings_code:
            resolved, error = await _validate_explicit_enterprise(session, business_settings_code)
            if error:
                errors.append(
                    "BusinessSettings.business_enterprise_code is configured but invalid: "
                    f"{error}"
                )
                return None, warnings, errors
            warnings.append(
                f"Using BusinessSettings.business_enterprise_code={business_settings_code} for offers refresh."
            )
            return resolved, warnings, errors

        candidates = await _load_business_enterprises(session)
        if not candidates:
            errors.append("No EnterpriseSettings rows found with data_format='Business'.")
            return None, warnings, errors
        if len(candidates) > 1:
            codes = ", ".join(_clean_text(item.enterprise_code) for item in candidates)
            errors.append(
                "Offers refresh enterprise selection is ambiguous. "
                f"Multiple Business enterprises found: {codes}."
            )
            return None, warnings, errors
        resolved = _clean_text(candidates[0].enterprise_code) or None
        if resolved is None:
            errors.append("Resolved Business enterprise has empty enterprise_code.")
            return None, warnings, errors
        warnings.append(
            f"Using the only Business enterprise enterprise_code={resolved} for offers refresh."
        )
        return resolved, warnings, errors


async def run_business_offers_refresh_once(enterprise_code: str | None = None) -> dict:
    resolved_code, warnings, errors = await resolve_business_refresh_enterprise_code(enterprise_code)
    if resolved_code is None:
        return {
            "status": "error",
            "enterprise_code": _clean_text(enterprise_code) or None,
            "suppliers_total": 0,
            "suppliers_processed": 0,
            "suppliers_blocked": 0,
            "suppliers_failed": 0,
            "suppliers_cleared": 0,
            "offers_rows_after": 0,
            "cities": [],
            "started_at": None,
            "finished_at": None,
            "duration_sec": 0.0,
            "warnings": warnings,
            "errors": [{"supplier_code": "__selector__", "message": error} for error in errors],
        }

    report = await refresh_business_offers(resolved_code, return_report=True)
    report["enterprise_code"] = resolved_code
    report["warnings"] = list(dict.fromkeys(list(warnings) + list(report.get("warnings") or [])))
    return report
