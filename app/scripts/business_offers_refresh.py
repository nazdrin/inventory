import argparse
import asyncio
import json
from typing import Any

from app.services.business_offers_refresh_service import run_business_offers_refresh_once


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh Business offers without stock export.")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    return str(value)


def _print_human_summary(report: dict[str, Any]) -> None:
    print(f"status: {report.get('status')}")
    print(f"enterprise_code: {report.get('enterprise_code')}")
    print(
        "suppliers:"
        f" total={report.get('suppliers_total')}"
        f" processed={report.get('suppliers_processed')}"
        f" blocked={report.get('suppliers_blocked')}"
        f" failed={report.get('suppliers_failed')}"
        f" cleared={report.get('suppliers_cleared')}"
    )
    print(
        "offers:"
        f" rows_after={report.get('offers_rows_after')}"
        f" cities={len(report.get('cities') or [])}"
        f" duration_sec={report.get('duration_sec')}"
    )
    warnings = list(report.get("warnings") or [])
    errors = list(report.get("errors") or [])
    if warnings:
        print("warnings:")
        for warning in warnings:
            print(f"- {warning}")
    if errors:
        print("errors:")
        for error in errors:
            supplier_code = error.get("supplier_code")
            message = error.get("message")
            print(f"- supplier={supplier_code}: {message}")


async def _amain() -> None:
    args = _parse_args()
    enterprise_code = str(args.enterprise_code or "").strip() or None
    report = await run_business_offers_refresh_once(enterprise_code)

    if bool(args.output_json):
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
        return

    _print_human_summary(report)


if __name__ == "__main__":
    asyncio.run(_amain())
