import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from sqlalchemy import select

from app.business.business_store_tabletki_outbound_mapper import (
    resolve_business_store_by_tabletki_branch,
    restore_salesdrive_products_for_tabletki_outbound,
)
from app.database import get_async_db
from app.models import BusinessStore


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Business Store outbound Tabletki status mapping test.")
    parser.add_argument("--branch", default="")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--store-code", default="")
    parser.add_argument("--internal-code", default="")
    parser.add_argument("--payload-json-file", default="")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _load_payload_from_file(path: str) -> dict[str, Any]:
    payload_path = Path(path)
    with payload_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("--payload-json-file must contain a JSON object")
    return data


def _build_mock_payload(*, branch: str, internal_code: str) -> dict[str, Any]:
    return {
        "data": [
            {
                "externalId": "TEST",
                "tabletkiOrder": "TEST",
                "branch": branch,
                "products": [
                    {
                        "parameter": internal_code,
                        "sku": internal_code,
                        "text": "Mock product",
                        "documentName": "Mock product",
                    }
                ],
            }
        ]
    }


async def _resolve_store_by_code(session, store_code: str | None) -> BusinessStore | None:
    normalized_store_code = _clean_text(store_code)
    if not normalized_store_code:
        return None

    return (
        await session.execute(
            select(BusinessStore).where(BusinessStore.store_code == normalized_store_code).limit(1)
        )
    ).scalar_one_or_none()


async def _amain() -> None:
    args = _parse_args()
    branch = _clean_text(args.branch)
    enterprise_code = _clean_text(args.enterprise_code)
    store_code = _clean_text(args.store_code)
    internal_code = _clean_text(args.internal_code)
    payload_json_file = _clean_text(args.payload_json_file)

    async with get_async_db(commit_on_exit=False) as session:
        store = await _resolve_store_by_code(session, store_code)
        if store is not None and not branch:
            branch = _clean_text(store.tabletki_branch)
        if store is not None and not enterprise_code:
            enterprise_code = _clean_text(store.enterprise_code)

        if payload_json_file:
            payload = _load_payload_from_file(payload_json_file)
            if not branch:
                data = payload.get("data")
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    branch = _clean_text(data[0].get("branch"))
                elif isinstance(data, dict):
                    branch = _clean_text(data.get("branch"))
        else:
            if not internal_code:
                raise ValueError("--internal-code is required when --payload-json-file is not provided")
            if not branch and store is None:
                raise ValueError("Either --branch or --store-code is required for mock payload mode")
            payload = _build_mock_payload(branch=branch or "", internal_code=internal_code)

        resolved_store = None
        if branch:
            resolved_store = await resolve_business_store_by_tabletki_branch(
                session,
                branch=branch,
                enterprise_code=enterprise_code,
            )

        result = await restore_salesdrive_products_for_tabletki_outbound(
            session,
            payload=payload,
            branch=branch,
            enterprise_code=enterprise_code,
        )

    if store is not None:
        result.setdefault("warnings", []).append(
            f"CLI store-code hint resolved to branch={store.tabletki_branch}."
        )
    if resolved_store is not None and result.get("store_code") is None:
        result["store_id"] = int(resolved_store.id)
        result["store_code"] = resolved_store.store_code

    if args.output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print(f"status: {result.get('status')}")
    print(f"store_found: {result.get('store_found')}")
    print(f"store_code: {result.get('store_code')}")
    print(f"branch: {result.get('branch')}")
    print(f"mapped_products: {result.get('mapped_products')}")
    if result.get("missing_mappings"):
        print(f"missing_mappings: {len(result['missing_mappings'])}")
    for warning in result.get("warnings") or []:
        print(f"warning: {warning}")
    for error in result.get("errors") or []:
        print(f"error: {error}")


if __name__ == "__main__":
    asyncio.run(_amain())
