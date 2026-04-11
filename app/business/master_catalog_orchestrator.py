import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Awaitable, Callable, Dict, List, Optional

from dotenv import load_dotenv

from app.business.catalog_categories_sync import sync_catalog_categories_from_raw
from app.business.d1_barcode_mapping_sync import sync_d1_supplier_mapping_by_barcode
from app.business.d1_content_sync import sync_d1_content
from app.business.d1_images_sync import sync_d1_images
from app.business.d1_master_feed_loader import load_d1_raw_supplier_feed
from app.business.d10_barcode_mapping_sync import sync_d10_supplier_mapping_by_barcode
from app.business.d10_content_sync import sync_d10_content
from app.business.d10_images_sync import sync_d10_images
from app.business.d10_master_feed_loader import load_d10_raw_supplier_feed
from app.business.d13_barcode_mapping_sync import sync_d13_supplier_mapping_by_barcode
from app.business.d13_content_sync import sync_d13_content
from app.business.d13_images_sync import sync_d13_images
from app.business.d13_master_feed_loader import load_d13_raw_supplier_feed
from app.business.d11_barcode_mapping_sync import sync_d11_supplier_mapping_by_barcode
from app.business.d11_master_feed_loader import load_d11_raw_supplier_feed
from app.business.d12_barcode_mapping_sync import sync_d12_supplier_mapping_by_barcode
from app.business.d12_master_feed_loader import load_d12_raw_supplier_feed
from app.business.d2_barcode_mapping_sync import sync_d2_supplier_mapping_by_barcode
from app.business.d2_content_sync import sync_d2_content
from app.business.d2_images_sync import sync_d2_images
from app.business.d2_master_feed_loader import load_d2_raw_supplier_feed
from app.business.d3_barcode_mapping_sync import sync_d3_supplier_mapping_by_barcode
from app.business.d3_content_sync import sync_d3_content
from app.business.d3_images_sync import sync_d3_images
from app.business.d3_master_feed_loader import load_d3_raw_supplier_feed
from app.business.d4_barcode_mapping_sync import sync_d4_supplier_mapping_by_barcode
from app.business.d4_master_feed_loader import load_d4_raw_supplier_feed
from app.business.d5_barcode_mapping_sync import sync_d5_supplier_mapping_by_barcode
from app.business.d5_content_sync import sync_d5_content
from app.business.d5_images_sync import sync_d5_images
from app.business.d5_master_feed_loader import load_d5_raw_supplier_feed
from app.business.d6_barcode_mapping_sync import sync_d6_supplier_mapping_by_barcode
from app.business.d6_master_dimensions_enrich import enrich_master_dimensions_from_d6
from app.business.d6_master_feed_loader import load_d6_raw_supplier_feed
from app.business.d7_barcode_mapping_sync import sync_d7_supplier_mapping_by_barcode
from app.business.d7_master_feed_loader import load_d7_raw_supplier_feed
from app.business.d8_barcode_mapping_sync import sync_d8_supplier_mapping_by_barcode
from app.business.d8_master_feed_loader import load_d8_raw_supplier_feed
from app.business.d9_barcode_mapping_sync import sync_d9_supplier_mapping_by_barcode
from app.business.d9_master_feed_loader import sync_d9_master_feed
from app.business.master_archive_import import import_master_archive
from app.business.master_catalog_coverage_report import build_master_catalog_coverage_report
from app.business.master_content_fallback_d10_select import select_d10_fallback_content
from app.business.master_content_fallback_d13_select import select_d13_fallback_content
from app.business.master_content_fallback_d2_select import select_d2_fallback_content
from app.business.master_content_fallback_d3_select import select_d3_fallback_content
from app.business.master_content_fallback_d5_select import select_d5_fallback_content
from app.business.master_content_select import select_master_content
from app.business.master_images_fallback_d10_select import select_d10_fallback_main_images
from app.business.master_images_fallback_d13_select import select_d13_fallback_main_images
from app.business.master_images_fallback_d2_select import select_d2_fallback_main_images
from app.business.master_images_fallback_d3_select import select_d3_fallback_main_images
from app.business.master_images_fallback_d5_select import select_d5_fallback_main_images
from app.business.master_main_image_select import select_master_main_images
from app.business.salesdrive_category_exporter import export_categories_to_salesdrive
from app.business.salesdrive_master_catalog_exporter import export_master_catalog_to_salesdrive
from app.business.tabletki_master_catalog_exporter import export_master_catalog_to_tabletki
from app.business.tabletki_master_catalog_loader import load_tabletki_master_catalog
from app.services.master_business_settings_resolver import load_master_business_settings_snapshot


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_catalog_orchestrator")


AsyncStep = Callable[[], Awaitable[Any]]


@dataclass
class StepResult:
    name: str
    status: str
    duration_sec: float
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "duration_sec": round(self.duration_sec, 3),
            "message": self.message,
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_step(name: str, fn: AsyncStep) -> Dict[str, Any]:
    return {"name": name, "fn": fn}


async def _require_enterprise(enterprise: Optional[str], purpose: str = "salesdrive") -> str:
    load_dotenv()
    explicit = (enterprise or "").strip()
    if explicit:
        return explicit

    settings = await load_master_business_settings_snapshot()
    if settings.inconsistency:
        logger.warning("Master orchestrator enterprise inconsistency for %s: %s", purpose, settings.inconsistency)
    if purpose == "salesdrive":
        return settings.resolve_weekly_salesdrive_enterprise()
    if purpose == "publish":
        return settings.resolve_publish_enterprise()
    return settings.resolve_primary_business_enterprise(purpose=purpose)


def _build_tabletki_steps() -> List[Dict[str, Any]]:
    return [
        _make_step("tabletki_master_catalog_loader", lambda: load_tabletki_master_catalog(mode="full", limit=0)),
        _make_step("catalog_categories_sync", lambda: sync_catalog_categories_from_raw(limit=0)),
    ]


def _build_suppliers_steps() -> List[Dict[str, Any]]:
    return [
        _make_step("d6_master_feed_loader", lambda: load_d6_raw_supplier_feed(limit=0)),
        _make_step("d6_barcode_mapping_sync", lambda: sync_d6_supplier_mapping_by_barcode(limit=0)),
        _make_step("d6_master_dimensions_enrich", lambda: enrich_master_dimensions_from_d6(limit=0)),
        _make_step("d1_master_feed_loader", lambda: load_d1_raw_supplier_feed(limit=0)),
        _make_step("d1_barcode_mapping_sync", lambda: sync_d1_supplier_mapping_by_barcode(limit=0)),
        _make_step("d1_images_sync", lambda: sync_d1_images(limit=0)),
        _make_step("d1_content_sync", lambda: sync_d1_content(limit=0)),
        _make_step("d2_master_feed_loader", lambda: load_d2_raw_supplier_feed(limit=0)),
        _make_step("d2_barcode_mapping_sync", lambda: sync_d2_supplier_mapping_by_barcode(limit=0)),
        _make_step("d2_images_sync", lambda: sync_d2_images(limit=0)),
        _make_step("d2_content_sync", lambda: sync_d2_content(limit=0)),
        _make_step("d3_master_feed_loader", lambda: load_d3_raw_supplier_feed(limit=0)),
        _make_step("d3_barcode_mapping_sync", lambda: sync_d3_supplier_mapping_by_barcode(limit=0)),
        _make_step("d3_images_sync", lambda: sync_d3_images(limit=0)),
        _make_step("d3_content_sync", lambda: sync_d3_content(limit=0)),
        _make_step("d5_master_feed_loader", lambda: load_d5_raw_supplier_feed(limit=0)),
        _make_step("d5_barcode_mapping_sync", lambda: sync_d5_supplier_mapping_by_barcode(limit=0)),
        _make_step("d5_images_sync", lambda: sync_d5_images(limit=0)),
        _make_step("d5_content_sync", lambda: sync_d5_content(limit=0)),
        _make_step("d4_master_feed_loader", lambda: load_d4_raw_supplier_feed(limit=0)),
        _make_step("d4_barcode_mapping_sync", lambda: sync_d4_supplier_mapping_by_barcode(limit=0)),
        _make_step("d7_master_feed_loader", lambda: load_d7_raw_supplier_feed(limit=0)),
        _make_step("d7_barcode_mapping_sync", lambda: sync_d7_supplier_mapping_by_barcode(limit=0)),
        _make_step("d8_master_feed_loader", lambda: load_d8_raw_supplier_feed(limit=0)),
        _make_step("d8_barcode_mapping_sync", lambda: sync_d8_supplier_mapping_by_barcode(limit=0)),
        _make_step("d9_master_feed_loader", lambda: sync_d9_master_feed(limit=0)),
        _make_step("d9_barcode_mapping_sync", lambda: sync_d9_supplier_mapping_by_barcode(limit=0)),
        _make_step("d11_master_feed_loader", lambda: load_d11_raw_supplier_feed(limit=0)),
        _make_step("d11_barcode_mapping_sync", lambda: sync_d11_supplier_mapping_by_barcode(limit=0)),
        _make_step("d12_master_feed_loader", lambda: load_d12_raw_supplier_feed(limit=0)),
        _make_step("d12_barcode_mapping_sync", lambda: sync_d12_supplier_mapping_by_barcode(limit=0)),
        _make_step("d10_master_feed_loader", lambda: load_d10_raw_supplier_feed(limit=0)),
        _make_step("d10_barcode_mapping_sync", lambda: sync_d10_supplier_mapping_by_barcode(limit=0)),
        _make_step("d10_images_sync", lambda: sync_d10_images(limit=0)),
        _make_step("d10_content_sync", lambda: sync_d10_content(limit=0)),
        _make_step("d13_master_feed_loader", lambda: load_d13_raw_supplier_feed(limit=0)),
        _make_step("d13_barcode_mapping_sync", lambda: sync_d13_supplier_mapping_by_barcode(limit=0)),
        _make_step("d13_images_sync", lambda: sync_d13_images(limit=0)),
        _make_step("d13_content_sync", lambda: sync_d13_content(limit=0)),
    ]


def _build_selection_steps() -> List[Dict[str, Any]]:
    return [
        _make_step("master_main_image_select", lambda: select_master_main_images(limit=0)),
        _make_step("master_content_select", lambda: select_master_content(limit=0)),
        _make_step("master_images_fallback_d2_select", select_d2_fallback_main_images),
        _make_step("master_content_fallback_d2_select", select_d2_fallback_content),
        _make_step("master_images_fallback_d3_select", select_d3_fallback_main_images),
        _make_step("master_content_fallback_d3_select", select_d3_fallback_content),
        _make_step("master_images_fallback_d5_select", select_d5_fallback_main_images),
        _make_step("master_content_fallback_d5_select", select_d5_fallback_content),
        _make_step("master_images_fallback_d10_select", select_d10_fallback_main_images),
        _make_step("master_content_fallback_d10_select", select_d10_fallback_content),
        _make_step("master_images_fallback_d13_select", select_d13_fallback_main_images),
        _make_step("master_content_fallback_d13_select", select_d13_fallback_content),
    ]


def _build_archive_steps() -> List[Dict[str, Any]]:
    return [_make_step("master_archive_import", lambda: import_master_archive(limit=0))]


async def _build_salesdrive_steps(enterprise: Optional[str], batch_size: int) -> List[Dict[str, Any]]:
    enterprise_code = await _require_enterprise(enterprise, purpose="salesdrive")
    return [
        _make_step(
            "salesdrive_category_exporter",
            lambda: export_categories_to_salesdrive(
                enterprise_code=enterprise_code,
                batch_size=batch_size,
                limit=0,
            ),
        ),
        _make_step(
            "salesdrive_master_catalog_exporter",
            lambda: export_master_catalog_to_salesdrive(
                enterprise_code=enterprise_code,
                batch_size=batch_size,
                limit=0,
            ),
        ),
    ]


async def _build_publish_steps(enterprise: Optional[str], limit: int, send: bool) -> List[Dict[str, Any]]:
    enterprise_code = await _require_enterprise(enterprise, purpose="publish")
    return [
        _make_step(
            "tabletki_master_catalog_exporter",
            lambda: export_master_catalog_to_tabletki(
                enterprise_code=enterprise_code,
                limit=limit,
                send=send,
            ),
        )
    ]


def _build_report_steps() -> List[Dict[str, Any]]:
    return [_make_step("master_catalog_coverage_report", build_master_catalog_coverage_report)]


async def _resolve_steps(mode: str, *, enterprise: Optional[str], batch_size: int, limit: int, send: bool, skip_salesdrive: bool, skip_archive: bool, skip_report: bool) -> List[Dict[str, Any]]:
    if mode == "weekly_enrichment":
        steps: List[Dict[str, Any]] = []
        steps.extend(_build_tabletki_steps())
        steps.extend(_build_suppliers_steps())
        steps.extend(_build_selection_steps())
        if not skip_report:
            steps.extend(_build_report_steps())
        return steps

    if mode == "full":
        steps: List[Dict[str, Any]] = []
        steps.extend(_build_tabletki_steps())
        steps.extend(_build_suppliers_steps())
        steps.extend(_build_selection_steps())
        if not skip_archive:
            steps.extend(_build_archive_steps())
        if not skip_salesdrive:
            steps.extend(await _build_salesdrive_steps(enterprise, batch_size))
        if not skip_report:
            steps.extend(_build_report_steps())
        return steps

    if mode == "tabletki":
        return _build_tabletki_steps()
    if mode == "suppliers":
        return _build_suppliers_steps()
    if mode == "selection":
        return _build_selection_steps()
    if mode == "archive":
        return [] if skip_archive else _build_archive_steps()
    if mode == "salesdrive":
        return [] if skip_salesdrive else await _build_salesdrive_steps(enterprise, batch_size)
    if mode == "publish":
        return await _build_publish_steps(enterprise, limit, send)
    if mode == "report":
        return [] if skip_report else _build_report_steps()

    raise RuntimeError(f"Неподдерживаемый mode: {mode}")


async def _run_step(step: Dict[str, Any]) -> StepResult:
    name = step["name"]
    fn: AsyncStep = step["fn"]
    logger.info("Старт шага: %s", name)
    started = perf_counter()
    try:
        result = await fn()
        message = None
        if isinstance(result, dict):
            if result.get("warnings_count"):
                message = f"warnings_count={result['warnings_count']}"
            elif "sent" in result or "offers_count" in result or "preview_path" in result:
                message = (
                    f"sent={result.get('sent', False)} "
                    f"offers_count={result.get('offers_count', 0)} "
                    f"preview_path={result.get('preview_path', '')}"
                ).strip()
        status = "warning" if message else "ok"
    except Exception as exc:
        logger.exception("Ошибка шага: %s", name)
        return StepResult(
            name=name,
            status="error",
            duration_sec=perf_counter() - started,
            message=str(exc),
        )

    logger.info("Завершён шаг: %s", name)
    return StepResult(
        name=name,
        status=status,
        duration_sec=perf_counter() - started,
        message=message,
    )


async def run_master_catalog_orchestrator(
    *,
    mode: str,
    fail_fast: bool = False,
    skip_salesdrive: bool = False,
    skip_archive: bool = False,
    skip_report: bool = False,
    enterprise: Optional[str] = None,
    batch_size: int = 100,
    limit: int = 0,
    send: bool = False,
) -> Dict[str, Any]:
    started_at = _utc_now_iso()
    steps = await _resolve_steps(
        mode,
        enterprise=enterprise,
        batch_size=batch_size,
        limit=limit,
        send=send,
        skip_salesdrive=skip_salesdrive,
        skip_archive=skip_archive,
        skip_report=skip_report,
    )

    step_results: List[StepResult] = []
    for step in steps:
        result = await _run_step(step)
        step_results.append(result)
        if fail_fast and result.status == "error":
            break

    return {
        "mode": mode,
        "started_at": started_at,
        "finished_at": _utc_now_iso(),
        "steps": [item.to_dict() for item in step_results],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Оркестратор нового master-каталога")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["tabletki", "suppliers", "selection", "archive", "salesdrive", "publish", "report", "weekly_enrichment", "full"],
        help="режим запуска orchestration pipeline",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="остановиться на первой ошибке",
    )
    parser.add_argument(
        "--skip-salesdrive",
        action="store_true",
        help="пропустить salesdrive шаги",
    )
    parser.add_argument(
        "--skip-archive",
        action="store_true",
        help="пропустить archive шаги",
    )
    parser.add_argument(
        "--skip-report",
        action="store_true",
        help="пропустить report шаги",
    )
    parser.add_argument(
        "--enterprise",
        help="enterprise code для salesdrive/publish режима",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="размер batch для salesdrive шагов",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="лимит записей для publish exporter (0 = без лимита)",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="для publish режима выполнить реальную отправку; без флага publish работает как dry-run",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await run_master_catalog_orchestrator(
        mode=args.mode,
        fail_fast=args.fail_fast,
        skip_salesdrive=args.skip_salesdrive,
        skip_archive=args.skip_archive,
        skip_report=args.skip_report,
        enterprise=args.enterprise,
        batch_size=args.batch_size,
        limit=args.limit,
        send=args.send,
    )
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())
