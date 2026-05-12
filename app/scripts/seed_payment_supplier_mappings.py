from __future__ import annotations

import asyncio
from dataclasses import dataclass

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import DropshipEnterprise, PaymentCounterpartySupplierMapping


SUPPLIER_NAME_TO_CODE = {
    "biotus": "D1",
    "dsn": "D2",
    "proteinplus": "D3",
    "dobavki.ua": "D4",
    "monsterlab": "D5",
    "sport-atlet": "D6",
    "pediakid": "D7",
    "suziria": "D8",
    "ortomerika": "D9",
    "zoohub": "D10",
    "toros": "D11",
    "vetstar": "D12",
    "zoocomplex": "D13",
}


@dataclass(frozen=True)
class MappingSeed:
    counterparty_pattern: str
    supplier_name: str
    match_type: str
    priority: int


MAPPING_SEEDS = [
    MappingSeed("ТОВ «МОН АМІ ГРУП»", "Pediakid", "exact", 10),
    MappingSeed("ФОП Щеглова Наталія Сергіївна", "Vetstar", "exact", 20),
    MappingSeed("ФІЗИЧНА ОСОБА-ПІДПРИЄМЕЦЬ ДЯЧЕНКО МИХАЙЛО ВАСИЛЬОВИЧ", "Toros", "exact", 30),
    MappingSeed("ТОВ Ніка-стор", "Biotus", "exact", 40),
    MappingSeed("ФОП Лягу Анатолій Іванович", "MonsterLab", "exact", 50),
    MappingSeed("ФІЗИЧНА ОСОБА - ПІДПРИЄМЕЦЬ ГРИНЬ НАТАЛІЯ СЕРГІЇВНА", "Suziria", "exact", 60),
    MappingSeed("Бібік Костянтин Тарасович", "Dsn", "exact", 70),
    MappingSeed('ТОВ "Квестом"', "Dsn", "exact", 80),
    MappingSeed("ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ \"ФАРМЕЛ\"", "Таблетки юа", "exact", 90),
    MappingSeed("ФОП Іваненко Михайло Володимирович", "Zoohub", "exact", 100),
    MappingSeed("ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ \"НОВА ПОШТА\"", "Нова Пошта", "exact", 110),
    MappingSeed("Небайдужий Збір", "Небайдужий Збір", "exact", 120),
    MappingSeed("ФОП СПЕРКАЧ МАРІЯ ВОЛОДИМИРІВНА", "Vetstar", "exact", 130),
    MappingSeed("Apple", "Apple", "exact", 140),
    MappingSeed("Послуги надання місця у Веб-мережі CH-543925", "Веб-мережа CH-543925", "contains", 150),
    MappingSeed("ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ДО ЮА", "Dobavki.ua", "exact", 160),
    MappingSeed("ФОП Шматко Олена", "Sport-atlet", "exact", 170),
    MappingSeed("ФОП Рахуба Віктор", "Biotus", "exact", 180),
    MappingSeed("ТОВ 'Імпорт.макс'", "Biotus", "exact", 190),
]


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    text = " ".join(value.strip().casefold().split())
    return text or None


def _supplier_code_for_name(supplier_name: str) -> str | None:
    return SUPPLIER_NAME_TO_CODE.get(str(supplier_name or "").strip().casefold())


async def seed_payment_supplier_mappings() -> dict[str, int]:
    created = 0
    updated = 0
    skipped = 0

    async with AsyncSessionLocal() as session:
        for seed in MAPPING_SEEDS:
            supplier_code = _supplier_code_for_name(seed.supplier_name)
            if supplier_code is None:
                print(
                    "skip non-dropship mapping: "
                    f"counterparty={seed.counterparty_pattern} supplier_name={seed.supplier_name}"
                )
                skipped += 1
                continue

            supplier = await session.scalar(select(DropshipEnterprise).where(DropshipEnterprise.code == supplier_code))
            if supplier is None:
                print(
                    "skip missing supplier code: "
                    f"counterparty={seed.counterparty_pattern} supplier_name={seed.supplier_name} supplier_code={supplier_code}"
                )
                skipped += 1
                continue

            field_scope = "search_text" if seed.match_type == "contains" else "counterparty_name"
            normalized_pattern = _normalize(seed.counterparty_pattern)
            mapping = await session.scalar(
                select(PaymentCounterpartySupplierMapping).where(
                    PaymentCounterpartySupplierMapping.supplier_code == supplier_code,
                    PaymentCounterpartySupplierMapping.match_type == seed.match_type,
                    PaymentCounterpartySupplierMapping.field_scope == field_scope,
                    PaymentCounterpartySupplierMapping.normalized_pattern == normalized_pattern,
                )
            )
            if mapping is None:
                mapping = PaymentCounterpartySupplierMapping(
                    supplier_code=supplier_code,
                    supplier_salesdrive_id=supplier.salesdrive_supplier_id,
                    match_type=seed.match_type,
                    field_scope=field_scope,
                    counterparty_pattern=seed.counterparty_pattern,
                    normalized_pattern=normalized_pattern,
                    priority=seed.priority,
                    is_active=True,
                    notes=f"Seeded from April 2026 payment mapping: {seed.supplier_name}",
                    created_by="seed_payment_supplier_mappings",
                    updated_by="seed_payment_supplier_mappings",
                )
                session.add(mapping)
                created += 1
            else:
                mapping.supplier_salesdrive_id = supplier.salesdrive_supplier_id
                mapping.priority = seed.priority
                mapping.is_active = True
                mapping.notes = f"Seeded from April 2026 payment mapping: {seed.supplier_name}"
                mapping.updated_by = "seed_payment_supplier_mappings"
                updated += 1

        await session.commit()

    return {"created": created, "updated": updated, "skipped": skipped}


async def _amain() -> None:
    result = await seed_payment_supplier_mappings()
    print(
        "payment supplier mappings seed complete: "
        f"created={result['created']} updated={result['updated']} skipped={result['skipped']}"
    )


if __name__ == "__main__":
    asyncio.run(_amain())
