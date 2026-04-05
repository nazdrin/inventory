from typing import Dict, Iterable, List, TypeVar


T = TypeVar("T")


def barcode_match_variants(barcode: str) -> List[str]:
    """Return exact barcode plus the variant with/without a single leading zero."""
    value = (barcode or "").strip()
    if not value:
        return []

    variants = [value]
    if value.isdigit():
        if value.startswith("0") and len(value) > 1:
            variants.append(value[1:])
        else:
            variants.append("0" + value)

    # Preserve order and remove duplicates.
    return list(dict.fromkeys(variants))


def collect_barcode_matches(index: Dict[str, List[T]], barcode: str) -> List[T]:
    """Collect deduplicated matches for exact barcode and its single-zero variant."""
    matches: List[T] = []
    seen_ids = set()

    for variant in barcode_match_variants(barcode):
        for item in index.get(variant, []):
            item_id = getattr(item, "id", None)
            dedupe_key = item_id if item_id is not None else id(item)
            if dedupe_key in seen_ids:
                continue
            seen_ids.add(dedupe_key)
            matches.append(item)

    return matches
