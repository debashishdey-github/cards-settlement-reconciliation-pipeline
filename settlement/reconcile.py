"""Stage 3 -- Build and reconcile marts.

Aggregates strictly by (business_date, currency) -- currencies are never
summed together, and late corrections post against their original
business_date rather than the date they were received. Reconciliation checks
both the exact total and a row-level hash, because a grand-total match alone
cannot rule out two offsetting errors.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .model import SettlementEvent

MartKey = tuple[str, str]  # (business_date, currency)


@dataclass(frozen=True)
class MartRow:
    business_date: str
    currency: str
    total_minor: int
    event_count: int
    row_hash: str  # sha256 over the sorted, normalized (event_id, amount_minor) pairs.
    late_correction_event_ids: tuple[str, ...]


def _row_hash(events: list[SettlementEvent]) -> str:
    normalized = sorted((event.event_id, event.amount_minor) for event in events)
    joined = ",".join(f"{eid}:{amt}" for eid, amt in normalized)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def build_marts(accepted: list[SettlementEvent]) -> dict[MartKey, MartRow]:
    buckets: dict[MartKey, list[SettlementEvent]] = {}
    for event in accepted:
        key = (event.business_date, event.currency)
        buckets.setdefault(key, []).append(event)

    marts: dict[MartKey, MartRow] = {}
    for key, bucket_events in buckets.items():
        business_date, currency = key
        marts[key] = MartRow(
            business_date=business_date,
            currency=currency,
            total_minor=sum(event.amount_minor for event in bucket_events),
            event_count=len(bucket_events),
            row_hash=_row_hash(bucket_events),
            late_correction_event_ids=tuple(
                event.event_id for event in bucket_events if event.is_late_correction
            ),
        )
    return marts


@dataclass(frozen=True)
class ReconciliationMismatch:
    key: MartKey
    field: str
    expected: object
    actual: object


def compare_to_source(
    marts: dict[MartKey, MartRow],
    source_totals: dict[MartKey, int],
) -> list[ReconciliationMismatch]:
    """Compare exact totals per (business_date, currency) -- never a grand total.

    A key present in one side and not the other is reported as a mismatch too,
    since a completely missing business_date/currency slice is exactly the
    kind of gap a total-only comparison would hide.
    """
    mismatches: list[ReconciliationMismatch] = []
    all_keys = set(marts) | set(source_totals)
    for key in sorted(all_keys):
        mart_total = marts[key].total_minor if key in marts else None
        source_total = source_totals.get(key)
        if mart_total != source_total:
            mismatches.append(ReconciliationMismatch(key, "total_minor", source_total, mart_total))
    return mismatches
