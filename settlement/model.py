"""Data model for the synthetic cards settlement pipeline.

A SettlementEvent is the atomic unit landed from the source system: one posting
or one reversal. Reversals are ordinary events with a negative amount_minor
that reference the event_id they reverse via `reversal_of` -- they are never
represented by deleting or overwriting the original posting.

`business_date` and `received_date` are deliberately separate fields: the
playbook's first step requires separating posting date from event time. A
same-day posting has business_date == received_date. A late correction has a
business_date in the past and a received_date of today -- it must still be
attributed to the original business_date, not silently folded into today.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SettlementEvent:
    event_id: str
    account: str
    currency: str
    amount_minor: int  # Integer minor units (e.g. cents/paise). Never a float.
    business_date: str  # YYYY-MM-DD, the date this amount is attributed to.
    received_date: str  # YYYY-MM-DD, the date the pipeline actually processed this event.
    reversal_of: str | None = None  # event_id of the posting this reverses, if any.

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id is required")
        if not self.currency or len(self.currency) != 3:
            raise ValueError(f"currency must be a 3-letter code, got {self.currency!r}")
        if not isinstance(self.amount_minor, int):
            raise TypeError("amount_minor must be an int (minor units), never a float")

    @property
    def is_late_correction(self) -> bool:
        return self.received_date != self.business_date


@dataclass(frozen=True)
class Manifest:
    """The immutable delivery manifest that must be verified before landing."""
    record_count: int
    checksum: str  # sha256 of the sorted event_ids, computed by the source system.
