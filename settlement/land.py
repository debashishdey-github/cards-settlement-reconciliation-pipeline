"""Stage 1 -- Land a controlled batch.

Verifies the batch against its immutable manifest (count + checksum) before
anything downstream is allowed to touch it. This is the playbook's first gate:
a delivery that fails this check must never reach the business-rules stage.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .model import Manifest, SettlementEvent


class ManifestMismatchError(ValueError):
    """Raised when a delivered batch does not match its own manifest."""


def compute_checksum(events: list[SettlementEvent]) -> str:
    """The manifest checksum is a sha256 of the sorted event_ids.

    Sorting first makes the checksum independent of delivery order, so a
    source system that redelivers the same events in a different order does
    not fail this check for the wrong reason.
    """
    joined = ",".join(sorted(event.event_id for event in events))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LandedBatch:
    events: tuple[SettlementEvent, ...]
    manifest: Manifest


def land_batch(events: list[SettlementEvent], manifest: Manifest) -> LandedBatch:
    """Verify record count and checksum before accepting the batch.

    Raises ManifestMismatchError on any mismatch -- there is no partial-accept
    mode here. A batch that fails this gate must be re-requested from source,
    not patched locally.
    """
    if len(events) != manifest.record_count:
        raise ManifestMismatchError(
            f"expected {manifest.record_count} records, landed {len(events)}"
        )
    actual_checksum = compute_checksum(events)
    if actual_checksum != manifest.checksum:
        raise ManifestMismatchError(
            f"checksum mismatch: manifest={manifest.checksum} actual={actual_checksum}"
        )
    return LandedBatch(events=tuple(events), manifest=manifest)
