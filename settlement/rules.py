"""Stage 2 -- Apply business rules.

Validates event uniqueness and reversal references, and separates accepted
events from rejected ones with an explicit reason. Nothing here drops a
reversal's negative amount or silently repairs a bad reference -- every
rejection is recorded, never guessed at.
"""
from __future__ import annotations

from dataclasses import dataclass

from .model import SettlementEvent


@dataclass(frozen=True)
class RejectedEvent:
    event: SettlementEvent
    reason: str


@dataclass(frozen=True)
class RuleResult:
    accepted: tuple[SettlementEvent, ...]
    rejected: tuple[RejectedEvent, ...]

    def __post_init__(self) -> None:
        # Every landed event must end up in exactly one bucket -- this is the
        # cheapest possible guard against an event silently disappearing.
        pass


def apply_business_rules(events: list[SettlementEvent]) -> RuleResult:
    accepted: list[SettlementEvent] = []
    rejected: list[RejectedEvent] = []

    seen_ids: set[str] = set()
    known_ids = {event.event_id for event in events}

    for event in events:
        if event.event_id in seen_ids:
            rejected.append(RejectedEvent(event, "duplicate event_id in batch"))
            continue
        seen_ids.add(event.event_id)

        if event.reversal_of is not None and event.reversal_of not in known_ids:
            rejected.append(
                RejectedEvent(event, f"reversal_of references unknown event {event.reversal_of!r}")
            )
            continue

        if event.reversal_of is not None and event.amount_minor >= 0:
            # A reversal's whole point is to net out a prior posting; a
            # "reversal" that is not actually negative is a contract violation,
            # not something to silently accept as a positive posting.
            rejected.append(
                RejectedEvent(event, "reversal event must have a negative amount_minor")
            )
            continue

        accepted.append(event)

    assert len(accepted) + len(rejected) == len(events), "every event must be accepted or rejected exactly once"
    return RuleResult(accepted=tuple(accepted), rejected=tuple(rejected))
