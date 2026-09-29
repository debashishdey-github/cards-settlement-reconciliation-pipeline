"""Wires land -> rules -> reconcile -> publish into one run, and prints a report.

Run directly (``python -m settlement.pipeline``) to see the full synthetic
fixture from the DataSparkle Academy playbook processed end to end, including
the crash-and-resume rehearsal.
"""
from __future__ import annotations

from dataclasses import dataclass

from .land import land_batch
from .model import Manifest, SettlementEvent
from .publish import Publisher, PublicationInterrupted
from .reconcile import MartKey, MartRow, build_marts, compare_to_source
from .rules import RuleResult, apply_business_rules


@dataclass(frozen=True)
class PipelineReport:
    landed_count: int
    accepted_count: int
    rejected_count: int
    marts: dict[MartKey, MartRow]
    mismatches: list


def run_pipeline(
    events: list[SettlementEvent],
    manifest: Manifest,
    source_totals: dict[MartKey, int],
) -> PipelineReport:
    landed = land_batch(events, manifest)
    result: RuleResult = apply_business_rules(list(landed.events))
    marts = build_marts(list(result.accepted))
    mismatches = compare_to_source(marts, source_totals)
    return PipelineReport(
        landed_count=len(landed.events),
        accepted_count=len(result.accepted),
        rejected_count=len(result.rejected),
        marts=marts,
        mismatches=mismatches,
    )


def _sample_events() -> list[SettlementEvent]:
    """The exact fixture from the DataSparkle Academy 'batch' domain playbook."""
    return [
        SettlementEvent("p1", "0001", "INR", 10000, "2026-09-01", "2026-09-01"),
        SettlementEvent("p2", "0001", "INR", 2500, "2026-09-01", "2026-09-01"),
        SettlementEvent("r1", "0001", "INR", -2500, "2026-09-01", "2026-09-01", reversal_of="p2"),
        SettlementEvent("p3", "0002", "USD", 4200, "2026-09-01", "2026-09-01"),
    ]


def _demo() -> None:
    from .land import compute_checksum

    events = _sample_events()
    manifest = Manifest(record_count=len(events), checksum=compute_checksum(events))
    source_totals: dict[MartKey, int] = {("2026-09-01", "INR"): 10000, ("2026-09-01", "USD"): 4200}

    report = run_pipeline(events, manifest, source_totals)
    print(f"Landed: {report.landed_count}  Accepted: {report.accepted_count}  Rejected: {report.rejected_count}")
    for key, row in sorted(report.marts.items()):
        print(f"  {key}: total_minor={row.total_minor} events={row.event_count}")
    if report.mismatches:
        print("RECONCILIATION FAILED:", report.mismatches)
    else:
        print("Reconciliation passed: every (business_date, currency) total matches source exactly.")

    print("\nRehearsing a crash during publication (not just before it)...")
    publisher = Publisher()
    try:
        publisher.publish(report.marts, simulate_crash_after=("2026-09-01", "INR"))
    except PublicationInterrupted as exc:
        print(f"  simulated crash: {exc}")
    print(f"  committed so far: {sorted(publisher.committed)}")
    resumed = publisher.publish(report.marts)  # A safe resume: republishes nothing already committed.
    print(f"  resume published: {resumed} (INR was not republished)")


if __name__ == "__main__":
    _demo()
