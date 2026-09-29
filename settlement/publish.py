"""Stage 4 -- Publish and operate.

Publication commits one (business_date, currency) partition at a time and
records each commit in a durable set before moving to the next partition.
This makes a crash *during* publication -- after some but not all partitions
have committed -- a safely resumable state, not a currency the pipeline
loses or silently double-publishes on retry.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .reconcile import MartKey, MartRow


class PublicationInterrupted(RuntimeError):
    """Raised to simulate a crash partway through publication."""


@dataclass
class Publisher:
    """A minimal stand-in for a durable target table's commit log.

    In production this set would be a table row per committed partition, not
    an in-memory Python set -- the point being demonstrated is the *protocol*
    (commit one partition, record it, move on), not the storage engine.
    """
    committed: set[MartKey] = field(default_factory=set)

    def publish(
        self,
        marts: dict[MartKey, MartRow],
        simulate_crash_after: MartKey | None = None,
    ) -> list[MartKey]:
        """Publish every not-yet-committed partition in a stable order.

        If simulate_crash_after is given, raises PublicationInterrupted
        immediately after that partition commits, leaving every partition
        ordered after it uncommitted -- exactly the partial-publish state the
        playbook's pitfalls section says teams most often skip testing.
        """
        newly_published: list[MartKey] = []
        for key in sorted(marts):
            if key in self.committed:
                continue  # Already published; a retry must not republish it.
            self.committed.add(key)
            newly_published.append(key)
            if simulate_crash_after is not None and key == simulate_crash_after:
                raise PublicationInterrupted(
                    f"simulated crash immediately after committing {key}; "
                    f"committed so far: {sorted(self.committed)}"
                )
        return newly_published

    def is_published(self, key: MartKey) -> bool:
        return key in self.committed
