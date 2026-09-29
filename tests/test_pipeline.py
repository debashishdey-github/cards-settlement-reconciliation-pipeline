from settlement.land import ManifestMismatchError, compute_checksum, land_batch
from settlement.model import Manifest, SettlementEvent
from settlement.pipeline import run_pipeline
from settlement.publish import Publisher, PublicationInterrupted
from settlement.reconcile import build_marts, compare_to_source
from settlement.rules import apply_business_rules


def make_event(event_id, account="0001", currency="INR", amount_minor=100,
                business_date="2026-09-01", received_date=None, reversal_of=None):
    return SettlementEvent(
        event_id=event_id, account=account, currency=currency, amount_minor=amount_minor,
        business_date=business_date, received_date=received_date or business_date,
        reversal_of=reversal_of,
    )


SAMPLE_EVENTS = [
    make_event("p1", amount_minor=10000),
    make_event("p2", amount_minor=2500),
    make_event("r1", amount_minor=-2500, reversal_of="p2"),
    make_event("p3", account="0002", currency="USD", amount_minor=4200),
]


# --- Acceptance gate: original and reversal handling matches the documented contract. ---

def test_reversal_nets_correctly_and_currencies_stay_separate():
    result = apply_business_rules(SAMPLE_EVENTS)
    marts = build_marts(list(result.accepted))
    assert marts[("2026-09-01", "INR")].total_minor == 10000
    assert marts[("2026-09-01", "USD")].total_minor == 4200
    assert set(marts.keys()) == {("2026-09-01", "INR"), ("2026-09-01", "USD")}


def test_currencies_are_never_summed_together():
    # There is no code path that can produce a single combined total across
    # currencies: build_marts groups by (business_date, currency), so this
    # test asserts the shape of the output rather than a specific number.
    marts = build_marts(SAMPLE_EVENTS)
    currencies_seen = {key[1] for key in marts}
    assert currencies_seen == {"INR", "USD"}
    assert len(marts) == 2  # One row per (date, currency) pair, never fewer.


def test_reversal_referencing_unknown_event_is_rejected():
    events = [make_event("p1"), make_event("r1", amount_minor=-100, reversal_of="does-not-exist")]
    result = apply_business_rules(events)
    assert len(result.accepted) == 1
    assert len(result.rejected) == 1
    assert "unknown event" in result.rejected[0].reason


def test_reversal_must_be_negative():
    events = [make_event("p1"), make_event("r1", amount_minor=100, reversal_of="p1")]
    result = apply_business_rules(events)
    assert len(result.rejected) == 1
    assert "negative" in result.rejected[0].reason


def test_duplicate_event_id_is_rejected():
    events = [make_event("p1", amount_minor=100), make_event("p1", amount_minor=999)]
    result = apply_business_rules(events)
    assert len(result.accepted) == 1
    assert len(result.rejected) == 1
    assert "duplicate" in result.rejected[0].reason


# --- Acceptance gate: late corrections remain traceable to the affected business date. ---

def test_late_correction_posts_against_original_business_date_not_received_date():
    late = make_event("c1", amount_minor=500, business_date="2026-08-15", received_date="2026-09-01")
    assert late.is_late_correction is True
    marts = build_marts([late])
    assert ("2026-08-15", "INR") in marts
    assert ("2026-09-01", "INR") not in marts
    assert marts[("2026-08-15", "INR")].late_correction_event_ids == ("c1",)


def test_same_day_event_is_not_flagged_as_a_late_correction():
    same_day = make_event("p1")
    assert same_day.is_late_correction is False


# --- Row-level reconciliation catches what a grand total would miss. ---

def test_row_hash_differs_even_when_total_matches():
    # Two different underlying event sets that happen to sum to the same total.
    set_a = [make_event("a1", amount_minor=700), make_event("a2", amount_minor=300)]
    set_b = [make_event("b1", amount_minor=1000)]
    marts_a = build_marts(set_a)
    marts_b = build_marts(set_b)
    key = ("2026-09-01", "INR")
    assert marts_a[key].total_minor == marts_b[key].total_minor == 1000
    assert marts_a[key].row_hash != marts_b[key].row_hash


def test_compare_to_source_flags_a_missing_currency_slice():
    marts = build_marts([make_event("p1", amount_minor=100)])
    source_totals = {("2026-09-01", "INR"): 100, ("2026-09-01", "USD"): 4200}  # USD never arrived.
    mismatches = compare_to_source(marts, source_totals)
    assert len(mismatches) == 1
    assert mismatches[0].key == ("2026-09-01", "USD")


# --- Acceptance gate: retrying a batch does not double count settlement values. ---

def test_rerunning_build_marts_on_the_same_accepted_events_is_idempotent():
    marts_first = build_marts(SAMPLE_EVENTS)
    marts_second = build_marts(SAMPLE_EVENTS)
    assert marts_first == marts_second


def test_publisher_does_not_republish_an_already_committed_partition():
    marts = build_marts(SAMPLE_EVENTS)
    publisher = Publisher()
    first_run = publisher.publish(marts)
    second_run = publisher.publish(marts)  # Simulates a retry of the same logical run.
    assert set(first_run) == set(marts.keys())
    assert second_run == []  # Nothing republished; everything was already committed.


# --- Acceptance gate: a simulated crash midway through publication is safely resumable. ---

def test_crash_during_publication_leaves_a_resumable_partial_state():
    marts = build_marts(SAMPLE_EVENTS)
    publisher = Publisher()
    crash_key = ("2026-09-01", "INR")
    try:
        publisher.publish(marts, simulate_crash_after=crash_key)
        assert False, "expected PublicationInterrupted"
    except PublicationInterrupted:
        pass
    assert publisher.is_published(crash_key)
    assert not publisher.is_published(("2026-09-01", "USD"))

    resumed = publisher.publish(marts)  # A safe resume after the crash.
    assert resumed == [("2026-09-01", "USD")]
    assert publisher.is_published(("2026-09-01", "USD"))
    assert publisher.committed == set(marts.keys())


# --- Manifest verification gate. ---

def test_manifest_checksum_mismatch_is_rejected_before_business_rules_run():
    events = [make_event("p1")]
    bad_manifest = Manifest(record_count=1, checksum="not-the-real-checksum")
    try:
        land_batch(events, bad_manifest)
        assert False, "expected ManifestMismatchError"
    except ManifestMismatchError:
        pass


def test_manifest_record_count_mismatch_is_rejected():
    events = [make_event("p1"), make_event("p2")]
    manifest = Manifest(record_count=1, checksum=compute_checksum(events))
    try:
        land_batch(events, manifest)
        assert False, "expected ManifestMismatchError"
    except ManifestMismatchError:
        pass


def test_full_pipeline_matches_the_documented_playbook_fixture():
    manifest = Manifest(record_count=len(SAMPLE_EVENTS), checksum=compute_checksum(SAMPLE_EVENTS))
    source_totals = {("2026-09-01", "INR"): 10000, ("2026-09-01", "USD"): 4200}
    report = run_pipeline(SAMPLE_EVENTS, manifest, source_totals)
    assert report.landed_count == 4
    assert report.accepted_count == 4
    assert report.rejected_count == 0
    assert report.mismatches == []
