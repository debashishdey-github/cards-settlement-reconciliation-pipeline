# Cards Settlement Reconciliation Pipeline

A dependency-free, fully tested reference implementation of the **cards settlement end-to-end** domain playbook from [DataSparkle Academy](https://datasparkle-academy.vercel.app/playbook-batch.html) — a mainframe-to-data-engineering field guide. It exists to answer one question concretely instead of only in prose: *what does "reconcile before you trust it" actually look like in code?*

This is a **learning reference**, not a production settlement system. It uses synthetic data, has no network/database/queue dependencies, and models the analytical reconciliation pattern only — not card-network certification or real settlement processing.

## What it demonstrates

A four-stage pipeline, each stage a small, independently tested module:

```
land.py  →  rules.py  →  reconcile.py  →  publish.py
(manifest)   (business    (marts, grouped   (idempotent,
 verify)      rules)       by currency)      crash-safe)
```

| Stage | File | What it proves |
|---|---|---|
| Land | `settlement/land.py` | A batch is rejected outright if its record count or checksum does not match its own manifest — no partial accept. |
| Business rules | `settlement/rules.py` | Reversals are explicit negative events tied to the posting they reverse (`reversal_of`), never silently dropped or repaired. Duplicates and dangling reversal references are rejected with a reason, not guessed at. |
| Reconcile | `settlement/reconcile.py` | Every mart is grouped by `(business_date, currency)` — currencies are **never** summed together. Reconciliation checks an exact total **and** a row-level hash, because two offsetting errors can leave a grand total looking perfectly fine. |
| Publish | `settlement/publish.py` | Publication commits one partition at a time and records each commit durably. A simulated crash *during* publication — after some but not all partitions have committed — leaves a state a retry can safely resume, without republishing what already landed. |

## Why this exists

The playbook this implements makes two claims that are easy to agree with in prose and easy to get wrong in code:

1. **"INR + USD is not a real number."** Every aggregation in this repo groups by currency. There is no code path that can produce a combined cross-currency total, because the grouping key includes currency from the first line of `build_marts`.
2. **"Rehearse a crash during publication, not just before it."** Most reconciliation demos stop at "what if the whole batch fails." The one that matters more is a partial failure: some currencies published, then a crash, then a retry. `tests/test_pipeline.py::test_crash_during_publication_leaves_a_resumable_partial_state` rehearses exactly that.

## Quick start

Requires Python 3.10+ and nothing else for the pipeline itself (pytest only for running the tests).

```bash
git clone https://github.com/debashishdey-github/cards-settlement-reconciliation-pipeline.git
cd cards-settlement-reconciliation-pipeline

# Run the end-to-end demo (uses the exact fixture from the playbook)
python3 -m settlement.pipeline

# Run the test suite
pip install pytest
pytest tests/ -v
```

Expected demo output:

```
Landed: 4  Accepted: 4  Rejected: 0
  ('2026-09-01', 'INR'): total_minor=10000 events=3
  ('2026-09-01', 'USD'): total_minor=4200 events=1
Reconciliation passed: every (business_date, currency) total matches source exactly.

Rehearsing a crash during publication (not just before it)...
  simulated crash: simulated crash immediately after committing ('2026-09-01', 'INR'); committed so far: [('2026-09-01', 'INR')]
  committed so far: [('2026-09-01', 'INR')]
  resume published: [('2026-09-01', 'USD')] (INR was not republished)
```

## Project structure

```
settlement/
├── model.py       # SettlementEvent, Manifest — the data contract
├── land.py        # Stage 1: manifest verification (count + checksum)
├── rules.py       # Stage 2: uniqueness, reversal validation
├── reconcile.py   # Stage 3: per-currency marts, exact total + row-hash comparison
├── publish.py     # Stage 4: idempotent, crash-safe partition commits
└── pipeline.py    # Wires the four stages together; runnable demo
tests/
└── test_pipeline.py  # 15 tests, one per acceptance gate in the source playbook
```

## Test coverage maps directly to the playbook's acceptance gates

| Playbook acceptance gate | Test |
|---|---|
| Original and reversal handling match the documented contract | `test_reversal_nets_correctly_and_currencies_stay_separate` |
| Late corrections remain traceable to the affected business date | `test_late_correction_posts_against_original_business_date_not_received_date` |
| Retrying a batch does not double count | `test_publisher_does_not_republish_an_already_committed_partition` |
| A crash midway through publication is safely resumable | `test_crash_during_publication_leaves_a_resumable_partial_state` |

## Related

Part of a set of reference implementations linked from [DataSparkle Academy](https://datasparkle-academy.vercel.app)'s migration playbooks. See also the [insurance policy history](https://github.com/debashishdey-github/insurance-policy-history-scd2), [telecom CDR](https://github.com/debashishdey-github/telecom-cdr-reconciliation-pipeline), and [clickstream sessionization](https://github.com/debashishdey-github/clickstream-sessionization-pipeline) reference pipelines for the same site's other domain playbooks.

## License

MIT — see [LICENSE](LICENSE).
