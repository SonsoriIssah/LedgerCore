# Guards for two performance fixes in main.py that are easy to undo by
# accident, because in both cases the "broken" version still returns
# correct results -- it's only slower, so nothing else in the suite notices.
#
# Unlike the other tests here, these need neither a running server nor a
# database: they inspect the query and the retry policy directly. Run them
# with plain `pytest tests/test_regressions.py`.

import asyncio

import pytest

import main


# --- The newest-audit-entry lookup must stay bounded ------------------------
#
# Every transfer reads the latest audit_log row to chain the next hash onto
# it. That query originally had no LIMIT, so Postgres returned the whole
# audit_log and SQLAlchemy built an ORM object for every row -- making each
# transfer's cost grow with the ledger's entire history. Measured on this
# repo before the fix: 48 ms at 0 audit rows, 103 ms at 5k, 526 ms at 25k,
# 1588 ms at 75k. With the LIMIT it stays flat (~35 ms) across that range.

def test_latest_audit_entry_query_is_bounded():
    compiled = str(main.latest_audit_entry_query().compile(compile_kwargs={"literal_binds": True}))

    assert "LIMIT" in compiled.upper(), (
        f"main.latest_audit_entry_query() has no LIMIT -- every transfer will load the "
        f"entire audit_log and build an ORM object per row, so transfer latency will grow "
        f"with ledger history. Query was:\n{compiled}"
    )
    assert "ORDER BY" in compiled.upper(), (
        f"the lookup must be ordered for 'newest' to mean anything. Query was:\n{compiled}"
    )


# --- Retry backoff must stay bounded and jittered ---------------------------
#
# Transfers retry on SERIALIZABLE conflicts. Without a delay between attempts
# the losers retry instantly and collide again in lockstep, burning all their
# attempts in a few milliseconds. Jitter is what breaks the lockstep -- a
# fixed delay would just move every loser to the same later instant.

def test_backoff_is_bounded_and_grows_with_attempt():
    ceilings = [main.BACKOFF_BASE_SECONDS * (2 ** attempt) for attempt in range(main.TRANSFER_MAX_ATTEMPTS)]

    assert ceilings == sorted(ceilings), "backoff ceiling should be non-decreasing across attempts"

    # Only the gaps BETWEEN attempts are slept -- never after the last one.
    worst_case_total = sum(ceilings[: main.TRANSFER_MAX_ATTEMPTS - 1])
    assert worst_case_total <= 0.5, (
        f"worst-case added delay before a 409 is {worst_case_total:.3f}s -- long enough "
        f"that a hot account could stall callers"
    )


@pytest.mark.asyncio
async def test_backoff_delay_is_jittered():
    """Assert on the delay actually *requested*, not the wall-clock elapsed.

    Measuring elapsed time can't tell jitter from a fixed delay: OS scheduling
    noise alone makes repeated `sleep(0.06)` calls land on different durations,
    so a wall-clock version of this test passes even with the jitter removed
    (verified). Capturing the argument handed to asyncio.sleep is exact.
    """
    requested = []

    async def record(seconds):
        requested.append(seconds)

    original = asyncio.sleep
    asyncio.sleep = record
    try:
        for _ in range(20):
            await main._backoff_delay(1)
    finally:
        asyncio.sleep = original

    assert len(set(requested)) > 1, (
        f"every retry asked for the same delay ({requested[0]}s) -- jitter isn't being "
        f"applied, so conflicting transfers will all wake at the same instant and "
        f"collide again in lockstep"
    )

    ceiling = main.BACKOFF_BASE_SECONDS * (2 ** 1)
    assert max(requested) <= ceiling, f"a delay exceeded its ceiling: {max(requested)}s > {ceiling}s"
    assert min(requested) >= 0


@pytest.mark.asyncio
async def test_backoff_never_sleeps_after_the_final_attempt():
    """A caller about to get a 409 shouldn't wait through a pointless sleep."""
    slept = []

    async def record(seconds):
        slept.append(seconds)

    original = asyncio.sleep
    asyncio.sleep = record
    try:
        for attempt in range(main.TRANSFER_MAX_ATTEMPTS):
            if attempt < main.TRANSFER_MAX_ATTEMPTS - 1:
                await main._backoff_delay(attempt)
    finally:
        asyncio.sleep = original

    assert len(slept) == main.TRANSFER_MAX_ATTEMPTS - 1, (
        f"expected {main.TRANSFER_MAX_ATTEMPTS - 1} sleeps across "
        f"{main.TRANSFER_MAX_ATTEMPTS} attempts, got {len(slept)}"
    )
