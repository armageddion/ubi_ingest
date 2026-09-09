from datetime import datetime, timezone

from daemon import CustomerJobRunner, cron_due, select_changed_articles
from state import StateStore


def article(article_id, value):
    return {"articleId": str(article_id), "data": {"value": value}}


def test_cron_due_detection():
    now = datetime(2026, 9, 4, 12, 15, tzinfo=timezone.utc)
    assert cron_due("15 12 * * *", now)
    assert not cron_due("0 12 * * *", now)
    assert not cron_due("15 12 * * *", now, now)


def test_first_run_full_push_and_unchanged_skip(tmp_path):
    store = StateStore(str(tmp_path / "state.db"))
    first = [article(1, "a"), article(2, "b")]
    changed, hashes, removed = select_changed_articles("cust", first, store)
    assert changed == first
    assert len(hashes) == 2
    assert not removed
    store.update_hashes("cust", hashes)

    changed, _, _ = select_changed_articles("cust", first, store)
    assert changed == []


def test_cache_updates_only_after_confirmed_push(tmp_path):
    store = StateStore(str(tmp_path / "state.db"))
    customer = {"name": "cust"}
    pushed = []

    def failed_process(customer, state_store):
        articles = [article(1, "a")]
        changed, hashes, _ = select_changed_articles(customer["name"], articles, state_store)
        pushed.extend(changed)
        return False

    assert not CustomerJobRunner(customer, store, failed_process).run()
    assert store.get_hashes("cust") == {}
    assert pushed


def test_overlapping_customer_run_is_skipped(tmp_path):
    store = StateStore(str(tmp_path / "state.db"))
    runner = CustomerJobRunner(
        {"name": "cust"}, store, lambda customer, state_store: True
    )
    assert runner.lock.acquire(blocking=False)
    try:
        assert not runner.run()
    finally:
        runner.lock.release()
