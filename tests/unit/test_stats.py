import logging

from configuration import Configuration
from entity import EntityRef
from stats import RunStats, log_effective_settings

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def test_warn_once_per_key(caplog):
    s = RunStats(mode="complete")
    with caplog.at_level(logging.WARNING):
        s.warn("x", "first")
        s.warn("x", "second")
    assert [r.getMessage() for r in caplog.records] == ["first"]


def test_delivery_count_high_water_and_unreadable_total():
    s = RunStats(mode="complete")
    for n in (1, 5, 3):
        s.note_delivery_count(n)
    s.unreadable["UnreadableBody:retry"] += 1
    s.unreadable["UnreadableBody:dead_lettered"] += 2
    assert s.max_delivery_count == 5 and s.unreadable_total() == 2


def test_summary_line_mentions_counts():
    s = RunStats(mode="defer_commit", received=3, deferred=3, committed=2, stop_reason="idle")
    line = s.summary_line()
    assert "received=3" in line and "deferred=3" in line and "committed=2" in line and "stop=idle" in line
    assert "skipped_scheduled" not in line  # zero counters are omitted


def test_summary_line_shows_skipped_scheduled():
    s = RunStats(mode="peek", received=2, written=1, skipped_scheduled=1, stop_reason="end_of_entity")
    assert "skipped_scheduled=1" in s.summary_line()


def test_effective_settings_marks_defaults(caplog):
    c = Configuration(**{"#connection_string": SAS, "source": {"entity_type": "queue", "queue_name": "q"}})  # ty: ignore[invalid-argument-type]
    with caplog.at_level(logging.INFO):
        log_effective_settings(c, EntityRef.from_source(c.source))
    text = caplog.records[-1].getMessage()
    assert "settlement_mode=complete (default)" in text and "entity=q" in text and "SharedAccessKey" not in text
