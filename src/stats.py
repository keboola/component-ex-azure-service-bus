"""Run statistics, warning aggregation and the effective-settings/summary log lines (Task 10, spec §6.12).

Every receive-loop, commit and settlement module funnels its outcome counts through one ``RunStats``
instance so ``component.py`` logs exactly one run summary at the end of a run, whatever mode ran.
``warn`` deduplicates by key so a condition re-checked every batch (e.g. the legacy-queue
``write_always`` gap, an approximate watermark, a prefetch > 1 note) is logged once per run, not once
per batch, while still being counted for the "N warning(s) raised" line. ``log_effective_settings`` is
the companion line logged once at run start: every behaviour-affecting setting, never an auth field,
with a `` (default)`` suffix on anything left at its model default.
"""

import logging
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import InitVar, dataclass, field
from typing import Any

from configuration import (
    AdvancedConfig,
    BodyConfig,
    Configuration,
    DestinationConfig,
    LimitsConfig,
    SettlementMode,
    SourceConfig,
)
from entity import EntityRef

logger = logging.getLogger(__name__)

_RETRY_DISPOSITION = "retry"


def _mark(value: Any, default: Any) -> str:
    """``<value>`` or ``<value> (default)`` when it equals the field's model default (§6.12)."""
    return f"{value} (default)" if value == default else f"{value}"


def _mark_table_name(table_name: str, default_table_name: str) -> str:
    """Like ``_mark``, but comparing the raw (possibly empty) model values first -- an explicit
    ``table_name`` that happens to be the literal string ``"derived"`` must never be marked
    ``(default)`` just because the empty default also displays as ``"derived"``."""
    display = table_name or "derived"
    return f"{display} (default)" if table_name == default_table_name else display


@dataclass
class RunStats:
    """Per-run counters plus the two spec §6.12 log lines.

    ``monotonic`` is the R-4 injectable clock (``None`` resolves to ``time.monotonic`` at
    construction) used to report ``duration_s`` in ``summary_line()``; it is an ``InitVar``, not a
    public field, since it is consumed once in ``__post_init__`` and never read back.
    """

    mode: str
    received: int = 0
    written: int = 0
    completed: int = 0
    deferred: int = 0
    deleted_on_receive: int = 0
    committed: int = 0
    already_gone: int = 0
    carried_forward: int = 0
    dropped_stale: int = 0
    orphans_recovered: int = 0
    settlement_failures: int = 0
    recoveries: int = 0
    unreadable_recycles: int = 0
    expired_skipped: int = 0
    skipped_scheduled: int = 0
    max_delivery_count: int = 0
    orphans_guarded: list[int] = field(default_factory=list)
    unreadable: Counter[str] = field(default_factory=Counter)
    stop_reason: str = ""
    warnings: dict[str, str] = field(default_factory=dict)
    monotonic: InitVar[Callable[[], float] | None] = None

    def __post_init__(self, monotonic: Callable[[], float] | None) -> None:
        self._monotonic = monotonic or time.monotonic
        self._start = self._monotonic()

    def warn(self, key: str, message: str) -> None:
        """Log ``message`` at WARNING and store it under ``key`` -- but only the first time ``key``
        is raised this run; a later call with the same key (even a different message) is silent."""
        if key in self.warnings:
            return
        self.warnings[key] = message
        logger.warning("%s", message)

    def note_delivery_count(self, n: int) -> None:
        """Feed the J8 delivery-count high-water mark from every message read this run."""
        self.max_delivery_count = max(self.max_delivery_count, n)

    def unreadable_total(self) -> int:
        """Unreadable bodies actually disposed of this run: every ``unreadable`` count whose
        disposition (the part after the last ``:``) is not ``retry`` -- a retry is a recycle
        attempt, not a final disposition, and is already counted separately in ``unreadable_recycles``."""
        return sum(count for key, count in self.unreadable.items() if key.rsplit(":", 1)[-1] != _RETRY_DISPOSITION)

    def summary_line(self) -> str:
        """One line of ``name=value`` pairs (§6.12); zero counters are omitted except
        ``received`` / ``written`` / ``stop``, which are always shown."""
        duration_s = round(self._monotonic() - self._start, 3)
        tokens = [f"mode={self.mode}", f"received={self.received}", f"written={self.written}"]
        optional_counts = (
            ("completed", self.completed),
            ("deferred", self.deferred),
            ("deleted_on_receive", self.deleted_on_receive),
            ("committed", self.committed),
            ("already_gone", self.already_gone),
            ("carried_forward", self.carried_forward),
            ("dropped_stale", self.dropped_stale),
            ("orphans_recovered", self.orphans_recovered),
            ("orphans_guarded", len(self.orphans_guarded)),
            ("settlement_failures", self.settlement_failures),
            ("recoveries", self.recoveries),
            ("unreadable_recycles", self.unreadable_recycles),
            ("expired_skipped", self.expired_skipped),
            ("skipped_scheduled", self.skipped_scheduled),
            ("max_delivery_count", self.max_delivery_count),
        )
        tokens.extend(f"{name}={value}" for name, value in optional_counts if value)
        tokens.extend(
            f"unreadable_{key.replace(':', '_')}={count}" for key, count in sorted(self.unreadable.items()) if count
        )
        tokens.append(f"stop={self.stop_reason}")
        tokens.append(f"duration_s={duration_s}")
        return " ".join(tokens)

    def log_summary(self) -> None:
        """One INFO line with ``summary_line()``, plus a second INFO line with the number of
        distinct warnings raised this run when at least one was."""
        logger.info(self.summary_line())
        if self.warnings:
            logger.info("%d warning(s) were raised during this run.", len(self.warnings))


def log_effective_settings(config: Configuration, entity: EntityRef) -> None:
    """One INFO line at run start naming every behaviour-affecting setting (§6.12) -- source,
    limits, body, destination and advanced values -- never an auth field. A value equal to its
    model's default is suffixed `` (default)``."""
    source = config.source
    default_source = SourceConfig(entity_type=source.entity_type)
    limits, default_limits = config.limits, LimitsConfig()
    body, default_body = config.body, BodyConfig()
    destination, default_destination = config.destination, DestinationConfig()
    advanced, default_advanced = config.advanced, AdvancedConfig()

    tokens = [
        f"settlement_mode={_mark(source.settlement_mode, default_source.settlement_mode)}",
        f"entity={entity.path}",
        f"sub_queue={_mark(entity.sub_queue, default_source.sub_queue)}",
        f"sessions={_mark(source.session_enabled, default_source.session_enabled)}",
    ]
    if source.settlement_mode is SettlementMode.PEEK:
        tokens.append(f"fetch_mode={_mark(source.fetch_mode, default_source.fetch_mode)}")
    else:  # peek ignores the idle timeout (§5.3)
        tokens.append(f"idle_timeout_seconds={_mark(source.idle_timeout_seconds, default_source.idle_timeout_seconds)}")
    tokens.extend(
        [
            f"max_messages={_mark(limits.max_messages, default_limits.max_messages)}",
            f"max_duration_seconds={_mark(limits.max_duration_seconds, default_limits.max_duration_seconds)}",
            f"stop_at_job_start={_mark(limits.stop_at_job_start, default_limits.stop_at_job_start)}",
            f"body_format={_mark(body.body_format, default_body.body_format)}",
            f"unreadable_body={_mark(body.unreadable_body, default_body.unreadable_body)}",
            f"load_type={_mark(destination.load_type, default_destination.load_type)}",
            f"primary_key={_mark(destination.primary_key, default_destination.primary_key)}",
            "table_name=" + _mark_table_name(destination.table_name, default_destination.table_name),
            f"batch_size={_mark(advanced.batch_size, default_advanced.batch_size)}",
            f"prefetch_count={_mark(advanced.prefetch_count, default_advanced.prefetch_count)}",
            f"recovery_wait_seconds={_mark(advanced.recovery_wait_seconds, default_advanced.recovery_wait_seconds)}",
        ]
    )
    logger.info(" ".join(tokens))
