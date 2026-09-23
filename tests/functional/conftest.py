"""SDK-mock datadir harness for the functional suite (spec §8, writer precedent).

Service Bus's data plane is AMQP 1.0, which ``vcrpy`` cannot record, so there are no cassettes:
every case runs the real ``src/component.py`` as ``__main__`` (``runpy``, real exit-code guard, real
``KBC_DATADIR``) against the in-repo :class:`~tests.fakes.broker.FakeBroker`. The autouse
``fake_broker`` fixture installs the fake SDK clients and puts every clock the component reads on the
broker's :class:`~tests.fakes.broker.FakeClock`: ``columns.utc_now`` (T0, ``extracted_at_utc``, lock
and expiry checks), ``time.monotonic`` (limits, run duration) and ``time.sleep`` (backoffs and polls
advance the fake clock instead of waiting). Configs come from ``tests/setup/configs.json`` (wrapped
format, dummy credentials only); broker seeds are built per test through the fake's API.

Each run advances the fake clock by ``JOB_START_DELAY_SECONDS`` before it starts, so everything a
test seeded beforehand was enqueued strictly before the job's T0 -- as on the platform, where
messages are sent before the job that extracts them starts.
"""

import copy
import csv
import json
import logging
import runpy
import time
from collections.abc import Callable, Iterator
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from azure.servicebus import ServiceBusSubQueue

import columns
from client import RedactingFilter
from tests.fakes.broker import FakeBroker, FakeServiceBusClient, install

ROOT = Path(__file__).resolve().parents[2]
COMPONENT_SCRIPT = ROOT / "src" / "component.py"
CONFIGS_FILE = ROOT / "tests" / "setup" / "configs.json"
CONFIGS: dict[str, dict] = {case["name"]: case["config"] for case in json.loads(CONFIGS_FILE.read_text())}

JOB_START_DELAY_SECONDS = 1
DUMMY_SAS_KEY = "ZmFrZWtleWZha2VrZXlmYWtla2V5ZmFrZWtleTEyMzQ1Njc4OTA="
DUMMY_CLIENT_SECRET = "dummy-secret"
DUMMY_CONNECTION_STRING = (
    f"Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey={DUMMY_SAS_KEY}"
)

# Platform variables a developer shell (or CI) may carry: a case sets the ones it needs through `env`.
_PLATFORM_ENV = (
    "KBC_BRANCHID",
    "KBC_CONFIGID",
    "KBC_CONFIGROWID",
    "KBC_STACKID",
    "KBC_PROJECT_FEATURE_GATES",
    "KBC_DATA_TYPE_SUPPORT",
    "KBC_COMPONENT_RUN_MODE",
    "KBC_LOGGER_ADDR",
    "KBC_LOGGER_PORT",
)


@dataclass
class CaseResult:
    """One component run: exit code, captured output and everything it left in ``data/out``.
    ``tables`` / ``manifests`` are keyed by the table file name (``"q.csv"``)."""

    exit_code: int
    out_dir: Path
    stdout: str
    stderr: str
    state: dict | None
    tables: dict[str, list[dict]]
    manifests: dict[str, dict]

    @property
    def log(self) -> str:
        """Everything the run printed: INFO goes to stdout, WARNING and above to stderr."""
        return self.stdout + self.stderr


@pytest.fixture(autouse=True)
def fake_broker(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeBroker]:
    """A fresh fake namespace per test, installed as the SDK, with the component's clocks on it."""
    broker = FakeBroker()
    install(monkeypatch, broker)
    for name in _PLATFORM_ENV:
        monkeypatch.delenv(name, raising=False)
    clock = broker.clock
    start = clock.now()
    monkeypatch.setattr(columns, "utc_now", clock.now)
    monkeypatch.setattr(time, "monotonic", lambda: (clock.now() - start).total_seconds())
    monkeypatch.setattr(time, "sleep", clock.advance)
    root, azure = logging.getLogger(), logging.getLogger("azure")
    levels = root.level, azure.level
    yield broker
    # Undo what ComponentBase / configure_logging / a sync action leave on the loggers.
    root.setLevel(levels[0])
    azure.setLevel(levels[1])
    for handler in list(root.handlers):
        if getattr(handler, "_keboola_owned", False):
            root.removeHandler(handler)
        for installed in [f for f in handler.filters if isinstance(f, RedactingFilter)]:
            handler.removeFilter(installed)


def _deep_merge(base: dict, overrides: dict) -> dict:
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _installed_broker() -> FakeBroker:
    broker = FakeServiceBusClient.broker
    assert broker is not None, "run_case needs the autouse fake_broker fixture"
    return broker


def _execute() -> int:
    """Run the component as ``__main__``; its guard exits 1 (user error) or 2 (unexpected)."""
    try:
        runpy.run_path(str(COMPONENT_SCRIPT), run_name="__main__")
    except SystemExit as exit_:
        code = exit_.code
        if code is None:
            return 0
        return code if isinstance(code, int) else 1
    return 0


def _read_outputs(out_dir: Path) -> tuple[dict | None, dict[str, list[dict]], dict[str, dict]]:
    state_file = out_dir / "state.json"
    state = json.loads(state_file.read_text()) if state_file.exists() else None
    tables: dict[str, list[dict]] = {}
    for path in sorted((out_dir / "tables").glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            tables[path.name] = list(csv.DictReader(handle))
    manifests = {
        path.name.removesuffix(".manifest"): json.loads(path.read_text())
        for path in sorted((out_dir / "tables").glob("*.manifest"))
    }
    return state, tables, manifests


def run_case(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    state: dict | None = None,
    env: dict[str, str] | None = None,
    overrides: dict | None = None,
) -> CaseResult:
    """Materialise ``<tmp_path>/data`` for the case (``overrides`` deep-merged into ``parameters``,
    ``state`` as ``in/state.json``), run the component once and read what it produced."""
    config = copy.deepcopy(CONFIGS[name])
    if overrides:
        config["parameters"] = _deep_merge(config.get("parameters", {}), overrides)
    data_dir = tmp_path / "data"
    for sub in ("in/tables", "in/files", "out/tables", "out/files"):
        (data_dir / sub).mkdir(parents=True, exist_ok=True)
    (data_dir / "config.json").write_text(json.dumps(config))
    if state is not None:
        (data_dir / "in" / "state.json").write_text(json.dumps(state))
    monkeypatch.setenv("KBC_DATADIR", str(data_dir))
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)

    capsys.readouterr()  # drop whatever was printed before this run
    _installed_broker().clock.advance(JOB_START_DELAY_SECONDS)
    exit_code = _execute()
    captured = capsys.readouterr()
    out_dir = data_dir / "out"
    out_state, tables, manifests = _read_outputs(out_dir)
    return CaseResult(
        exit_code=exit_code,
        out_dir=out_dir,
        stdout=captured.out,
        stderr=captured.err,
        state=out_state,
        tables=tables,
        manifests=manifests,
    )


def run_chain(
    names: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    before_each: Callable[[int], None] | None = None,
    discard_state: AbstractSet[int] = frozenset(),
    state: dict | None = None,
    env: dict[str, str] | None = None,
    overrides: dict | None = None,
) -> list[CaseResult]:
    """Run ``names`` in order on the same broker, each in ``<tmp_path>/run<N>``. A run starts from
    the previous run's ``out/state.json`` -- or from the previous run's *input* state when that run
    wrote none (it failed) or its index is in ``discard_state`` (another row of the same job failed,
    so the platform discarded the job's state, spec §2.1 / §6.10). ``before_each(i)`` seeds the
    broker or injects faults right before run ``i`` (0-based)."""
    results: list[CaseResult] = []
    current = state
    for index, name in enumerate(names):
        if before_each is not None:
            before_each(index)
        result = run_case(
            name, tmp_path / f"run{index + 1}", monkeypatch, capsys, state=current, env=env, overrides=overrides
        )
        results.append(result)
        if result.state is not None and index not in discard_state:
            current = result.state
    return results


def run_twice(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    **kw: Any,
) -> tuple[CaseResult, CaseResult]:
    """Two chained runs of one case (the ``run_chain`` rule; ``kw`` goes to ``run_chain``)."""
    first, second = run_chain([name, name], tmp_path, monkeypatch, capsys, **kw)
    return first, second


def sync_result(result: CaseResult) -> Any:
    """A sync action's JSON result: the last line of its stdout."""
    return json.loads(result.stdout.strip().splitlines()[-1])


def csv_header(result: CaseResult, table: str = "q.csv") -> list[str]:
    """The header row of an output CSV (``tables`` holds only its data rows)."""
    with (result.out_dir / "tables" / table).open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle))


def peek_stored(queue: str, sub_queue: ServiceBusSubQueue | None = None) -> list[Any]:
    """Every message stored on a queue (or one of its sub-queues), peeked through the fake SDK, for
    assertions on properties the broker set (e.g. a dead-letter reason). Call it after the run: it
    adds a client and a receiver to the broker's records."""
    client = FakeServiceBusClient.from_connection_string(DUMMY_CONNECTION_STRING)
    with client, client.get_queue_receiver(queue, sub_queue=sub_queue) as receiver:
        return receiver.peek_messages(250, sequence_number=1)


def summary(result: CaseResult) -> dict[str, str]:
    """The run summary line's ``name=value`` tokens (spec §6.12), e.g. ``{"committed": "3", ...}``."""
    (line,) = [line for line in result.stdout.splitlines() if line.startswith("mode=")]
    return dict(token.split("=", 1) for token in line.split())
