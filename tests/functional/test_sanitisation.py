"""Sanitisation gate (spec §8): committed fixtures, expected outputs and component_config may carry
only the dummy secrets. A ``<...>`` placeholder (e.g. a schema field's example connection string)
is not a secret value; surfaced errors are checked for redaction in the sync-action cases 02 / 03.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DUMMY_KEYS = {"ZmFrZWtleWZha2VrZXlmYWtla2V5ZmFrZWtleTEyMzQ1Njc4OTA=", "c2VjcmV0"}
DUMMY_CLIENT_SECRETS = {"dummy-secret"}
PLACEHOLDER = re.compile(r"<[^<>]+>")


def _scanned() -> list[Path]:
    tests = ROOT / "tests"
    component_config = ROOT / "component_config"
    # The Docker test stage copies component_config/ (Dockerfile); an empty scan would pass silently.
    assert component_config.is_dir(), "component_config/ is missing: the sanitisation gate would scan nothing"
    paths = [*tests.rglob("*.json"), *tests.rglob("*.csv"), *component_config.rglob("*")]
    return [path for path in paths if path.is_file()]


# (pattern, allowed values): a SAS key may only be a dummy, a SAS token signature only the mask.
_SAS_VALUES = (
    (re.compile(r"SharedAccessKey=([^;\"\s]+)"), DUMMY_KEYS),
    (re.compile(r"sig=([^&;\"\s]+)"), {"***"}),
)


def leaked_sas_values(text: str) -> list[str]:
    """Every ``SharedAccessKey=`` / ``sig=`` value in ``text`` that is neither allowed nor a ``<...>``
    placeholder -- each occurrence checked on its own."""
    return [
        value
        for pattern, allowed in _SAS_VALUES
        for value in pattern.findall(text)
        if value not in allowed and not PLACEHOLDER.fullmatch(value)
    ]


def test_leak_check_inspects_every_value():
    # a masked signature elsewhere in the same file must not excuse a real one
    text = '{"a": "sr=x&sig=***&se=1", "b": "sig=<signature>", "c": "sr=x&sig=abc%2Bdef&se=1"}'
    assert leaked_sas_values(text) == ["abc%2Bdef"]
    assert leaked_sas_values("SharedAccessKey=<key>;SharedAccessKey=cmVhbA==") == ["cmVhbA=="]


def test_committed_fixtures_hold_only_dummy_secrets():
    for path in _scanned():
        assert leaked_sas_values(path.read_text(errors="ignore")) == [], path


def test_functional_configs_hold_only_dummy_client_secrets():
    cases = json.loads((ROOT / "tests" / "setup" / "configs.json").read_text())
    secrets = {case["config"]["parameters"].get("#client_secret") for case in cases} - {None}
    assert secrets <= DUMMY_CLIENT_SECRETS
