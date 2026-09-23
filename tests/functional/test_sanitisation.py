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


def test_committed_fixtures_hold_only_dummy_secrets():
    for path in _scanned():
        text = path.read_text(errors="ignore")
        for key in re.findall(r"SharedAccessKey=([^;\"\s]+)", text):
            assert key in DUMMY_KEYS or PLACEHOLDER.fullmatch(key), path
        assert "sig=" not in text or "sig=***" in text, path


def test_functional_configs_hold_only_dummy_client_secrets():
    cases = json.loads((ROOT / "tests" / "setup" / "configs.json").read_text())
    secrets = {case["config"]["parameters"].get("#client_secret") for case in cases} - {None}
    assert secrets <= DUMMY_CLIENT_SECRETS
