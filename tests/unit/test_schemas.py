import json
from pathlib import Path

from pydantic import BaseModel

import configuration
from component import Component

ROOT = Path(__file__).resolve().parents[2] / "component_config"
ROOT_SCHEMA = json.loads((ROOT / "configSchema.json").read_text())
ROW_SCHEMA = json.loads((ROOT / "configRowSchema.json").read_text())
SYNC_ACTIONS = {"testConnection", "listQueues", "listTopics", "listSubscriptions", "previewMessages"}


def walk(schema: dict, path: str = ""):
    for name, prop in schema.get("properties", {}).items():
        yield f"{path}{name}", prop, schema
        if prop.get("type") == "object":
            yield from walk(prop, f"{path}{name}.")


def test_removed_dev_branch_override_absent():
    text = json.dumps(ROOT_SCHEMA) + json.dumps(ROW_SCHEMA)
    assert "destructive_in_branch" not in text


def test_every_enum_has_matching_titles():
    for _, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA)):
        if "enum" in prop and prop.get("enum"):
            titles = prop.get("options", {}).get("enum_titles")
            assert titles and len(titles) == len(prop["enum"]), prop


def test_enum_values_match_model():
    expected = {
        "source.entity_type": configuration.EntityType,
        "source.sub_queue": configuration.SubQueue,
        "source.settlement_mode": configuration.SettlementMode,
        "source.fetch_mode": configuration.FetchMode,
        "body.body_format": configuration.BodyFormat,
        "body.unreadable_body": configuration.UnreadablePolicy,
        "destination.load_type": configuration.LoadType,
        "destination.primary_key": configuration.PrimaryKey,
    }
    props = {path: prop for path, prop, _ in walk(ROW_SCHEMA)}
    for path, enum_cls in expected.items():
        assert props[path]["enum"] == [e.value for e in enum_cls], path
    root = {path: prop for path, prop, _ in walk(ROOT_SCHEMA)}
    assert root["auth_type"]["enum"] == [e.value for e in configuration.AuthType]


def test_secret_fields_are_hash_prefixed_passwords():
    root = {path: prop for path, prop, _ in walk(ROOT_SCHEMA)}
    for key in ("#connection_string", "#client_secret"):
        assert root[key]["format"] == "password"


def test_dependencies_reference_siblings():
    for _, prop, parent in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA)):
        for dep in prop.get("options", {}).get("dependencies", {}):
            assert dep in parent["properties"], dep


def test_async_actions_exist_in_code():
    actions = {
        prop["options"]["async"]["action"]
        for _, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA))
        if "async" in prop.get("options", {})
    }
    assert actions <= SYNC_ACTIONS and {"listQueues", "listTopics", "listSubscriptions"} <= actions
    from keboola.component import base

    assert Component is not None and SYNC_ACTIONS <= set(base._SYNC_ACTION_MAPPING)


def test_async_selects_declare_empty_enum():
    # Sync-action buttons also carry `options.async` (label + action) but are not selects.
    for _, prop, _ in walk(ROW_SCHEMA):
        if prop.get("type") != "button" and "async" in prop.get("options", {}):
            assert prop.get("enum") == [], prop


def test_async_autoload_is_array_form():
    # The Keboola UI autoloads only an array (json-editor `helpers.ts` `shouldAutoload`: not
    # `Array.isArray(autoload)` -> false; `[]` loads on open, a path list once those fields are set);
    # a boolean `true` never autoloads, whatever older docs say.
    autoloads = {
        name: prop["options"]["async"]["autoload"]
        for name, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA))
        if "autoload" in prop.get("options", {}).get("async", {})
    }
    assert autoloads == {
        "source.queue_name": [],
        "source.topic_name": [],
        "source.subscription_name": ["parameters.source.topic_name"],
    }


def test_message_id_primary_key_warns():
    props = {path: prop for path, prop, _ in walk(ROW_SCHEMA)}
    text = json.dumps(props["destination.primary_key"])
    assert "may be empty or reused" in text


def test_required_is_array_form():
    for _, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA)):
        assert not isinstance(prop.get("required"), bool)


def test_ui_options():
    assert json.loads((ROOT / "uiOptions.md").read_text()) == ["genericDockerUI", "genericDockerUI-rows"]


def test_root_schema_not_empty():
    assert ROOT_SCHEMA.get("properties")


def _model_paths(model: type[BaseModel], prefix: str = "") -> set[str]:
    paths: set[str] = set()
    for name, field in model.model_fields.items():
        key = f"{prefix}{field.alias or name}"
        if isinstance(field.annotation, type) and issubclass(field.annotation, BaseModel):
            paths |= _model_paths(field.annotation, f"{key}.")
        else:
            paths.add(key)
    return paths


def test_schema_fields_match_model():
    # Every model field has exactly one schema field (root + row) and vice versa.
    schema_paths = {
        path
        for path, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA))
        if prop.get("type") not in ("object", "button")
    }
    assert schema_paths == _model_paths(configuration.Configuration)


def test_portal_urls_point_at_main():
    repo = "https://github.com/keboola/component-ex-azure-service-bus"
    assert (ROOT / "sourceCodeUrl.md").read_text().strip() == repo
    assert (ROOT / "documentationUrl.md").read_text().strip() == f"{repo}/blob/main/README.md"
    assert (ROOT / "licenseUrl.md").read_text().strip() == f"{repo}/blob/main/LICENSE.md"


def test_test_connection_is_a_root_button_only():
    # Phase 8 (maintainer decision): the row form offers Preview Messages, which proves entity access;
    # Test Connection (the management probe) lives on the configuration only.
    def buttons(schema: dict) -> dict[str, str]:
        return {
            name: prop.get("options", {}).get("async", {}).get("action") or prop["format"]
            for name, prop, _ in walk(schema)
            if prop.get("type") == "button"
        }

    assert buttons(ROOT_SCHEMA) == {"test_connection": "test-connection"}
    assert buttons(ROW_SCHEMA) == {"source.preview_messages": "previewMessages"}
