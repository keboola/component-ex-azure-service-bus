"""Typed configuration models for keboola.ex-azure-service-bus (Task 2).

Keboola merges the config-root (auth) parameters and the config-row (source /
limits / body / destination / advanced) parameters into a single flat
``parameters`` dict, so ``Configuration`` extends ``AuthConfiguration`` rather
than nesting it. Each sub-section is its own model so later modules (client,
entity, receiver, output, ...) can depend on a narrow, self-documenting slice
instead of the whole configuration.
"""

import re
from enum import StrEnum
from typing import Any, Self

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_-]*[A-Za-z0-9])?$")


class AuthType(StrEnum):
    CONNECTION_STRING = "connection_string"
    SERVICE_PRINCIPAL = "service_principal"


class EntityType(StrEnum):
    QUEUE = "queue"
    SUBSCRIPTION = "subscription"


class SubQueue(StrEnum):
    NONE = "none"
    DEAD_LETTER = "dead_letter"
    TRANSFER_DEAD_LETTER = "transfer_dead_letter"


class SettlementMode(StrEnum):
    COMPLETE = "complete"
    DEFER_COMMIT = "defer_commit"
    RECEIVE_AND_DELETE = "receive_and_delete"
    PEEK = "peek"

    @property
    def is_destructive(self) -> bool:
        return self is not SettlementMode.PEEK


class FetchMode(StrEnum):
    INCREMENTAL_FETCH = "incremental_fetch"
    FULL_FETCH = "full_fetch"


class BodyFormat(StrEnum):
    TEXT = "text"
    BASE64 = "base64"
    JSON_FLATTEN = "json_flatten"


class UnreadablePolicy(StrEnum):
    DEAD_LETTER = "dead_letter"
    LEAVE = "leave"
    FAIL = "fail"


class LoadType(StrEnum):
    INCREMENTAL_LOAD = "incremental_load"
    FULL_LOAD = "full_load"


class PrimaryKey(StrEnum):
    SEQUENCE_NUMBER = "sequence_number"
    MESSAGE_ID = "message_id"
    SOURCE_ENTITY_SEQUENCE_NUMBER = "source_entity_sequence_number"

    @property
    def columns(self) -> list[str]:
        if self is PrimaryKey.SOURCE_ENTITY_SEQUENCE_NUMBER:
            return ["source_entity", "sequence_number"]
        return [self.value]


class AuthConfiguration(BaseModel):
    """The config-root auth block, identical to the sibling writer's.

    Kept separate from ``Configuration`` so sync actions that only need auth
    (e.g. the root ``testConnection``) can build a partial model straight from
    the merged ``parameters`` dict without the row fields being required.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    auth_type: AuthType = AuthType.CONNECTION_STRING
    connection_string: str = Field("", alias="#connection_string")
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = Field("", alias="#client_secret")
    fully_qualified_namespace: str = ""

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            located = [(".".join(str(part) for part in err["loc"]) or "configuration", err) for err in e.errors()]
            message = "Validation Error: " + ", ".join(f"{loc}: {err['msg']}" for loc, err in located)
            raise UserException(message) from e

    @model_validator(mode="after")
    def _validate_auth(self) -> Self:
        if self.auth_type == AuthType.CONNECTION_STRING and not self.connection_string:
            raise ValueError("`#connection_string` is required for connection_string auth.")
        if self.auth_type == AuthType.SERVICE_PRINCIPAL:
            required = {
                "tenant_id": self.tenant_id,
                "client_id": self.client_id,
                "#client_secret": self.client_secret,
                "fully_qualified_namespace": self.fully_qualified_namespace,
            }
            missing = [name for name, value in required.items() if not value]
            if missing:
                raise ValueError(f"Service principal auth requires: {', '.join(missing)}.")
        return self


class SourceConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    entity_type: EntityType
    queue_name: str | None = None
    topic_name: str | None = None
    subscription_name: str | None = None
    sub_queue: SubQueue = SubQueue.NONE
    session_enabled: bool = False
    settlement_mode: SettlementMode = SettlementMode.COMPLETE
    fetch_mode: FetchMode = FetchMode.INCREMENTAL_FETCH
    idle_timeout_seconds: int = Field(10, ge=1, le=300)


class LimitsConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    max_messages: int = Field(0, ge=0)
    stop_at_job_start: bool = True


class BodyConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    body_format: BodyFormat = BodyFormat.TEXT
    unreadable_body: UnreadablePolicy = UnreadablePolicy.DEAD_LETTER


class DestinationConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    table_name: str = ""
    load_type: LoadType = LoadType.INCREMENTAL_LOAD
    primary_key: PrimaryKey = PrimaryKey.SEQUENCE_NUMBER

    @property
    def incremental(self) -> bool:
        return self.load_type is LoadType.INCREMENTAL_LOAD


class AdvancedConfig(BaseModel):
    """The ``advanced`` section: ignored (model defaults) unless ``advanced_options`` is on.
    ``max_duration_seconds`` defaults to 3000 -- below the platform's default one-hour job timeout,
    leaving time for the import (the component is not told the job timeout)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    max_duration_seconds: int = Field(3000, ge=60, le=43200)
    batch_size: int = Field(100, ge=1, le=5000)
    prefetch_count: int = Field(1, ge=1, le=1000)
    recovery_wait_seconds: int = Field(0, ge=0, le=330)


class SourceSelection(BaseModel):
    """A row's ``source`` block as ``listSubscriptions`` reads it: the form may still be half filled
    in, so only ``topic_name`` is typed and every other key is ignored."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    topic_name: str | None = None


class SyncActionConfiguration(AuthConfiguration):
    """Partial model for ``listSubscriptions`` (spec §5.4): the auth block plus the selected topic of
    a possibly partial ``source``, never a validated row."""

    source: SourceSelection | None = None

    @property
    def topic_name(self) -> str | None:
        return self.source.topic_name if self.source is not None else None


class EntityConfiguration(AuthConfiguration):
    """Auth plus the row's validated, normalised ``source``: what every action that opens the entity
    needs. ``previewMessages`` (spec §5.4) validates only this, so a half-edited unrelated field -- an
    invalid table name, a batch size out of range -- never blocks it; the other sections are ignored.
    ``Configuration`` extends it, so a run and the sync action read the source identically."""

    source: SourceConfig

    @model_validator(mode="after")
    def _normalise_source(self) -> Self:
        source = self.source
        if source.entity_type is EntityType.QUEUE:
            if not source.queue_name:
                raise ValueError("`source.queue_name` is required when `entity_type` is `queue`.")
            source.topic_name = None
            source.subscription_name = None
        else:
            missing = [
                name
                for name, value in (("topic_name", source.topic_name), ("subscription_name", source.subscription_name))
                if not value
            ]
            if missing:
                raise ValueError(f"`source.{missing[0]}` is required when `entity_type` is `subscription`.")
            source.queue_name = None

        if source.settlement_mode is not SettlementMode.PEEK:
            source.fetch_mode = FetchMode.INCREMENTAL_FETCH
        if source.sub_queue is not SubQueue.NONE:
            source.session_enabled = False
        return self


class Configuration(EntityConfiguration):
    """The full row configuration: root auth fields, the row's source and its other sections."""

    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    body: BodyConfig = Field(default_factory=BodyConfig)
    destination: DestinationConfig = Field(default_factory=DestinationConfig)
    advanced: AdvancedConfig = Field(default_factory=AdvancedConfig)
    advanced_options: bool = False

    @model_validator(mode="after")
    def _apply_advanced_gate(self) -> Self:
        if not self.advanced_options:
            self.advanced = AdvancedConfig()
        return self

    @model_validator(mode="after")
    def _refuse_unsafe(self) -> Self:
        source = self.source
        if source.settlement_mode is SettlementMode.RECEIVE_AND_DELETE and self.advanced.prefetch_count > 1:
            raise ValueError(
                "receive_and_delete requires prefetch_count 1: buffered messages are already deleted on the broker."
            )
        if (
            source.settlement_mode is SettlementMode.PEEK
            and source.fetch_mode is FetchMode.INCREMENTAL_FETCH
            and (source.session_enabled or source.sub_queue is not SubQueue.NONE)
        ):
            raise ValueError(
                "peek with incremental_fetch is not supported on session entities or sub-queues; use full_fetch."
            )
        return self

    @model_validator(mode="after")
    def _validate_table_name(self) -> Self:
        table_name = self.destination.table_name
        if table_name and not _TABLE_NAME_RE.match(table_name):
            raise ValueError(
                "`destination.table_name` may contain only letters, digits, '-' and '_' "
                "and must not start or end with '-' or '_'."
            )
        return self
