Azure Service Bus Extractor
===========================

Reads messages from an Azure Service Bus **queue** or **topic subscription** — or their dead-letter
/ transfer dead-letter sub-queues — and writes them to a Keboola Storage table.

**Table of Contents:**

[TOC]

Functionality
=============

Configuration is **row-based**: the connection to the namespace is set once at the configuration
level, and each row reads one source entity into one destination table, one output row per message.
The broker metadata (sequence number, enqueued time, delivery count, dead-letter reason, AMQP
header / properties, ...) is written as typed columns; the body is written as text, base64, or —
for JSON payloads — flattened into its own columns.

A row can either **consume** the entity (settle messages so they leave Service Bus — the row is
the entity's consumer) or **browse** it non-destructively with Peek mode (nothing is ever deleted;
the entity is read by someone else too).

Prerequisites
==============

- An Azure Service Bus namespace with the target **queue**, or **topic and subscription**, already
  created. The extractor does not create entities.
- Credentials that grant **Listen** rights (SAS) or the **Azure Service Bus Data Receiver** RBAC
  role (service principal) on the entity or namespace — see Authentication below.

Authentication
==============

Set once for the whole configuration. Choose one method:

- **Connection string (SAS)** — a shared access signature connection string for the namespace or
  entity, with **Listen** rights. The entity dropdowns and the configuration's **Test Connection**
  additionally need **Manage** rights; a Listen-only connection string still works for reading, but
  the entity name has to be typed instead of picked from a list, and **Preview Messages** in a row
  is the way to check access.
- **Service principal (Entra ID)** — a Microsoft Entra ID application identity: tenant ID, client
  ID, client secret, and the fully qualified namespace host name (e.g.
  `my-namespace.servicebus.windows.net`). Grant it the **Azure Service Bus Data Receiver** RBAC
  role — it covers receiving, peeking, completing, abandoning, deferring, dead-lettering, and
  listing / reading entity properties.

Provisioning
============

| Namespace setting | Effect | What to do |
|---|---|---|
| IP firewall ("Selected networks") | rejected connections are reported as unauthorized, indistinguishable from bad credentials | allowlist the Keboola stack's egress IP addresses: https://help.keboola.com/components/ip-addresses (the list can change) |
| Public network access disabled / private endpoints only | not reachable from Keboola-hosted jobs | unsupported — allow public access from the allowlisted IPs |
| `disableLocalAuth = true` | SAS connection strings stop working | use the service principal method |
| Network security perimeter | as IP firewall | as above |

An authentication failure names all of "credentials, rights, entity name or IP firewall" together,
since Service Bus reports all four the same way.

Configuration
=============

Source (per row)
----------------

Each row reads one queue, one topic subscription, or one of their dead-letter / transfer
dead-letter sub-queues:

- **Entity type & name** — the queue, or the topic and subscription, to read from.
- **Sub-queue** — read the entity's dead-letter or transfer dead-letter queue instead of the main
  entity. Sub-queues never require sessions.
- **Sessions** — enable when the entity requires sessions; must match the entity's own setting.
- **Settlement mode** — see [Settlement modes and guarantees](#settlement-modes-and-guarantees)
  below.
- **Fetch mode** — shown only for Peek mode: `incremental_fetch` (sequence-number cursor, default)
  or `full_fetch` (re-peek the whole entity every run). Incremental fetch is refused on partitioned
  entities, session entities, and sub-queues — use full fetch there.
- **Idle timeout** — how long one receive call waits for new messages (destructive modes only).
  When a receive returns nothing, the run peeks the entity to check that it is really drained: it
  ends only when no message in scope is left, and otherwise reconnects and continues — a busy or
  throttled namespace can briefly hand out nothing. If the run stops while messages remain (Max
  Messages, Max Duration, or repeated empty receives), a warning says how many are left; a
  throttled namespace is reported in the run summary.

Limits
------

- **Max messages** — stop after this many messages (`0` = no limit).
- **Stop at job start** — stop once a batch holds only messages enqueued after the job started,
  instead of also draining messages that arrive mid-run (approximate on partitioned and
  session-enabled entities).

Body
----

- **Body format** — `text` (decoded with the message's content-type charset, default), `base64`
  (raw bytes, base64-encoded), or `json_flatten` (flatten a JSON body into columns, see
  [Body formats and JSON flattening](#body-formats-and-json-flattening)). Changing it changes the
  output columns — drop the existing table first.
- **Unreadable body** — `dead_letter` (default), `leave` in place, or `fail` the run when a body
  cannot be decoded, is not valid JSON (flatten mode), or is too large for a Storage cell. On
  sub-queues, dead-lettering degrades to `leave` (the broker rejects dead-lettering a DLQ message).

Destination
-----------

- **Table name** — empty derives a name from the entity (queue → `<queue>`; subscription →
  `<topic>_<subscription>`; `_dead_letter` / `_transfer_dead_letter` appended for sub-queues;
  characters other than letters, digits, `-` and `_` become `_`). Different entities can derive the
  same name (e.g. `orders.eu` and `orders_eu`, or topic `a_b` + subscription `c` and topic `a` +
  subscription `b_c`); rows of one configuration share its bucket, so give such rows an explicit
  table name.
- **Load type** — `incremental_load` (upsert, default) or `full_load` (the table holds only this
  run's messages).
- **Primary key** — `sequence_number` (default; unique per entity, not globally), `message_id`
  (producer-set: may be empty or reused, in which case upserts merge different messages), or the
  source entity + sequence number pair (for a table shared by several rows). Changing it changes
  the output key — drop the existing table first.

Advanced
--------

Gated behind an **Advanced options** checkbox (while it is off, every value below is its default):

- **Max duration** — stop reading after this many seconds (60–43,200; default 3,000). It is
  checked between batches, so what was read is imported and the rest stays for the next run. The
  platform does not tell the component the job timeout, so keep it well below the configuration's
  job timeout (1 hour by default) to leave time for the import — a job killed by the timeout may
  import nothing of what it read.
- **Batch size** (1–5,000, default 100), **prefetch count** (1–1,000, default 1; must be 1 for
  `receive_and_delete`), and an optional post-recovery catch-up wait in seconds. Lower the batch
  size for entities with large message bodies, to keep memory use and lock duration in check.

Settlement modes and guarantees
================================

`source.settlement_mode` picks how messages leave the source and what a run guarantees. Default
**`complete`**.

| Mode | Deleted from Service Bus | Guarantee into Storage | Notes / loss window |
|---|---|---|---|
| `complete` (default) | right after the batch's rows are written locally | at-least-once for failures before a batch is settled | a job that fails after deleting messages still uploads the rows written so far |
| `defer_commit` | at the start of the **next** run, once its input state proves the previous import succeeded | at-least-once end to end | **exclusive consumer required** — no other application may defer on the same entity; best-effort orphan recovery on partitioned entities and sub-queues, impossible on session entities |
| `receive_and_delete` | by the broker on delivery | at-most-once | a crash between receive and write loses that batch |
| `peek` | never | non-destructive; a failed run is re-exported next run | messages removed by other consumers or by TTL before the peek are never seen |

Extraction modes (Load Type × Fetch Mode)
------------------------------------------

`destination.load_type` (incremental upsert or full replace) always applies. `source.fetch_mode`
only exists for Peek mode, since the other three modes have exactly one fetch behaviour — each
message is delivered once, then removed or hidden. Useful combinations: consume ×
`incremental_load` accumulates history (default); consume × `full_load` is a delta / staging table
of only this run's messages; `full_fetch` × `full_load` mirrors what the entity currently holds;
`incremental_fetch` × `incremental_load` accumulates; `incremental_fetch` × `full_load` is a delta
of newly peeked messages.

In Peek mode, deferred messages are exported with `state` = `DEFERRED`. Expired messages that
Service Bus has not purged yet, and scheduled messages that are not active yet, are skipped and
counted in the run summary (`expired_skipped`, `skipped_scheduled`). A scheduled message is
exported once Service Bus activates it. Activation gives it a new sequence number and enqueue time,
so the message is exported exactly once, under that new sequence number.

Dev branches
------------

Every settlement mode runs in every branch. A development branch reads the **same production
Service Bus entity** as the default branch: a destructive mode (everything except `peek`) run in a
branch consumes and removes production messages, which then reach only the branch's table. Use
**Peek Only** in branches, or point the branch configuration at a separate test entity. A
`defer_commit` run in a branch is a second consumer of the production entity: its deferrals live in
the branch's state, and either the production configuration's orphan scan or the branch's own next
run may delete them first.

The component does not block destructive modes in branches: the platform gives a job no signal that
tells a development branch from the default one (`KBC_BRANCHID` is set for default-branch jobs as
well), so such a check would also refuse production runs. Project admins who want the platform to
guard branch runs can enable the `dev-branch-configuration-unsafe` feature.

Output
======

Columns
-------

One output table per row, at `/data/out/tables/<table>.csv`, with a header row and a native-typed
manifest (`primary_key` from `destination.primary_key`, `incremental` from `destination.load_type`).
Fixed metadata columns, in order:

`sequence_number` (INTEGER), `message_id`, `enqueued_time_utc` (TIMESTAMP),
`enqueued_sequence_number` (INTEGER), `session_id`, `partition_key`, `subject`, `correlation_id`,
`content_type`, `reply_to`, `reply_to_session_id`, `to_address`, `application_properties` (JSON),
`delivery_count` (INTEGER), `state`, `time_to_live_seconds` (FLOAT), `expires_at_utc` (TIMESTAMP),
`scheduled_enqueue_time_utc` (TIMESTAMP), `dead_letter_reason`, `dead_letter_error_description`,
`dead_letter_source`, `body_type`, `message_annotations` (JSON), `amqp_durable` (BOOLEAN),
`amqp_priority` (INTEGER), `amqp_first_acquirer` (BOOLEAN), `amqp_user_id`,
`amqp_content_encoding`, `amqp_creation_time_utc` (TIMESTAMP), `amqp_absolute_expiry_time_utc`
(TIMESTAMP), `amqp_group_sequence` (INTEGER), `amqp_reply_to_group_id`, `source_entity`,
`settlement_mode`, `extracted_at_utc` (TIMESTAMP). Every unlisted column is STRING.

Then, depending on `body.body_format`: a single `body` column (`text` / `base64` modes), or the
flattened `body_*` columns followed by `body_unmapped` (`json_flatten` mode). Timestamps are
`YYYY-MM-DD HH:MM:SS.ffffff` UTC; `application_properties` / `message_annotations` stay JSON
because their key set is producer-defined and varies per message; the fixed AMQP header /
properties fields are flattened into their own scalar columns instead.

`state` is the value Service Bus reports (`ACTIVE`, `DEFERRED` or `SCHEDULED`). With
azure-servicebus 7.14.3, a message that was activated from a schedule can still report
`SCHEDULED`. The extractor exports that value as reported and does not rewrite it.

Body formats and JSON flattening
---------------------------------

- **`text`** — the body decoded with the message's content-type charset (UTF-8 if unknown),
  invalid bytes replaced.
- **`base64`** — the raw body bytes, base64-encoded.
- **`json_flatten`** — the body parsed as JSON and flattened recursively: objects expand into one
  column per leaf (`order.customer.id` → `body_order_customer_id`); arrays stay one column holding
  their compact JSON; a non-object top-level value becomes one column, `body_value`. A body that is
  not valid JSON is treated as unreadable (`body.unreadable_body`).

**Known behaviour and limits of flatten mode:**

- A run only ever writes the columns of the flatten registry it loaded from its **input**
  `state.json`, plus the reserved `body_unmapped` column. A JSON key seen for the **first time**
  in a run is not yet a column: its value goes into `body_unmapped` (a compact JSON object keyed by
  the column name it has been assigned), and it becomes a real column starting with the **next**
  run — a one-run column lag that applies after the first-ever run, after any state reset, and
  after any run whose state was not saved (e.g. because the job failed after its rows were
  uploaded, or the table import failed).
- Column names are capped at 64 characters (longer paths are hashed) and the registry is capped at
  1,000 columns per row; past the cap, new keys still land in `body_unmapped` under a name that is
  not saved, the run's already-written rows are kept, and only then does it fail — switch to `text`
  format if you expect to exceed the cap.
- The metadata column `body_type` shares the `body_` prefix with flattened keys, so a JSON key
  named `type` is renamed to `body_type_2` to avoid a collision (every metadata column name is
  reserved in the registry).
- Resetting state, or letting several flatten rows share one output table, can make an import miss
  a column the table already has and fail the whole run's output. Give each flatten row its own
  table, drop the table when you reset its state, and prefer `defer_commit` when using flatten mode
  so a failed import never loses messages.

Failure safety (`write_always`)
---------------------------------

A run's output table is only reliably uploaded on a **successful** job. To limit what a failed job
loses after it has already started deleting messages, the manifest is written with
`write_always: false` and switched to `true` at the first destructive settle in `complete` and
`receive_and_delete` mode only:

| Mode | `write_always` | Job fails before any delete | Job fails after ≥ 1 deleted batch |
|---|---|---|---|
| `complete` | armed just before the first `complete_message` | nothing uploaded, table unchanged | rows written so far are uploaded (incremental: upserted; full load: the table becomes this run's delta) |
| `defer_commit` | never | nothing uploaded, table unchanged | nothing uploaded, table unchanged — the deferrals become orphans recovered by the next successful run on the same entity |
| `receive_and_delete` | armed as soon as a receive call returns messages | nothing uploaded, table unchanged | rows written so far are uploaded |
| `peek` | never | nothing uploaded, table unchanged; the next run re-peeks from the saved cursor | n/a — nothing is ever deleted |

**Legacy job queue:** projects still on the legacy job queue do not support `write_always` at all.
On that queue, a `complete` or `receive_and_delete` run logs a WARNING that this safety net is
unavailable — a job that fails after deleting messages uploads nothing and those messages are
lost. Use `defer_commit` for failure-safe consumption on such projects.

State
=====

Each row keeps its own `state.json`: the `defer_commit` pending-commit set (grouped by entity /
session / partition, as sequence-number ranges), the Peek-mode cursor, and the JSON-flatten column
registry. State is written once, at the end of the run, and only becomes visible to the next run
if that row's job succeeds and its table import succeeds; a failing row never discards another
row's state. The whole state is kept under 256 KiB; an
unrecognised state version fails the run rather than silently reinterpreting it (reset the state
to recover).

Sync actions
============

- **Test Connection** (configuration) — lists the namespace's queues to verify the credentials: a
  service principal, or a connection string with **Manage** rights. A Listen-only connection string
  cannot be tested here; use Preview Messages in a row.
- **Preview Messages** (row) — peeks up to 10 messages and shows sequence number, enqueued time,
  message ID, subject, state, and the start of the decoded body. It proves that the credentials can
  read the row's entity. Nothing is settled (a session-enabled entity's session is held briefly).

The list-entity dropdowns (queues / topics / subscriptions) need a service principal or a
Manage-rights connection string; with a Listen-only connection string, type the entity name
instead.

The UI runs the actions with the configuration's pinned `runtime.tag` when one is set, otherwise
with the Developer Portal's default image tag.

Each action gives up after 20 seconds with a message saying that Azure Service Bus did not respond
in time: the platform stops UI actions after 30 seconds and would otherwise show only a generic
"Internal Server Error". A busy or throttled namespace — for example while a large extraction runs
on it — can take that long; try again a minute later. The first action after the configuration's
component version (image tag) changes can also take long enough to end with "Internal Server
Error" while the platform prepares the new version; clicking again works.

Limitations
===========

- **`defer_commit` requires exclusive consumption** of the entity: no other application may defer
  messages there, or its deferrals look like this configuration's own orphans and get extracted and
  deleted.
- Orphan recovery after a failed `defer_commit` run is best-effort on partitioned entities and
  sub-queues, and impossible on session entities (deferrals stay pending until recovered manually).
- AMQP over WebSockets is not supported (see below); the extractor connects over AMQP/TLS 5671
  only.

Future: AMQP over WebSockets
=============================

Not implemented. It would only help environments that block outbound port 5671 — it does not help
with IP firewalls or private endpoints, both of which still require allowlisting or public access.
If you need it, open a feature request.

Development
===========

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired
path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository and set up the environment with [uv](https://docs.astral.sh/uv/):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-ex-azure-service-bus component-ex-azure-service-bus
cd component-ex-azure-service-bus
uv sync --all-groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite, lint, and type checks:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run ty check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Service Bus speaks AMQP, which HTTP cassette recorders (VCR) cannot capture, so every test runs
offline against an in-repo SDK double instead of a live namespace:

- `tests/fakes/broker.py` — `FakeBroker`, a stand-in for `azure-servicebus` that models queues,
  subscriptions, dead-letter sub-queues, sessions, partitions, locks, deferral and the management
  API; `tests/fakes/test_broker.py` checks the double against the broker behaviour it models.
- `tests/unit/` — one test module per source module.
- `tests/functional/` — the datadir suite: it runs `src/component.py` end to end on the configs in
  `tests/setup/configs.json` (dummy credentials only) against the fake broker and checks the output
  tables, manifests, state and exit codes, including every sync action; `test_sanitisation.py`
  fails if a committed fixture carries anything but the dummy secrets.

Or run the same checks the CI image runs, in Docker:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose build
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
============

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
