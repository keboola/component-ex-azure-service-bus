# keboola.ex-azure-service-bus — Design Spec

> Type: extractor
> Component ID: keboola.ex-azure-service-bus
> Status: approved (2026-09-23 — see §15 for the approval record)
> Date: 2026-09-23
> Pair: `keboola.wr-azure-service-bus` (the writer; config style, auth UX, error mapping and the
> SDK-mock test harness are mirrored from it)

**Evidence labels** (used for every load-bearing fact): **[live]** = run against the CF test
namespace on 2026-09-23 during Phase-2 research; **[source]** = read from package or platform
source code (named); **[docs]** = Microsoft Learn / PyPI / Keboola documentation; **[inferred]** = a
reasoned read that is not proven — the implementation must not present it as fact, and each one is
either verified in Phase 7 or listed in §10.

---

## 1. Overview & source system

A Keboola **extractor** that receives messages from an **Azure Service Bus** queue or topic
subscription (or their dead-letter sub-queues) and writes them to a Keboola Storage table — one
config row per source entity, one output row per message, with the broker metadata as typed columns
and the body as text, base64 or flattened JSON columns.

- **Source system:** Azure Service Bus (Standard / Premium namespaces) via the official Python SDK
  `azure-servicebus` over AMQP 1.0. SDK docs:
  https://learn.microsoft.com/python/api/overview/azure/servicebus-readme · quotas:
  https://learn.microsoft.com/azure/service-bus-messaging/service-bus-quotas · deferral:
  https://learn.microsoft.com/azure/service-bus-messaging/message-deferral
- **Primary use case:** land event / command traffic that Azure producers publish to Service Bus in
  Keboola Storage, either **consuming** it (the extractor is the entity's consumer and removes what
  it exported) or **browsing** it (non-destructive peek, the entity is consumed by someone else).
- **Background:** a customer's in-house extractor proved the behavioural patterns worth keeping
  (bounded chunked receive → persist → settle, a "backlog as of job start" stop, capped connection
  recovery with a no-progress guard, dead-lettering of unreadable bodies after a retry on a fresh
  connection, delivery-count high-water mark, effective-settings log line). This component is written
  from scratch; nothing is copied. Patterns deliberately **not** carried over: direct Storage upload
  (Keboola output mapping is used instead — no `forward_token`), the deprecated `uamqp` transport, a
  management connection string, and a sizing-advisor script.

## 2. Keboola mapping

### 2.1 Platform facts that shape the design

- **State and output become durable only after the job succeeds [source: docker-bundle
  `Runner.php`, job-queue `JobHandler.php` / `JobRowHandler.php`].** `out/state.json` of run N is
  visible to run N+1 **iff** run N's container exited 0 **and** every output-mapping import of the
  job succeeded. A multi-row configuration runs as a container job with **one child job per row**
  [live, Phase 7], so this holds **per row**: each row has its own state, saved only if that row's
  own job succeeds and its imports succeed — another row failing does not discard it [inferred from
  the job structure, Phase-8 audit; supersedes the Phase-3 "all-or-nothing across the job" reading].
  Nothing the container does is durable in Storage before it exits, so "delete the message after it is in
  Storage" is impossible inside the container. This is the fact every settlement mode (§2.3) is
  measured against.
- **`write_always` tables are uploaded even when the job fails [source: `keboola/output-mapping`
  `src/TableLoader.php` — "If it is a failed job, we only want to upload if the table has
  write_always = true"; `src/Configuration/Table/Manifest.php` extends `BaseConfiguration`, which
  declares `write_always`, and the loader resolves the table configuration from the manifest plus
  the mapping].** The manifest is written with `write_always: false` and is **switched to `true`
  at the first destructive settle, in C1 and C3 only** — C1 just before its first
  `complete_message`, C3 as soon as a RECEIVE_AND_DELETE receive first **returns messages** (the
  broker has deleted them by then; they are armed before anything is written, and a C3 run that
  receives nothing never arms). It is never `true` in C2 (nothing is deleted before the next run's commit; a failed
  run's deferrals are recovered by the orphan scan) or in C4 (nothing is deleted). Without it, a
  C1/C3 run that fails after it has deleted messages would upload nothing and lose them; with it,
  the rows written so far still reach Storage — and a failed C2 / C4 run, or a C1 / C3 run that
  failed before deleting anything, never overwrites a `full_load` table with a partial or empty
  file. Per-mode × load-type behaviour: §6.9. (This corrects the Phase-2 research table, which
  listed C1 as at-least-once for container crashes — that only held for crashes *before* the first
  settle.)
- **A terminated, cancelled or timed-out job uploads nothing** [inferred: output mapping does not
  run for a terminated container]. `advanced.max_duration_seconds` (default 3000, Phase 8) keeps
  runs well inside the job timeout, which the platform does not pass to the component; the residual loss window is listed per mode in §2.3.

### 2.2 Constructs

- **Source entity → output table.** Each config row reads exactly one source entity (a queue, a
  topic subscription, or the dead-letter / transfer-dead-letter sub-queue of either) and writes one
  output table (§6.9). Table name derived from the entity unless overridden (G1).
- **Config rows (Tier A convention, applied):** *several independent entities → config rows, one
  row per source entity.* Root = auth (entered once); row = source, reading mode, limits, body
  handling, destination. Each row has its own `state.json` (C2 pending-commit set, C4 cursor,
  flattening column registry). **Rows run sequentially by default**; parallelism is opt-in and not
  enabled. The component always receives one merged `config.json` (root + row `parameters`).
- **Do not rely on row ordering for durability.** Each row runs as its own child job with its own
  state (§2.1); a row never reads another row's output or state, and parallel rows are opt-in, so
  nothing may assume row N's output is in Storage when row N+1 runs. Every row is self-contained.
- **Secrets → `#`-prefixed keys:** `#connection_string`, `#client_secret` (encrypted at rest as
  `KBC::ProjectSecure`; the container receives plaintext).
- **Sync actions:** `testConnection`, `listQueues`, `listTopics`, `listSubscriptions`,
  `previewMessages` (§5.4). Registered in the Developer Portal in Phase 6 (`entityInfo` was removed
  in Phase 8).
- **Output bucket:** the portal app gets `defaultBucket: true` (stage `in`) in Phase 6; manifests
  carry no `destination`, so tables land in `in.c-keboola-ex-azure-service-bus-<configId>` and the
  table name is the output file name.
- **No `forward_token`:** output goes through `/data/out/tables` + manifests (works on every stack).

### 2.3 Settlement modes and guarantees (the user-facing picker — maintainer decision)

`source.settlement_mode` is a user-facing picker. Default **C1** (maintainer decision).

| Mode (value) | How | Deleted from Service Bus when | Guarantee into Storage | Loss / residue windows |
|---|---|---|---|---|
| **C1 `complete`** (default) | PEEK_LOCK receive → write rows → `complete_message` per batch | right after the batch's rows are written locally | at-least-once for failures before a batch is settled; `write_always` is switched on just before the first complete, so the rows of already-settled batches are uploaded even if the run later fails | (a) a Storage import fails after the job (maintainer-accepted); (b) the job is terminated / cancelled / timed out; (c) a hard kill (OOM, SIGKILL) mid-write truncates the CSV so the import fails |
| **C2 `defer_commit`** | PEEK_LOCK receive → write rows → `defer_message`; the deferred sequence numbers go to `out/state.json`; the **next** run deletes them by sequence number (RECEIVE_AND_DELETE deferred receive) before it receives anything | at the start of the next run, whose input state proves the previous import succeeded | **at-least-once end-to-end** (PK deduplicates re-extraction), within the limits in §6.4 | orphans: deferrals of a failed / lost-state run are recovered by the C2 orphan scan — best-effort on partitioned entities and sub-queues, impossible on session entities (run WARNING); exclusive-consumer requirement |
| **C3 `receive_and_delete`** | RECEIVE_AND_DELETE receive → write rows | by the broker on delivery | **at-most-once**; `write_always` is switched on when the first receive returns messages | everything in C1 plus: a crash between receive and write loses that batch; the local receive buffer at close (drained best-effort) |
| **C4 `peek`** | `peek_messages` pages, nothing locked or settled | never | non-destructive; a failed run is re-exported next run | messages removed by other consumers or TTL before the peek are never seen; the entity grows unless someone else consumes it |

C5 (lock without settlement) is excluded (§4). Why C2 exists — the Kafka comparison
(`keboola.ex-kafka`, public Keboola repo [source]): Kafka's extractor never commits broker offsets;
start offsets come from `state.json`, so a failed import leaves state unadvanced and the next run
re-reads the same range — possible because a Kafka read is non-destructive. The Service Bus analog
of that read is C4, but a queue must eventually delete; **C2 is "state is the commit" for a queue**:
hide (defer) what was exported, delete it only once a later run sees the state that proves the
import succeeded.

### 2.4 Extraction modes (Load Type × Fetch Mode — `extraction-modes.md`)

- **Load Type** `destination.load_type`: `incremental_load` (default; manifest `incremental: true` +
  PK → upsert of redeliveries) or `full_load`. Always visible, never gated.
- **Fetch Mode** exists only for **C4** (`source.fetch_mode`, gated on `settlement_mode = peek`):
  - `incremental_fetch` (default) — sequence-number cursor in state; API evidence:
    `peek_messages(max_message_count≤250, sequence_number=cursor+1)` [live]. Allowed only where a
    single cursor is sound: non-partitioned, non-session, main entity (not a sub-queue). Refused
    with a `UserException` on partitioned entities (seq paging skips partitions: 28/32 seen
    [live]), on session entities (a run can skip sessions, so a global cursor would pass unseen
    messages; per-session cursors would grow state with the session count), and on sub-queues
    (dead-lettered messages keep their original, lower sequence numbers [live: 929 → 929]).
  - `full_fetch` — peek the whole entity from the start every run; stateless.
  - `date_window` is **not offered**: peek has no time filter; a time window would be a full scan
    with a client-side filter (= `full_fetch` + a downstream filter).
- **Destructive modes C1/C2/C3 have exactly one fetch behaviour — "consume"** (each message is
  delivered once, then removed or hidden; the entity itself is the cursor), so the Fetch Mode field
  is omitted for them per the convention ("only one mode possible → omit the field and say why").
  State holds no watermark for them (C2 holds the pending-commit set, §6.8).
- **Combinations (all valid, none blocked):** consume × `incremental_load` = **accumulate**
  (default); consume × `full_load` = **delta / staging table** holding only this run's messages —
  deliberate, for pipelines where a downstream transformation accumulates history; C4 `full_fetch` ×
  `full_load` = **mirror** of what the entity currently holds; C4 `full_fetch` × `incremental_load` =
  refresh-by-upsert (messages removed by other consumers stay in the table); C4 `incremental_fetch`
  × `incremental_load` = accumulate; C4 `incremental_fetch` × `full_load` = delta of newly peeked
  messages (deliberate staging table).

### 2.5 Dev branches (lead decision, Phase 7 — supersedes the original guard)

**Every mode runs in every branch; there is no automatic dev-branch guard.** The original design
refused destructive modes (C1/C2/C3) when `KBC_BRANCHID` was set, relying on the keboola-context
claim that the variable is absent on the default branch. That claim is **wrong on current stacks**
(storage branches / queue v2): the Queue sets `KBC_BRANCHID` for default-branch jobs too [live,
Phase 7: a default-branch job was refused; an environment dump in the default branch and in a dev
branch showed the same 12 `KBC_*` variables, differing only in the `KBC_BRANCHID` value]. No branch
type, name or default flag is exposed, and without `forward_token` (§2.2) the component cannot ask
the API which id is the default branch — so no reliable check exists, and the guard (with its hidden
`destructive_in_branch` override) was removed. A config that still carries the key keeps validating
(`extra="ignore"`).

Documented instead (README, `configuration_description.md`, the `settlement_mode` tooltip):
**a dev branch reads the same production entity**, so a destructive run in a branch consumes and
removes production messages, which then reach only the branch's table — use Peek or a separate test
entity in branches; project admins can enable the platform feature
`dev-branch-configuration-unsafe` to have the platform guard branch runs. A C2 run in a branch is a
*second consumer* of the production entity: its deferrals live in the branch's state, the production
config's orphan scan may recover them (re-extract into the production table and delete them on its
next run), **or** the branch's next run may delete them first.

## 3. Authentication, connection & provisioning

### 3.1 Auth methods (mirror the writer)

| # | Method | `auth_type` | Fields | Rights needed | Status |
|---|---|---|---|---|---|
| B1 | SAS connection string | `connection_string` (default) | `#connection_string` | **Listen** on the entity or namespace; the entity dropdowns and the root `testConnection` additionally need **Manage** | in scope, live-verified |
| B2 | Entra ID service principal (client secret) | `service_principal` | `tenant_id`, `client_id`, `#client_secret`, `fully_qualified_namespace` | RBAC **Azure Service Bus Data Receiver** — covers receive, peek, complete, abandon, defer, dead-letter, DLQ receive **and** listing entities / reading properties, counts and rules | in scope, live-verified |

- **SDK surface.** `ServiceBusClient.from_connection_string(conn_str, …)` or
  `ServiceBusClient(fully_qualified_namespace, credential=ClientSecretCredential(tenant, client,
  secret), …)` for the data plane (AMQP); `ServiceBusAdministrationClient` (same two constructors)
  for the management plane (HTTPS 443) used only by the dropdowns, the root `testConnection` and the
  optional metadata pre-checks (L1/L2). SAS Listen gets `401` on every management call [live]; a SAS policy
  with Manage can list entities [docs, not probed].
- **Transport is fixed: pyamqp over AMQP/TLS 5671.** No transport field; `uamqp_transport` is never
  set (deprecated since 7.14.2; `uamqp` 1.6.11 has no prebuilt cp314 wheel [docs: PyPI]).
- **SDK pin:** `azure-servicebus>=7.14.3,<7.15` (resolves 7.14.3, latest GA) and
  `azure-identity>=1.19,<2`. The 7.15.0b2 beta is not shipped. Upgrade path: move to 7.15.0 GA when it
  ships (awaited settlements, release-on-close, trailing-field padding) and re-run the Phase-2
  multi-frame matrix then.

### 3.2 Provisioning — steps the customer must do

| Customer setting | Effect on a Keboola job | Step / handling |
|---|---|---|
| Default namespace (public access, local auth on) | works | SAS: copy a Listen (or Send+Listen) policy's connection string from *Shared access policies*. SP: app registration + client secret + **Azure Service Bus Data Receiver** role on the namespace or entity (granting it needs Owner / User Access Administrator on that scope). |
| **IP firewall** ("Selected networks" + IP rules; Standard & Premium) [docs: ip-filtering] | rejected connections are reported "as unauthorized" with no mention of the IP rule — **indistinguishable from bad credentials** [docs] | allowlist the **Keboola stack's egress IP addresses** (per stack; https://help.keboola.com/components/ip-addresses — the list "can change in the future"). The auth-failure `UserException` names "credentials, rights, entity name **or IP firewall**" (J1). |
| **Public network access disabled / private endpoints only** (Premium) | not reachable from Keboola-hosted jobs [inferred: Keboola documents static egress IPs, not private connectivity into customer VNets] | unsupported — the customer must allow public access from the allowlisted IPs. |
| **`disableLocalAuth = true`** (all tiers) [docs] | SAS keys cannot produce tokens → connection-string auth fails | use the service-principal method (B2). |
| Network security perimeter [docs] | as IP firewall / private access | as above. |

- **Outbound AMQP 5671** from Keboola is verified on the GCP us-east4 stack (the writer's cf-dev jobs
  used the default transport) [live]; other stacks are **[inferred]** to allow it (§10).
- **Test environment (CF):** dedicated `ex-*` entities in the CF test namespace (the namespace name,
  tenant and service-principal ids are not recorded in this repo; Phase 7 obtains SAS keys with `az`
  and the service-principal secret from the writer's gitignored `secrets.json`): queues `ex-test-queue`, `ex-test-queue-session`
  (sessions), `ex-test-queue-large`, `ex-test-queue-partitioned`; topic `ex-test-topic` with
  subscriptions `ex-test-topic-sub` and `ex-test-topic-sub-session` (sessions); subscription
  `ex-test-sub` on the writer's test topic (writer → extractor round trip). The writer's own
  entities are never received from. Test SP has Data Sender + Data Receiver. **No blocker.**

### 3.3 The multi-frame receive defect — strategy (Phase-2 verdict)

The pyamqp reassembly code for multi-frame (> 64 KB at the broker's 65,536-byte max frame)
deliveries is byte-identical in 7.14.3 and 7.15.0b2 with no upstream fix [source], but ≈3,850
deliveries of 1–250 KB (≈3,400 multi-frame, 180 over the management link) came back byte-exact on
both versions in every receive mode tried [live]. The failure is condition-dependent and poisons a
link once it happens; the probable trigger is concurrent frame dispatch from SDK background threads
[inferred, not reproduced]. Strategy, all in scope:

1. **Thread-free receiver profile** for every receiver: `prefetch_count ≥ 1` (default 1) +
   **receiver-level** `keep_alive=0` (client-level `keep_alive` is not forwarded to receivers
   [source]; with `prefetch_count = 0` the SDK always starts a 5 s keep-alive thread [source + live]).
   Verified: no background AMQP thread, 152/152 multi-frame byte-exact [live]. No
   `AutoLockRenewer`. Every receive, peek, deferred receive, settlement and lock renewal is issued
   from the main thread.
2. **Short receive → write → settle cycle** (local disk writes are far inside the 60 s default lock);
   a main-thread `renew_message_lock()` before settling when a lock is near expiry (D10).
3. **Connection recycling** on any receive-path failure that is not a config / auth / entity error
   (J2): close receiver + client, open a fresh `ServiceBusClient` (new connection = clean
   reassembly buffer), continue. Capped, with a no-progress guard.
4. **Never write or settle an unreadable body as if it were processed** (C6/F7, §6.6).

## 4. Capability inventory & scope

Baseline = the Phase-2 capability inventory **rev 2 + rev 3 delta** (groups A–L). Every entry appears
below. Default posture is in scope; each **Excluded** row carries its reason and the maintainer's
sign-off (approval given 2026-09-23, §15 — this spec is the sign-off artifact). "[decided]"
= a maintainer decision from Phase 2 (rounds 1–2).

### A. Source entity types

| Capability | Verdict | Rationale / where |
|---|---|---|
| A1. Queue (`get_queue_receiver`) | **In scope** | `source.entity_type = queue`. |
| A2. Topic subscription (`get_subscription_receiver`) | **In scope** | `source.entity_type = subscription`. |
| A3. Dead-letter sub-queue of a queue or subscription | **In scope** | `source.sub_queue = dead_letter`. C2 works there (defer + commit via the sub-queue receiver [live]); C2 orphan recovery is best-effort (§6.4). |
| A4. Transfer-dead-letter sub-queue | **In scope** | `source.sub_queue = transfer_dead_letter`; same code path as A3. |
| A5a. Session-enabled entities — all sessions (`NEXT_AVAILABLE_SESSION` loop until timeout) | **In scope** | `source.session_enabled`; loop in §6.5. |
| A5b. Explicit list of session ids | **Excluded** — maintainer decision (round 1): out of v1. | Signed off 2026-09-23. |
| A6. Deferred messages by sequence number as a user-facing source | **Excluded** — niche (users rarely hold sequence numbers); the primitive is used internally by C2 (H2/H3). | Signed off 2026-09-23. |
| A7. Scheduled / deferred messages via peek (state column) | **In scope** within C4 | deferred messages are exported with `state` = `DEFERRED`; a scheduled message is skipped while pending activation (counted as `skipped_scheduled`) and exported once active, under the new sequence number activation assigns [live, Phase 7] (lead decision P4-7, §6.7); subscription peek does not show pending scheduled topic messages [live]. |
| A8. Partitioned entities | **In scope**, per mode | C1/C3 unaffected; C2 commits per partition + best-effort orphan scan + run WARNING [decided]; D4 watermark approximate + WARNING; C4 `incremental_fetch` refused (`full_fetch` allowed) (§6.7). |
| A9. Several sources per configuration | **In scope** | Config rows, one source per row (Tier A). |

### B. Authentication

| Capability | Verdict | Rationale / where |
|---|---|---|
| B1. SAS connection string (namespace- or entity-level; Listen) | **In scope (default)** | §3.1. |
| B2. Entra ID service principal, client secret (Data Receiver) | **In scope** | §3.1; the only option under `disableLocalAuth`. |
| B3. Managed identity | **Excluded** — locked decision: Keboola jobs do not run as an Azure identity, so MI cannot work (same as the writer). | Signed off (Phase-1 locked decision). |
| B4. Service principal with certificate (`ClientCertificateCredential`) | **Excluded** — no demand; client secret covers the SP method; addable as a third `auth_type` if asked. | Signed off 2026-09-23. |
| B5. `AzureSasCredential` / `AzureNamedKeyCredential` | **Excluded** — the connection string already covers SAS; a bare token/key adds a second SAS UX with no new capability. | Signed off 2026-09-23. |

### C. Read / settlement modes

| Capability | Verdict | Rationale / where |
|---|---|---|
| C1. PEEK_LOCK + complete in-container | **In scope — default** [decided] | §2.3, §6.5. |
| C2. PEEK_LOCK + defer → commit next run (state 2PC) | **In scope**, allowed on **all** entity types [decided] | §6.3–§6.4; limits stated in §6.4. |
| C3. RECEIVE_AND_DELETE | **In scope**, opt-in with warning [decided] | UI tooltip + run WARNING; buffer drained before close (§6.5). |
| C4. Peek / browse (+ `full_fetch`) | **In scope** [decided] | §6.7. |
| C5. PEEK_LOCK without settlement (dry run) | **Excluded** — C4 dominates it: same non-destructive result without burning delivery counts (S5 dead-letters at MaxDeliveryCount). | Signed off 2026-09-23. |
| C6. Unreadable-message disposition | **In scope** — default dead-letter `UnreadableBody` after a fresh-connection retry, bounded share [decided] | `body.unreadable_body`; §6.6. |
| C7. Drain a DLQ | **In scope** | A3/A4 + C1/C2/C3. |
| C8. `abandon_message` as an internal disposition | **In scope (internal)** | First-failure retry of an unreadable body and buffer drain at stop (§6.5–§6.6). |

### D. Stop conditions / run bounds / receiver profile

| Capability | Verdict | Rationale / where |
|---|---|---|
| D1. Max messages per run | **In scope** | `limits.max_messages` (0 = no limit). |
| D2. Max run duration | **In scope** | `advanced.max_duration_seconds` (default 3000; Phase 8 moved it from `limits` and lowered it from 3600, below the default one-hour job timeout). |
| D3. Idle timeout | **In scope** | `source.idle_timeout_seconds` (default 10; destructive modes). |
| D4. Watermark: backlog as of job start | **In scope** | `limits.stop_at_job_start` (default on); approximate + WARNING on partitioned entities (§6.5). |
| D5. Peek end-of-log | **In scope** | C4 stops on an empty page. |
| D6. Catch-up wait for locked stragglers after a recovery | **In scope** (bounded option) | `advanced.recovery_wait_seconds` (default 0 = leave them to the next run). |
| D7. Receive batch size | **In scope (advanced)** | `advanced.batch_size` (default 100). |
| D8. Prefetch count | **In scope (advanced)** | `advanced.prefetch_count` (default 1 = thread-free profile; larger → WARNING; refused with C3). |
| D9. AutoLockRenewer / max lock renewal duration | **Excluded** by design — renewal threads are implicated in the multi-frame defect (§3.3); the short cycle plus D10 make it unnecessary. | Signed off 2026-09-23. |
| D10. Manual `renew_message_lock()` / session `renew_lock()` from the main thread | **In scope (internal)** | Before settling when a lock is within 10 s of expiry. |
| D11. Receiver-level `keep_alive=0` | **In scope (internal default)** | Thread-free profile; not user-facing. |

### E. Output columns — message metadata (fixed schema, §6.9)

| Capability | Verdict | Column |
|---|---|---|
| E1. `message_id` | **In scope** | `message_id` STRING |
| E2. `sequence_number` (PK default [decided]) | **In scope** | `sequence_number` INTEGER |
| E3. `enqueued_sequence_number` | **In scope** | `enqueued_sequence_number` INTEGER |
| E4. `enqueued_time_utc` | **In scope** | `enqueued_time_utc` TIMESTAMP |
| E5. `content_type` | **In scope** | `content_type` STRING |
| E6. `correlation_id` | **In scope** | `correlation_id` STRING |
| E7. `subject` (label) | **In scope** | `subject` STRING |
| E8. `session_id` | **In scope** | `session_id` STRING |
| E9. `reply_to` | **In scope** | `reply_to` STRING |
| E10. `reply_to_session_id` | **In scope** | `reply_to_session_id` STRING |
| E11. `to` | **In scope** | `to_address` STRING (renamed: `to` is an SQL reserved word) |
| E12. `partition_key` | **In scope** | `partition_key` STRING |
| E13. `application_properties` | **In scope** | `application_properties` STRING (JSON; bytes decoded; AMQP timestamps arrive as epoch-ms integers [live]) |
| E14. `delivery_count` | **In scope** | `delivery_count` INTEGER |
| E15. `dead_letter_reason` | **In scope** | `dead_letter_reason` STRING |
| E16. `dead_letter_error_description` | **In scope** | `dead_letter_error_description` STRING |
| E17. `dead_letter_source` | **In scope** | `dead_letter_source` STRING |
| E18. `time_to_live` | **In scope** | `time_to_live_seconds` FLOAT |
| E19. `expires_at_utc` | **In scope** | `expires_at_utc` TIMESTAMP |
| E20. `scheduled_enqueue_time_utc` | **In scope** | `scheduled_enqueue_time_utc` TIMESTAMP |
| E21. `state` | **In scope** | `state` STRING (`ACTIVE` / `DEFERRED` / `SCHEDULED`), as the broker reports it (§6.9) |
| E22. `body_type` | **In scope** | `body_type` STRING (`DATA` / `VALUE` / `SEQUENCE`) |
| E23. AMQP message annotations | **In scope** | `message_annotations` STRING (JSON) |
| E24. AMQP header / properties extras | **In scope** | fixed scalar columns: `amqp_durable` BOOLEAN, `amqp_priority` INTEGER, `amqp_first_acquirer` BOOLEAN, `amqp_user_id` STRING, `amqp_content_encoding` STRING, `amqp_creation_time_utc` TIMESTAMP, `amqp_absolute_expiry_time_utc` TIMESTAMP, `amqp_group_sequence` INTEGER, `amqp_reply_to_group_id` STRING |
| E25. Extraction metadata | **In scope** | `source_entity` STRING (entity path incl. sub-queue), `settlement_mode` STRING, `extracted_at_utc` TIMESTAMP |
| `lock_token`, `locked_until_utc` | **Excluded** — runtime-only lock handles, meaningless after the run. | Signed off 2026-09-23. |

The column set is fixed (no per-column toggles): a stable schema is what keeps the Storage import from
failing (§6.9), and unused columns simply stay empty.

### F. Body handling (`body.body_format`, §6.10)

| Capability | Verdict | Rationale / where |
|---|---|---|
| F1. DATA → text (UTF-8, `errors='replace'`) | **In scope — default** | `body_format = text`. |
| F2. DATA → base64 | **In scope** | `body_format = base64`. |
| F3. VALUE / SEQUENCE → JSON (recursive bytes→str) | **In scope (automatic)** | by `body_type`. |
| F4. Multi-section DATA → concatenated | **In scope (automatic)** | `b"".join(sections)`. |
| F5. Charset from `content_type` | **In scope (automatic)** | `charset=` parameter; unknown charset → UTF-8. |
| F6. JSON body flattening | **In scope** [decided] | `body_format = json_flatten`: nested dot-path columns, arrays stay JSON strings, **no raw `body` column** [decided], plus the reserved JSON column `body_unmapped` holding keys first seen in the current run — they become columns from the next run [gate lead decisions]; naming, drift and typing rules in §6.10. |
| F7. Unreadable-body policy | **In scope** | `body.unreadable_body` = `dead_letter` (default) / `leave` / `fail`; also covers non-JSON bodies under F6 and oversize cells (§6.6). |
| F8. Large bodies (Premium up to 100 MB) | **In scope** | memory bound = batch × body size (lower `advanced.batch_size` for large bodies — README guidance); a body whose output cell would exceed 16 MiB goes through the unreadable policy with reason `BodyTooLarge` (§6.6). |

### G. Output table / load behaviour

| Capability | Verdict | Rationale / where |
|---|---|---|
| G1. One output table per row, default-bucket naming, configurable name | **In scope** | `destination.table_name` (empty = derived, §6.9). |
| G2. Primary key — default `sequence_number` [decided]; alternatives | **In scope** | `destination.primary_key` = `sequence_number` / `message_id` / `source_entity_sequence_number` (composite). Why a picker although a broker id exists: `sequence_number` is unique only per entity, so rows of several entities sharing one table need the composite key, and producers that set stable business ids (e.g. for broker duplicate detection) may want one row per `message_id` across re-sends. `message_id` is producer-set — it may be empty or reused, in which case upserts merge different messages; the UI description says so. |
| G3. Load type full / incremental | **In scope** | `destination.load_type` (§2.4). |
| G4. Native-types `schema` manifest | **In scope** | broker-typed columns typed (§6.9); portal `dataTypeSupport = authoritative` in Phase 6. |
| G5. Empty run behaviour | **In scope** | decided: always write the table (header-only CSV + manifest) and succeed. |

### H. State / incremental bookkeeping (§6.8)

| Capability | Verdict | Rationale / where |
|---|---|---|
| H1. Peek cursor (C4 `incremental_fetch`) | **In scope** | `peek_cursor` in state. |
| H2. Pending-commit set (C2) | **In scope** | keyed by entity (incl. sub-queue), `session_id`, partition id, with `max_body_bytes` per group, sequence numbers stored as ranges (§6.3). |
| H3. Orphan scan — C2 only | **In scope** | §6.4. |
| H4. Session state (`get_state()`) as an output column | **Excluded** — application-private data, the extractor must never `set_state`; no demand. | Signed off 2026-09-23. |
| H5. Own pending-set commit at the start of every destructive mode | **In scope** | C1/C2/C3 commit only *this config's* stored pending set (§6.3); no orphan scan outside C2. |

### I. Sync actions (§5.4)

| Capability | Verdict | Rationale / where |
|---|---|---|
| I1. `testConnection` | **In scope** | root button only: management probe (Phase 8 — the row button, which peeked one message, was removed; a row's Preview Messages proves entity access). |
| I2. Entity dropdowns (queues / topics / subscriptions) | **In scope** | `listQueues` / `listTopics` / `listSubscriptions`: work with SP (Data Receiver) and with a Manage SAS; a Listen-only SAS gets an empty list and types the name (creatable select). |
| I3. Preview messages (peek N) | **In scope** | `previewMessages` (10 messages, non-destructive). |
| I4. Entity info | **Removed (Phase 8, maintainer decision)** | the `entityInfo` action and its **Show Entity Details** button: low value — a run already fails clearly on a session mismatch and logs the counts when management reads are allowed. |
| I5. Session list dropdown | **Excluded** — needs 7.15+ `list_*_sessions`, and it only serves A5b (excluded). | Signed off 2026-09-23. |

### J. Error handling / robustness (§6.11)

| Capability | Verdict | Where |
|---|---|---|
| J1. `UserException` mapping (incl. auth / entity missing / IP firewall ambiguity) | **In scope** | §6.11. |
| J2. Connection recycling, capped, no-progress guard | **In scope** | §6.5. |
| J3. SDK retry options; `retry_total=0` on a dedicated commit client + component-level bounded retry | **In scope** (fixed, not user-facing) | §6.3. |
| J4. Throttling (ServerBusy) via SDK retry | **In scope** | main client keeps SDK defaults; commit client retries transient errors itself (§6.3). |
| J5. Settlement-failure tolerance + count | **In scope** | lock-lost settles counted, WARNING, never fatal. |
| J6. Partitioned-entity detection / refusal for unsound modes | **In scope** | §6.7. |
| J7. Secret redaction; `azure.*` logger level control | **In scope** | §6.12. |
| J8. Delivery-count high-water mark in the run summary | **In scope** | §6.12. |
| J9. Run summary log | **In scope** | §6.12. |
| J10. Refuse unsafe combinations up front | **In scope** | refused: C3 + prefetch > 1; C4 `incremental_fetch` on partitioned / session / sub-queue entities. C2 is never refused — WARNING where orphan recovery is best-effort / impossible [decided]. |
| J11. Dev-branch guard (`KBC_BRANCHID`) | **Excluded** (Phase-7 lead decision) — the platform gives no dev-branch signal (`KBC_BRANCHID` is set on default-branch jobs too); documented instead | §2.5. |

### K. Transport / client options

| Capability | Verdict | Rationale / where |
|---|---|---|
| K1. AMQP over TCP 5671 | **In scope** | the only transport. |
| K2. AMQP over WebSockets 443 | **Excluded — no code path** [decided, round 2]; §11 documents how it could be added. | Signed off 2026-09-23. |
| K3. Custom endpoint / HTTP proxy | **Excluded** — no demand; an HTTP proxy forces the WebSocket transport (K2, excluded). | Signed off 2026-09-23. |
| K4. uamqp transport | **Excluded** — no prebuilt cp314 wheel; deprecated since 7.14.2. | Signed off 2026-09-23. |
| K5. `auto_reconnect` | **In scope (internal)** | SDK default `True` kept deliberately; no user field. |
| K6. `socket_timeout` as an option | **Excluded** as a user option — no user value; the SDK default applies. | Signed off 2026-09-23. |
| K7. `connection_verify` / `ssl_context` | **Excluded** — no demand; Service Bus endpoints use public CAs. | Signed off 2026-09-23. |
| K8. `user_agent` / `client_identifier` | **In scope (internal)** | `user_agent="keboola.ex-azure-service-bus"`; per-row `client_identifier` (§6.12). |

### L. Management-derived data (management plane)

| Capability | Verdict | Rationale / where |
|---|---|---|
| L1. Entity runtime counts in the log | **In scope** | INFO line at run start when the credentials allow management reads; silently skipped otherwise. |
| L2. Entity metadata pre-checks | **In scope** | `requires_session` vs `session_enabled`, `enable_partitioning`, `lock_duration`, `max_delivery_count` (§6.7); heuristics when unavailable. |
| L3. Subscription rules / filters as data | **Excluded** — configuration metadata, not message data (no longer shown anywhere since `entityInfo` was removed, I4). | Signed off 2026-09-23. |
| L4. Service batch delete | **Excluded** — deletes by message count + enqueue-time cutoff (not by sequence number), unsupported on partitioned entities, locked messages ineligible, and absent from the Python SDK (7.14.3 / 7.15.0b2) [docs + source] — it could not target a C2 pending set. | Signed off 2026-09-23. |

**Totals:** 106 inventory rows — **89 in scope, 17 excluded** (A5b, A6, B3, B4, B5, C5, D9, lock
handles, H4, I5, K2, K3, K4, K6, K7, L3, L4).

**Mechanics (no REST pagination):** "pages" are receive batches (`max_message_count`, no 250 cap on
receive [live: 300 in one call]), peek pages (hard cap 250 per call [live]) and deferred-receive
calls (RECEIVE_AND_DELETE cap 250 per call — 251+ rejected with nothing deleted [live]). Rate limits:
Standard 1,000 credits/s per namespace (1 per received or peeked message, 10 per management op); the
SDK retries ServerBusy (50009) [docs]. No bulk/async export exists.

## 5. Configuration & schema

Layout: **config rows**. The actual `configSchema.json` / `configRowSchema.json` (with
`options.dependencies`, async selects and buttons) is built by `component-build-ui`; fields are
described here, not written as JSON.

### 5.1 Root (config-level) parameters — the auth block (identical to the writer)

- `auth_type` — enum, required: `connection_string` (default) · `service_principal`.
- `#connection_string` — secret, required when `auth_type = connection_string`.
- `tenant_id`, `client_id`, `#client_secret`, `fully_qualified_namespace` — required when
  `auth_type = service_principal` (`#client_secret` secret).
- A **Test Connection** button (`format: test-connection` → `testConnection`), because
  `genericDockerUI(-rows)` does not auto-render one for a registered sync action (writer lesson).

### 5.2 Row parameters

**`source`** — what to read and how (grid layout):
- `entity_type` — enum, required, no default: `queue` · `subscription`.
- `queue_name` — required iff `entity_type = queue` (creatable async select `listQueues`).
- `topic_name`, `subscription_name` — required iff `entity_type = subscription` (creatable async
  selects `listTopics`, `listSubscriptions` — the latter reloads when `topic_name` changes).
- `sub_queue` — enum, default `none`: `none` · `dead_letter` · `transfer_dead_letter`.
- `session_enabled` — boolean, default `false`; shown only when `sub_queue = none` (sub-queues never
  require sessions [docs]); must match the entity's `requires_session`.
- `settlement_mode` — enum, default `complete`: `complete` · `defer_commit` ·
  `receive_and_delete` · `peek` (§2.3).
- `fetch_mode` — enum, default `incremental_fetch`: `incremental_fetch` · `full_fetch`; shown only
  when `settlement_mode = peek`.
- `idle_timeout_seconds` — integer 1–300, default 10; shown only for the destructive modes (one
  receive call waits this long; an empty result is verified by a peek before the run stops, §6.5).
- Button: **Preview Messages** (`previewMessages`) — the row's access check. (Phase 8: the row's
  **Test Connection** and **Show Entity Details** buttons were removed; Test Connection is a root
  button only.)

**`limits`** — run bounds (all modes):
- `max_messages` — integer ≥ 0, default `0` (= no limit).
- `stop_at_job_start` — boolean, default `true` (D4 watermark).

**`body`**:
- `body_format` — enum, default `text`: `text` · `base64` · `json_flatten`.
- `unreadable_body` — enum, default `dead_letter`: `dead_letter` · `leave` · `fail`.

**`destination`**:
- `table_name` — string, default `""` (empty = derived from the entity, §6.9).
- `load_type` — enum, default `incremental_load`: `incremental_load` · `full_load`.
- `primary_key` — enum, default `sequence_number`: `sequence_number` · `message_id` ·
  `source_entity_sequence_number`.

**`advanced_options`** — boolean, default `false`; gates the **`advanced`** section:
- `max_duration_seconds` — integer 60–43,200, default 3000 (Phase 8: moved from `limits`, was 3600).
  The platform does not pass the job timeout to the component, so the default stays below the
  default one-hour job timeout and leaves time for the import; the tooltip says to keep it well
  below the configuration's job timeout, since a job killed by the timeout may import nothing.
- `batch_size` — integer 1–5,000, default 100.
- `prefetch_count` — integer 1–1,000, default 1.
- `recovery_wait_seconds` — integer 0–330, default 0.

No hidden parameters: the former `destructive_in_branch` override was removed with the dev-branch
guard (§2.5); a leftover key is ignored.

### 5.3 Validation rules (Pydantic `model_validator`s, raised as `UserException` exit 1)

- Auth fields required per `auth_type` (writer's `_validate_auth`, unchanged).
- `queue_name` required for `queue`; `topic_name` + `subscription_name` required for
  `subscription`; the names of the *other* entity type are nulled (tolerates leftovers from
  switching in the UI, like the writer's `column`).
- `fetch_mode` is reset to its default unless `settlement_mode = peek`; `idle_timeout_seconds` is
  ignored in peek mode; `session_enabled` is forced `false` when `sub_queue ≠ none`; `advanced`
  values are ignored (model defaults apply) unless `advanced_options` is `true`.
- Refused combinations (J10): `receive_and_delete` with `prefetch_count > 1` (the local buffer is
  already deleted on the broker); `peek` + `incremental_fetch` with `session_enabled` or with a
  sub-queue (use `full_fetch`). The partitioned refusal is a runtime check (§6.7).
- `destination.table_name`, when set, must match the Storage rule (alphanumerics, `-`, `_`; not
  starting or ending with `-` / `_` [docs: help.keboola.com/storage/tables]) — else `UserException`
  (an explicit name is never silently rewritten).

### 5.4 Sync actions

| Action | Context | Behaviour | Failure |
|---|---|---|---|
| `testConnection` | root button (a row `source`, if sent, is ignored) | management `list_queues` (first item) — proves SP / Manage-SAS auth; a Listen-only SAS (401) gets a failure explaining that it cannot be tested there and that a row's Preview Messages checks Listen access | `UserException` |
| `listQueues`, `listTopics`, `listSubscriptions` | row | management listing → `[{value, label}]` sorted by name; **SAS 401 → empty list** (the creatable select lets the user type the name); SP errors → `UserException` | missing topic → `UserException` |
| `previewMessages` | row | peek up to 10 messages; returns a markdown table (sequence number, enqueued time, message id, subject, state, first 120 characters of the text-decoded body); nothing locked or settled (session caveat as above) | mapped `UserException` |

- Image tag [live, Phase 8 — corrects the earlier writer lesson]: the UI sends the configuration's
  `runtime.tag` as the request's `tag`, so the buttons run a pinned branch build; a call without a
  `tag` (the Sync Actions API used directly, e.g. `kbagent component sync-action`) runs the Developer
  Portal default tag — for an unreleased component the `0.0.1` bootstrap, which answers every action
  with HTTP 500.
- **Deadline (Phase 8):** the platform stops a sync action after 30 seconds (image pull excluded) and
  then answers a generic HTTP 500 "Internal Server Error" instead of the action's message [docs]. The
  SDK's own retries (three, exponential backoff, 60-second operation and auth timeouts) can outlast
  that on a busy, throttled or unreachable namespace [live: a concurrent drain of the same
  Standard-tier namespace slowed one `testConnection` from ~1.5 s to 8–9 s; against a broker that
  accepts TCP but never answers, `previewMessages` ran 181 s]. Every action therefore runs in a
  daemon worker thread and gives up after `SYNC_ACTION_DEADLINE_SECONDS` = 20 with a `UserException`
  ("Azure Service Bus did not respond within 20 seconds … busy or throttled … or unreachable. Try
  again in a minute."), leaving the container start-up and the reply their share of the 30 seconds.
  The deadline cannot cover the platform's own start-up: the first action after a configuration is
  pinned to a new image tag waits for that image on the sync-action worker [live, Phase 8: the first
  root `testConnection` on a fresh `-31` tag answered HTTP 500 after 46 s, a retry answered the
  action's own message in seconds; the first call on another unused tag took 10.6 s] — the README
  says to retry once.
  The session peek of `previewMessages` opens its `NEXT_AVAILABLE_SESSION` receiver on a
  `retry_total=0` client: the SDK retries a timed-out session accept three times with backoff, so
  an entity without an available session took ~34 s instead of the 5-second accept wait [live] —
  over the platform limit on its own; now ~6 s and "no messages to preview".
- Row-level sync actions receive the root `parameters` merged with the row's [inferred — verified in
  Phase 6/7; the actions only need the fields they read, via partial models (§7)].
- **Each action validates only what it reads (Phase 8):** the list actions and `testConnection`
  validate the auth block (plus, for `listSubscriptions`, the selected topic); `previewMessages`
  validates auth + `source` (`EntityConfiguration`, with the run's source normalisation) and ignores
  every other section, so a half-edited unrelated field (an invalid table name, a batch size out of
  range) never blocks it.
  Only `run` validates the whole row. The auth block is parsed inside the action (not in
  `Component.__init__`), so its validation error reaches the UI through the sync-action error path
  (stderr, exit 1).

### 5.5 UI scope & config shape

| Field | Level | Required | User-facing or internal | Default | In a NEW config's saved params? |
|---|---|---|---|---|---|
| `auth_type` | root | yes | user-facing enum | `connection_string` | yes — visible explicit choice |
| `#connection_string` | root | iff SAS | user-facing secret | — | only when the SAS branch is active |
| `tenant_id` / `client_id` / `#client_secret` / `fully_qualified_namespace` | root | iff SP | user-facing (`#client_secret` secret) | — | only when the SP branch is active |
| `source.entity_type` | row | yes | user-facing enum | none | yes — the user's explicit choice (no silent default) |
| `source.queue_name` | row | iff queue | user-facing creatable select | — | only when `entity_type = queue` |
| `source.topic_name` / `subscription_name` | row | iff subscription | user-facing creatable selects | — | only when `entity_type = subscription` |
| `source.sub_queue` | row | no | user-facing enum | `none` | yes — visible, self-explanatory |
| `source.session_enabled` | row | no | user-facing checkbox, gated `sub_queue = none` | `false` | only while visible (`sub_queue = none`) |
| `source.settlement_mode` | row | no | user-facing enum | `complete` | yes — visible; the maintainer-chosen default |
| `source.fetch_mode` | row | no | user-facing enum, gated `settlement_mode = peek` | `incremental_fetch` | only in peek mode |
| `source.idle_timeout_seconds` | row | no | user-facing integer, gated on destructive modes | 10 | only in destructive modes |
| `limits.max_messages` | row | no | user-facing integer | 0 (no limit) | yes — visible, description says "0 = no limit" |
| `limits.stop_at_job_start` | row | no | user-facing checkbox | `true` | yes — visible |
| `body.body_format` | row | no | user-facing enum | `text` | yes — visible |
| `body.unreadable_body` | row | no | user-facing enum | `dead_letter` | yes — visible |
| `destination.table_name` | row | no | user-facing text | `""` | yes — visible, "empty = derived" |
| `destination.load_type` | row | no | user-facing enum | `incremental_load` | yes — visible |
| `destination.primary_key` | row | no | user-facing enum | `sequence_number` | yes — visible |
| `advanced_options` | row | no | user-facing checkbox | `false` | yes — visible |
| `advanced.max_duration_seconds` / `batch_size` / `prefetch_count` / `recovery_wait_seconds` | row | no | user-facing, gated `advanced_options = true` | 3000 / 100 / 1 / 0 | only once `advanced_options` is on |
| transport, receiver `keep_alive=0`, SDK retry, recovery cap, orphan-scan K and page cap, commit byte cap, state budget, unreadable abort share | — | — | **internal constants — not in any schema** | §6 | never |

Gated fields rely on the generic UI dropping values of fields whose `options.dependencies` are
unmet (writer precedent, passed its Phase-7 fresh-config gate); the model defaults them at runtime.

### 5.6 UI presentation

| Field | Widget | Notes |
|---|---|---|
| `auth_type`, `entity_type`, `sub_queue`, `settlement_mode`, `fetch_mode`, `body_format`, `unreadable_body`, `load_type`, `primary_key` | `enum` + `enum_titles` | stores the value; titles e.g. "Delete After Batch Is Written (Default)", "Delete On Next Successful Run (Safest)", "Delete On Receive (At Most Once)", "Peek Only (Never Delete)" |
| `settlement_mode` tooltip | markdown tooltip | one line per mode with its guarantee and loss window (§2.3); C3 warns explicitly; C2 names the exclusive-consumer rule |
| `queue_name`, `topic_name`, `subscription_name` | creatable async `select`, `autoload` | queues/topics autoload (`"autoload": []`); subscriptions autoload watching `topic_name` (`["parameters.source.topic_name"]`) — the UI autoloads only an **array** (`[]` on open, a path list once those fields are set; a boolean `true` never autoloads) [source: keboola/ui json-editor `helpers.ts` `shouldAutoload`, Phase 8]; tooltip: "listing needs a service principal or a Manage connection string — with a Listen-only connection string type the name" |
| `#connection_string`, `#client_secret` | password | |
| `session_enabled`, `stop_at_job_start`, `advanced_options` | checkbox | |
| integers | number input with `minimum` / `maximum` | |
| `table_name` | text | placeholder = derived name pattern |
| buttons | `format: test-connection` (root only), sync-action button **Preview Messages** (row) | Title Case labels |
| sections | `source` (`grid-strict`, 2 per row), `limits`, `body`, `destination`, `advanced` — named `type: object` sections | |

Recurring review catches, pre-decided: two separate pickers for Load Type and Fetch Mode (Fetch Mode
exists only for peek, §2.4); `load_type` is never gated; per-row `table_name` override present; every
`enum` stores the value; **tooltips on `body_format` and `primary_key` warn that changing them
changes the output columns / key, so the existing table must be dropped first**; tooltips use
backticks around placeholder tokens; the PK is chosen from a fixed enum of keys (the columns are
fixed), not a free-text column list, and the `message_id` option's description warns: "Producer-set:
may be empty or reused, in which case upserts merge different messages." (§4-G2 says why the picker
exists at all.)

## 6. Runtime design

### 6.1 Run sequence (destructive modes C1/C2/C3)

1. Parse and validate the config (every mode runs in every branch — §2.5).
2. Load the row state (§6.8); T0 = now (UTC) for the D4 watermark.
3. Optional metadata pre-check + counts log (L1/L2) via the management client; any management
   failure → DEBUG log + heuristics (§6.7).
4. Open the output table and write its manifest with `write_always: false` (§6.9) — before anything
   is settled. C1 switches it to `true` just before its first complete, C3 when its first receive
   returns messages (§6.9).
5. **H5 — commit this config's stored pending set** (§6.3). Runs before any receive. A failure here
   fails the run with nothing extracted and the input state intact (next run retries).
6. **C2 only — H3 orphan scan** (§6.4). **C1/C3 — foreign-deferral probe** (§6.4, detect-only).
7. Receive loop (§6.5) with per-mode settlement.
8. Close the output (flush + close, also on failure — §6.9), write the new state (§6.8),
   log the run summary (§6.12).

C4 runs 1–4, then the peek loop (§6.7) and 8. C4 never commits or deletes anything; a pending set
left in state by an earlier C2 period is **carried forward untouched** with a WARNING ("N messages
deferred by an earlier defer-commit run are still pending; they are deleted by the next run in a
destructive mode").

### 6.2 Receiver profile (every receiver)

`get_queue_receiver` / `get_subscription_receiver` with `sub_queue` (`ServiceBusSubQueue`),
`session_id` (`NEXT_AVAILABLE_SESSION` or a stored id), `receive_mode` (PEEK_LOCK for C1/C2/C4 and
recovery, RECEIVE_AND_DELETE for C3 and the commit), `prefetch_count=advanced.prefetch_count`
(≥ 1), **`keep_alive=0`**, `client_identifier`. Two `ServiceBusClient`s: the **receive client**
(SDK default retries) and, created lazily only when a deferred receive is needed, the **commit
client** with `retry_total=0` (client level — `get_*_receiver(retry_total=…)` raises `TypeError` on
7.14.3 because the client already forwards its own [live + source]).

### 6.3 H2 / H5 — pending set and commit

- **What is stored (C2, at defer time):** for every deferred message: its entity (entity type,
  names, sub-queue), `session_id`, partition id (`sequence_number >> 48` [live]) and body size. Stored
  as groups keyed by (entity, `session_id`, partition) with the **maximum body size in the group**
  and the sequence numbers as sorted **inclusive ranges** (consecutive deferrals collapse to one
  range) — see §6.8.
- **Commit (H5, start of every destructive mode):** for each stored group, open a
  RECEIVE_AND_DELETE receiver on the **commit client** for that entity / sub-queue / session and call
  `receive_deferred_messages(chunk, timeout=60)` — one partition per call (mixed partitions are
  rejected [live]), **≤ 250 sequence numbers per call** (broker limit [live]) **and ≤ 16 MiB per
  call**: `n = clamp(floor(16 MiB / max_body_bytes), 1, 250)` (largest verified call: 14.5 MB, 1.56 s
  [live]; single bodies > 16 MiB go one per call — unverified above 16 MB, Premium only). The
  chunks are generated lazily from the stored ranges, one at a time — a range can cover millions of
  sequence numbers, so it is never expanded into a list (nor when a group is carried forward). The
  returned messages are discarded (their rows were imported by the run that deferred them).
- **Not found:** a `MessageNotFoundError` fails the whole call [live] → bisect the chunk; a sequence
  number that fails alone is treated as already committed (settled earlier, or expired — a deferred
  message's TTL is checked only when it is received by sequence number [docs + live]).
- **Transient errors (J3/J4, carried item 3):** the commit client has no SDK retries, so the
  component retries `ServiceBusServerBusyError`, `ServiceBusConnectionError`,
  `ServiceBusCommunicationError` and `OperationTimeoutError` itself — each call is retried up to 3
  times (2 s / 4 s / 8 s backoff). If they are exhausted the run fails with a `UserException` ("could not
  delete the N messages deferred by the previous run; nothing was extracted; the next run retries")
  — **failing is safe** because H5 runs before any receive and the input state is untouched.
- **Session locked by another receiver** (`SessionCannotBeLockedError`) after the retries → that
  group is **carried forward** into the new pending set with a WARNING; the run continues.
- **Stored entity ≠ configured entity** (the user changed the row's source): the stored groups are
  still committed against their stored entity; if that entity is gone or no longer authorised
  (`MessagingEntityNotFoundError` / auth error) → WARNING naming the count ("left DEFERRED on
  `<entity>` — recover them with a defer-commit row on that entity") and the groups are dropped.
- After H5 the committed groups are removed; C1/C3 then never defer, so their out-state carries no
  pending set (a C2 → C1/C3 switch leaves nothing behind).

### 6.4 H3 — orphan scan (C2 only) and the foreign-deferral probe (C1/C3)

**Where orphans come from** (deferred messages never expire and are invisible to receive [docs +
live]): any C2 run whose state is lost or never written — the job failed anywhere incl. the Storage
import (the case C2 exists for); the config was deleted or its state reset; a **failed final C2 run
followed by a switch to C1/C3/C4** (that run's deferrals are in no state, and no scan runs outside
C2); a C2 run in a dev branch (branch-scoped state, §2.5); two concurrent jobs
of one config (last state writer wins [keboola-context: config-rows]).

**H3 on plain entities** (non-partitioned queue or subscription, no sessions, not a sub-queue),
after H5, on the main thread:
1. Peek from sequence 1 in pages of 250 (explicit `sequence_number`).
2. Every `DEFERRED` message is an orphan (H5 already deleted this config's stored set): receive it
   by sequence number in PEEK_LOCK on the commit client (grouped, chunked and bisected as in §6.3) →
   write its row → `defer_message` → add to the new pending set. If `defer_message` fails (e.g. the
   upstream lock-token issue #42454, not reproduced on 7.14.3 [live]), the message stays `DEFERRED`
   (a lapsed lock leaves a deferred-received message `DEFERRED` [live]) and is re-extracted next
   run — duplicate rows are deduplicated by the PK.
3. Skip without counting toward the stop rule: `SCHEDULED`; `ACTIVE` with `delivery_count ≥ 1`
   (redelivered stragglers); `ACTIVE` past `expires_at_utc` (expired, not yet purged — peeks as
   `ACTIVE`, `delivery_count 0` [live]); `ACTIVE` carrying a `scheduled_enqueue_time_utc`
   (an activated scheduled message keeps that property [live, Phase 7]; on 7.14.3 it can even
   report `SCHEDULED`, skipped above).
4. **Stop after K consecutive qualifying messages** (`ACTIVE`, `delivery_count == 0`, not expired,
   never scheduled) with no `DEFERRED` among them, or at the end of the entity.
   **K = max(100, 2 × (batch_size + prefetch_count + 1))** (carried item 2). Why this bound: a
   currently-locked message peeks exactly like a never-delivered one [live], so K must exceed the
   largest cluster of locked messages a previous run can leave in front of its later deferrals.
   Each connection interruption leaves at most one batch plus the local buffer locked
   (`batch_size + prefetch_count + 1`, link credit included). The no-progress guard (§6.5) counts
   **written rows and unreadable dispositions** as progress. In C2 every written row is deferred,
   which breaks a locked cluster, so whenever the progress between two interruptions includes a
   written row, no locked cluster exceeds `batch_size + prefetch_count + 1` and the factor 2 is
   margin. Of the dispositions, `dead_letter` removes the message (a gap, not a lock) and an
   unreadable retry abandons it (unlocked at once), but **`leave`** — chosen by policy, or reached
   after the retry budget is spent (§6.6) — keeps the message locked until its lock lapses, and a
   locked message looks never-delivered. When the only progress between interruptions was `leave`
   dispositions, or left messages sit next to an interrupted batch, a cluster can exceed the bound:
   the scan may then stop early, and the orphans behind it surface on a later run once the locks
   lapse (≤ the entity lock duration, 5 min max) — **delayed recovery, not loss**. Invariant
   [inferred from in-order delivery on a non-partitioned entity with a single consumer]: every
   deferral was made on a message delivered earlier, so K never-delivered messages lie after every
   deferral.
5. **Page cap: 20 pages (5,000 messages)** → WARNING "orphan scan incomplete" — never a silent pass.
   Steady-state cost: 1–2 peek calls.

**H3 on other entity types (C2 allowed everywhere [decided]):**

| Entity type | H5 commit | H3 orphan scan | Run WARNING |
|---|---|---|---|
| plain queue / subscription | ✅ | bounded scan above | only if the page cap is hit |
| partitioned | ✅ grouped by partition [live] | **best-effort capped full scan** in cursor mode (no explicit sequence number — explicit paging missed partitions, cursor mode visited all 16 [live]); K rule not used (valid only per partition) | every run: "orphan recovery is best-effort on partitioned entities"; plus "incomplete" on the cap |
| DLQ / transfer-DLQ sub-queue | ✅ via the sub-queue receiver [live] | **best-effort capped full scan** — DLQ messages keep their original sequence numbers and arrive with any `delivery_count` [live], so the K rule is invalid | every run: best-effort notice; plus "incomplete" on the cap |
| session entity | ✅ per stored `session_id` | **impossible on 7.14.3** — sessions holding only deferred messages are not handed out by `NEXT_AVAILABLE_SESSION` and cannot be enumerated [live] | every run: "orphan recovery is not available for session entities; deferrals of a failed run stay DEFERRED until recovered manually" |

**Orphan recovery guard:** an orphan whose peeked `delivery_count ≥ max_delivery_count − 1` (from L2
metadata; 9 when unknown) is not recovered and is listed (first 20 sequence numbers) in a WARNING.
The guard uses the broker's delivery count because it survives failed runs (state does not);
deferred receives count toward MaxDeliveryCount, and a deferred message received by sequence number
often enough is dead-lettered [live, Phase 7] (§10 risk 7) — the reason the guard exists.

**Explicit C2 limits (spec + UI tooltip + README):** (1) **exclusive consumer** — one defer-commit
config per entity path and no other application deferring there (a foreign deferral looks like an
orphan and would be extracted and later deleted); (2) partitioned — per-partition commit,
best-effort recovery + WARNING; (3) sub-queues — sub-queue receiver commit, best-effort recovery +
WARNING; (4) sessions — per-session commit, no recovery + WARNING (revisit with 7.15
`list_*_sessions`, whose coverage of deferred-only sessions is unknown); (5) config deletion / state
reset strands the pending set (visible as `DEFERRED` in Service Bus Explorer) — the next C2 run on
that entity recovers it (not on sessions); (6) dev branches (§2.5); (7) TTL — an orphan recovered
after its TTL is gone; (8) delivery count — each deferred receive increments it (guard above).

**Foreign-deferral probe (C1/C3, carried item 1):** after H5, one peek page (≤ 250 messages) from
the start of the entity (cursor mode on partitioned entities; skipped on session entities, where a
peek needs a session). If it contains `DEFERRED` messages → WARNING "N deferred messages on `<entity>`
are not owned by this configuration's state; if they came from a failed defer-commit run of this
configuration, run it once in defer-commit mode to recover them". Detect-only — C1/C3 never touch
another application's deferrals.

### 6.5 Receive loop (C1/C2/C3)

- **Batch:** `receive_messages(max_message_count=min(batch_size, remaining), max_wait_time=
  idle_timeout_seconds)`. For each message: map to a row (§6.9–§6.10); rows of the batch are written
  and flushed; then each message is settled — C1 `complete_message`, C2 `defer_message` + add to the
  pending set (only after the call returned), C3 nothing (already deleted). Before settling, a
  message whose `locked_until_utc` is within 10 s is renewed from the main thread (D10). **`write_always`
  switch (§6.9):** C1 arms it just before its first `complete_message`, C3 as soon as a receive call
  (the stop drain included) first returns messages, before writing them; C2 never.
- **Stop conditions (checked between batches):** empty receive (idle) — **verified first (Phase 8)**,
  see *Empty receives* below; `max_messages` reached;
  `max_duration_seconds` elapsed; **watermark** — every message of the batch has
  `enqueued_time_utc ≥ T0` (the batch is still processed; redelivered messages keep their original
  enqueue time [live]; messages whose `state` is `SCHEDULED` are ignored by every watermark check —
  harmless either way: a pending one is skipped in C4 (§6.7), and a message activated from a
  schedule can still report `SCHEDULED` on 7.14.3 [live, Phase 7], so ignoring it at most delays a
  stop); **C2 state
  budget** — the serialised **projected output state** (the whole `state.json` this run would write:
  pending set + peek cursor + flatten registry + carried keys) reaches 256 KiB (Keboola documents a
  ~1 MB state limit [docs: developers.keboola.com config-file]) → stop + WARNING; the unreadable
  abort share (§6.6).
- **Empty receives (Phase 8, lead decision):** an empty receive is not proof of a drained entity
  [live: a 1M-message C1 run (batch 5000, prefetch 1000) stopped `idle` after 77,077 messages with
  927,756 still active and no error; the Standard-tier namespace reported ServerBusy (throttling), and
  pyamqp re-flows link credit only when the local credit reaches 0, so a stalled refill is a second
  candidate. Reproduced locally with the fix in place: a 60k-message drain with the same batch /
  prefetch hit 4 empty receives while ≥ 250 messages were still receivable, no server-busy report
  was logged (so a stalled link, not throttling, there), and the retries drained all 60,003.
  Validated live on the platform (Phase 8 rerun, same 1M-message row, branch build `-31`): one run
  drained all 927,756 remaining messages — `received=written=completed=927756
  empty_receive_retries=22 stop=idle duration_s=1758`, ~528 messages/s, every empty receive resolved
  by its first reconnect; the output table holds 1,004,833 unique sequence numbers (77,077 + 927,756)
  and the queue 0 active]. On an empty receive of a plain entity the loop peeks one page (250, the broker's cap)
  past the highest *processed* sequence number (the stop drain's abandoned messages sit below it and
  redeliver; from the start in cursor mode on partitioned entities) and counts the
  messages a receive would still hand out: not `DEFERRED` (C2's own and foreign deferrals stay in the
  entity), not pending activation, not expired, and — with `stop_at_job_start` — enqueued before T0.
  The peek always runs on a dedicated receiver, which has its own connection (a 7.14.3 client shares
  none), never on the open receive link. A peek is a management request that services its whole
  connection while it waits for the reply [source: pyamqp `ManagementOperation.execute` →
  `Connection.listen`], so on the receive link's connection it would pull messages sent against the
  link's outstanding credit into the local buffer, and the close after the check would discard them
  (C3: already deleted on the broker; C1 / C2: locked until expiry).
  None → stop `idle`. Otherwise the loop closes the receiver **and** its client (a fresh link and
  connection), backs off 2 / 4 / 8 / 16 / 30 s (bounded by `max_duration_seconds`) and continues —
  no stop drain: the receive just returned empty and pyamqp works only inside calls. The retry counter
  resets when a receive returns messages; the 6th consecutive empty-but-not-drained receive stops the
  run as `receive_stalled`. A peek cannot see another consumer's message lock [live, Phase 8: a
  locked message peeks `ACTIVE`, `locked_until_utc` None, `delivery_count` 0], so messages locked by a
  competing consumer look receivable: such a run spends the retries (~2 min at the default idle
  timeout) and ends `receive_stalled` with the WARNING below — nothing is lost. On a partitioned
  entity in C2 the run's own deferrals stay in the entity and can fill the one page peeked from the
  start, so there the check can miss receivable messages and stop `idle` as before [inferred]. Session entities
  keep their `NEXT_AVAILABLE_SESSION` loop (a peek there needs a session).
- **Messages left behind:** after a `max_messages`, `max_duration` or `receive_stalled` stop, a
  WARNING names what is left — peeked on plain entities ("N receivable message(s) are still in …",
  "at least 250" when the page is full), the management active count on session entities (also after
  `no_more_sessions` when `stop_at_job_start` is off; skipped in C2, whose deferrals count as active,
  and without management access). A partial extraction never looks like a complete drain.
- **Throttling (Phase 8):** the SDK reports every retryable AMQP error at INFO before retrying; a
  `ThrottleCounter` handler on `azure.servicebus` counts the `com.microsoft:server-busy` reports
  without printing them (the logger reaches the job log only in debug mode). A non-zero count is a
  summary token (`throttled=`); it and the loop's reconnects after empty receives
  (`empty_receive_retries=`) share one WARNING with the tier hint (Standard: about 1,000 operations
  per second per namespace — lower batch / prefetch, fewer concurrent consumers, or Premium), because
  a throttled namespace mostly shows as empty receives [live, Phase 8 rerun: Azure counted 43
  throttled requests during the run, the SDK reported none as an error, and 22 receives came back
  empty]. A
  `ServiceBusServerBusyError` that surfaces (retries exhausted, a sync action) maps to a throttling
  `UserException` (§6.11).
- **Watermark on partitioned entities:** receive order is not enqueue order [live], so the stop is
  approximate (can end early or late, never loses data) → run WARNING, not a refusal.
- **Sessions (A5a):** loop `NEXT_AVAILABLE_SESSION` receivers (`max_wait_time = idle_timeout`);
  `OperationTimeoutError` = no more sessions → done. Each session drains until an empty receive or
  a watermark batch, then its receiver closes (releasing the session lock); the session lock is
  renewed from the main thread (`receiver.session.renew_lock()`) when within 10 s of expiry. A
  session handed out a second time in the same run ends the session loop (it still holds messages
  enqueued after T0); `SessionCannotBeLockedError` → skip that session. Global limits apply across
  sessions.
- **Stop drain (C8):** at a stop other than idle, one `receive_messages(max_message_count=
  prefetch_count + 1, max_wait_time=1)` drains the local buffer: C1/C2 **abandon** those messages
  (immediately available again, instead of after lock expiry — 7.14.3 does not release them on
  close [live]); **C3 writes them** (they are already deleted on the broker [source: receiver
  docstring]), even past `max_messages`. The same drain runs **before the run raises** the
  batch processor's own `UserException` (`fail` policy, flatten cap, abort share — §6.6, §6.10):
  C3 writes the buffered, already-deleted messages (a further failure while processing that drained
  batch is swallowed — its rows are already written — and never masks the original error), C1 / C2
  abandon them; then the original exception propagates.
- **Connection recycling (J2):** any exception from receive / peek / settle that is not a config,
  auth or entity error (§6.11) closes the receiver and receive client and opens fresh ones, then
  continues. Cap **5 connection recoveries per run**; **no-progress guard:** if a connection failure
  happens and no progress was made since the previous one, the run fails — *progress* = at least one
  row written **or** one unreadable-body disposition (dead-lettered / left / skipped). Recycles
  requested by the unreadable-body retry (§6.6) are **not** connection failures: they have their own
  budget, never count toward the 5 and never trip the guard. Nor is the batch processor's own
  `UserException` (`fail` policy, flatten cap, abort share): it is never recycled or counted as a
  recovery — it ends the run after the stop drain above (C4: it propagates directly). Exhausted → a `ServiceBusError`
  becomes a `UserException` ("Lost the connection to Service Bus N times in this run (limit 5, or twice without writing a row or disposing an unreadable message in between); last error: <redacted>. Messages that were not settled redeliver on the next run."); anything else
  re-raises (exit 2). Messages of an interrupted batch stay locked until lock expiry and redeliver
  with `delivery_count + 1` [live]; the PK deduplicates. **Optional catch-up (D6):** with
  `recovery_wait_seconds > 0` and at least one recovery, before finishing the run keeps receiving
  for up to that many seconds (bounded by `max_duration_seconds`) to collect the redelivered
  stragglers.
- **Settlement failures (J5):** `MessageLockLostError` on complete / defer / abandon / dead-letter
  is counted and reported (WARNING in the summary), never fatal — the message redelivers and the PK
  deduplicates. 7.14.3 settles are pre-settled (fire-and-forget; broker rejections are silent
  [source]), so the count is a lower bound.
- **Memory:** rows stream to disk; message objects are dropped after settling — memory ≈ batch size ×
  body size.

### 6.6 Unreadable bodies (C6 / F7) — `body.unreadable_body`

| Case | C1 / C2 | C3 | C4 |
|---|---|---|---|
| **Body access / decode raises** (not text replacement — `errors='replace'` never fails) | 1st failure: `abandon_message`, remember (sequence number, connection generation), recycle the connection after the batch; the message redelivers at once on the new connection. 2nd failure on a **different** connection → policy: `dead_letter` → `dead_letter_message(reason="UnreadableBody", error_description=<exception class>)`; `leave` → not settled (the lock lapses; it redelivers later); `fail` → `UserException` after the rest of the batch is settled | the message is already deleted — the row cannot be written: WARNING with sequence number + message id (at-most-once); `fail` → the readable rest of the batch is written, then `UserException` | incremental fetch (plain entities): 1st failure → recycle, then re-peek from the first unreadable message's sequence number, skipping sequence numbers already written in this run (no duplicate rows); 2nd → skipped with WARNING and the cursor moves past it. Full fetch on partitioned or session entities: the 1st failure is final (skipped with WARNING) — cursor-mode and per-session peeks cannot resume at a sequence number, and a full fetch re-reads the message next run anyway. `fail` → the readable rest of the page is written, then `UserException` (state is not saved, so the cursor stays) |
| **`NotJson`** — `json_flatten` and the body is not valid JSON in its charset (deterministic → no retry) | `dead_letter` (reason `NotJson`) / `leave` / `fail` (after the batch) | skipped + WARNING / `fail` (after the batch) | skipped + WARNING / `fail` (after the page) |
| **`BodyTooLarge`** — the encoded output cell would exceed 16 MiB (Storage cell limit [inferred: the Snowflake VARCHAR maximum; verified against a typed import in Phase 7]) | `dead_letter` (reason `BodyTooLarge`) / `leave` / `fail` (after the batch) | skipped + WARNING / `fail` (after the batch) | skipped + WARNING / `fail` (after the page) |

- **Write first, fail after (gate cycle 2):** the `fail` policy and the flatten column cap (§6.10)
  never raise inside the per-message loop. The failure is recorded; the batch's readable rows are
  written and settled first; only then does the run raise — the same pattern as the abort share.
  In C1 / C2 the failing message itself is left unsettled (its lock lapses and it redelivers); in C3
  it was already deleted. Before the exception leaves the receive loop, the stop drain (§6.5) runs:
  in C3 the messages already in the local receive buffer — deleted on the broker — are written too,
  so no deleted message other than the unreadable one itself is lost. The exception is never
  treated as a connection failure (§6.5).
- **Sub-queue sources:** dead-lettering a DLQ message is rejected by the broker (silently on
  7.14.3 [live]) → on sub-queues `dead_letter` degrades to `leave` with a WARNING.
- A message `leave`-d once that redelivers later in the same run (its lock lapsed) is left again
  without a new retry and counted once.
- An unreadable body is **never** written as an empty row and never completed / deferred as
  processed. Message bodies are never logged — only sequence number and message id.
- **Retry budget (own counter):** the fresh-connection retry costs one recycle per batch that holds
  unreadable bodies (all suspects of a batch share it), counted in `unreadable_recycles`, cap **50 per
  run** — separate from the 5 connection recoveries (§6.5). Once the budget is spent, a first failure
  goes straight to the final disposition (WARNING "unreadable-body retry budget exhausted"), so the
  run keeps going and the abort share below stays reachable long before any cap.
- **Abort share:** checked after every batch — if unreadable dispositions exceed 10 % of the
  messages received **and** number ≥ 10, the run fails with a `UserException` (a systemic problem —
  wrong body format, a poisoned producer). Every unreadable message is counted in the summary.

### 6.7 C4 peek loop, partition and session handling (J6 / L2)

- **Metadata (L2):** via the management client when the credentials allow — `requires_session`,
  `enable_partitioning`, `lock_duration`, `max_delivery_count`. A mismatch between
  `requires_session` and `session_enabled` → `UserException` before anything is received. Without
  management access: partitioned = any seen `sequence_number >> 48 ≠ 0` (heuristic; misses the
  lowest partition id [inferred]); the session mismatch surfaces as the broker's "requires sessions"
  `ServiceBusError`, mapped to a `UserException` telling the user to enable **Sessions**; lock
  duration 60 s; max delivery count 10.
- **`incremental_fetch`:** `peek_messages(250, sequence_number=cursor + 1)` until an empty page,
  `max_messages`, `max_duration_seconds`, or (with `stop_at_job_start`) the first non-`SCHEDULED`
  message enqueued at or after T0 (not exported; `SCHEDULED` messages never stop the peek, §6.5).
  The cursor becomes the highest sequence number **processed** — written, skipped as unreadable
  (§6.6), skipped as expired or skipped as pending activation (below) — so a skipped last message
  is not re-peeked forever; messages
  after a watermark / limit stop were not processed and are peeked again next run. A partitioned
  entity detected mid-run (heuristic) → `UserException` "use Full Fetch" (peek is non-destructive,
  so aborting is safe; the cursor stays).
- **`full_fetch`:** plain entities peek from sequence 1 with explicit paging; partitioned entities use
  cursor-mode peek (no explicit sequence number, verified to visit all partitions [live]); session
  entities loop `NEXT_AVAILABLE_SESSION` receivers opened with the fixed internal
  `SESSION_ACCEPT_WAIT_SECONDS = 5` (not `idle_timeout_seconds`, which is hidden and ignored in peek
  mode — a hidden field never drives behaviour; `previewMessages` uses the same
  constant) (each session's receiver peeks from its start; sessions
  locked by other consumers are skipped and deferred-only sessions are invisible — WARNING once per
  run; the peek briefly holds the session lock [live]). The watermark on partitioned / session
  entities is approximate (WARNING).
- **Expired-not-purged** messages (peek returns them [live]) are skipped and counted (INFO,
  `expired_skipped`). `DEFERRED` messages are exported with their `state` (A7).
- **Scheduled messages (lead decision P4-7, after the Phase-7 probe):** activation re-enqueues a
  scheduled message — it gets a **new sequence number** and a new `enqueued_time_utc` (the
  activation time), while `scheduled_enqueue_time_utc` survives [live, Phase 7]; a peeked pending
  message reports its send time as `enqueued_time_utc` [live, Phase 7]. Exporting the pending record
  would export the message twice under two primary keys, so C4 (both fetch modes) **skips a message
  pending activation**: counted as `skipped_scheduled` (summary, plus one INFO line per run), marked
  processed so the incremental cursor moves past its old sequence number (the activated copy gets a
  higher one), never counted toward `max_messages`; it is exported once active, under its new
  sequence number. *Pending* = `state == SCHEDULED` and not (`scheduled_enqueue_time_utc` and
  `enqueued_time_utc` both set and `enqueued_time_utc ≥ scheduled_enqueue_time_utc`): a received
  message activated from a schedule can still report `SCHEDULED` on 7.14.3 [live, Phase 7], and a
  peek of one may too [inferred] — its enqueue time (the activation time) is at or after the
  schedule, the pending record's (the send time) before it. The check runs before the expiry check:
  a pending message's `expires_at_utc` counts from its send time (the SDK adds `time_to_live` to
  `enqueued_time_utc` [source]). The `state` column is not rewritten (§6.9); the README states the
  skip.

### 6.8 State layout (row-scoped `state.json`)

```json
{
  "version": 1,
  "pending_commit": [
    {
      "entity": {"entity_type": "queue", "queue_name": "orders", "topic_name": null,
                 "subscription_name": null, "sub_queue": "none"},
      "groups": [
        {"session_id": null, "partition": 0, "max_body_bytes": 2048,
         "ranges": [[101, 350], [352, 400]]}
      ],
      "deferred_at_utc": "2026-09-23 10:00:00.000000"
    }
  ],
  "peek_cursor": {"entity_path": "orders", "last_sequence_number": 12345},
  "flatten_columns": [
    {"path_sha1": "ab92721575f7721ca4f4199f83f11a59f6bb9c88", "column": "body_order_customer_id"}
  ]
}
```

- **Merge rule:** every run starts from the input state, updates only the keys its mode owns, and
  writes every other key back unchanged — C4 carries `pending_commit` forward; destructive modes
  carry `peek_cursor` forward; `flatten_columns` persists across body-format switches. A cursor whose
  `entity_path` differs from the configured entity is reset (INFO).
- Missing / empty state (first run) → defaults; unknown `version` → `UserException` telling the
  user to reset state (no silent reinterpretation). Timestamps UTC.
- Written once, at the end of the run, via `write_state_file`; it becomes durable only if the job
  succeeds (§2.1) — which is exactly the C2 commit marker.
- **Budget:** the whole serialised state (pending set, cursor, flatten registry and carried keys)
  is kept ≤ 256 KiB; C2 measures the projected output state before each batch and stops receiving
  at the budget (§6.5). The registry is capped by construction — ≤ 1,000 entries of
  `{path_sha1 (40 hex), column (≤ 64 chars)}`, ≤ 133 bytes each, so ≤ ~130 KiB — which always
  leaves ≥ ~126 KiB for the pending set: a large registry can never stop a C2 run before its first
  batch;
  ranges keep normal pending sets tiny (a non-partitioned single-consumer entity defers contiguous
  runs).

### 6.9 Output table & manifest

- **File / name:** `/data/out/tables/<table>.csv`; `<table>` = `destination.table_name` or derived:
  queue → `<queue>`; subscription → `<topic>_<subscription>`; + `_dead_letter` /
  `_transfer_dead_letter` for sub-queues; characters outside `[A-Za-z0-9_-]` → `_`, leading /
  trailing `_` and `-` stripped. Stable per row.
- **Manifest (every run, via the library's `create_out_table_definition` + `write_manifest`):**
  one `schema` definition with native types; the library picks the manifest format from
  `KBC_DATA_TYPE_SUPPORT` (the keboola-context reference maps `none` / `hints` → legacy `columns` +
  `column_metadata` and `authoritative` → `schema`; keboola-component 1.11.0
  `_expects_legacy_manifest` also emits the `schema` format for `hints` [source]) — so the component
  never branches on the variable, and **only `authoritative` makes the native types binding**, which
  is why Phase 6 sets portal `dataTypeSupport = authoritative`. Then `primary_key` from
  `destination.primary_key`, `incremental` from `destination.load_type`, **`write_always`** per the
  table below,
  `has_header: true` (the CSV is written with a header row — the pairing native-data-types requires).
  No `destination` (default bucket).
- **Column order and types:** `sequence_number` INTEGER, `message_id`, `enqueued_time_utc`
  TIMESTAMP, `enqueued_sequence_number` INTEGER, `session_id`, `partition_key`, `subject`,
  `correlation_id`, `content_type`, `reply_to`, `reply_to_session_id`, `to_address`,
  `application_properties` (JSON), `delivery_count` INTEGER, `state`, `time_to_live_seconds` FLOAT,
  `expires_at_utc` TIMESTAMP, `scheduled_enqueue_time_utc` TIMESTAMP, `dead_letter_reason`,
  `dead_letter_error_description`, `dead_letter_source`, `body_type`, `message_annotations` (JSON),
  `amqp_durable` BOOLEAN, `amqp_priority` INTEGER, `amqp_first_acquirer` BOOLEAN, `amqp_user_id`,
  `amqp_content_encoding`, `amqp_creation_time_utc` TIMESTAMP, `amqp_absolute_expiry_time_utc`
  TIMESTAMP, `amqp_group_sequence` INTEGER, `amqp_reply_to_group_id`, `source_entity`,
  `settlement_mode`, `extracted_at_utc` TIMESTAMP, then **`body`** (text / base64 modes) **or** the
  input registry's flattened `body_*` columns followed by `body_unmapped` (flatten mode, §6.10). Unlisted types are STRING. Types are native only for
  broker-assigned values whose Python types were observed [live]; producer-controlled content (body,
  flattened keys, property maps) is STRING, per the native-types rule for unverified types.
- **Value formats:** timestamps `YYYY-MM-DD HH:MM:SS.ffffff` in UTC (verified against a typed
  import in Phase 7); `None` → empty; booleans `true` / `false`; JSON columns compact
  (`ensure_ascii=False`), bytes decoded UTF-8 with replacement, datetimes ISO-8601.
- **`state`** is the broker-reported value (`ACTIVE` / `DEFERRED` / `SCHEDULED`), never rewritten:
  a message activated from a schedule can report `SCHEDULED` on 7.14.3 [live, Phase 7] — in C1–C3
  and, once active, in C4 (§6.7) it is exported with that value.
- **Why `application_properties` / `message_annotations` stay JSON:** they are producer-defined key
  maps whose key set varies per message (the "variable-length" case the output checklist allows);
  flattening them would make the column set unbounded. The fixed-key AMQP header / properties are
  flattened into scalar columns (E24).
- **Durability on failure (`write_always`, lead decision gate 1):** the manifest is first written
  with `write_always: false`; C1 rewrites it with `true` immediately before its first
  `complete_message`, C3 as soon as a RECEIVE_AND_DELETE receive first returns messages (before
  writing them); C2 and C4 never set it. The H5 commit does not switch it on (it deletes messages whose rows the previous,
  successful job already imported), nor do dead-lettering or abandoning (nothing is lost). In every
  body format rows stream straight into the output CSV (the flatten column set is fixed when the run
  starts, §6.10); the file is flushed per batch and closed in a `finally` block, and a failure while
  closing is logged and never masks the original error.
- **Legacy job queue (Phase-4 amendment 3):** keboola-component 1.11 **deliberately** omits
  `write_always` from the manifest when `is_legacy_queue` is true (a project without the `queuev2`
  feature) [source: `interface.py` `is_legacy_queue`, `dao.py` `OUTPUT_MANIFEST_LEGACY_EXCLUDES`].
  **[inferred]** the reason is that the legacy queue's output mapping does not support the key. The
  component does **not** override the library (no manifest post-processing). Instead, a C1 / C3 run
  on the legacy queue logs a WARNING that the failed-run upload safety net is unavailable on this
  project — a job that fails after it deleted messages uploads nothing and those messages are lost —
  and recommends `defer_commit`. C2 / C4 never arm the switch, so they are unaffected. The Phase-7
  manifest check stays (cf-dev runs on the new queue).

  | Mode | `write_always` | Job fails before anything was deleted | Job fails after ≥ 1 deleted batch — `incremental_load` | Job fails after ≥ 1 deleted batch — `full_load` |
  |---|---|---|---|---|
  | C1 `complete` | `false` → `true` just before the first complete | nothing uploaded, table unchanged; locked messages redeliver | rows written so far are upserted (flatten: keys first seen in this run are in `body_unmapped`, §6.10) | the table is replaced by the rows written so far — this run's delta, the only copy of the deleted messages |
  | C2 `defer_commit` | never | nothing uploaded, table unchanged | nothing uploaded, table unchanged; the deferrals become orphans recovered by the next C2 run (not on sessions, §6.4) | same — a failed run never truncates a full-load table |
  | C3 `receive_and_delete` | `false` → `true` when the first receive returns messages | nothing uploaded, table unchanged (no receive has returned messages, so it was never armed) | rows written so far are upserted | the table is replaced by the rows written so far |
  | C4 `peek` | never | nothing uploaded, table unchanged; the next run re-peeks from the saved cursor | n/a (nothing is deleted) | n/a |

  A successful job always uploads the table (`write_always` is irrelevant then). Separately: a
  run's output state is durable only if its **own** (row) job succeeds **and** its imports succeed
  (§2.1), so a job that fails after a `write_always` upload, or an import that fails after the job,
  discards the run's state — possibly with its rows already in Storage. That is the reason for the
  flatten column rule in §6.10, which holds in every mode.
- **Empty run (G5):** header-only CSV + manifest; succeeds. With `full_load` this empties the table
  (mirror / delta semantics, §2.4).
- **Rows sharing one table:** fixed-schema rows may share a table name (use the composite PK when
  different entities share one); flatten-mode rows must not (§6.10).

### 6.10 Body formats and JSON flattening (F1–F6)

- **`text`:** DATA sections joined → decoded with the `content_type` charset (unknown → UTF-8),
  `errors='replace'`; VALUE / SEQUENCE → recursive bytes→str → compact JSON. Column `body`.
- **`base64`:** DATA → base64 of the raw bytes; VALUE / SEQUENCE → base64 of their UTF-8 JSON text
  (the column is uniformly base64). Column `body`.
- **`json_flatten`:** DATA → strict decode in the charset → `json.loads(parse_float=Decimal)`;
  VALUE bodies are already structured (bytes→str) and flatten without parsing; SEQUENCE is a
  top-level array. Failure → `NotJson` (§6.6).
  - Objects expand recursively (any depth) into one column per leaf; **arrays stay one column holding
    their compact JSON**; strings as-is; numbers as their JSON text (Decimal keeps `1.10`); booleans
    `true` / `false`; `null` → empty; an empty object → `{}`.
  - A top-level value that is not an object (array or scalar) → one column `body_value` (JSON text).
  - **Column name** = `body_` + the path's keys joined with `_`, ASCII-folded (NFKD, combining marks
    dropped), every character outside `[A-Za-z0-9_]` → `_`, trailing `_` stripped (Storage names must
    not start or end with `_` / `-` [docs]; dashes are also replaced for BigQuery-backed projects
    [inferred]); names longer than 64 characters → first 55 + `_` + 8 hex of SHA-1 of the path
    [inferred cap, verified in Phase 7]. The `body_` prefix keeps flattened keys from colliding with
    most metadata columns (`message_id`, `subject`, …) but **not all**: the metadata column
    `body_type` shares the prefix, so a JSON key `type` would map onto it. The registry therefore
    reserves **every** metadata column name (plus `body_unmapped`), and the key `type` becomes
    `body_type_2` (Phase-4 amendment 5, pinned by a unit test). The maintainer's dot-path (`order.customer.id`)
    is the logical path; Storage forbids `.` in column names, so it lands as
    `body_order_customer_id`.
  - **Collisions** (two distinct paths → one name, e.g. key `"a.b"` vs nested `a` → `b`): the path
    registered first keeps the name; later ones get `_2`, `_3`, … The **registry** (SHA-1 of the
    path's key list → column name) is persisted in state, so names are stable across runs. Storing
    the hash instead of the path bounds an entry to ≤ 133 bytes — ≤ ~130 KiB at the 1,000-column
    cap (§6.8).
  - **Why the column set must track saved state:** Storage adds new columns on import [docs] but
    rejects an import that misses an existing table column ("Some columns are missing in the CSV
    file" [docs]). So a table must never gain a column that the registry of the *saved* state does
    not contain — otherwise the next run (starting from that state) omits it and every later import
    fails, while C1 / C3 keep deleting messages. A run cannot know whether its state will be saved:
    its own job may still fail after a `write_always` upload (§6.9), and an import that fails after
    the job discards the state too (§2.1).
  - **Reserved column `body_unmapped`** (STRING, JSON): always present in flatten mode, last in the
    column order, reserved in the registry (a JSON key `unmapped` becomes `body_unmapped_2`). Per
    row it holds a compact JSON object `{"<column name>": "<value>", …}` keyed by the **registry
    column name** each key has been assigned — so `"a.b"` and nested `a → b` stay distinct as
    `body_a_b` / `body_a_b_2` — for the row's keys that are not columns in this run; empty when there
    are none.
  - **Column rule (lead decision, gate cycle 2) — identical in every mode:** a run materialises
    **exactly** the columns of the registry loaded from its **input** state, plus `body_unmapped`;
    every such column is written in every run (empty when absent). A key first seen in this run is
    registered (name assigned, cap checked), its values go to `body_unmapped`, and it is saved in the
    output state's registry; it becomes a real column from the **next** run. Invariant: no job adds a
    column that is outside the registry it started from, and saved registries only grow, so the
    latest saved registry always covers every column of the table — whatever the job outcome.
  - **Resulting behaviour (README "known behaviour"):** every key lands in `body_unmapped` in the run
    that first sees it and is a column from the next run (a one-run column lag in every mode); the
    first-ever run, and the first run after a state reset, put every key in `body_unmapped`; keys
    discovered by a failed run are not saved, so they are in `body_unmapped` for one more run;
    values already written to `body_unmapped` stay there (rows are never rewritten).
  - **Cap:** 1,000 registered columns per row. With the registry full, a newly seen key is not
    registered; its value still goes to `body_unmapped`, keyed by a **provisional** name — the name
    `column_name_for` would give it, suffixed `_2`, `_3`, … if taken — which is **reused for the same
    path hash for the rest of the run** (Phase-4 amendment 4), so every row of the run keys that path
    identically; provisional names are never saved. The batch is written and settled like any other,
    and only then does the run fail with a `UserException` suggesting `text` (write first, fail after
    — §6.6); before it propagates, the C3 stop drain writes the buffered, already-deleted messages
    (§6.5), so nothing is lost in C3.
  - **Streaming write:** the column set is fixed when the run starts (fixed columns + the input
    registry's columns + `body_unmapped`), so flattened rows stream straight into the output CSV like
    the text format — no staging file, no rewrite at close.
  - **Known limits (tooltip + README):** resetting state, or sharing the output table between
    flatten rows, can make an import miss previously seen columns and fail — in `complete` /
    `receive_and_delete` modes that failed import loses the run's messages, so drop the table when
    resetting state, give each flatten row its own table, and prefer `defer_commit` with
    flattening.

### 6.11 Error mapping (J1) — `UserException` (exit 1) vs unexpected (exit 2)

| Condition [live unless noted] | Exception | Result |
|---|---|---|
| malformed connection string / missing key | `ValueError` from `from_connection_string` — caught **only** in `ServiceBusConnector` | `UserException` "invalid connection string" (redacted) |
| entity-level SAS whose `EntityPath` ≠ configured entity | `ValueError` from `get_*_receiver` — caught **only** in `EntityRef.open_receiver` | `UserException` (writer wording) |
| malformed service-principal `tenant_id` | `ValueError` from `ClientSecretCredential(...)` (before any network call) — caught **only** in `ServiceBusConnector`'s credential builder | `UserException` "Invalid service principal settings" (redacted) |
| wrong SAS key, **missing queue via SAS**, IP-firewall rejection [docs] | `ServiceBusAuthenticationError` | `UserException` "authentication failed, the entity does not exist, or the namespace's IP firewall rejected the Keboola stack (Service Bus reports all three as unauthorized) — check Listen rights on `<entity>`, the name, and the allowlisted egress IPs" |
| missing Listen / Data Receiver | `ServiceBusAuthorizationError` | `UserException` naming the right |
| missing subscription | `MessagingEntityNotFoundError` | `UserException` |
| entity `ReceiveDisabled` | `MessagingEntityDisabledError` | `UserException` |
| management call denied (SP without Data Receiver, SAS without Manage) / unknown topic — the endpoint's own 401 / 403, never a token failure (next rows) | `ClientAuthenticationError`, `HttpResponseError` 401 / 403 / `ResourceNotFoundError` (azure.core) | `UserException` naming the Data Receiver role / Manage rights, or "not found", always with `(details: …)` (a Listen-only SAS listing returns `[]` instead, §5.4) |
| unknown namespace host | `ServiceBusConnectionError` at connect | `UserException` "cannot reach namespace" |
| throttled namespace (ServerBusy) surfacing after the SDK's retries — a sync action, a commit, recoveries exhausted [Phase 8] | `ServiceBusServerBusyError` | `UserException` "the namespace is throttled … Standard tier about 1,000 operations per second … fewer consumers or Premium"; in the receive loop a recycle first (§6.5) |
| SP credentials rejected — wrong / expired secret, unknown client ID or tenant (the Entra ID token cannot be acquired) | data plane: plain `ServiceBusError` "Handler failed: Authentication failed: AADSTS…" [live: "Authentication failed: AADSTS…"; the "Handler failed" wrapper and its `inner_exception` = the credential's error per SDK source]; management plane: azure-identity's `ClientAuthenticationError` "Authentication failed: AADSTS…" re-raised unchanged (an unknown tenant fails in MSAL's authority discovery with no AADSTS code), or `CredentialUnavailableError` [SDK source, identity 1.25] | `UserException` "The service principal credentials were rejected (check tenant ID, client ID and client secret). (details: …)" on every path — sync actions, the pre-checks and runs (never recycled). `client.is_credential_failure` recognises it (an AADSTS code, `CredentialUnavailableError`, or raised inside azure-identity, following `inner_exception`); `is_management_denied` excludes it, so it is never reported as a missing role |
| session entity without `session_enabled` (or the reverse) | `ServiceBusError` text / L2 mismatch | `UserException` "enable / disable Sessions" |
| refused combinations (J10), state version, table name, flatten cap, unreadable share, commit retries exhausted, recoveries exhausted on a `ServiceBusError` | component checks | `UserException` |
| `MessageLockLostError` on settle; `SessionCannotBeLockedError`; `OperationTimeoutError` (no session) | — | counted / skipped / normal end |
| receive-path `TypeError` / `BufferError` (multi-frame symptoms) and other unexpected errors | — | recycle (§6.5); exhausted → exit 2 |

`ValueError` is never mapped anywhere else: `to_user_exception` handles `ServiceBusError` subclasses
and `azure.core` errors only, and the receive loop does not treat `ValueError` as fatal-and-user —
a `ValueError` from any other place is a bug and exits 2.

Redaction (J7): `SharedAccessKey=…`, `sig=…` (SAS tokens) and the literal `#client_secret` /
`#connection_string` values are masked in every surfaced message and log line; `UserException`
messages carry `(details: <redacted SDK message>)` like the writer.

### 6.12 Logging, summary, client identity

- stdout = INFO / WARNING, stderr = ERROR (library default handler).
- **`azure.*` loggers:** level CRITICAL (the SDK logs its own ERROR tracebacks for errors the component
  handles); INFO when the root logger is at DEBUG (the platform `debug` parameter, consumed by the
  component base — no `debug` field in the model), always through the redaction filter. Phase 8:
  `azure.servicebus` is always at INFO for the `ThrottleCounter` and propagates to the job log only
  in debug mode (§6.5).
- **Platform debug runs record HTTP** (keboola-component 1.11: `KBC_COMPONENT_RUN_MODE=debug` wraps the
  run in `keboola.vcr` and writes a cassette to `/data/out/files/` [source: `base.py`,
  `vcr/recorder.py`]). Only the management-plane and Entra ID token calls are HTTP (AMQP sockets are
  not intercepted [inferred]); the library's `DefaultSanitizer` keeps only whitelisted headers (so the
  SAS `Authorization` header is dropped), redacts `client_secret` / `access_token` fields and every
  `#`-prefixed config value — no component-specific `VCR_SANITIZERS` are needed. Phase 7 runs one
  debug job and the Phase-5/7 gates grep its cassette for secrets.
- **Effective-settings line** at start: mode, entity path, sub-queue, sessions, fetch mode, limits,
  body format, load type, PK, batch / prefetch — `(default)` marked.
- **Run summary** (J8/J9): received, written, completed / deferred / deleted-on-receive, committed
  (H5) + already-gone, orphans recovered / skipped by the guard, unreadable (per reason and
  disposition), settlement failures, recoveries, empty-receive retries and throttled requests (§6.5,
  Phase 8), C4 skips (`expired_skipped`, `skipped_scheduled`, §6.7), stop reason, **delivery-count
  high-water mark**, duration; WARNINGs for C3 (at-most-once), best-effort / impossible orphan
  recovery, scan cap, state budget, approximate watermark, prefetch > 1, messages left behind and
  throttling (§6.5).
- `user_agent="keboola.ex-azure-service-bus"`; `client_identifier =
  "kbc-<KBC_CONFIGID or local>-<KBC_CONFIGROWID or root>"` (≤ 64 chars; `KBC_CONFIGID` may be a hash
  for inline-config jobs — used only as a label).
- L1 counts (active / dead-letter / scheduled / transfer-DLQ) logged at INFO when available.

## 7. Code architecture

Thin orchestrator + one module per concern (mirrors the writer's split, extended for receive-side
state):

| Module | Responsibility |
|---|---|
| `src/configuration.py` | Pydantic v2 models: `AuthConfiguration` (flat root auth fields, writer's `_validate_auth`), `SourceConfig`, `LimitsConfig`, `BodyConfig`, `DestinationConfig`, `AdvancedConfig`, `Configuration` (merged root + row); `StrEnum`s for every enum; `extra="ignore"`; `ValidationError` → `UserException` with field paths; §5.3 validators. Partial models: `AuthConfiguration` (list actions, `testConnection`), `SyncActionConfiguration` (auth + a possibly partial `source`: `listSubscriptions`' topic), `EntityConfiguration` (auth + the validated, normalised `source`: `previewMessages`), `Configuration` (extends `EntityConfiguration`; `run` only). |
| `src/client.py` | `ServiceBusConnector` — credential factory (SAS / SP), `receive_client()`, `commit_client()` (`retry_total=0`), `admin_client()`, `user_agent`; `redact_secrets`, `to_user_exception` (§6.11). Pyamqp only. |
| `src/entity.py` | `EntityRef` (entity type, names, sub-queue → receiver kwargs, entity path, derived table name, state key); `EntityInfo` (L2 metadata or heuristics: sessions, partitioning, lock duration, max delivery count); `partition_of(seq)`; management helpers for the dropdowns and the root `testConnection`. |
| `src/receiver.py` | `ReceiveLoop` — batches, sessions loop, stop conditions, lock renewal, stop drain, recycling with cap + no-progress guard, catch-up wait; the helpers it shares with the peek pager: `RecoveryTracker`, `StopReason`, receiver profile (§6.2), open / close / session-renew helpers, the watermark test. |
| `src/peek.py` | `PeekPager` — C4 incremental / full paging, partition / session variants, expired and pending-scheduled skips (§6.7). |
| `src/settlement.py` | `Settler` per mode (`complete`, `defer` → pending set, `none`); `UnreadableHandler` (abandon / dead-letter / leave / fail, suspect tracking, sub-queue degradation, abort share); `BatchProcessor` — the per-batch pipeline shared by the receive loop, the orphan recovery and the peek pager: map rows → write + flush → settle. |
| `src/commit.py` | `PendingCommitter` (H5: grouping, count + byte chunking, bisection, transient retry, carry-forward, stale-entity drop); `OrphanScanner` (H3: plain bounded scan with K + page cap, best-effort full scan, session WARNING, recovery guard); `ForeignDeferralProbe` (C1/C3). |
| `src/state.py` | `ExtractorState` Pydantic model (§6.8): load / defaults / version check, merge rule, range encoding (ranges expanded only lazily), budget measurement, `to_dict()`. |
| `src/body.py` | `decode_body` (DATA / VALUE / SEQUENCE, charset, text / base64), `flatten_json`, `FlattenRegistry` (naming, collisions, cap), errors `BodyDecodeError` / `NotJsonError` / `BodyTooLargeError`. |
| `src/columns.py` | fixed column catalogue (name → base type), message → metadata row mapping (E1–E25), value formatting (timestamps, JSON, bytes), `previewMessages` markdown rendering. |
| `src/output.py` | `OutputTable` — one streaming CSV writer for every body format (flatten: fixed columns + input-registry columns + `body_unmapped`, §6.10); manifest (schema, PK, incremental, `has_header`, `write_always` false until `arm_write_always()`; on the legacy queue the library omits the key and the component only warns, §6.9) via a callback into `ComponentBase.write_manifest`; context manager that flushes and closes on exit, including on exceptions. |
| `src/stats.py` | `RunStats` counters, effective-settings line, summary, WARNING aggregation. |
| `src/component.py` | `Component(ComponentBase)`: the `ServiceBusConnector` is a cached property built on first use — by `run()` before any step, or inside a sync action so an auth validation error takes the sync-action error path — from `AuthConfiguration`; building it installs the redacting log filter (no network until a client is opened); `run()` ≤ 30 lines delegating to `_load_run_config`, `_start_run`, `_open_output`, `_commit_pending`, `_reconcile_deferrals`, `_consume`, `_peek`, `_finish`; `@sync_action`s (§5.4, read through the typed partial models); the scaffold's `__main__` guard (`UserException` → exit 1 with redacted message, else exit 2). |

- **Typing:** built-in generics, `collections.abc` iterators, full hints, `@staticmethod` where
  `self` is unused; ruff with `I`, `UP`, `G` (template config); PEP 758 `except A, B:` is valid on
  3.14.
- **Dependencies:** `azure-servicebus>=7.14.3,<7.15`, `azure-identity>=1.19,<2`,
  `keboola-component>=1.11` (manifest `write_always` + `has_header` support as read in 1.11.0),
  `pydantic`; remove the scaffold's unused `keboola-http-client` and `keboola-utils` unless the
  implementation uses `keboola.utils.header_normalizer` for §6.10 character normalisation (allowed if
  it matches the rules exactly). No `websocket-client` (K2 excluded).
- No scratch files at all (flatten streams too); nothing but the output table is written under
  `/data/out/tables/`.

## 8. Testing — enumerate the cases up front

**Approach (writer precedent, tracker Phase-5 AMQP note):** the data plane is AMQP, which vcrpy cannot
record, so the functional suite runs the full component (`runpy` of `src/component.py` as
`__main__`, real exit-code guard, real datadir) against an **SDK-mock harness**: an autouse fixture
patches `ServiceBusClient`, `ServiceBusAdministrationClient` and `ClientSecretCredential` inside
`client.py` with fakes backed by a **`FakeBroker`** that models entities, sub-queues, sessions,
partitions (`seq >> 48`), message states (ACTIVE / DEFERRED / SCHEDULED), locks with a controllable
clock, delivery counts, the peek 250 cap, deferred-receive rules (one partition per call, RAD ≤ 250,
all-or-nothing `MessageNotFoundError`, each PEEK_LOCK deferred receive counting toward
MaxDeliveryCount and dead-lettering at it), settlement, dead-lettering (rejected on DLQ), NEXT_AVAILABLE
sessions (`OperationTimeoutError` when none), the management plane (lists / properties / 401 for a
Listen SAS) and failure injection (auth errors, a `TypeError` on the N-th receive, body-access
errors, lock loss). `from_connection_string` validates through the real SDK parser offline (writer
trick). Two-run cases copy `out/state.json` to the next run's `in/state.json` and keep the same
broker. The management client is HTTP (VCR-recordable in principle) but is mocked too — one harness,
no tenant ids in cassettes. Real AMQP behaviour is proven live in Phase 7.

**Files:** `tests/fakes/broker.py` (the double) + `tests/conftest.py`; `tests/unit/test_*.py`;
`tests/functional/` = `conftest.py` (datadir harness), `test_sync_actions.py` (cases 01–14),
`test_runs.py` (20–45), `test_bodies_and_robustness.py` (46–69), `test_sanitisation.py`, `expected/`.

**Fixtures:** `tests/setup/configs.json` (wrapped format, dummy credentials only; real values only in
the gitignored `secrets.json`); broker seeds are built per test via the fixture API; one golden
expected output (`tests/functional/expected/20_run_c1_queue/`: CSV + manifest) is compared byte-for-byte;
other cases assert on the produced CSV, manifest and `out/state.json`. Sanitisation axis: grep
fixtures, logs and expected files for `SharedAccessKey=` values, `sig=`, and the dummy secrets —
only dummies may appear, and surfaced errors must be redacted.

| Case | Kind | Covers |
|---|---|---|
| `01_testConnection_manage_sas_ignores_source` | sync ok | I1, root probe with a Manage SAS; a row source is ignored, no receiver opened, nothing locked |
| `02_testConnection_bad_conn_string` | sync fail | malformed SAS → redacted `UserException` |
| `03_previewMessages_auth_or_missing` | sync fail | `ServiceBusAuthenticationError` on the receiver → J1 wording incl. IP firewall |
| `04_previewMessages_session_empty` | sync ok | session entity, `OperationTimeoutError` → "no messages to preview" |
| `05_testConnection_root_sp` | sync ok | root context, SP, management listing |
| `06_testConnection_root_listen_sas` | sync fail | root context, 401 → guidance message |
| `07_listQueues_sp` | sync ok | I2 |
| `08_listQueues_listen_sas_empty` | sync ok | I2 SAS 401 → `[]` |
| `09_listTopics_sp_auth_failure` | sync fail | SP error → `UserException`; variant `credentials_rejected`: an AADSTS token failure → the credentials message, not the role |
| `10_listTopics_sp` | sync ok | I2 |
| `11_listSubscriptions_sp` | sync ok | I2 |
| `12_listSubscriptions_missing_topic` | sync fail | not found → `UserException` |
| `13_previewMessages` | sync ok | I3, markdown table, nothing settled |
| `14_previewMessages_missing_entity` | sync fail | J1 |
| `20_run_c1_queue` | run | C1, fixed schema golden output, manifest (schema, PK, incremental, `write_always` true after the first complete, `has_header`), completes |
| `21_run_c1_subscription_sp` | run | A2, B2; variant `credentials_rejected`: the run fails (exit 1) with the credentials message, nothing received |
| `22_run_c2_first_run_defers` | run | C2 defer, pending set ranges in state |
| `23_run_c2_second_run_commits` | two-run | H5 commit (RAD, ≤ 250 + byte chunking), new deferrals |
| `24_run_c2_commit_not_found_bisection` | run | already-gone sequence numbers treated as committed |
| `25_run_c2_commit_transient_retry_exhausted` | run fail | carried item 3: exit 1, nothing received, state intact |
| `26_run_c2_orphan_recovery_plain` | run | H3 plain: orphan recovered → row → re-deferred; K stop; skip rules |
| `27_run_c2_orphan_scan_locked_cluster` | run | locked messages peek as never-delivered; K > cluster keeps scanning past them |
| `28_run_c2_orphan_scan_cap_warning` | run | page cap → WARNING |
| `29_run_c2_orphan_guard_delivery_count` | run | recovery guard WARNING; the orphan reaches the threshold through nine earlier deferred receives |
| `30_run_c2_partitioned` | run | per-partition commit groups, best-effort scan WARNING |
| `31_run_c2_session_entity` | two-run | per-session commit, "recovery impossible" WARNING |
| `32_run_c2_dlq` | two-run | sub-queue commit, best-effort WARNING |
| `33_run_c2_state_budget` | run | stop at budget + WARNING |
| `34_run_c3_receive_and_delete` | run | C3, at-most-once WARNING, stop drain writes buffer |
| `35_run_c3_prefetch_refused` | run fail | J10 |
| `36_run_c4_incremental_two_runs` | two-run | H1 cursor, nothing settled, second run from cursor |
| `37_run_c4_full_fetch` | run | full fetch, DEFERRED state column, pending SCHEDULED and expired skipped (`skipped_scheduled`, `expired_skipped`) |
| `38_run_c4_incremental_partitioned_refused` | run fail | J6 |
| `39_run_c4_leftover_pending_carried` | run | C4 carries `pending_commit` + WARNING |
| `40_run_c1_leftover_pending_committed` | run | H5 in C1 after a C2 period |
| `41_run_c1_foreign_deferral_probe` | run | detect-only WARNING |
| `42_run_sessions_loop` | run | A5a: two sessions drained, revisit / timeout end |
| `43_run_session_mismatch` | run fail | "enable Sessions" `UserException` |
| `44_run_dlq_c1` | run | A3, dead-letter columns |
| `45_run_tdlq_c1` | run | A4 |
| `46_run_body_base64` | run | F2 |
| `47_run_body_value_sequence` | run | F3 |
| `48_run_body_charset_multisection` | run | F4, F5 |
| `49_run_flatten_json` | two-run (C1) | run 1: no `body` column, no `body_*` key columns yet, `body_unmapped` keyed by registry column names (`body_a_b` vs `body_a_b_2` for `"a.b"` / nested `a → b`), arrays as JSON, registry in state; run 2 (same body shape): those keys are real columns, `body_unmapped` empty |
| `50_run_flatten_new_keys_second_run` | three-run (C2) | run 1 `{"x":1}`, run 2 `{"y":2}`, run 3 `{"y":3}`: run 2 header has `body_x` (empty) but not `body_y`, `body_unmapped` = `{"body_y":"2"}`; run 3 header has `body_x` and `body_y` |
| `51_run_flatten_not_json` | run | `NotJson` dead-letter |
| `52_run_unreadable_body_two_connections` | run | C6: abandon + recycle, dead-letter `UnreadableBody` on 2nd failure |
| `53_run_unreadable_on_dlq_degrades` | run | dead-letter → leave on sub-queue + WARNING |
| `54_run_unreadable_share_abort` | run fail | abort share |
| `55_run_receive_failure_recycle` | run | J2: injected `TypeError` → recycle → all messages written once |
| `56_run_recoveries_exhausted` | run fail | no-progress guard / cap → exit 2 |
| `57_run_watermark_stop` | run | D4: messages enqueued after T0 stay |
| `58_run_max_messages` | run | D1 |
| `59_run_empty_entity` | run edge | G5 header-only table, exit 0 |
| `60_run_full_load_composite_pk` | run | G2 / G3 manifest |
| `61_run_branch_id_c1` | run | `KBC_BRANCHID` set (as on every job on current stacks): C1 runs and consumes (§2.5) |
| `62_run_removed_override_key_ignored` | run | a config still carrying `destructive_in_branch` runs; the key is ignored |
| `63_run_dev_branch_peek` | run | C4 runs with `KBC_BRANCHID` set |
| `64_run_missing_creds` | run fail | auth validation |
| `65_run_failure_write_always_per_mode` | run fail | the §6.9 table: C1 / C3 failing after one settled batch leave the CSV + a `write_always: true` manifest; C1 failing before the first complete, C3 failing before any receive returned messages, C2 failing mid-run and C4 failing mid-run (peek-error injection) leave `write_always: false` |
| `66_run_flatten_failed_run_new_keys` | three-run (C2) | run 1 fails after writing rows with key `a` → CSV has no `body_a`, `body_unmapped` filled, no state; run 2 (from run 1's input state) succeeds → still no `body_a`, `body_unmapped` filled, registry saved; run 3 → `body_a` is a column |
| `67_run_flatten_discarded_state` | two-run (C4) | state discarded (e.g. the job failed after the upload): run 1 succeeds with new keys (header has no new columns); run 2 starts from run 1's **input** state → its header equals run 1's, so the next import can never miss a column |
| `68_run_unreadable_fail_writes_rest_first` | run fail (C3) | policy `fail`, one unreadable body in a batch of 3 → the 2 readable rows are in the CSV (manifest `write_always: true`), then exit 1 |
| `69_run_flatten_cap_writes_rest_first` | run fail (C3) | registry cap reached mid-batch → the whole batch is written (over-cap key in `body_unmapped`), then exit 1 |

Plus **unit tests** per module (configuration validators; redaction, the log `RedactingFilter` and
`configure_logging` levels (J7); entity paths, table names and the `EntityPath` `ValueError`
mapping; the L1 counts log line; range encoding, state merge and whole-state budget; commit
chunking / bisection / retry; K computation and scan skip rules; body decoding matrix; flatten
naming / collisions / hashed registry / cap / input-registry column rule / `body_unmapped`; column mapping and formats; manifest
fields incl. legacy vs authoritative and the `write_always` switch; unreadable retry budget and
progress accounting; session lock renewal; peek re-peek dedupe; `SCHEDULED` watermark exclusion;
C4 skip of scheduled messages pending activation and the single export after activation; every
sync action giving up within its deadline against an `unresponsive` fake namespace, §5.4).

## 9. Deployment & validation (cf-dev, Phase 7)

Strategy only — no secrets or concrete namespace / tenant / SP identifiers here. Phase 7 obtains SAS
connection strings with `az servicebus namespace authorization-rule keys list` and the
service-principal secret from the writer's gitignored `secrets.json`, and stores them only as
encrypted `#` values in the cf-dev configs.

- **Image:** CI builds the `initial-implementation` branch; use the **newest** branch build tag and
  set it as **`runtime.tag` on each cf-dev config** (via kbagent) — never promote the default tag.
- **Seeding with canonical tooling:** messages are sent with the **writer component** in cf-dev (a
  writer config per `ex-*` target): small JSON rows, 70–250 KB rows (multi-frame), rows with
  `session_id`, rows to `ex-test-topic` without a session id (they land in the session subscription's
  DLQ with "Session id is null" [live]).
- **Smoke matrix (each: job id, `success`, resolved image tag = the branch build, output rows, entity
  counts afterwards):**
  1. C1 on `ex-test-queue-large` (SAS), 70–250 KB bodies — rows byte-intact, queue drained.
  2. C2 on `ex-test-topic-sub` (SP) — run 1 defers (entity shows DEFERRED), run 2 commits (gone) and
     defers the next batch.
  3. C3 on `ex-test-queue` — at-most-once WARNING, queue drained.
  4. C4 incremental twice + full fetch on `ex-test-queue` — nothing removed; cursor advances.
  5. Sessions C1 on `ex-test-queue-session`.
  6. DLQ C1 on `ex-test-topic-sub-session` dead-letter sub-queue.
  7. C2 on `ex-test-queue-partitioned` — per-partition commit, best-effort WARNING.
  8. Flatten on a JSON-body entity, two runs with new keys — columns appended, import succeeds; and
     the Storage import checks the spec relies on (extra columns added; typed TIMESTAMP / INTEGER /
     FLOAT / BOOLEAN columns load; header vs `has_header`).
  9. (Removed: the dev-branch guard — §2.5. A default-branch job carries `KBC_BRANCHID` too.)
  10. Probes for [inferred] items (§10): an activated scheduled message keeps
      `scheduled_enqueue_time_utc`; the `enqueued_time_utc` a peeked `SCHEDULED` message reports
      (scheduled time or original enqueue time); repeated deferred receives near MaxDeliveryCount;
      column-name length handling; a C1 job whose second row fails uploads the first row's table
      (write_always) with no unregistered flatten columns.
  11. One platform **debug** job (C4, non-destructive): succeeds, and its HTTP cassette in
      `out/files` contains no secret (grep for the SAS key, `sig=`, the client secret).
- **Fresh-config UI acceptance (runtime gate):** create a new config + row in the cf-dev UI, save,
  read the stored parameters back — only user-set values and visible defaults (§5.5), gated fields
  absent while hidden, every enum stores its value, `rows ≥ 1`.
- **Sync actions** become exercisable from the UI only once a release makes the portal default tag
  carry them (writer lesson); Phase 6/7 records that.
- **Teardown:** purge the `ex-*` entities (incl. deferred leftovers); `ex-test-sub` stays until the
  round trip is done, then is deleted with the other `ex-*` entities.

## 10. Open risks & blockers

No blockers. Ranked risks:

1. **Multi-frame defect not reproduced** — intermittent in production. Mitigated by the thread-free
   profile, recycling and the unreadable guard; upgrade to 7.15 GA when released (owner: maintainer).
2. **C1 loss windows** (import failure after the job — accepted; terminated / cancelled job; hard kill
   mid-write). `write_always`, switched on before the first complete, removes the "failed run"
   window; C2 is the safe alternative and the tooltip says so.
3. **C2 orphan recovery** is bounded by inferred invariants (K rule on plain entities),
   best-effort on partitioned / sub-queues and impossible on sessions — WARNINGs make every such run
   visible.
4. **[inferred]** `write_always` read from the component manifest is honoured on failed jobs (source
   read, not live-injected); the terminated-job case uploads nothing. The manifest is rewritten when
   the switch flips; the platform reads whatever manifest exists when the container exits.
   keboola-component 1.11 deliberately drops `write_always` from the manifest on the legacy job
   queue (a project without the `queuev2` feature) [source: `interface.py` `is_legacy_queue`,
   `dao.py` `OUTPUT_MANIFEST_LEGACY_EXCLUDES`] — **[inferred]** the legacy queue does not honour it;
   the component does not override the library and warns in C1 / C3 on such projects (§6.9), and
   the Phase-7 manifest check stays.
5. **Wrong and harmless** — the assumption that a peeked `SCHEDULED` message's `enqueued_time_utc`
   may be its future scheduled time: it reports its **send time** [live, Phase 7]. The watermark
   still ignores `SCHEDULED`-state messages (a pending one is skipped in C4, an activated one may
   report `SCHEDULED`, §6.5, §6.7).
6. **Confirmed** — `scheduled_enqueue_time_utc` survives activation [live, Phase 7], so the H3 skip
   rule holds. Activation also assigns a new sequence number and enqueue time, which is why C4
   skips scheduled messages pending activation (P4-7, §6.7).
7. **Confirmed** — deferred receives count toward MaxDeliveryCount and can dead-letter the message
   [live, Phase 7]; the orphan delivery-count guard (§6.4) stays.
8. **[inferred]** Storage column-name length cap (64) and dash handling; timestamp literal format
   for typed imports; import semantics for typed tables with new columns — Phase-7 checks.
9. **[inferred]** row-level sync actions receive merged root parameters; SAS Manage can list
   entities.
10. **[inferred]** a platform debug run (HTTP recording via `keboola.vcr`) leaves the AMQP data plane
   untouched and its cassette carries no secret — Phase-7 debug job + cassette grep.
11. **[inferred]** outbound 5671 on stacks other than GCP us-east4; private-endpoint namespaces
   unreachable.
12. **Flatten + state reset / shared table** can fail the import (documented; C2 recommended).
13. **Premium > 16 MB bodies** over the management link unverified (commit one per call).

## 11. Future: AMQP over WebSockets (K2) — documentation note, no code path

K2 is excluded from v1 by maintainer decision; this note (repeated in the README) records how it could
be added on request: add one advanced root-level enum `transport` (`amqp_tcp` default,
`amqp_websocket`) mapped to `transport_type=TransportType.AmqpOverWebsocket` on every
`ServiceBusClient` the connector builds, and add the `websocket-client` dependency (the sync
WebSocket transport raises `ImportError` without it [live]). Nothing else changes — receivers,
settlement, commit and auth are transport-agnostic (a 150 KB body arrived intact over WebSocket
[live]). It is purely client-side (outbound 443 to `<namespace>.servicebus.windows.net/$servicebus/
websocket`, no Keboola infrastructure); its only benefit is a network that blocks outbound 5671 — it
does not help with IP firewalls or private endpoints. The management client is already HTTPS.

## 12. Grounding reconciliation (keboola-context)

A fresh-context subagent read every behaviour-relevant `keboola-context` reference in full and checked
this spec against it. Every `corrected:` item is folded into the sections cited.

- `[architecture-conventions.md]` → correct — config rows, `#` secrets, two extraction pickers,
  test-connection sync action, native types + PK, `UserException` exit 1, client separated from
  `component.py`, default-bucket naming (§2.2, §3.1, §5, §4-G4, §6.11, §7). (Its phrase "rows run in
  parallel" conflicts with `config-rows.md`; the spec follows `config-rows.md`: sequential by
  default, parallelism opt-in.)
- `[config-rows.md]` → corrected: the reference says "outputs from row N are committed to Storage
  before row N+1 starts"; the Phase-3 reading of docker-bundle `Runner.php` (each row's imports
  queued and awaited after all rows, state persisted for all rows at once) was in turn superseded in
  Phase 8: a multi-row configuration runs one child job per row [live, Phase 7], each with its own
  state, durable only if that row's job and its imports succeed. Folded: §2.1 states it, §2.2 says
  not to rely on row ordering (every row is self-contained), §2.1 / §6.9 justify `write_always` by the
  row's own job failing. Sequential-by-default rows, per-row state and the single merged
  `config.json` (§2.2, §6.8, §8 fixtures) follow the reference.
- `[extraction-modes.md]` → correct — `destination.load_type` always visible; `source.fetch_mode`
  gated to peek; the fetch-mode field omitted for the destructive modes under the "only one mode
  possible" clause; `date_window` not offered with the reason; both delta / staging pairings stated
  as deliberate (§2.4).
- `[incremental-state.md]` → correct — the cursor is a sequence number: T0 is captured before the
  loop (§6.1), the cursor is the highest sequence number actually written, and state is written once
  at the end and is durable only on job success (§6.7, §6.8).
- `[native-data-types.md]` → corrected: §6.9 implied that `hints` already yields binding native
  types. Folded: §6.9 now says the component builds one `schema`, the library selects the manifest
  format (reference: `none` / `hints` → legacy; library 1.11.0 source also emits `schema` for
  `hints`), and only `authoritative` — set in Phase 6 (§4-G4, §14) — makes the types binding. The
  header row + `has_header: true` pairing is stated (§6.9).
- `[encryption.md]` → correct — `#connection_string` / `#client_secret`, `KBC::ProjectSecure`,
  plaintext in the container (§2.2).
- `[default-bucket.md]` → correct — `defaultBucket: true`, no manifest `destination` (§2.2, §6.9).
- `[output-mapping.md]` → correct — `write_always` switched on in C1 / C3 before the first
  destructive settle, never in C2 / C4 (§6.9, gate lead decisions); no scratch files at all, nothing
  but the output table under `/data/out/tables/`; incremental + PK = upsert; no sliced tables
  (§2.1, §6.9, §6.10, §7).
- `[exit-codes.md]` → correct — `UserException` → exit 1, unexpected → exit 2, `__main__` guard
  (§6.11, §7).
- `[environment-variables.md]` → correct at spec time; **corrected in Phase 7**: its claim that `KBC_BRANCHID` is
  absent on the default branch is wrong on current stacks (it is set for default-branch jobs too), so the
  dev-branch guard was removed (§2.5);
  `KBC_CONFIGROWID` absent on non-row runs (§6.12), `KBC_CONFIGID` may be a hash (§6.12),
  `KBC_DATA_TYPE_SUPPORT` handled by the library (§6.9); no `forward_token` (§2.2).
- `[telemetry.md]` → N/A — about querying usage telemetry, not runtime behaviour.

## 13. Phase-2 "carried to Phase 3" items — where each is addressed

| # | Item | Addressed in |
|---|---|---|
| 1 | failed final C2 run + switch to C1/C3/C4 strands deferrals | §6.4 orphan-source list; C1/C3 foreign-deferral probe (detect-only WARNING); §6.1 + §6.8 C4 carries a leftover pending set forward untouched with a WARNING |
| 2 | K stop rule vs no-progress guard | §6.4 step 4: K = max(100, 2 × (batch_size + prefetch_count + 1)), derived from the §6.5 no-progress guard |
| 3 | commit client `retry_total=0` vs J4 | §6.3: component-level bounded retry of transient errors; exhausted → the run fails safely (H5 runs before any receive, input state intact) |
| 4 | dev-branch C2 + main C2 = two consumers | §2.5 stated plainly (branch H5 vs main H3 race) |
| 5 | `scheduled_enqueue_time_utc`-after-activation assumption | §6.4 step 3 (confirmed [live, Phase 7]); §9 item 10 probe; §10 risk 6 |
| 6 | stale "only if K2" pin wording | §3.1 / §7 pins list no `websocket-client`; §11 is the only K2 mention |

## 14. Phase gates (pointers — the standards live in the checklists)

- **Phase 4 (implementation)** — static gate: `component-checklist-review` on `architecture`,
  `typing`, `configuration`, `error-handling`, `logging`, `output-state`, `infra` (owner
  `component-develop`, `## Exit gate`). Design choices that pre-satisfy it: §7 (connector built on
  first use, thin `run()`), §5.3 (typed model, aliases, `extra`), §6.8–§6.9 (manifest + state), §6.11–§6.12.
- **Phase 5 (tests)** — `testing`, `credentials`, `output-state`; the case list is §8; the AMQP note
  replaces cassettes with the SDK-mock harness.
- **Phase 6 (portal)** — `schema-ui`, `sync-actions`, `component-config`, `configuration`; portal
  `dataTypeSupport = authoritative`, `defaultBucket = true`, sync actions registered, uiOptions
  `genericDockerUI`, `genericDockerUI-rows`.
- **Phase 7 (runtime)** — §9; no silent default in a fresh config (§5.5), enums store values,
  `rows ≥ 1`.
- **Phase 8** — full `component-checklist-review`.

## 15. Approval record (2026-09-23)

The maintainer delegated approval to the Component Factory lead, who approved this spec on
2026-09-23 with **yes to every open proposal and new decision, as written, no overrides**:

| # | Decision | Where |
|---|---|---|
| 1 | Non-JSON body with flattening on → C1/C2 dead-letter with reason `NotJson`, no reconnect retry; C3/C4 skip + WARNING; `fail` aborts | §6.6 |
| 2 | C2 orphan scan on DLQ / TDLQ and partitioned entities = best-effort capped full scan | §6.4 |
| 3 | WARNING on every C2 run where orphan recovery is best-effort (partitioned, sub-queues) or impossible (sessions), plus "incomplete" when the page cap is hit | §6.4, §6.12 |
| 4 | Orphan-scan page cap 20 pages (5,000 messages); K = max(100, 2 × (batch_size + prefetch_count + 1)) | §6.4 |
| 5 | Commit byte cap 16 MiB per deferred-receive call | §6.3 |
| 6 | Flattening: `body_` prefix, Storage-safe normalisation (ASCII fold, `[A-Za-z0-9_]`, ≤ 64 chars with hash), `_2`/`_3` collision suffixes with a `path_sha1 → column` registry in row state, monotonic column set, all flattened columns STRING, arrays as JSON, top-level non-object → `body_value`, 1,000-column cap; documented state-reset / shared-table limit, defer-commit recommended | §6.10 |
| 7 | Entity dropdowns for SP (Data Receiver) and Manage SAS; a Listen-only SAS gets an empty list and types the name | §5.4, §5.6 |
| N1 | `write_always` in the manifest, manifest before the first settle (the switch rule was refined by G1 below; flatten now streams like text, cycle 2) | §2.1, §6.9 |
| N2 | C4 `incremental_fetch` refused on partitioned, session and sub-queue entities (`full_fetch` allowed) | §2.4, §6.7 |
| N3 | D4 watermark on partitioned entities is approximate → WARNING, not a refusal | §6.5 |
| N4 | `application_properties` / `message_annotations` stay JSON; AMQP header / properties extras become scalar `amqp_*` columns; `to` → `to_address` | §4-E, §6.9 |
| N5 | On sub-queue sources `dead_letter` degrades to `leave`; new reason `BodyTooLarge` (cell > 16 MiB) | §6.6 |
| N6 | C2 state budget 256 KiB (stop + WARNING); pending sequence numbers stored as ranges | §6.5, §6.8 |
| N7 | Orphan recovery guard on the broker's `delivery_count` (≥ max_delivery_count − 1, else 9) | §6.4 |
| N8 | C4 skips expired-but-not-purged messages and counts them | §6.7 |

The 17 exclusions in §4 were signed off together with these decisions.

**Amendments after Phase-3 gate cycle 1 (lead decisions, 2026-09-23 — supersede N1 where they
differ):**

| # | Decision | Where |
|---|---|---|
| G1 | `write_always` starts `false`; C1 switches it on just before the first complete, C3 when its first receive returns messages (cycle-2 refinement); never in C2 / C4; per-mode × load-type table | §2.1, §6.1, §6.9 |
| G2 | Flatten mode carries the reserved JSON column `body_unmapped`; a run materialises the input-state registry columns + `body_unmapped` — superseded in detail by G4 | §6.10 |
| G3 | Unreadable-body retries have their own recycle budget; dispositions count as progress; the abort share is reachable before any cap | §6.5, §6.6 |
| G4 (cycle 2) | In **every** mode a run writes exactly the input-state registry columns + `body_unmapped`; new keys always go to `body_unmapped` in the run that discovers them and are columns from the next run (the cross-row state discard is independent of `write_always`) [Phase 8: state is per row — the cross-row reason is withdrawn; the rule stands on the job failing after a `write_always` upload and on import failures, §6.9 / §6.10] | §6.9, §6.10 |
| G5 (cycle 2) | `fail` policy and the flatten cap: record, write and settle the readable rest of the batch, then raise | §6.6, §6.10 |

**Decisions the author made while applying them (flagged to the lead):**
- (Cycle 1, superseded by G4) promotion deferred only on `write_always` tables.
- `body_unmapped` is keyed by the registry column name (cycle 2; it was the dot-path, which let
  `"a.b"` collide with nested `a → b`), with the same string renderings as the columns.
- Cycle 2: the registry stores `path_sha1 → column` (bounded ≤ ~130 KiB at the cap) instead of the
  path; flatten rows stream like text rows (the column set is fixed at run start); C3 arms
  `write_always` at the first receive that returns messages; the C4 cursor is the highest processed
  sequence number; C4 full fetch on partitioned / session entities does not retry an unreadable body
  (the next full fetch re-reads it); the legacy-queue `write_always` fallback (superseded by
  Phase-4 amendment 3 below: no override, WARNING instead).
- Unreadable retry budget = 50 recycles per run; when exhausted, first failures take the final
  disposition directly.
- C4 re-peek after an unreadable retry resumes at the first unreadable sequence number and skips
  sequence numbers already written (no duplicate rows).
- `SESSION_ACCEPT_WAIT_SECONDS = 5` for every session receiver outside the destructive receive loop.
- `ValueError` is mapped only at its two known sources (connection-string parse, `EntityPath`
  mismatch); elsewhere it is a bug (exit 2).

**Amendments carried into Phase 4 (lead, from the Phase-3 gate, 2026-09-23):**

| # | Decision | Where |
|---|---|---|
| P4-1 | Before the batch processor's `fail` / flatten-cap / abort-share `UserException` leaves the receive loop, the stop drain runs; in C3 the buffered, already-deleted messages are written, so "nothing is lost in C3" holds | §6.5, §6.6, §6.10 |
| P4-2 | That `UserException` is never recycled as a connection failure (no recovery counted; C4 propagates it directly) | §6.5 |
| P4-3 | Legacy job queue: the library's deliberate omission of `write_always` is respected (no manifest override); C1 / C3 log a WARNING that the failed-run safety net is unavailable; platform reason labelled [inferred] | §6.9, §10 |
| P4-4 | Over-cap provisional flatten names are reused per path hash for the whole run | §6.10 |
| P4-5 | JSON key `type` vs metadata column `body_type`: the registry reserves every metadata name, so `type` → `body_type_2` (unit-tested); the prefix claim is corrected | §6.10 |
| P4-6 | Approval item 6 wording: `path_sha1 → column` registry | §15 |
| P4-R3 | `to_user_exception` also maps management-plane `AzureError`s (denied → names the Data Receiver role / Manage rights; `ResourceNotFoundError` → not found) | §6.11 |
| P4-7 | (After the Phase-7 probe: activation gives a scheduled message a new sequence number.) C4, both fetch modes, skips `SCHEDULED` messages pending activation, counts them as `skipped_scheduled` and exports them once active, under their new sequence number; the `state` column keeps the broker-reported value | §4-A7, §6.5, §6.7, §6.9, §6.12, §10 |
| P4-8 | (Phase 7: `KBC_BRANCHID` is set on default-branch jobs too on current stacks — a default-branch C3 job was refused; the platform exposes no branch type / name / default flag and, without `forward_token`, the default branch id cannot be resolved.) The automatic dev-branch guard and the hidden `destructive_in_branch` override are removed; every mode runs in every branch. Documented instead: a dev branch reads the same production entity — use Peek or a separate test entity in branches; admins can enable `dev-branch-configuration-unsafe`. The keboola-context `environment-variables.md` claim "absent on the default branch" is wrong on current stacks | §2.5, §4-J11, §5.2, §5.5, §6.1, §6.11, §8, §9, §12 |
| P4-9 | (Phase-8 audit follow-up.) A malformed service-principal `tenant_id` makes azure-identity's `ClientSecretCredential` raise `ValueError` before any network call; it is mapped to a `UserException` in the connector's credential builder (a third known `ValueError` source) instead of exiting 2 | §6.11 |
| P4-10 | (Phase 8, maintainer report: a row-level **Test Connection** answered "Internal Server Error" while Preview Messages worked.) Not reproducible afterwards with the same payload (local, production image, platform UI); the namespace was throttled during a concurrent 1M-message drain at the time. The component can only exit 0 / 1 inside a sync action, so an HTTP 500 means the action outlived the platform's 30-second limit. Every sync action now gives up after 20 seconds with a user error (§5.4); a reproducible instance was found live — a session entity without an available session took ~34 s through the SDK's accept retries — and that peek no longer retries. The UI's use of `runtime.tag` for sync actions is recorded as verified | §5.4 |
| P4-11 | (Phase 8, maintainer decision.) The row form's **Test Connection** and **Show Entity Details** buttons are removed, and so is the `entityInfo` action; `testConnection` is the root management probe only (a row source is ignored) and a Listen-only SAS is pointed at the row's **Preview Messages**, which proves entity access | §4-I, §5.2, §5.4, §5.6, §8 |
| P4-12 | (Phase 8, maintainer decision.) **Max Duration** moves to the Advanced section (`advanced.max_duration_seconds`) with default 3000 s (was `limits.max_duration_seconds`, 3600): below the default one-hour job timeout, which the component is not told, leaving time for the import. Like every advanced value it is its default while `advanced_options` is off; a leftover `limits.max_duration_seconds` is ignored | §2.1, §4-D, §5.2, §5.5 |
| P4-13 | (Phase 8, live: a 1M-message C1 run stopped `idle` after 77,077 messages with 927,756 still active.) An empty receive is verified by a peek before the run stops idle; if receivable messages remain, the loop reopens the connection, backs off and continues (at most 5 times in a row, then `receive_stalled`); a stop that leaves messages behind logs a WARNING with the remaining count; throttling (ServerBusy) is counted quietly into the summary with a tier hint; the idle-timeout tooltip no longer says an empty result means drained | §5.2, §6.5, §6.11, §6.12 |
