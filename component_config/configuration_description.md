### Authentication

Set once for the whole configuration. Choose **Connection string (SAS)** (a policy with Listen rights) or **Service principal (Entra ID)** (tenant ID, client ID, client secret, namespace host name; needs the **Azure Service Bus Data Receiver** role), then fill in the fields for that method.

### Source (per row)

Each row reads one queue, one topic subscription, or one of their dead-letter / transfer dead-letter sub-queues:

- **Entity type & name** — the queue, or the topic and subscription, to read from. The entity must already exist in the namespace.
- **Sub-queue** — read the entity's dead-letter or transfer dead-letter queue instead of the main entity.
- **Sessions** — enable when the entity requires sessions (not available on sub-queues, which never require sessions).
- **Settlement mode** — `complete` (delete right after each batch is written, default), `defer_commit` (delete only once the next run proves the previous import succeeded — requires this row to be the entity's only consumer), `receive_and_delete` (delete on receive, at-most-once), or `peek` (never delete — for browsing an entity someone else consumes).
- **Fetch mode** (peek only) — `incremental_fetch` (sequence-number cursor; not available on partitioned or session entities, or sub-queues) or `full_fetch` (re-peek the whole entity every run).
- **Idle timeout** — how long one receive call waits before the entity is treated as drained (destructive modes only).

### Limits

- **Max messages** — stop after this many messages (`0` = no limit).
- **Max duration** — stop after this many seconds.
- **Stop at job start** — stop once a batch holds only messages enqueued after the job started, instead of also draining messages that arrive mid-run (approximate on partitioned and session-enabled entities).

### Body

- **Body format** — `text`, `base64`, or `json_flatten` (flatten a JSON body into columns). Changing it changes the output columns — drop the existing table first.
- **Unreadable body** — `dead_letter`, `leave` in place, or `fail` the run.

### Destination

- **Table name** — empty derives a name from the entity.
- **Load type** — `incremental_load` (upsert, default) or `full_load` (replace the table with this run's messages).
- **Primary key** — `sequence_number` (default), `message_id` (producer-set: may be empty or reused, in which case upserts merge different messages), or the source entity + sequence number pair (for a table shared by several rows). Changing it changes the output key — drop the existing table first.

### Advanced

Batch size, prefetch count, and an optional post-recovery catch-up wait. The defaults suit most entities; lower the batch size when messages carry large bodies.

Development branches read the same production Service Bus entity as the default branch: a destructive settlement mode (everything except `peek`) run in a branch consumes and removes production messages. Use Peek Only in branches, or point the branch configuration at a separate test entity. Project admins can enable the `dev-branch-configuration-unsafe` feature to have the platform guard branch runs.
