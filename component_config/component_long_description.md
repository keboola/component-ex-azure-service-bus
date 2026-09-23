The Azure Service Bus extractor reads messages from an Azure Service Bus **queue** or **topic subscription** — or their dead-letter / transfer dead-letter sub-queues — and writes them to a Keboola Storage table, one row per message, with the broker metadata as typed columns and the body as text, base64, or flattened JSON.

Configuration is row-based: the connection to the namespace is set once at the configuration level, and each row reads one source entity into one destination table.

**Authentication** — connect with a shared access signature (SAS) connection string with Listen rights, or an Entra ID service principal (tenant ID, client ID, client secret, namespace host name) with the **Azure Service Bus Data Receiver** role.

**Settlement mode** picks how messages leave the source and what guarantee a run gives: `complete` (default; deletes each batch right after it is written, at-least-once), `defer_commit` (deletes only once the next run proves the previous import succeeded — at-least-once end to end, but requires this row to be the entity's only consumer), `receive_and_delete` (deletes on receive, at-most-once), or `peek` (never deletes — for browsing an entity someone else consumes).

**Body & destination** — read the body as raw text, base64, or flatten a JSON body into columns; load incrementally (upsert, default) or fully replace the table each run; pick the primary key that fits how the table is used.

Destructive settlement modes refuse to run in a development branch unless explicitly overridden in debug mode, since they consume and remove production messages.

Use the **Test Connection**, **Preview Messages**, and **Show Entity Details** actions to verify credentials, inspect sample messages, and check entity properties before running. The source entity must already exist in the namespace — the extractor does not create it.
