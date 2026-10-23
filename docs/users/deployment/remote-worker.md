# Remote workers

This page covers running `flowfile_worker` on a host other than core's: what the worker and core need in their environment, how core reaches the worker, and what does not run there. It assumes the [Docker deployment](docker.md). The design behind it is the [compute protocol](../../for-developers/compute-protocol.md).

## How it works

Core sends each node's plan to the worker and takes back a bounded sample of the rows, the schema and the row count. It never opens a file the worker wrote: results stay in the worker's own storage, except a result that crosses to another backend through an [exchange store](#nodes-on-several-backends). The worker reads sources and writes destinations itself, and resolves a stored connection from the catalog database with the master key, so a plan carries no credentials. All of this needs `FLOWFILE_COMPUTE_PROTOCOL` on, which is core's default.

A node's result, the outputs of its operation (a random split's groups, a custom node's outputs, a fitted model) and what it stored beside them stay on the worker while core holds them, and go together: when the node runs again with other settings, when it is removed or its flow is closed, when a published API call has answered, when a headless run exits, and when the node fails after storing part of its outputs. A core that stops without closing its flows leaves them on the worker until its age sweep, which forgets a result 24 hours after it was made (`CACHE_EXPIRATION_TIME`), the next time the worker receives work. The sweep is the backstop for a core that died, not how a result is meant to go.

## The worker's environment

The `flowfile-worker` image starts `python -m flowfile_worker --host 0.0.0.0` and listens on port `63579`; `--host` and `--port` change both.

| Variable | Value |
|---|---|
| `FLOWFILE_MODE` | `docker`. The image sets it. Only in this mode does the worker read the master key from its environment (or the `flowfile_master_key` Docker secret). |
| `FLOWFILE_INTERNAL_TOKEN` | Core's value. The worker refuses a request without it on every route except its API docs (`/docs`, which the image's health check calls, `/redoc` and `/openapi.json`) and `POST /shutdown`, which it accepts only from a loopback address; see [Add the worker to core](#add-the-worker-to-core). |
| `FLOWFILE_MASTER_KEY` | Core's value. |
| `FLOWFILE_DATABASE_URL` | The PostgreSQL catalog core uses, for example `postgresql://flowfile:<password>@<catalog host>:5432/flowfile`. |
| `CORE_HOST`, `CORE_PORT` | Where the worker posts node log lines to core (`/raw_logs`, signed with its token). `CORE_PORT` defaults to `63578`. |
| `FLOWFILE_STORAGE_DIR` | The worker's own storage, where it keeps results. Defaults to `/app/internal_storage` in Docker mode. |
| `FLOWFILE_WORKER_LOCAL_FILES` | Leave unset. Set to `1`, the worker declares local files among the storage it reads and writes, which is true only for a worker on core's host. |
| `FLOWFILE_WORKER_TLS_CERT`, `FLOWFILE_WORKER_TLS_KEY` | Optional: the paths of a PEM certificate and its private key in the worker's container. Set both, and the worker serves `https` and `wss` itself; see [TLS](#tls). |

The worker reaches the catalog database and every database, Kafka broker, REST endpoint and storage endpoint the flows use, by the names their connections use. For an Azure service principal or a GCS service account it also reaches the provider's token endpoint, since it exchanges the stored secret for a bearer token itself. Core reaches the worker's port.

A connection names one endpoint for every process that opens it, so the name must lead to the service from where each process runs: core's container and the worker's. A connection whose endpoint is the worker host's own LAN address, for example, times out from the worker's container when that host's firewall drops traffic from Docker's bridge network to the host, which is ufw's default, while core on another host reaches it. When the storage service runs in a container on the worker's host, give it a network alias on the worker's Docker network, add the same name to core's `extra_hosts` with the worker host's address, and use that name in the connection. Until then a read through the connection retries for about a minute before its node fails.

## Core's environment

| Variable | Value |
|---|---|
| `FLOWFILE_DATABASE_URL`, `FLOWFILE_MASTER_KEY`, `FLOWFILE_INTERNAL_TOKEN` | The worker's values. |
| `FLOWFILE_WORKER_URL` | The default worker's URL, for example `http://<worker host>:63579`. Without it core uses `http://<WORKER_HOST>:<FLOWFILE_WORKER_PORT>`, the port defaulting to `63579`. |
| `FLOWFILE_COMPUTE_BACKEND` | Optional: a `worker:<name>` id (see [Add the worker to core](#add-the-worker-to-core)) that becomes the default backend. |
| `FLOWFILE_COMPUTE_PROTOCOL` | Leave unset. Switched off, core opens the worker's result files itself, and a worker on another host does not share them. |
| `FLOWFILE_EXCHANGE_URI` | Optional: an `s3://` root where results cross between backends, for example `s3://flowfile-exchange/runs`. Set, each node runs on the backend its own **Compute backend** names; see [Nodes on several backends](#nodes-on-several-backends). |
| `FLOWFILE_EXCHANGE_CONNECTION` | With `FLOWFILE_EXCHANGE_URI`: the name of a stored S3 cloud connection an administrator owns, for example `exchange`. Core and every worker open the store with it whoever runs the flow, so no user needs a connection of that name. |
| `FLOWFILE_EXCHANGE_MAX_ROWS` | Optional: the most rows an input may cross between backends with, for example `50000000`. Core checks it before the run from the row counts it already has, so an edge whose size core cannot know before the run crosses at any size. Unset, there is no limit. |

## The shared catalog and the master key

Core and the worker use one PostgreSQL catalog database. A SQLite catalog is a file on core's host, so it serves only a worker on that host. The worker reads a stored cloud connection from the catalog when a plan names it; on a catalog of its own, the read fails with *A worker resolves a plan's cloud connection against the catalog core writes, so it must use core's catalog database.*

Both services hold the same master key, because the worker decrypts a stored connection's secrets when the node runs. With another key, a cloud read fails with a message that says *Core and the worker must share the same master key.*

Connections with the `aws-cli`, `iam_role`, `env_vars` or `managed_identity` method store no credentials of their own and authenticate as the host that runs the node. On a remote worker that is the worker host's AWS profile or role, environment, or managed identity, not core's.

## Storage a remote worker can use

A worker on another host reads object storage (`s3`, `az`, `gs`) and `http` and `https` URLs, and writes object storage and the artifact store's presigned `https` URLs. It does not read or write local files. Core compares every node of a run with that before the run starts; a node the worker cannot run stops the run before any node starts, and its row on the run report says `cannot run on backend "<id>": <reason>`, then which setting runs the node elsewhere: its **Compute backend** under **General Settings**, the flow's **Execution location** under **Flow Settings**, or either.

- **Catalog tables** must be on object storage. A catalog gets its storage when it is created: the **Object storage (optional)** path and **Storage connection** in the **Create Catalog** dialog. A catalog created without either takes `FLOWFILE_CATALOG_STORAGE_URI` and `FLOWFILE_CATALOG_STORAGE_CONNECTION` from core's environment when they are set. A **Read from Catalog** or **Write to Catalog** node on a catalog that keeps its tables on local disk, the seeded **General** catalog included, is refused with `namespace "<name>" is on local disk; remote execution needs an object-storage namespace`. Core also reads and commits a catalog's tables itself, with its own access: the freshness check at the start of a run, enabling a table's change feed, and reading an SCD2 write's result. So core needs access to the buckets of its object-storage catalogs, through the catalog's storage connection; credentials that only the worker holds do not give it that.
- **Files**: a **Read data** or **Write data** node on a local path is refused (`it reads storage this backend does not read: file`), and so is a file uploaded through the File Manager, which is a local path on core's host. URLs and object-storage paths work.
- **Models**: a **Train Model** node with **Save to catalog** on, and an **Apply Model** node whose **Model source** is **From catalog**, move the model between the worker and the artifact store. That needs the S3 artifact store on core (`FLOWFILE_ARTIFACT_STORAGE=s3`, `FLOWFILE_S3_BUCKET`, and `FLOWFILE_S3_ENDPOINT_URL` for an S3-compatible store) on an `https` endpoint that the worker reaches by the same name, because core presigns the URLs the worker uploads to and downloads from. The worker needs no credentials for the store; a private certificate authority goes in the worker's `REQUESTS_CA_BUNDLE`. With the filesystem store, the default, both nodes are refused. A model trained and applied in one flow without the catalog stays on the worker and needs neither.

## Versions and the handshake

Core checks each worker the first time it uses it after it starts, and again after a call cannot reach it. The check compares the worker's versions with core's and runs a few small plans on the worker: one per expression plugin (`polars_grouper`, `polars_ds`) and one with a Python function. The **Handshake** button on the worker's row on the Backends page runs the same check on demand; run it after adding a worker and after upgrading either side.

- **Refuses the worker.** The worker contract (the protocol version: the shape of everything core and the worker exchange), the Polars version, and the Polars runtime's index width. A worker refused by one of them runs nothing, and the error names each difference with both values: `Backend "<id>" does not match core (<field>: core <value>, backend <value>; …)`.
- **Refuses some nodes on the worker.** A plugin the worker does not have, has in another version, or cannot load from the path core's plans carry; and a Python version other than core's, to the patch release, when core's Python functions do not run there. The nodes that need the missing piece are refused on that worker before the run starts, with the reason, and every other node runs there: a plugin refuses the nodes that use it, and Python refuses the formula nodes (**Formula**, **Multi-field formula**, and **Filter data**, **Gate** and **Rename columns** when they use a formula) and a cloud storage read of a table's changes.
- **Shown, not compared.** The app version, the platform (operating system and CPU architecture) and the plugins' library paths.

Every configured worker is listed for every user under **Execution location** and a node's **Compute backend**, with what its last check said beside the id: `ok`, `ok, without <what it lacks>`, `refused: <why>`, or `not checked`.

Core and worker images of one release, built together, match. Two releases that speak one worker contract work together too, since the app version is not compared, and a worker refuses a request field it does not know rather than ignoring it. Upgrade the workers when a release changes the protocol version or Polars. A desktop app, or a Windows core, with a worker in a Linux container keeps plugins at other library paths, so the nodes that use a plugin, the formula nodes among them, are refused there.

## Add the worker to core

There are two ways:

- **As the default worker.** Point core at it with `FLOWFILE_WORKER_URL`, or `WORKER_HOST` and `FLOWFILE_WORKER_PORT`. Flows whose **Execution location** is **Auto (default worker)** run there.
- **On Settings → Execution → Backends** (administrators only). Under **Add a worker**, enter a **Name** (the worker's id becomes `worker:<name>`), its **URL** (`http://` or `https://`), its **Token** and, for an `https://` worker whose certificate is not public, its **CA certificate (PEM, optional)** (see [TLS](#tls)), click **Add**, then click **Handshake** on its row (see [Versions and the handshake](#versions-and-the-handshake)). While the URL is plain `http://` to a host other than `localhost`, `127.0.0.1` or `[::1]`, the page warns under it that the token and every plan cross the network in clear. The id then appears under **Execution location** in [Flow settings](../visual-editor/building-flows.md#flow-settings) for every user, with its last check beside it, and a flow set to it runs there. Setting core's `FLOWFILE_COMPUTE_BACKEND` to the id makes it the default backend instead.

To change a worker's URL, token or CA certificate, **Remove** it and add it again. A row changed in the catalog database directly is not seen by a running core that has already used that worker, until core restarts.

!!! warning "The token is code execution on the worker"
    A plan can carry Python code that the worker runs when it loads the plan, and an operation runs node code there, so whoever holds the token and reaches the worker's port can run code on the worker, with its access to the catalog database, the master key and every stored connection. Keep the worker's port off untrusted networks, and beyond a trusted network serve the worker over [TLS](#tls), so neither the token nor a plan crosses in clear. The single-host `docker-compose.yml` stays plain HTTP inside its own Docker network.

    Enter core's `FLOWFILE_INTERNAL_TOKEN` as the **Token**, and start the worker with that same value. The worker accepts one token, and core signs some of its calls to every worker with its own internal token: the writes, the Delta operations, the visualisation sessions and the catalog services. The version check signs with the worker's own **Token**.

## TLS

Given a certificate and its key, the worker serves `https` and `wss` itself, so a worker with a self-signed certificate needs no proxy in front of it. Set `FLOWFILE_WORKER_TLS_CERT` and `FLOWFILE_WORKER_TLS_KEY` in [the worker's environment](#the-workers-environment) and add the worker with an `https://` URL whose host the certificate names. Core verifies the certificate against the public certificate authorities, or, for one they do not sign, against the **CA certificate** on the worker's row: paste the certificate of the authority that signed it, or the self-signed certificate itself, as PEM. Core then trusts that certificate for that URL on every call it makes to the worker. A worker whose certificate core cannot verify is unreachable: its **Handshake** says core could not reach it, naming `CERTIFICATE_VERIFY_FAILED`. A proxy in front of the worker that terminates TLS still works, with the proxy's certificate.

The default worker (`FLOWFILE_WORKER_URL`) has no CA setting: add a worker with a private certificate on the Backends page.

The image's health check calls `http://localhost:63579/docs`, which a worker serving TLS does not answer; override it, for example with `curl -fk https://localhost:63579/docs`.

## Nodes on several backends

With an exchange store configured (`FLOWFILE_EXCHANGE_URI` and `FLOWFILE_EXCHANGE_CONNECTION` in [core's environment](#cores-environment)), each node runs on the backend its own **Compute backend** names ([A node's own backend](../visual-editor/building-flows.md#a-nodes-own-backend)), and a node left on **Auto** runs on the flow's. Without one, every node of a flow runs on the flow's backend.

Where an edge joins two nodes on different backends, the upstream result is exported to `<uri>/<flow id>/<run id>/`, and the node that reads it imports it on its own backend and stores its own result there, so nothing refers to the exported file after the run. Core removes the run's prefix when the run ends, whether the run succeeded or failed. A store core cannot use (the URI without a connection, a scheme other than `s3`, a connection name no administrator owns) refuses the reading node before any node starts, and its row on the run report says `cannot run on backend "<id>": <reason>`, then which setting runs the node elsewhere: its **Compute backend** under **General Settings**, the flow's **Execution location** under **Flow Settings**, or either.

`FLOWFILE_EXCHANGE_MAX_ROWS` is a best-effort limit. Core checks it before the run against the row counts it already has: from an earlier run of the open flow while nothing up to the upstream node has changed, or a **Manual input**'s own rows. An edge whose size core cannot know before the run, as on a flow's first run after it is opened, crosses at any size. Above the limit the reading node is refused before any node starts with `the input from node <n> would cross from backend "<id>" with <rows> rows of <columns> columns, above FLOWFILE_EXCHANGE_MAX_ROWS=<limit>`. In **Development** mode it can also refuse a node whose input has not changed since the last run, when nothing would cross.

A core that stops in the middle of a run leaves that run's prefix in the bucket, and so does a removal that fails, which is a warning in the flow log. Give the bucket a lifecycle rule that expires objects under the URI's prefix after longer than the longest run takes, a day for example.

Core and every worker open the store at its connection's endpoint, so that endpoint must lead to the store from each of them; see [The worker's environment](#the-workers-environment).

## Limitations

- **Kernel nodes.** A **Python Script** node, or a custom node that runs on a kernel, is refused on a remote worker: `it runs on a notebook kernel, which reads its inputs from the kernel shared volume on core's host, where this backend does not write`. A flow with such a node runs on a worker on core's host.
- **Plugin layout.** On a worker whose plugins sit at other library paths than core's, the nodes that use a plugin are refused (see [Versions and the handshake](#versions-and-the-handshake)); the worker still runs every other node.
- **Headless and scheduled runs.** `flowfile run flow`, and a run a schedule starts, compute on a worker embedded in their own process, which for a scheduled run is inside core's container, unless the run passes `--backend <id>` or the flow's **Execution location** is a backend id. A remote worker runs a scheduled flow's nodes only when the flow names it; a **Google Analytics** read is the exception, running on the default worker in every run. A named worker that cannot be reached fails the run, which does not compute in its own process instead.
- **Node logs.** A node's log lines reach core's flow log only when the worker reaches `CORE_HOST` on `CORE_PORT` with core's token; otherwise they stay in the worker's own output.

Hosting a worker on Kubernetes or behind an ingress, and running and scaling several workers, are outside this page.
