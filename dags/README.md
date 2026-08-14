# Demo DAGs

Apache Airflow **3.x** examples covering file-transfer patterns: uploading to an
FTPS server, waiting on a file with a sensor, streaming between two servers, and
streaming into Azure Blob Storage.

They also work through a progression in **how much of a provider you reuse** —
using an operator as shipped (#1), extending a sensor (#7), overriding one method
of a transfer operator (#4), and writing an operator from scratch when the
provider has nothing (#5, #6).

Each DAG is a **standalone single file** — no shared helper module, no subfolders,
no cross-DAG imports. Copy one out of this repo and it still works. That is why
`MyFTPSHook` appears in more than one file: duplicating a small helper is
preferred here over coupling two demos together.

Written for a **KubernetesExecutor** deployment, where every task runs in its own
ephemeral pod. Most of the design decisions here follow from that; see
[Two constraints](#two-constraints-that-shape-every-dag-here).

---

## Run them in this order

The DAGs build on each other. Run top to bottom the first time.

| # | DAG | Demonstrates | Depends on |
|---|---|---|---|
| 1 | `dag_ftps_simple_transfer.py` | provider operator, file upload | — |
| 2 | `dag_ftps_sensor.py` | sensor in reschedule mode | a file uploaded by #1 |
| 3 | `dag_ftps_to_sftp_stream_transfer.py` | streaming between two servers | a file uploaded by #1 |
| 4 | `dag_sftp_to_blob_stream.py` | streaming into object storage | a file in the SFTP source directory |
| 5 | `dag_ftps_to_blob_stream.py` | bridging a push API to a pull API | a file uploaded by #1 |
| 6 | `dag_blob_to_sftp_stream.py` | streaming back out of object storage | a blob in the container (#4 or #5 writes one) |
| 7 | `dag_wasb_prefix_suffix_sensor.py` | extending a provider sensor | a blob in the container |
| 8 | `dag_cyclic.py` | non-overlapping scheduled runs | — |

**Start with #1.** It uploads a file to the FTPS server. Both #2 and #3 expect a
file to already be there, so running them first means the sensor waits out its
timeout and the stream transfer fails with "not found".

**#4/#5 → #6/#7 chain**: #4 and #5 both write a blob into the container; #6
streams one back out to SFTP and #7 finds it there. #4 needs a file in its SFTP source directory first — see
[SFTP source directory](#sftp-source-directory-4); #3 delivers into the SFTP
user's `incoming/`, so pointing #4 at that path chains all three. #5 reads from
the FTPS server, so it only needs #1 to have run.

**#4, #5 and #6 are the same problem with different plumbing** — read them
together. Which side controls the loop decides the design: a source that hands
back a readable composes straight into a destination that pulls (#4, #6), while a
source that pushes to a callback needs a pipe in between (#5). See
[The pipe is not always needed](#the-pipe-is-not-always-needed-read-4-5-and-6-together)
below.

**#8 needs no connection at all** and is the only scheduled DAG here; the rest are
manual-trigger only.

---

## Prerequisites

Create these before running anything. Names are what the DAGs reference — change
them in one place at the top of each file if yours differ.

### Airflow Connections

| Conn ID | Type | Fields |
|---|---|---|
| `ftps_test_001` | **`FTP`** | host, login, password, port `21` |
| `sftp_test_001` | `SFTP` | host, login, password, port `22` |
| `wasb-nickstorageairflow002` | `wasb` | login = storage account; SAS in extra (#4, #5, #6, #7) |

For SAS auth, put the token in the connection's **extra** as
`{"sas_token": "?sp=...&sig=..."}` and set **login to the storage account name** —
`WasbHook` builds the account URL from it. The container is not part of the
connection; it is passed per operation.

> **There is no `FTPS` connection type.** The `ftp` provider registers
> `conn_type="ftp"` for both `FTPHook` and `FTPSHook`. TLS is selected in code by
> which hook you import, not by the connection. Choosing `FTP` in the UI is correct.

### Airflow Variable

| Key | Value |
|---|---|
| `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

Only needed if your FTPS server uses a **self-signed or private-CA** certificate.
With a publicly trusted cert you can drop `MyFTPSHook` and use the stock
`FTPSHook`/`FTPSFileTransmitOperator` directly.

### Host addressing

Use an address the **worker pods** can resolve. A hostname that works from your
laptop (VPN, `/etc/hosts`, mesh network) often does not resolve inside the cluster
— use the IP or an in-cluster DNS name in the connection.

### Providers

```
apache-airflow-providers-ftp                 # for #1, #2, #3, #5
apache-airflow-providers-sftp                # for #3, #4, #6
apache-airflow-providers-microsoft-azure     # for #4, #5, #6, #7
```

Both must be in the **Airflow image**, not just your local venv — they cannot be
`pip install`ed into a running worker.

### Fixture file

`files/probe.txt` ships with the repo and is uploaded by #1. The DAG folder is
mounted into every pod, so `dags/files/` is readable at
`<AIRFLOW_HOME>/dags/files/` at runtime.

### SFTP source directory (#4)

#4 reads from a directory on the **SFTP server** — not from `dags/files/`. Create
it and drop a file in before the first run:

```bash
sftp <sftp-user>@<sftp-host>
sftp> mkdir outgoing
sftp> put probe.txt outgoing/
```

The directory must exist and the connection's user must be able to read it. Note
that #4's default source is the SFTP user's `outgoing/`, while #3 *delivers* into
`incoming/` — point #4 at `incoming/` if you want to chain them directly.

---

## 1. `dag_ftps_simple_transfer.py`

`nix-dag-ftps-simple-transfer` — uploads a file from the DAG folder to FTPS.

```
dags/files/probe.txt  ──put──▶  FTPS /upload/probe.txt
```

Trigger from the UI, or:

```bash
airflow dags trigger nix-dag-ftps-simple-transfer
```

Optional conf: `{"filename": "other.txt"}` — the file must exist in `dags/files/`.

**Pattern: prefer a provider operator over `PythonOperator`.** This uses
`FTPSFileTransmitOperator` from the `ftp` provider rather than a hand-written
callable. You get templated fields, logging, and retry handling for free.

**Pattern: needing a custom hook is not a reason to abandon the operator.**
Subclass it and override the `hook` property — two lines, and the operator's own
logic stays intact:

```python
class MyFTPSFileTransmitOperator(FTPSFileTransmitOperator):
    @cached_property
    def hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)
```

---

## 2. `dag_ftps_sensor.py`

`nix-dag-ftps-sensor` — waits for a file to appear, then reports its size and mtime.

```
FTPS /upload/<file>  ──poll every 60s, up to 1h──▶  report
```

```bash
airflow dags trigger nix-dag-ftps-sensor --conf '{"filename":"probe.txt"}'
```

**Pattern: always `mode="reschedule"` with an explicit `poke_interval` and
`timeout`.**

```python
FTPSSensor(
    task_id="wait_for_file",
    path="/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}",
    mode="reschedule",   # frees the worker slot between pokes
    poke_interval=60,
    timeout=60 * 60,
)
```

`mode="poke"` holds a worker for the entire wait — on KubernetesExecutor that is a
pod sitting idle for an hour. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

To see it actually wait, trigger with a filename that does not exist yet, then run
#1 to upload it and watch the sensor pick it up on its next poke.

---

## 3. `dag_ftps_to_sftp_stream_transfer.py`

`nix-dag-ftps-to-sftp-stream` — streams a file between two servers.

```
FTPS  ──get──▶  stream  ──put──▶  SFTP
```

```bash
airflow dags trigger nix-dag-ftps-to-sftp-stream
```

**Pattern: get and put are one task, not two.** Separate tasks land in separate
pods, so a file downloaded by "get" would not exist for "put".

Bytes move through an `os.pipe()` from FTPS `retrbinary` straight into paramiko's
`putfo` on a worker thread. Both sockets are open at once, nothing touches disk,
and peak memory is one 8 KiB chunk regardless of file size — a multi-GB file
transfers in constant memory. `BUFFER_LIMIT` caps a single transfer rather than
letting it run unbounded.

This is one of the few cases where `PythonOperator` is right: the provider transfer
operators move between a remote host and **local disk**, so none of them does a
direct server-to-server hop.

**The source is not deleted** after a successful transfer, so re-running re-sends
the same file. That is deliberate — a failed put must not lose the only copy. For a
real pickup pattern, delete only after the verify step confirms the size matches.

---

## 4. `dag_sftp_to_blob_stream.py`

`nix-dag-sftp-to-blob-stream` — streams a file from SFTP into an Azure Blob
container without staging it on disk.

```
SFTP  ──open──▶  stream  ──upload──▶  Blob container
```

```bash
airflow dags trigger nix-dag-sftp-to-blob-stream \
  --conf '{"source_path":"/home/airflowsftp/outgoing/probe.*","blob_prefix":"incoming/"}'
```

`source_path` must be a **directory or a wildcard**, never a bare filename — see
the trap below. `.../outgoing/*.csv` transfers a whole matching set in one run.

**Pattern: override the one method that is wrong, keep the rest of the operator.**
The provider *does* ship `SFTPToWasbOperator` — but its `copy_files_to_wasb`
downloads each file to a `NamedTemporaryFile` before uploading it, so the full file
lands on the worker pod's disk. Only that method needs replacing:

```python
class StreamingSFTPToWasbOperator(SFTPToWasbOperator):
    def copy_files_to_wasb(self, sftp_files):
        # get_managed_conn(), not get_conn() — see trap 2
        with self.sftp_hook.get_managed_conn() as sftp_client:
            for file in sftp_files:
                size = sftp_client.stat(file.sftp_file_path).st_size
                with sftp_client.open(file.sftp_file_path, "rb") as remote:
                    remote.prefetch(size)       # else one round-trip per read
                    wasb_hook.upload(
                        container_name=..., blob_name=..., data=remote,
                        length=size,
                        max_concurrency=1,      # >1 needs a seekable source
                    )
```

Wildcard expansion, blob naming, templated fields and `move_object` are all
inherited unchanged — a future provider fix to any of them still applies here.

**`WasbHook.upload` takes a file object, not just a path.** It reads a 4 MiB block,
uploads it, then reads the next, so peak memory is one block regardless of file
size. Contrast #3, which needed an `os.pipe()` because neither side would accept a
stream — here the destination pulls, so no pipe or thread is needed.

`length=size` is load-bearing: it lets the SDK skip seeking the stream to measure
it, which an SFTP handle would fail. So is `max_concurrency=1` — parallel block
upload seeks the source to split it, and the default would break on a
non-seekable read.

### Three traps, each hidden behind the last

All three parse cleanly, pass the integrity tests, and only fail against a live
server. Each one's error message points somewhere other than the real cause.

**1. A bare filename in `source_path` fails as "file not found".** With no `*`, the
inherited `get_tree_behavior` passes the path straight to `SFTPHook.get_tree_map`,
which `listdir`s it. `listdir` on a regular file returns SFTP status 2:

```
FileNotFoundError: [Errno 2] No such file
  ... get_tree_map -> walktree -> list_directory_with_attr -> listdir_attr
```

The file is right there and readable. `walktree`/`listdir_attr` in the traceback
means *"not a directory"*, not *"not found"*.

**2. `get_conn()` hands back a closed socket.** Every decorated `SFTPHook` method
wraps itself in `handle_connection_management`, which enters `get_managed_conn()`
and **closes the session on exit**. The inherited `get_sftp_files_map()` lists the
source that way, so the connection is already shut before the copy starts:

```
OSError: Socket is closed
  ... copy_files_to_wasb -> sftp_client.stat -> _send_packet -> channel.send
```

Use `get_managed_conn()`. It is refcounted, so opening it once around the whole
loop holds one session for every file instead of reconnecting per file.

**3. `max_block_size` is not an upload argument.** It reads as one, but it comes
from the *client's* `StorageConfiguration`. `upload_blob()` forwards unrecognised
kwargs down the pipeline until they reach the HTTP transport:

```
TypeError: Session.request() got an unexpected keyword argument 'max_block_size'
```

The SDK's 4 MiB default is the block size you want anyway; changing it means
configuring the `BlobServiceClient`, which `WasbHook` builds internally.
`max_concurrency`, by contrast, *is* a real per-call parameter.

**`wasb_overwrite_object=True`** so a retry can replace a partial blob left by a
task that died mid-upload. **The source is not deleted** (`move_object=False`), so
re-running re-sends; flip it once you trust the verify step.

---

## 5. `dag_ftps_to_blob_stream.py`

`nix-dag-ftps-to-blob-stream` — streams a file from FTPS into an Azure Blob
container without staging it on disk.

```
FTPS  ──retrbinary──▶  os.pipe()  ──upload──▶  Blob container
```

```bash
airflow dags trigger nix-dag-ftps-to-blob-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'
```

Needs a file on the FTPS server, so run #1 first. A successful run logs the two
ends of the transfer and the independent check:

```
[ftps_to_blob] streaming /upload/probe.txt -> wasb://data001/incoming/probe.txt (205 bytes)
[ftps_to_blob] transferred 205 bytes to incoming/probe.txt
[ftps_to_blob] verified incoming/probe.txt — 205 bytes
```

The byte count appears three times on purpose — reported by FTPS, counted through
the pipe, then read back from Azure in a separate pod. A truncated transfer breaks
the chain and fails the run instead of passing quietly.

**Pattern: when no provider operator exists, write one.** #4 could subclass
`SFTPToWasbOperator`; there is no FTP/FTPS equivalent here. The `microsoft-azure`
provider ships only `sftp_to_wasb`, `s3_to_wasb`, `local_to_wasb` and
`oracle_to_azure_data_lake`. Check before assuming symmetry — "there's an SFTP
one, so there's an FTP one" is wrong.

So `FTPStoBlobStreamOperator` subclasses `BaseOperator` (from **`airflow.sdk`** in
3.x) directly. That is worth doing over a `PythonOperator` for three reasons:

```python
class FTPStoBlobStreamOperator(BaseOperator):
    template_fields = ("remote_path", "container_name", "blob_name")

    def __init__(self, *, ftp_conn_id, wasb_conn_id, remote_path,
                 container_name, blob_name, overwrite=True, **kwargs):
        super().__init__(**kwargs)
        ...

    @cached_property                    # built on use, not at parse time
    def ftps_hook(self): return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)

    def execute(self, context): ...
```

- **Templated fields** render from `dag_run.conf`, so the resolved paths show up
  in the UI's *Rendered Template* tab. A callable that reads `dag_run.conf`
  internally shows nothing there — you cannot see what path a past run used.
  Here `remote_path` is authored as
  `/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}` and renders to
  `/upload/probe.txt`, which is what the tab shows after the run.
- **Configuration is constructor arguments**, so a second task streaming a
  different file is one more operator call, not a second copy of the function.
- **Hooks are `cached_property`**, so building the operator at parse time opens
  no connection.

### Push versus pull: why #5 needs a pipe

This is the interesting difference between #4 and #5, and it is decided entirely
by which side controls the loop:

| | source API | destination API | bridge |
|---|---|---|---|
| #4 SFTP → Blob | `open()` returns a readable | `upload()` reads | none needed |
| #5 FTPS → Blob | `retrbinary()` **pushes** to a callback | `upload()` **pulls** | `os.pipe()` |

`ftplib` never hands back a file object — it calls a callback per chunk. Azure's
`upload()` wants something to `read()`. Two pushers and no puller, so a pipe sits
between them, with `retrbinary` writing one end and `upload` reading the other
from a worker thread:

```python
read_fd, write_fd = os.pipe()

def _upload():
    with os.fdopen(read_fd, "rb") as reader:
        wasb_hook.upload(container_name=..., blob_name=..., data=reader,
                         length=expected,      # required — a pipe cannot be seeked
                         max_concurrency=1)

uploader = threading.Thread(target=_upload, daemon=True); uploader.start()
try:
    with os.fdopen(write_fd, "wb") as writer:
        ftps.get_conn().retrbinary(f"RETR {src}", writer.write, blocksize=8192)
finally:
    uploader.join(timeout=300)   # outside the `with` — closing it is what signals EOF
```

Three details that are easy to get wrong:

- **`join()` must be outside the `with`.** Closing the write end is what raises
  EOF for the reader. Joining inside would wait forever on a reader that can
  never see the stream end.
- **A thread cannot raise into its caller.** The upload exception is stashed in a
  list and re-raised on the main thread; without that the task passes green while
  the upload silently failed.
- **`length=` is mandatory for a pipe.** Without it the SDK tries to seek the
  stream to measure it, which a pipe cannot do.
- **`BrokenPipeError` must be swallowed, not raised.** If the upload fails early
  the reader closes, and the next FTPS write — or the `with` block's own close —
  raises `BrokenPipeError`, which would mask the real cause. Catch it, then
  re-raise the stashed upload exception so the log says *"azure exploded"* rather
  than *"Broken pipe"*.

The pipe also gives **backpressure** for free: if Azure is slower than the FTPS
read, the pipe fills, the write blocks, and the two sides self-throttle to the
slower one rather than buffering the difference.

**One honest caveat about memory.** The transfer itself moves 8 KiB at a time, but
the Azure SDK does a **single-shot upload** for blobs at or under
`max_single_put_size` (64 MiB by default), and that path does one
`stream.read(length)` — buffering the whole file. Above that threshold it switches
to block upload and streams in constant memory. So: constant-memory for large
files, up-to-64-MiB-buffered for small ones.

---

## 6. `dag_blob_to_sftp_stream.py`

`nix-dag-blob-to-sftp-stream` — streams a blob back out of Azure Blob Storage to
the SFTP server.

```
Blob container  ──download──▶  stream  ──putfo──▶  SFTP
```

```bash
airflow dags trigger nix-dag-blob-to-sftp-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'
```

Needs a blob in the container, so run #4 or #5 first. A successful run logs:

```
[blob_to_sftp] streaming wasb://data001/incoming/probe.txt -> .../incoming/probe.txt (118 bytes)
[blob_to_sftp] transferred 118 bytes to .../incoming/probe.txt
[blob_to_sftp] verified .../incoming/probe.txt — 118 bytes
```

### The pipe is not always needed — read #4, #5 and #6 together

These three demos differ only in **which side controls the loop**, and that alone
decides whether you need a pipe:

| Direction | source | destination | bridge |
|---|---|---|---|
| #4 SFTP → Blob | `open()` returns a readable | `upload()` **pulls** | none |
| #5 FTPS → Blob | `retrbinary()` **pushes** | `upload()` **pulls** | `os.pipe()` + thread |
| #6 Blob → SFTP | `download()` returns a readable | `putfo()` **pulls** | none |

`WasbHook.download()` returns a `StorageStreamDownloader`, whose `read(size)`
returns bytes and an empty result at EOF — the readable contract paramiko's
`putfo` expects. A reader on one side and a puller on the other compose directly:

```python
downloader = self.wasb_hook.download(container_name=..., blob_name=...)
with self.sftp_hook.get_managed_conn() as sftp_client:
    attrs = sftp_client.putfo(downloader, remote_path,
                              file_size=expected, confirm=True)
```

**Reach for a pipe only when both sides push, or both pull** — that is #5 and
nothing else here. Adding one to #6 would be a thread and a pair of file
descriptors buying nothing.

`confirm=True` makes paramiko `stat` the file afterwards and compare sizes, so a
short write raises inside the transfer task rather than surfacing downstream.

One paramiko detail worth knowing if you turn that off: with `confirm=False`,
`putfo` returns an **empty `SFTPAttributes` whose `st_size` is `None`**, not
`None` itself. Code that compares `attrs.st_size` against the expected size will
report a bogus "wrote None" mismatch unless it treats that case as "nothing to
compare".

---

## 7. `dag_wasb_prefix_suffix_sensor.py`

`nix-dag-wasb-prefix-suffix-sensor` — waits for an Azure blob matching **both** a
prefix and a suffix.

```bash
airflow dags trigger nix-dag-wasb-prefix-suffix-sensor \
  --conf '{"prefix":"incoming/","suffix":".csv"}'
```

**Pattern: extend a provider sensor rather than writing one from scratch.** The
provider ships `WasbBlobSensor` (exact name) and `WasbPrefixSensor` (prefix only);
neither expresses *"any `.csv` under `incoming/`"*. Subclassing `WasbPrefixSensor`
and overriding `poke()` inherits its connection handling and templated fields:

```python
class WasbPrefixSensorWithSuffix(WasbPrefixSensor):
    template_fields = ("container_name", "prefix", "suffix")

    def poke(self, context) -> bool:
        blobs = hook.get_blobs_list(container_name=..., prefix=self.prefix, delimiter="")
        matched = [b for b in blobs if b.lower().endswith(self.suffix.lower())]
        ...
```

**Push the filter as far server-side as it goes.** The prefix is passed to Azure so
it filters before returning; only the suffix is matched locally. Filtering entirely
client-side would list the whole container on every poke.

Note `get_blobs_list` defaults to `delimiter="/"`, which stops at the first level —
pass `delimiter=""` to recurse.

The sensor pushes matching names to XCom (`matched_blobs`) so the downstream task
acts on exactly what satisfied it, rather than re-listing and possibly seeing a
different set.

---

## 8. `dag_cyclic.py`

`nix-dag-cyclic` — a **cyclic job** in the Control-M sense: fires every 5 minutes,
with only one run ever active. The task sleeps 4 minutes to stand in for real work.

Unlike the others this one is **scheduled**, so just unpause it:

```bash
airflow dags unpause nix-dag-cyclic
```

**Pattern: `max_active_runs=1` is what makes a schedule cyclic.** Without it, a run
that overruns its interval executes concurrently with the next one. With it, the
next run waits.

```python
with DAG(
    schedule="*/5 * * * *",
    max_active_runs=1,                      # no two runs at once
    catchup=False,                          # a cycle, not a history
    dagrun_timeout=timedelta(minutes=9),    # a wedged run must not block the cycle
):
```

`dagrun_timeout` matters more than it looks: with `max_active_runs=1`, one hung run
blocks the cycle indefinitely. The timeout bounds that.

**Pattern: there is no timeout callback — a timeout arrives as a failure.** To log
one distinctly you have to identify it yourself, at two levels:

```python
def on_task_timeout_or_failure(context):
    if isinstance(context.get("exception"), AirflowTaskTimeout):
        log.error("[cyclic][TIMEOUT] %s exceeded execution_timeout", ...)
    else:
        log.error("[cyclic][FAILED] %s failed: %s", ...)
```

| Layer | Setting | Callback fires when |
|---|---|---|
| Task | `execution_timeout=6min` | one task overruns → raises `AirflowTaskTimeout` |
| DAG run | `dagrun_timeout=9min` | the whole run overruns |

Both are needed. A `dagrun_timeout` kills the run **without** failing an individual
task, so the task callback may never fire — the DAG-level `on_failure_callback` is
the backstop. It infers a timeout by comparing the run's duration against
`dagrun_timeout`, because the context carries no explicit "timed out" flag.

Import `AirflowTaskTimeout` from **`airflow.sdk.exceptions`**; the
`airflow.exceptions` path is deprecated in 3.x.

Grep logs for `[cyclic][TIMEOUT]` to find blocked cycles.

**Where Airflow differs from Control-M**, worth knowing before relying on it:

- Control-M measures the gap from when the previous run *ends*; Airflow's cron fires
  on wall-clock time regardless.
- If a run overruns, Airflow **queues** the missed interval rather than skipping it —
  so the next run may start immediately after, with no gap. `max_active_runs=1`
  prevents overlap, not catch-up.
- For "wait N minutes after the previous run finishes", use
  `schedule=timedelta(minutes=5)` instead. That measures from the previous run —
  closer to Control-M — but drifts relative to the clock.

Watch the grid view for ~15 minutes: runs should be strictly sequential. Triggering
manually while one is active shows the new run `queued` until the active one ends.

> Runs **288 times a day** while unpaused, holding a worker for 4 minutes each time.
> Pause it when you are done.

---

## Deploying

Deployment is environment-specific. In this repo:

```bash
./scripts/deploy-dag.sh dag_ftps_sensor      # one DAG
./scripts/deploy-dag.sh --all                # all of them
```

It checks connectivity, parses the DAG, runs the integrity tests, then copies the
files — refusing to ship anything that fails. It also syncs `dags/files/`.

If your deployment uses git-sync, a baked image, or object storage instead, the
only requirement is that `dags/files/` lands beside the DAGs.

New DAGs land **paused**:

```bash
airflow dags unpause <dag_id>
```

Allow time for the DAG processor to pick up a new file before assuming it failed.
It parses the whole DAG folder on a cycle, so on a busy instance a new file can
take **minutes**, not seconds, to appear. Absent from the UI *and* absent from
`list-import-errors` means "not parsed yet" — a genuinely broken file shows up in
the import-error list rather than staying invisible.

---

## Local validation

```bash
uv sync
.venv/bin/python dags/dag_<name>.py     # parse — exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

`tests/test_dag_integrity.py` applies to every `dags/dag_*.py` automatically. It
enforces the project's constraints: no deprecated Airflow 2.x APIs, `doc_md` on the
DAG and every task, `catchup=False`, no top-level I/O, standalone files.

A clean local parse does not prove a third-party import exists in the **server**
image. Check import errors after deploying.

### What a parse check cannot catch

Every bug worth reading about in this file parsed cleanly and passed the integrity
tests. They only failed against a live server:

| Bug | Where it surfaces |
|---|---|
| a path that is a file where a directory is expected | first `listdir` |
| a hook whose connection was already closed | first call on the dead client |
| a kwarg the SDK forwards to its HTTP transport | inside the request |
| a provider missing from the server image | dag-processor import |

So treat the local checks as a fast filter, not proof. The first real run against
the target systems is the actual test.

For the threaded parts specifically — the `os.pipe()` bridge in #5 — a parse proves
nothing at all: a deadlock or a swallowed exception looks identical to working code
until it runs. Those are worth exercising directly against fake hooks, feeding
`execute()` a fake that pushes known bytes and asserting on what the fake upload
received, plus one case per failure mode (source missing, oversize, upload raises,
truncated read).

That is how #5's `BrokenPipeError` masking was found — the happy path passed on the
first try, and only the "upload raises" case revealed that the real error was being
hidden behind a broken pipe. It would have looked fine in any test that only
transfers a file successfully.

---

## Two constraints that shape every DAG here

### Tasks do not share a filesystem

Under KubernetesExecutor every task runs in its own ephemeral pod. A file written
to local disk by one task does not exist for the next — it fails at runtime with
`FileNotFoundError`, and a parse check will not catch it.

Options, in order of preference:

1. **Read from shared storage.** The DAG folder is mounted into every pod, so
   `dags/files/` is readable at runtime.
2. **Do it in one task.** Build the payload in memory and pass a buffer to the hook.
3. **Pass a reference, not a file.** Write to object storage, push the key to XCom.

Never assume `/tmp` persists between tasks.

### Self-signed certificates need a custom hook

Stock `FTPSHook.get_conn()` hardcodes `ssl.create_default_context()` with no
parameter for a CA, so a self-signed cert fails with `CERTIFICATE_VERIFY_FAILED`.
No connection setting can override it.

Each FTPS DAG here subclasses the hook:

```python
class MyFTPSHook(FTPSHook):
    def get_conn(self):
        params = self.get_connection(self.ftp_conn_id)
        context = ssl.create_default_context(cadata=Variable.get("ftps_ca_cert"))
        conn = ftplib.FTP_TLS(params.host, params.login, params.password, context=context)
        conn.prot_p()      # stock hook omits this — without it data is plaintext
        conn.set_pasv(params.extra_dejson.get("passive", True))
        return conn
```

Two things worth noting: verification stays **on** — the CA is trusted, not
bypassed, so this is not `verify=False`. And `prot_p()` is missing from the stock
hook, meaning the control channel is encrypted but the **data** channel is not.

With a publicly trusted certificate, none of this is needed.

---

## Airflow 3.x notes

These examples are 3.x-only. The most common porting mistakes:

```python
from airflow.providers.standard.operators.python import PythonOperator   # ✅ 3.x
from airflow.operators.python import PythonOperator                      # ❌ 2.x shim

from airflow.sdk import Variable                                         # ✅ 3.x
from airflow.models import Variable                                      # ❌ fails in workers

schedule=None                                                            # ✅
schedule_interval=None                                                   # ❌ removed
```

The 2.x import paths still work as deprecation shims — they parse and even run, so
nothing obvious tells you they are wrong. The integrity tests fail the build on
them.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `CERTIFICATE_VERIFY_FAILED` | using stock `FTPSHook` with a self-signed cert, or `ftps_ca_cert` not set |
| `FileNotFoundError: /tmp/...` | assumed two tasks share a disk |
| `550` from FTPS | file not there — run #1 first |
| `553 Could not create file` | wrote to a read-only directory; FTPS chroots often make the root non-writable |
| Data transfer hangs after login | passive-mode port range not reachable from the worker |
| `ResourceExistsError` on upload | blob already there and `wasb_overwrite_object=False` |
| `unsupported operation: seek` on upload | streaming a non-seekable source with `max_concurrency` > 1 |
| `FileNotFoundError` from `listdir_attr`, file exists | #4 `source_path` is a bare filename — needs a directory or `*` |
| `OSError: Socket is closed` mid-transfer | used `get_conn()` after a hook method closed the managed session |
| `Session.request() got an unexpected keyword argument` | passed a client-config kwarg (e.g. `max_block_size`) to `upload()` |
| `No such file` writing over SFTP | destination directory does not exist — `putfo` will not create it |
| size mismatch reporting `wrote None` | `putfo(confirm=False)` returns empty `SFTPAttributes`; nothing to compare |
| DAG missing from UI, no import error | processor has not parsed it yet |
| DAG present but never runs | still paused |
| Works locally, fails on the server | provider missing from the server image, or a connection/variable not set |

Check import errors first — an entry there invalidates everything else:

```bash
airflow dags list-import-errors
```
