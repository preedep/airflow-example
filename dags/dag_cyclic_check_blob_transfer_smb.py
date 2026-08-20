"""nix-dag-cyclic-check-blob-transfer-smb — poll Blob Storage on a cycle, stream hits to SMB."""

import logging
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-cyclic-check-blob-transfer-smb

A **cyclic pickup job**: every 5 minutes during business hours it checks a blob
prefix, and if anything is waiting it streams the blobs **Azure Blob Storage →
SMB share** without staging them on disk. When the prefix is empty the run
short-circuits and ends `success` with everything downstream skipped.

```
every 5 min, 08:00-19:00 Mon-Fri
        │
        ▼
   check_for_blobs ──(nothing)──▶ skipped, run ends success
        │
     (found)
        ▼
   stream_to_smb ──readinto──▶ SMB share
        │                      <SMB_DIR>/<name>
        ▼
   report_cycle
```

This is the Blob → SMB counterpart of `nix-dag-cyclic-check-s3-transfer-sftp`.
Same cyclic shape, but both halves behave differently: Azure's list is flat
rather than hierarchical, and its copy is **asynchronous**, which changes what
"archive after transfer" has to do.

#### Schedule

`schedule="*/5 8-18 * * 1-5"` — every 5 minutes from 08:00 through 18:55,
Monday to Friday. The last fire is **18:55**, the interval that covers the run
up to the 19:00 boundary; `8-19` would keep firing until 19:55 and run an hour
past the window.

Cron fires in the **scheduler's timezone**. If your operators think in local
time and the deployment runs UTC, set `timezone` on the DAG or convert the hour
range.

| Setting | Value | Why |
|---|---|---|
| `schedule` | `*/5 8-18 * * 1-5` | the business-hours cycle |
| `max_active_runs` | `1` | **the cyclic guarantee** — a slow transfer never overlaps the next |
| `catchup` | `False` | never backfill; a missed pickup window is gone, not owed |
| `dagrun_timeout` | 30 min | a wedged transfer must not block the cycle indefinitely |

#### Why a short-circuit and not a sensor

`WasbPrefixSensor` → transfer is the obvious build and the wrong shape for a
cycle. A sensor **waits**, which duplicates the 5-minute schedule and holds the
one slot `max_active_runs=1` allows; and a sensor that finds nothing ends the
run **failed** on timeout. An empty prefix at 08:05 on a quiet morning is a
normal outcome, not an incident.

`ShortCircuitOperator` returns a boolean instead: `False` skips everything
downstream and the run ends `success`, so a quiet cycle is visibly distinct from
a broken one.

#### Listing is flat here, not hierarchical

`WasbHook.get_blobs_list` calls `walk_blobs` with `delimiter="/"`, which is a
*hierarchical* listing: it returns virtual directory prefixes as entries
alongside blobs. Feeding those to a transfer tries to download a directory.

This DAG uses **`get_blobs_list_recursive`**, which calls `list_blobs` and
returns a flat list of real blob names. Its `endswith` argument does the suffix
filter server-side-ish, in the same pass.

Compare the S3 demo, where the equivalent trap is the zero-byte directory
marker: same class of problem, different provider, different fix.

#### Idempotency: the archive copy is asynchronous

Every 5 minutes the same prefix is listed again, so the run **must not**
re-transfer what a previous cycle moved. As with S3 there is no move operation —
copy then delete is the move. Unlike S3, **Azure's copy does not block**:

```python
copy = dest.start_copy_from_url(source_url)     # copy_status: 'pending' or 'success'
```

`start_copy_from_url` returns as soon as the service accepts the job. Deleting
the source immediately — which is what a naive port of the S3 version does —
races the copy and can destroy the blob before it has been read.

So `_archive` **polls `get_blob_properties().copy.status`** until it leaves
`pending`, and deletes the source only on `success`. A copy that fails or is
aborted raises, leaving the source in place for the next cycle to retry.

Small blobs usually complete synchronously (`copy_status == 'success'` on the
first response), so the poll costs nothing in the common case. It is the large
ones that would silently lose data without it.

Set `ARCHIVE_PREFIX = ""` to leave blobs in place — then every cycle
re-transfers everything, which is fine for a demo and wrong for production.

The batch is capped at `MAX_FILES_PER_RUN`; a backlog drains over several cycles.

#### Streaming, and the inverted composition

`SambaHook.open_file()` hands back a file object opened for **writing**, so
there is nothing for the destination to pull. Azure's `StorageStreamDownloader`
supplies the missing half with `readinto(stream)`, which **pushes** into any
writable:

```python
with samba_hook.open_file(target, mode="wb") as handle:
    written = downloader.readinto(handle)
```

Still no pipe: one side drives the loop and the other is a plain file object. A
pipe is only needed when both sides push, or both pull. `readinto` also returns
the byte count, which is what the size check compares.

Peak memory is one SDK chunk, not the blob size — and unlike the Azure *upload*
paths, a download has no `max_single_put_size` equivalent, so this direction is
constant-memory at any size.

#### Write-then-rename, and why not `replace()`

Each file is written as `<name>.part` and renamed once the last byte lands, so a
consumer watching the share never sees a partial file.

**Delete-then-rename, not `replace()`.** SMB has an atomic overwrite-rename
(`FILE_RENAME_INFORMATION` with `ReplaceIfExists`) which `smbclient.replace`
issues, and Samba refuses it with `STATUS_ACCESS_DENIED` even between two files
the account just created and can delete outright. `rename` onto a *free* name
works, so the target is unlinked first.

The trade-off is honest: there is a brief window where neither name holds the
final file, so unlike SFTP's `posix_rename` this is **not** atomic. It still
prevents a consumer seeing a *partial* file, which is the point.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `smb_test_001` | SMB destination (conn type `samba`) |

For the SMB connection: **host** is an address the worker pods can resolve,
**schema** is the share name, **login**/**password** are the SMB credentials.
`SambaHook` joins the UNC path itself, so paths here are relative to the share.

The share directory must be writable by the SMB user; a directory owned by
another account yields `STATUS_ACCESS_DENIED`, which is a server-side filesystem
permission rather than an Airflow problem.

For SAS auth on the Azure side the token goes in **extra** as `sas_token`, and
`login` is the storage account name.

#### Cost note

This DAG fires **132 times a day** while unpaused, and Azure charges per list
operation plus egress for data leaving the region. Pause it when you are done.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WASB_CONN_ID = "wasb-nickstorageairflow002"
SMB_CONN_ID = "smb_test_001"

CONTAINER = "data001"

# The prefix polled each cycle. Blob Storage has no real directories — this is a
# literal name prefix, and the listing below is flat, so nested names match too.
SOURCE_PREFIX = "incoming/"

# Where a blob is moved after a successful transfer, so the next cycle does not
# pick it up again. "" leaves blobs in place and re-transfers every cycle.
ARCHIVE_PREFIX = "archive/"

# Relative to the share named in the connection's schema field — SambaHook joins
# the UNC path itself. "" writes to the share root.
SMB_DIR = ""

# A backlog drains over several cycles rather than one run holding a pod for an
# hour — and keeps the run inside the 5-minute window on a normal day.
MAX_FILES_PER_RUN = 10

MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded

# How long to wait for an asynchronous server-side copy before giving up. The
# copy is Azure-side, so this bounds a wedged archive, not a transfer.
COPY_TIMEOUT_SECONDS = 120
COPY_POLL_SECONDS = 2


def _human(num_bytes: int) -> str:
    """Format a byte count for logs: 52428800 -> '50.0 MiB'."""
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GiB"


def _summary(label: str, num_bytes: int, elapsed: float) -> str:
    """One-line transfer summary: size, wall time, and throughput."""
    # Guard against a divide-by-zero on a transfer that completes inside the
    # clock's resolution — a tiny fixture on a fast link really can.
    rate = f"{num_bytes / elapsed / 1024 / 1024:.1f} MiB/s" if elapsed > 0 else "n/a"
    return f"[{label}] done: {_human(num_bytes)} in {elapsed:.1f}s ({rate})"


# --------------------------------------------------------------------------- #
# Operator
# --------------------------------------------------------------------------- #


class CyclicBlobToSMBStreamOperator(BaseOperator):
    """Stream a discovered batch of blobs from Azure Blob Storage to an SMB share.

    No provider ships a Blob → SMB transfer, so this subclasses `BaseOperator`
    directly — the same reason `BlobToSMBStreamOperator` does in
    `dag_blob_to_smb_stream.py`. This variant moves a *batch* handed to it by an
    upstream check, and archives each blob it has transferred so the next cycle
    does not see it again.

    The composition is inverted compared with most demos here: `open_file()`
    returns a *writable*, so the Azure downloader pushes into it with
    `readinto()` rather than the destination pulling. Nothing touches local disk
    and no pipe is needed.

    :param blobs_task_id: task whose XCom holds the list of blob names to move.
    :param smb_dir: destination directory relative to the share; the blob's
        basename is appended. "" writes to the share root.
    :param archive_prefix: copy-then-delete each blob here after a successful
        transfer, so the next cycle does not see it. "" leaves it in place.
    :param temp_suffix: write to `<path><temp_suffix>` and rename on success, so
        a consumer never sees a partial file. "" writes directly.
    :param max_bytes: reject a blob larger than this before any bytes move.
    """

    # Keyword-only, so these can never be confused with BaseOperator's own
    # positional parameters.
    def __init__(
        self,
        *,
        wasb_conn_id: str,
        samba_conn_id: str,
        container_name: str,
        blobs_task_id: str,
        smb_dir: str = SMB_DIR,
        archive_prefix: str = ARCHIVE_PREFIX,
        temp_suffix: str = ".part",
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.samba_conn_id = samba_conn_id
        self.container_name = container_name
        self.blobs_task_id = blobs_task_id
        self.smb_dir = smb_dir
        self.archive_prefix = archive_prefix
        self.temp_suffix = temp_suffix
        self.max_bytes = max_bytes

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    @cached_property
    def samba_hook(self) -> SambaHook:
        # The share comes from the connection's schema field; SambaHook joins the
        # UNC path itself, so every path below is relative to the share.
        return SambaHook(samba_conn_id=self.samba_conn_id)

    def _stream_one(self, blob_name: str) -> dict[str, Any]:
        """Stream a single blob onto the share, returning its record."""
        basename = blob_name.rsplit("/", 1)[-1]
        destination = f"{self.smb_dir.rstrip('/')}/{basename}" if self.smb_dir else basename

        container = self.wasb_hook.get_conn().get_container_client(self.container_name)

        # Size up front so an oversized blob fails before any bytes move, and so
        # the check below has an authoritative number to compare against.
        expected = container.get_blob_client(blob_name).get_blob_properties().size
        if expected > self.max_bytes:
            raise ValueError(
                f"{blob_name} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[cyclic_blob_smb] streaming wasb://%s/%s -> smb:%s (%s bytes)",
            self.container_name,
            blob_name,
            destination,
            expected,
        )

        started = time.monotonic()

        downloader = self.wasb_hook.download(
            container_name=self.container_name, blob_name=blob_name
        )

        # Write to a temporary name and rename once complete, so a consumer
        # watching the share never sees a partial file.
        target = destination + self.temp_suffix if self.temp_suffix else destination

        # open_file returns a *writable*, so the download pushes into it rather
        # than the destination pulling. readinto returns the byte count written,
        # which is what the check below compares.
        with self.samba_hook.open_file(target, mode="wb") as handle:
            # BYTE PATH — nothing touches the worker pod's disk:
            #   HTTPS GET (Azure) -> readinto pushes chunks -> SMB write.
            written = downloader.readinto(handle)

        # Checked *before* the rename, so a truncated transfer never replaces a
        # good previous file.
        if written != expected:
            raise ValueError(
                f"size mismatch for {blob_name}: wrote {written}, expected {expected}"
            )

        if self.temp_suffix:
            # Delete-then-rename, not replace(). SMB's atomic overwrite-rename
            # (FILE_RENAME_INFORMATION with ReplaceIfExists, which `replace`
            # issues) is refused by Samba with STATUS_ACCESS_DENIED even when
            # the account owns both files and can delete them outright — so it
            # is a server-side restriction, not a permissions problem.
            #
            # `rename` onto a *free* name works, hence: unlink the target first,
            # then rename. The trade-off is a brief window where neither name
            # holds the final file, unlike SFTP's genuinely atomic posix_rename.
            try:
                self.samba_hook.unlink(destination)
            except Exception:
                # Nothing to replace on a first run; the rename below is then
                # the whole operation.
                pass
            self.samba_hook.rename(target, destination)

        self.log.info(
            "%s -> %s",
            _summary("cyclic_blob_smb", written, time.monotonic() - started),
            destination,
        )
        return {"blob": blob_name, "destination": destination, "size": written}

    def _archive(self, blob_name: str) -> str:
        """Move a transferred blob out of the polled prefix.

        Blob Storage has no move operation, so copy-then-delete is the move —
        but unlike S3's `copy_object`, `start_copy_from_url` is **asynchronous**:
        it returns as soon as the service accepts the job. Deleting the source
        straight away races the copy and can destroy the blob before it is read,
        so this polls the destination's copy status first.
        """
        archived_name = f"{self.archive_prefix.rstrip('/')}/{blob_name.rsplit('/', 1)[-1]}"

        container = self.wasb_hook.get_conn().get_container_client(self.container_name)
        source = container.get_blob_client(blob_name)
        destination = container.get_blob_client(archived_name)

        copy = destination.start_copy_from_url(source.url)
        status = copy.get("copy_status")

        # Small blobs usually complete synchronously, so this loop is normally
        # skipped entirely. It is the large ones that would silently lose data.
        deadline = time.monotonic() + COPY_TIMEOUT_SECONDS
        while status == "pending":
            if time.monotonic() > deadline:
                # Abort rather than leave a half-copied blob accruing storage.
                # The source is untouched, so the next cycle retries.
                destination.abort_copy(copy["copy_id"])
                raise TimeoutError(
                    f"archive copy of {blob_name} still pending after "
                    f"{COPY_TIMEOUT_SECONDS}s — aborted, source left in place"
                )
            time.sleep(COPY_POLL_SECONDS)
            status = destination.get_blob_properties().copy.status

        if status != "success":
            raise ValueError(f"archive copy of {blob_name} ended {status!r}, source left in place")

        # Only now is the copy durable, so deleting the source cannot lose data.
        source.delete_blob()

        self.log.info("[cyclic_blob_smb] archived %s -> %s", blob_name, archived_name)
        return archived_name

    def execute(self, context) -> dict[str, Any]:
        blobs = context["ti"].xcom_pull(task_ids=self.blobs_task_id, key="pending_blobs")
        if not blobs:
            # The short-circuit only lets this task run after pushing a non-empty
            # list, so an empty pull means the contract broke (task renamed, XCom
            # cleared) rather than "nothing to do". Fail rather than no-op.
            raise ValueError(f"{self.blobs_task_id} pushed no blobs")

        transferred: list[dict[str, Any]] = []
        for blob_name in blobs:
            record = self._stream_one(blob_name)
            # Archive per blob rather than after the batch: if file 7 of 10
            # fails, the first six stay archived and the retry resumes at the
            # remainder instead of re-sending everything.
            if self.archive_prefix:
                record["archived_to"] = self._archive(blob_name)
            transferred.append(record)

        total = sum(item["size"] for item in transferred)
        self.log.info(
            "[cyclic_blob_smb] batch complete: %d file(s), %s",
            len(transferred),
            _human(total),
        )
        return {"count": len(transferred), "total_bytes": total, "files": transferred}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def check_for_blobs(ti=None, **context):
    """List the polled prefix; return False to skip the cycle when it is empty.

    The return value drives `ShortCircuitOperator`: False skips everything
    downstream and the run ends `success`, so an empty prefix is a normal quiet
    cycle rather than a failed one.
    """
    task_log = logging.getLogger("airflow.task")

    conf = context["dag_run"].conf or {}
    prefix = conf.get("prefix", SOURCE_PREFIX)
    suffix = conf.get("suffix", "")

    hook = WasbHook(wasb_conn_id=WASB_CONN_ID)

    # get_blobs_list_recursive, NOT get_blobs_list: the latter calls walk_blobs
    # with delimiter="/", a *hierarchical* listing that returns virtual directory
    # prefixes as entries. Handing one of those to the transfer tries to download
    # a directory. This one calls list_blobs and returns real blob names only.
    blobs = hook.get_blobs_list_recursive(
        container_name=CONTAINER, prefix=prefix, endswith=suffix
    )

    # A name ending in "/" is a placeholder blob some tools create to fake an
    # empty folder — not a file, and not something to transfer.
    blobs = [name for name in blobs if not name.endswith("/")]

    # The archive lives under the same container; if it is nested inside the
    # polled prefix, skip it or the cycle re-transfers its own output forever.
    if ARCHIVE_PREFIX:
        blobs = [name for name in blobs if not name.startswith(ARCHIVE_PREFIX)]

    if not blobs:
        task_log.info(
            "[cyclic_blob_smb] nothing at wasb://%s/%s — skipping this cycle",
            CONTAINER,
            prefix,
        )
        return False

    # sorted() makes the batch deterministic, so a retry picks the same files in
    # the same order as the attempt it is replacing.
    batch = sorted(blobs)[:MAX_FILES_PER_RUN]
    if len(blobs) > MAX_FILES_PER_RUN:
        task_log.info(
            "[cyclic_blob_smb] %d blob(s) waiting, taking %d this cycle — "
            "the rest drain on the next one",
            len(blobs),
            MAX_FILES_PER_RUN,
        )

    task_log.info("[cyclic_blob_smb] picked up %d blob(s): %s", len(batch), batch)

    # Hand the exact names downstream so the transfer does not re-list and
    # possibly see a different set — blobs land and are archived continuously.
    ti.xcom_push(key="pending_blobs", value=batch)
    return True


def report_cycle(ti=None, **context):
    """Log what this cycle moved, and confirm each file landed at the size we sent."""
    task_log = logging.getLogger("airflow.task")

    result = ti.xcom_pull(task_ids="stream_to_smb")
    if not result:
        raise ValueError("stream_to_smb pushed no result")

    # Separate pod, so a fresh connection: an independent read of server state
    # rather than a re-check of the transfer task's own handle.
    hook = SambaHook(samba_conn_id=SMB_CONN_ID)
    for item in result["files"]:
        actual = hook.stat(item["destination"]).st_size
        if actual != item["size"]:
            raise ValueError(
                f"{item['destination']}: expected {item['size']} bytes, found {actual}"
            )
        task_log.info(
            "[cyclic_blob_smb] verified %s — %s bytes", item["destination"], actual
        )

    task_log.info(
        "[cyclic_blob_smb] cycle moved %d file(s), %s",
        result["count"],
        _human(result["total_bytes"]),
    )
    return {"count": result["count"], "total_bytes": result["total_bytes"]}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-cyclic-check-blob-transfer-smb",
    description="Every 5 min on weekdays 08:00-19:00, stream any waiting blob to an SMB share",
    # Every 5 minutes from 08:00 through 18:55, Mon-Fri. The hour range stops at
    # 18 on purpose: "8-19" would keep firing until 19:55, an hour past the
    # window. Fires in the scheduler's timezone.
    schedule="*/5 8-18 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,  # a missed pickup window is gone, not owed
    max_active_runs=1,  # the cyclic guarantee — a slow transfer never overlaps the next check
    dagrun_timeout=timedelta(minutes=30),  # a wedged transfer must not block the cycle
    tags=["demo", "cyclic", "azure", "wasb", "smb", "samba", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    check = ShortCircuitOperator(
        task_id="check_for_blobs",
        python_callable=check_for_blobs,
        doc_md="""
Lists the polled blob prefix and decides whether this cycle has work.

Returns `False` when the prefix is empty, which **skips** the transfer and the
report — the run ends `success`, so a quiet cycle is visibly different from a
broken one. A sensor would instead wait out its timeout and mark the run failed.

Uses `get_blobs_list_recursive`, **not** `get_blobs_list`: the latter walks
blobs with `delimiter="/"`, a hierarchical listing that returns virtual
directory prefixes as entries, and handing one to the transfer tries to download
a directory.

Filters out placeholder "folder" blobs and anything under the archive prefix —
without the latter the cycle would re-transfer its own output forever.

Pushes the batch to XCom as `pending_blobs`, capped at `MAX_FILES_PER_RUN` so a
backlog drains over several cycles rather than one long run.
""",
    )

    stream = CyclicBlobToSMBStreamOperator(
        task_id="stream_to_smb",
        wasb_conn_id=WASB_CONN_ID,
        samba_conn_id=SMB_CONN_ID,
        container_name=CONTAINER,
        blobs_task_id="check_for_blobs",
        doc_md="""
Streams each blob the check found onto the SMB share without staging it on disk.

Subclasses `BaseOperator` directly because no provider ships a Blob → SMB
transfer. **The composition is inverted**: `SambaHook.open_file()` returns a
*writable*, so the Azure downloader pushes into it with `readinto(handle)`
rather than the destination pulling. Still no pipe — one side drives the loop
and the other is a plain file object.

Each file is written as `<name>.part` and renamed on success, so a consumer
never sees a partial file. The size check runs *before* the rename. Unlink then
rename, not `replace()` — Samba refuses SMB's atomic overwrite-rename.

Archives each blob after its own transfer, **polling the copy to completion
first**: `start_copy_from_url` is asynchronous, so deleting the source
immediately would race the copy and could destroy data.
""",
    )

    report = PythonOperator(
        task_id="report_cycle",
        python_callable=report_cycle,
        doc_md="""
`stat`s every destination on the share and compares its size against the byte
count recorded by the transfer, so a truncated file fails the run instead of
passing silently.

Runs in its own pod with a fresh connection, so it is an independent read of
server state.
""",
    )

    # Both edges are data dependencies, not just ordering: the transfer consumes
    # pending_blobs and the report consumes the transfer's result.
    check >> stream >> report
