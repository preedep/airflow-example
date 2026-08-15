"""nix-dag-blob-to-sftp-stream — stream a blob from Azure Blob Storage to the SFTP server."""

import logging
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-blob-to-sftp-stream

Streams a blob **Azure Blob Storage → SFTP** without staging it on disk.

```
Blob container  ──download──▶  dag (stream)  ──putfo──▶  SFTP server
 <BLOB_PREFIX><file>                                     SFTP_DIR/<file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "blob_prefix": "incoming/"}
```

Defaults to `probe.txt` and prefix `incoming/`.

#### Why this one needs no pipe

The three transfer demos differ only in which side controls the loop, and that
decides the whole design:

| Direction | source | destination | bridge |
|---|---|---|---|
| SFTP → Blob | `open()` returns a readable | `upload()` **pulls** | none |
| FTPS → Blob | `retrbinary()` **pushes** | `upload()` **pulls** | `os.pipe()` + thread |
| Blob → SFTP *(this one)* | `download()` returns a readable | `putfo()` **pulls** | none |

`WasbHook.download()` returns a `StorageStreamDownloader`, whose `read(size)`
returns bytes and an empty result at EOF — the readable contract paramiko's
`putfo` needs. A reader on one side and a puller on the other compose directly,
so there is no pipe and no worker thread here.

That absence is the point: reach for the pipe only when both sides push or both
pull, as in the FTPS demo.

#### Memory

`putfo` reads in **32 KiB** chunks and the downloader fetches on demand, so peak
memory is one chunk rather than the file size — a multi-GB blob transfers in
constant memory.

That size is not configurable: paramiko hardcodes `reader.read(32768)` inside
`_transfer_with_callback` and offers no parameter for it. The other transfer
demos expose a `chunk_size` argument because their underlying calls
(`retrbinary`, `storbinary`) take a `blocksize`; this one deliberately does not,
rather than offering a knob that does nothing.

`MAX_BYTES` caps a single transfer; a larger blob fails before any bytes move
rather than running unbounded.

#### Write-then-rename

The transfer writes to `<remote_path>.part` and renames it once the last byte
lands. Two reasons, both of which matter on a shared drop directory:

- A consumer polling the destination never sees a **partial file**. Without this
  a downstream job can pick up a half-written file that looks complete.
- A failed run leaves an obvious `.part` rather than a truncated file that is
  indistinguishable from a good one.

The rename is a metadata operation — no data is re-transferred — so it is
effectively free on SFTP. `temp_suffix=""` disables it.

**`posix_rename`, not `rename`.** Plain SFTP `rename` fails when the target
exists:

```
OSError: Failure
```

so a retry over a previous run's file would break. `posix_rename` overwrites
atomically, which is exactly what this pattern needs.

Note the object-store demos do **not** do this: neither Blob nor S3 has a rename,
so the equivalent would be a full server-side copy — and neither makes a
partially-uploaded object visible in the first place.

#### Idempotency

`putfo` overwrites the destination, so a retry replaces whatever a failed run
left behind rather than appending to it. `confirm=True` makes paramiko `stat`
the file afterwards and compare sizes, so a truncated write fails inside the
transfer task instead of surfacing later.

#### Source is not deleted

A successful run leaves the blob in the container, so re-running re-transfers it.
Deliberate for a demo: deleting is destructive and a failed write must not lose
the only copy. For a real drain pattern, delete only after `verify` confirms the
size matches.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `sftp_test_001` | SFTP destination (conn type `SFTP`) |

Set the SFTP connection to an address the **worker pods** can resolve. A hostname
that works from a laptop (VPN, mesh network, `/etc/hosts`) often does not resolve
inside the cluster. The destination directory must exist and be writable by the
connection's user — `putfo` does not create parent directories.

For SAS auth the token goes in the Azure connection's **extra** as `sas_token`,
and `login` is the **storage account name** — the hook builds the account URL
from it. The container is not part of the connection; it is passed per operation.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WASB_CONN_ID = "wasb-nickstorageairflow002"
SFTP_CONN_ID = "sftp_test_001"

CONTAINER = "data001"

# Adjust to your server. SFTP_DIR must exist and be writable by the connection's
# user; putfo will not create it.
SFTP_DIR = "/home/airflowsftp/incoming"

# Peak memory of the transfer is one chunk, not the file size — the downloader
# fetches on demand, so chunks do not accumulate.
#
# There is deliberately no CHUNK_SIZE constant here, unlike the other transfer
# demos: paramiko's putfo hardcodes `reader.read(32768)` in
# `_transfer_with_callback` and exposes no parameter for it. An operator argument
# would be a knob that silently does nothing.
MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


# --------------------------------------------------------------------------- #
# Operator
# --------------------------------------------------------------------------- #


class BlobToSFTPStreamOperator(BaseOperator):
    """Stream a single blob from Azure Blob Storage to an SFTP server.

    No provider ships a Blob → SFTP transfer, so this subclasses `BaseOperator`
    directly rather than extending an existing operator.

    Both SDKs cooperate here: `WasbHook.download()` returns a readable and
    paramiko's `putfo` pulls from one, so the two compose without the pipe and
    worker thread the FTPS→Blob demo needs. Nothing touches local disk.

    :param wasb_conn_id: Azure Blob connection (conn type `wasb`).
    :param sftp_conn_id: SFTP connection (conn type `SFTP`).
    :param container_name: source blob container.
    :param blob_name: source blob name, including any prefix.
    :param remote_path: absolute destination path on the SFTP server. The parent
        directory must already exist.
    :param max_bytes: reject a blob larger than this before any bytes move.
    :param confirm: `stat` the file after writing and compare sizes, so a
        truncated write fails here rather than downstream.
    :param temp_suffix: write to `<remote_path><temp_suffix>` and rename on
        success, so a consumer never sees a partial file. Set to "" to write
        directly to the final name.
    """

    # Rendered from dag_run.conf at run time, which is why the paths are operator
    # arguments rather than being read from context inside the task. It also puts
    # the resolved values in the UI's Rendered Template tab.
    template_fields = ("container_name", "blob_name", "remote_path")

    # Keyword-only, so these can never be confused with BaseOperator's own
    # positional parameters.
    def __init__(
        self,
        *,
        wasb_conn_id: str,
        sftp_conn_id: str,
        container_name: str,
        blob_name: str,
        remote_path: str,
        max_bytes: int = MAX_BYTES,
        confirm: bool = True,
        temp_suffix: str = ".part",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.sftp_conn_id = sftp_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.remote_path = remote_path
        self.max_bytes = max_bytes
        self.confirm = confirm
        self.temp_suffix = temp_suffix

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    @cached_property
    def sftp_hook(self) -> SFTPHook:
        return SFTPHook(ssh_conn_id=self.sftp_conn_id)

    def execute(self, context) -> dict[str, Any]:
        client = self.wasb_hook.get_conn().get_container_client(self.container_name)
        blob_client = client.get_blob_client(self.blob_name)

        # Size up front so an oversized blob fails before any bytes move, and so
        # the verify step has an authoritative number to compare against.
        expected = blob_client.get_blob_properties().size
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.blob_name} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[blob_to_sftp] streaming wasb://%s/%s -> %s (%s bytes)",
            self.container_name,
            self.blob_name,
            self.remote_path,
            expected,
        )

        started = time.monotonic()

        # download() returns a StorageStreamDownloader, whose read(size) returns
        # bytes and empty at EOF — exactly what putfo expects of a file object.
        # No pipe or thread: one side reads, the other pulls.
        downloader = self.wasb_hook.download(
            container_name=self.container_name, blob_name=self.blob_name
        )

        # Write to a temporary name and rename once the transfer completes, so a
        # consumer watching the destination directory never sees a partial file
        # and a failed run leaves an obvious <name>.part rather than a truncated
        # file that looks finished. See `temp_suffix` on the operator.
        target = self.remote_path + self.temp_suffix if self.temp_suffix else self.remote_path

        # get_managed_conn(), not get_conn(): every decorated SFTPHook method
        # closes its session on exit, so a connection fetched outside this
        # context manager can already be dead by the time it is used.
        with self.sftp_hook.get_managed_conn() as sftp_client:
            # confirm=True makes paramiko stat the file afterwards and compare
            # sizes, so a short write raises here rather than passing silently.
            # BYTE PATH — nothing touches the worker pod's disk:
            #   HTTPS GET -> downloader -> putfo reads 32 KiB -> SFTP socket.
            # putfo pulls, so only one 32 KiB chunk exists at any moment.
            attrs = sftp_client.putfo(
                downloader,
                target,
                file_size=expected,
                confirm=self.confirm,
            )

            if self.temp_suffix:
                # posix_rename, not rename: plain SFTP rename fails with
                # "OSError: Failure" when the target already exists, so a retry
                # over a previous run's file would break. posix_rename overwrites
                # atomically, which is the behaviour this pattern needs.
                sftp_client.posix_rename(target, self.remote_path)
                self.log.info(
                    "[blob_to_sftp] renamed %s -> %s", target, self.remote_path
                )

        # With confirm=True paramiko has already stat'ed the file and raised on a
        # mismatch; this re-checks its number to fail with a message naming the
        # blob. With confirm=False it returns an empty SFTPAttributes whose
        # st_size is None — there is nothing to compare, so trust the byte count
        # the transfer reported rather than inventing a mismatch.
        sent = attrs.st_size if attrs is not None else None
        if sent is None:
            sent = expected
        elif sent != expected:
            raise ValueError(
                f"size mismatch for {self.blob_name}: wrote {sent}, expected {expected}"
            )

        self.log.info(_summary("blob_to_sftp", sent, time.monotonic() - started))
        return {"blob": self.blob_name, "destination": self.remote_path, "size": sent}


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
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm the file exists on the SFTP side with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the path and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    dst, expected = result["destination"], result["size"]

    # Separate pod, so a fresh connection: this is an independent read of server
    # state, not a re-check of the streaming task's own handle.
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    with hook.get_managed_conn() as sftp_client:
        actual = sftp_client.stat(dst).st_size

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[blob_to_sftp] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-blob-to-sftp-stream",
    description="Stream a blob from Azure Blob Storage to the SFTP server",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "azure", "wasb", "sftp", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = BlobToSFTPStreamOperator(
        task_id="stream_transfer",
        wasb_conn_id=WASB_CONN_ID,
        sftp_conn_id=SFTP_CONN_ID,
        container_name=CONTAINER,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        blob_name=(
            "{{ dag_run.conf.get('blob_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        remote_path=f"{SFTP_DIR}/{{{{ dag_run.conf.get('filename', 'probe.txt') }}}}",
        doc_md="""
Opens the Azure and SFTP connections at once and pipes the blob through in
32 KiB chunks.

`BlobToSFTPStreamOperator` subclasses `BaseOperator` directly because no
provider ships a Blob → SFTP transfer — the `microsoft-azure` provider has only
`sftp_to_wasb`, `s3_to_wasb` and `local_to_wasb`, all pointing the other way.

**No pipe here, unlike the FTPS→Blob demo.** `WasbHook.download()` returns a
readable and paramiko's `putfo` pulls from one, so the two SDKs compose
directly. A pipe is only needed when both sides push, or both pull.

Fails on a size mismatch between the blob's reported size and what was written.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`stat`s the destination over SFTP and compares its size against the number of
bytes written, so a truncated transfer fails the run instead of passing
silently.

Runs in its own pod with a fresh connection, so it is an independent check of
server state rather than a re-read through the transfer task's handle.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
