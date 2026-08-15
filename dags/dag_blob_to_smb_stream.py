"""nix-dag-blob-to-smb-stream — stream a blob from Azure Blob Storage to an SMB share."""

import logging
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-blob-to-smb-stream

Streams a blob **Azure Blob Storage → SMB share** without staging it on disk.

```
Blob container  ──download──▶  dag (stream)  ──readinto──▶  SMB share
 <BLOB_PREFIX><file>                                        <SMB_DIR>/<file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "blob_prefix": "incoming/"}
```

Defaults to `probe.txt` and prefix `incoming/`.

#### The destination is a *writable*, which inverts the usual composition

Every other transfer demo here pairs a **readable source** with a **destination
that pulls**. SMB is the other way round: `SambaHook.open_file()` hands back a
file object opened for *writing*, so nothing is there to pull.

The Azure downloader supplies the missing half. `StorageStreamDownloader` has
`readinto(stream)`, which **pushes** its content into any writable:

```python
with self.samba_hook.open_file(target, mode="wb") as handle:
    written = downloader.readinto(handle)
```

So the four shapes across these demos are:

| Source | Destination | Bridge |
|---|---|---|
| readable (`open`, `download`) | pulls (`upload`, `putfo`, `storbinary`) | none |
| **pushes** (`retrbinary`) | pulls (`upload`) | `os.pipe()` + thread |
| readable with `readinto` | **writable** (`open_file`) | none — the source pushes |

Still no pipe: one side drives the loop and the other is a plain file object.
A pipe is only needed when both sides push, or both pull.

`readinto` also returns the byte count it wrote, which is what the size check
below compares against.

#### Write-then-rename

The transfer writes to `<name>.part` and renames once the last byte lands, so a
consumer watching the share never sees a partial file.

**Delete-then-rename, not `replace()`** — and this one is worth knowing before
you copy the pattern to another SMB server.

SMB has an atomic overwrite-rename (`FILE_RENAME_INFORMATION` with
`ReplaceIfExists`), which is what `smbclient.replace` issues. Samba refuses it:

```
SMBOSError: [NtStatus 0xc0000022] STATUS_ACCESS_DENIED
```

That is **not** a permissions problem. It fails between two files the account
just created and can delete outright — verified by deleting the same target
successfully immediately afterwards. `rename` onto a *free* name works, so the
DAG unlinks the target first and then renames.

The trade-off is honest: there is a brief window where neither name holds the
final file, so this is not atomic the way SFTP's `posix_rename` is. It still
prevents a consumer seeing a *partial* file, which is the point of the pattern.

`temp_suffix=""` disables it.

#### Paths are relative to the share

`SambaHook` takes the share from the connection's **schema** field and joins
paths itself, so this DAG passes `probe.txt`, not
`\\\\host\\share\\probe.txt`. Set `schema` to the share name when creating the
connection.

#### Memory

`readinto` writes in the SDK's own chunks and the blob is fetched on demand, so
peak memory is one chunk rather than the file size.

The usual Azure caveat applies in reverse here: `readinto` streams a *download*,
which has no `max_single_put_size` equivalent, so this direction is
constant-memory at any size — unlike the upload paths in #5 and #11.

`MAX_BYTES` caps a single transfer; a larger blob fails before any bytes move.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `smb_test_001` | SMB destination (conn type `samba`) |

For the SMB connection: **host** is an address the worker pods can resolve,
**schema** is the share name, **login**/**password** are the SMB credentials. Set
`share_type` in extra to `windows` for a Windows-style share; the default is
`posix`.

The share directory must be writable by the SMB user. A directory owned by
another account yields:

```
SMBOSError: [NtStatus 0xc0000022] STATUS_ACCESS_DENIED
```

which is a filesystem permission on the server, not an Airflow problem — the
share can list fine and still refuse writes.

For SAS auth on the Azure side the token goes in **extra** as `sas_token`, and
`login` is the storage account name.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WASB_CONN_ID = "wasb-nickstorageairflow002"
SMB_CONN_ID = "smb_test_001"

CONTAINER = "data001"

# Relative to the share named in the connection's schema field — SambaHook joins
# the UNC path itself. "" writes to the share root.
SMB_DIR = ""

MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


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


class BlobToSMBStreamOperator(BaseOperator):
    """Stream a single blob from Azure Blob Storage to an SMB share.

    No provider ships a Blob → SMB transfer, so this subclasses `BaseOperator`
    directly.

    The composition is inverted compared with the other demos: `open_file()`
    returns a *writable*, so the Azure downloader pushes into it with
    `readinto()` rather than the destination pulling. Nothing touches local disk
    and no pipe is needed.

    :param wasb_conn_id: Azure Blob connection (conn type `wasb`).
    :param samba_conn_id: SMB connection (conn type `samba`).
    :param container_name: source blob container.
    :param blob_name: source blob name, including any prefix.
    :param remote_path: destination path **relative to the share** named in the
        connection's schema field.
    :param temp_suffix: write to `<remote_path><temp_suffix>` and rename on
        success, so a consumer never sees a partial file. "" writes directly.
    :param max_bytes: reject a blob larger than this before any bytes move.
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
        samba_conn_id: str,
        container_name: str,
        blob_name: str,
        remote_path: str,
        temp_suffix: str = ".part",
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.samba_conn_id = samba_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.remote_path = remote_path
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

    def execute(self, context) -> dict[str, Any]:
        client = self.wasb_hook.get_conn().get_container_client(self.container_name)
        blob_client = client.get_blob_client(self.blob_name)

        # Size up front so an oversized blob fails before any bytes move, and so
        # verify has an authoritative number to compare against.
        expected = blob_client.get_blob_properties().size
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.blob_name} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[blob_to_smb] streaming wasb://%s/%s -> smb:%s (%s bytes)",
            self.container_name,
            self.blob_name,
            self.remote_path,
            expected,
        )

        started = time.monotonic()

        downloader = self.wasb_hook.download(
            container_name=self.container_name, blob_name=self.blob_name
        )

        # Write to a temporary name and rename once complete, so a consumer
        # watching the share never sees a partial file.
        target = self.remote_path + self.temp_suffix if self.temp_suffix else self.remote_path

        # open_file returns a *writable*, so the download pushes into it rather
        # than the destination pulling. readinto returns the byte count written,
        # which is what the check below compares.
        with self.samba_hook.open_file(target, mode="wb") as handle:
            # BYTE PATH — nothing touches the worker pod's disk:
            #   HTTPS GET (Azure) -> readinto pushes chunks -> SMB write.
            # Inverted from the others: the destination is a *writable*, so the
            # source drives the loop rather than being pulled from.
            written = downloader.readinto(handle)

        if written != expected:
            raise ValueError(
                f"size mismatch for {self.blob_name}: wrote {written}, expected {expected}"
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
                self.samba_hook.unlink(self.remote_path)
            except Exception:
                # Nothing to replace on a first run; the rename below is then
                # the whole operation.
                pass
            self.samba_hook.rename(target, self.remote_path)
            self.log.info("[blob_to_smb] renamed %s -> %s", target, self.remote_path)

        self.log.info(
            "%s -> %s",
            _summary("blob_to_smb", written, time.monotonic() - started),
            self.remote_path,
        )
        return {"blob": self.blob_name, "destination": self.remote_path, "size": written}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm the file exists on the share with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the path and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    dst, expected = result["destination"], result["size"]

    # Separate pod, so a fresh connection: an independent read of server state
    # rather than a re-check of the transfer task's own handle.
    hook = SambaHook(samba_conn_id=SMB_CONN_ID)
    actual = hook.stat(dst).st_size

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[blob_to_smb] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-blob-to-smb-stream",
    description="Stream a blob from Azure Blob Storage to an SMB share",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "azure", "wasb", "smb", "samba", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = BlobToSMBStreamOperator(
        task_id="stream_transfer",
        wasb_conn_id=WASB_CONN_ID,
        samba_conn_id=SMB_CONN_ID,
        container_name=CONTAINER,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        blob_name=(
            "{{ dag_run.conf.get('blob_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        remote_path="{{ dag_run.conf.get('filename', 'probe.txt') }}",
        doc_md="""
Opens the Azure and SMB connections at once and streams the blob across without
staging it on disk.

`BlobToSMBStreamOperator` subclasses `BaseOperator` directly because no provider
ships a Blob → SMB transfer.

**The composition is inverted here.** `SambaHook.open_file()` returns a
*writable*, not a readable, so the Azure downloader pushes into it with
`readinto(handle)` rather than the destination pulling. Still no pipe — one side
drives the loop and the other is a plain file object.

Writes to `<name>.part` and `replace()`s it on success, so a consumer watching
the share never sees a partial file.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`stat`s the destination on the share and compares its size against the number of
bytes written, so a truncated transfer fails the run instead of passing
silently.

Runs in its own pod with a fresh connection, so it is an independent read of
server state.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
