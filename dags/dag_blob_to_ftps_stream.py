"""nix-dag-blob-to-ftps-stream — stream a blob from Azure Blob Storage to the FTPS server."""

import ftplib
import logging
import ssl
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-blob-to-ftps-stream

Streams a blob **Azure Blob Storage → FTPS** without staging it on disk.

```
Blob container  ──download──▶  dag (stream)  ──storbinary──▶  FTPS server
 <BLOB_PREFIX><file>                                          FTPS_DIR/<file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "blob_prefix": "incoming/"}
```

Defaults to `probe.txt` and prefix `incoming/`.

#### Why this needs no pipe, even though FTPS is involved

The FTPS→Blob demo needs an `os.pipe()` and a worker thread. This one — the same
protocol, the same two SDKs, opposite direction — needs neither. The reason is
that **`ftplib` changes which side controls the loop depending on direction**:

| ftplib call | direction | control |
|---|---|---|
| `retrbinary(cmd, callback)` | reading | **pushes** to your callback |
| `storbinary(cmd, fp)` | writing | **pulls** via `fp.read(blocksize)` |

`storbinary` loops on `fp.read(blocksize)`, which is exactly what
`WasbHook.download()` returns — a `StorageStreamDownloader` whose `read(size)`
yields bytes and an empty result at EOF. Reader on one side, puller on the other,
so they compose directly.

So across the four transfer demos, only one needs a pipe:

| Direction | source | destination | bridge |
|---|---|---|---|
| SFTP → Blob | readable | `upload()` pulls | none |
| FTPS → Blob | `retrbinary` **pushes** | `upload()` **pulls** | `os.pipe()` + thread |
| Blob → SFTP | readable | `putfo()` pulls | none |
| Blob → FTPS *(this one)* | readable | `storbinary()` pulls | none |

Reach for a pipe only when both sides push or both pull — and check the specific
call, not the protocol.

#### Memory

`storbinary` reads in 8 KiB blocks and the downloader fetches on demand, so peak
memory is one block rather than the file size. A multi-GB blob transfers in
constant memory.

`MAX_BYTES` caps a single transfer; a larger blob fails before any bytes move
rather than running unbounded.

#### Write-then-rename

The transfer writes to `<remote_path>.part` and renames it once the last byte
lands, so a consumer polling the directory never sees a partial file and a failed
run leaves an obvious `.part` rather than a truncated file that looks complete.
The rename is a metadata operation (RNFR/RNTO), so no data is re-transferred.
`temp_suffix=""` disables it.

One caveat worth knowing: whether `rename` overwrites an existing target is
**server-dependent**. It does on the server tested here; a stricter server may
require deleting the target first. SFTP has the same split — plain `rename`
fails on an existing target, which is why the SFTP demos use `posix_rename`.

The object-store demos do **not** do this: neither Blob nor S3 has a rename, so
the equivalent would be a full server-side copy — and neither makes a
partially-uploaded object visible in the first place.

#### Idempotency

`STOR` truncates and replaces an existing file, so a retry overwrites whatever a
failed run left behind rather than appending to it.

Unlike the SFTP demo there is no `confirm` equivalent — `storbinary` returns the
server's response code, not a size — so the `verify` task is the only check that
the bytes landed intact.

#### Source is not deleted

A successful run leaves the blob in the container, so re-running re-transfers it.
Deliberate for a demo: deleting is destructive and a failed write must not lose
the only copy. For a real drain pattern, delete only after `verify` confirms the
size matches.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `ftps_test_001` | FTPS destination (conn type `FTP`) |
| Variable | `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

Set the FTPS connection to an address the **worker pods** can resolve. A hostname
that works from a laptop (VPN, mesh network, `/etc/hosts`) often does not resolve
inside the cluster.

The destination directory must exist and be writable by the connection's user.
FTPS chroots often make the login root itself non-writable — a `553 Could not
create file` means the directory, not the credentials, is the problem.

For SAS auth the token goes in the Azure connection's **extra** as `sas_token`,
and `login` is the **storage account name** — the hook builds the account URL
from it. The container is not part of the connection; it is passed per operation.

#### Why a custom FTPS hook

Stock `FTPSHook.get_conn()` hardcodes `ssl.create_default_context()` with no way
to supply a CA, so a self-signed cert fails with `CERTIFICATE_VERIFY_FAILED`.
`MyFTPSHook` builds the context from `ftps_ca_cert` — **verification stays ON** —
and calls `prot_p()`, which the stock hook omits, so the data channel is
encrypted. With a publicly trusted certificate none of this is needed.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WASB_CONN_ID = "wasb-nickstorageairflow002"
FTPS_CONN_ID = "ftps_test_001"

CONTAINER = "data001"

# Adjust to your server. FTPS_DIR is inside the FTPS user's chroot and must be
# writable by the connection's user.
FTPS_DIR = "/upload"

# Bytes per storbinary read. Peak memory of the transfer, not the file size —
# the downloader fetches on demand, so blocks do not accumulate.
CHUNK_SIZE = 8192
MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


# --------------------------------------------------------------------------- #
# Hook override
# --------------------------------------------------------------------------- #


class MyFTPSHook(FTPSHook):
    """FTPSHook that trusts a private CA and encrypts data transfers."""

    def get_conn(self) -> ftplib.FTP:
        # Imported inside the method, not at module scope: the DAG file is
        # re-parsed every dag-processor cycle, and Variable.get() at import time
        # would be a DB round-trip on every one of them.
        from airflow.sdk import Variable

        # The base hook treats self.conn as its connection cache; honouring it
        # keeps one FTPS session per hook instance rather than one per call.
        if self.conn is not None:
            return self.conn

        params = self.get_connection(self.ftp_conn_id)

        # cadata adds the private CA *in addition to* the system trust store;
        # it does not disable verification. check_hostname stays on, so the
        # certificate's CN/SAN must still match the connection's host.
        context = ssl.create_default_context(cadata=Variable.get("ftps_ca_cert"))

        # ftplib.FTP_TLS takes no port argument, so the port can only be set on
        # the class. This mutates global state — acceptable because each task
        # runs in its own pod, but it would leak across DAGs in one process.
        if params.port:
            ftplib.FTP_TLS.port = params.port

        conn = ftplib.FTP_TLS(params.host, params.login, params.password, context=context)
        conn.prot_p()  # stock FTPSHook omits this — without it data is plaintext
        conn.set_pasv(params.extra_dejson.get("passive", True))

        self.conn = conn
        return self.conn


# --------------------------------------------------------------------------- #
# Operator
# --------------------------------------------------------------------------- #


class BlobToFTPSStreamOperator(BaseOperator):
    """Stream a single blob from Azure Blob Storage to an FTPS server.

    No provider ships a Blob → FTP/FTPS transfer, so this subclasses
    `BaseOperator` directly rather than extending an existing operator.

    `WasbHook.download()` returns a readable and `ftplib.storbinary` pulls from
    one via `fp.read(blocksize)`, so the two compose without the pipe and worker
    thread the FTPS→Blob direction needs. Nothing touches local disk.

    :param wasb_conn_id: Azure Blob connection (conn type `wasb`).
    :param ftp_conn_id: FTPS connection (conn type `FTP`).
    :param container_name: source blob container.
    :param blob_name: source blob name, including any prefix.
    :param remote_path: destination path on the FTPS server. The parent
        directory must already exist and be writable.
    :param max_bytes: reject a blob larger than this before any bytes move.
    :param chunk_size: bytes per `storbinary` read.
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
        ftp_conn_id: str,
        container_name: str,
        blob_name: str,
        remote_path: str,
        max_bytes: int = MAX_BYTES,
        chunk_size: int = CHUNK_SIZE,
        temp_suffix: str = ".part",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.ftp_conn_id = ftp_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.remote_path = remote_path
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size
        self.temp_suffix = temp_suffix

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    @cached_property
    def ftps_hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)

    def execute(self, context) -> dict[str, Any]:
        client = self.wasb_hook.get_conn().get_container_client(self.container_name)
        blob_client = client.get_blob_client(self.blob_name)

        # Size up front so an oversized blob fails before any bytes move, and so
        # verify has an authoritative number to compare against. storbinary
        # returns a response code, not a byte count, so this is the only figure
        # the transfer itself can report.
        expected = blob_client.get_blob_properties().size
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.blob_name} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[blob_to_ftps] streaming wasb://%s/%s -> %s (%s bytes)",
            self.container_name,
            self.blob_name,
            self.remote_path,
            expected,
        )

        started = time.monotonic()

        # download() returns a StorageStreamDownloader, whose read(size) returns
        # bytes and empty at EOF — exactly what storbinary's read loop expects.
        # No pipe or thread: one side reads, the other pulls.
        downloader = self.wasb_hook.download(
            container_name=self.container_name, blob_name=self.blob_name
        )

        # The hook's context manager closes the FTPS session on exit, so the
        # whole transfer happens inside it.
        # Write to a temporary name and rename once the transfer completes, so a
        # consumer watching the directory never sees a partial file and a failed
        # run leaves an obvious <name>.part rather than a truncated file.
        target = self.remote_path + self.temp_suffix if self.temp_suffix else self.remote_path

        with self.ftps_hook as ftps:
            conn = ftps.get_conn()
            # storbinary drives the transfer, calling downloader.read() per
            # block until it returns empty. STOR truncates any existing file,
            # which is what makes a retry replace rather than append.
            # BYTE PATH — nothing touches the worker pod's disk:
            #   HTTPS GET -> downloader -> storbinary reads 8 KiB -> FTPS data
            #   socket. storbinary pulls, so one block is resident at a time.
            response = conn.storbinary(
                f"STOR {target}", downloader, blocksize=self.chunk_size
            )

            if self.temp_suffix:
                # RNFR/RNTO. Unlike SFTP's plain rename, this overwrites an
                # existing target on the servers tested here — but that is
                # server-dependent, so a strict server may need a delete first.
                conn.rename(target, self.remote_path)
                self.log.info(
                    "[blob_to_ftps] renamed %s -> %s", target, self.remote_path
                )

        self.log.info(
            "%s -> %s (%s)",
            _summary("blob_to_ftps", expected, time.monotonic() - started),
            self.remote_path,
            response,
        )
        return {
            "blob": self.blob_name,
            "destination": self.remote_path,
            "size": expected,
        }


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
    """Confirm the file exists on the FTPS side with the size we sent.

    This matters more here than in the other transfer demos: `storbinary`
    reports only a response code, so nothing in the transfer task itself proves
    the byte count landed intact.
    """
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the path and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    dst, expected = result["destination"], result["size"]

    # Separate pod, so a fresh connection: this is an independent read of server
    # state, not a re-check of the streaming task's own handle.
    hook = MyFTPSHook(ftp_conn_id=FTPS_CONN_ID)
    with hook as ftps:
        actual = ftps.get_size(dst)

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[blob_to_ftps] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-blob-to-ftps-stream",
    description="Stream a blob from Azure Blob Storage to the FTPS server",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "azure", "wasb", "ftps", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = BlobToFTPSStreamOperator(
        task_id="stream_transfer",
        wasb_conn_id=WASB_CONN_ID,
        ftp_conn_id=FTPS_CONN_ID,
        container_name=CONTAINER,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        blob_name=(
            "{{ dag_run.conf.get('blob_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        remote_path=f"{FTPS_DIR}/{{{{ dag_run.conf.get('filename', 'probe.txt') }}}}",
        doc_md="""
Opens the Azure and FTPS connections at once and streams the blob through in
8 KiB blocks.

`BlobToFTPSStreamOperator` subclasses `BaseOperator` directly because no
provider ships a Blob → FTP/FTPS transfer.

**No pipe here, unlike the FTPS→Blob demo** — same protocol, opposite direction.
`storbinary` *pulls* via `fp.read(blocksize)` while `retrbinary` *pushes* to a
callback, and `WasbHook.download()` returns a readable, so these two compose
directly.

`STOR` truncates an existing file, so a retry replaces rather than appends.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`SIZE`s the destination over FTPS and compares it against the blob's size, so a
truncated transfer fails the run instead of passing silently.

Load-bearing here rather than belt-and-braces: `storbinary` returns only a
response code, so this is the sole check that the right number of bytes landed.

Runs in its own pod with a fresh connection, so it is an independent read of
server state.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
