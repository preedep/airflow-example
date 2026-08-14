"""nix-dag-ftps-to-blob-stream — stream a file from the FTPS server to Azure Blob Storage."""

import ftplib
import logging
import ssl
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-ftps-to-blob-stream

Streams a file **FTPS → Azure Blob Storage** without staging it on disk.

```
FTPS server  ──retrbinary──▶  os.pipe()  ──upload──▶  Blob container
 FTPS_DIR/<file>                                      <BLOB_PREFIX><file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "blob_prefix": "incoming/"}
```

Defaults to `probe.txt` and prefix `incoming/`.

#### Why a custom operator

Unlike the SFTP→Blob demo, there is **no provider operator to subclass**. The
`microsoft-azure` provider ships `sftp_to_wasb`, `s3_to_wasb` and `local_to_wasb`,
but nothing for FTP/FTPS — so there is no wildcard expansion or blob naming to
inherit.

`FTPStoBlobStreamOperator` therefore subclasses `BaseOperator` directly. Writing
an operator rather than a `PythonOperator` buys three things a callable cannot:

- **Templated fields.** `remote_path`, `container_name` and `blob_name` render
  from `dag_run.conf`, so the resolved values appear in the UI's *Rendered
  Template* tab. A callable that reads `dag_run.conf` internally shows nothing
  there — you cannot see what path a past run actually used.
- **Reusable configuration.** Connection ids, size cap, chunk size and join
  timeout are constructor arguments, so a second task streaming a different file
  is one more operator call rather than a second copy of the function.
- **Lazily-built hooks.** `ftps_hook`/`wasb_hook` are `cached_property`, so
  constructing the operator at parse time opens no connection.

#### Why a pipe, when the SFTP demo needed none

The direction of control differs, and it decides the whole design.

| | source API | destination API | bridge |
|---|---|---|---|
| SFTP → Blob | `open()` returns a readable | `upload()` reads | none needed |
| FTPS → Blob | `retrbinary()` **pushes** to a callback | `upload()` **pulls** | `os.pipe()` |

`ftplib` never hands back a file object — it calls a callback per chunk. Azure's
`upload()` wants something to `read()`. Two pushers and no puller, so a pipe sits
between them: `retrbinary` writes one end on the main thread while `upload` reads
the other from a worker thread.

The pipe also gives **backpressure** for free. If Azure is slower than the FTPS
read, the pipe fills, the write blocks, and the two sides self-throttle to the
slower one instead of buffering the difference in memory.

#### Memory

Bytes move in 8 KiB chunks through the pipe, so the *transfer* holds one chunk.

One caveat, stated plainly: the Azure SDK does a **single-shot upload** for blobs
at or under `max_single_put_size` (64 MiB by default), and that path does one
`stream.read(length)` — buffering the whole file in memory. Above that threshold
it switches to block upload and streams in constant memory. So this is
constant-memory for large files, and up-to-64-MiB-buffered for small ones.

`MAX_BYTES` caps a single transfer; a larger file fails before any bytes move
rather than running unbounded.

#### Overwrite behaviour

`overwrite=True`, so re-running replaces the blob instead of failing with
`ResourceExistsError`. That keeps a retry idempotent — a task that died
mid-upload leaves a partial blob, and the retry must be able to replace it.

#### Source is not deleted

A successful run leaves the file on the FTPS server, so re-running re-transfers
it. Deliberate for a demo: deleting is destructive and a failed upload must not
lose the only copy. For a real pickup pattern, delete only after `verify`
confirms the size matches.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `ftps_test_001` | FTPS source (conn type `FTP`) |
| Connection | `wasb-nickstorageairflow002` | Azure Blob destination (conn type `wasb`) |
| Variable | `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

Set the FTPS connection to an address the **worker pods** can resolve. A hostname
that works from a laptop (VPN, mesh network, `/etc/hosts`) often does not resolve
inside the cluster.

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

FTPS_CONN_ID = "ftps_test_001"
WASB_CONN_ID = "wasb-nickstorageairflow002"

CONTAINER = "data001"

# Adjust to your server. FTPS_DIR is inside the FTPS user's chroot.
FTPS_DIR = "/upload"

# Bytes per retrbinary callback — the amount held in flight by the pipe itself.
CHUNK_SIZE = 8192
MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded

# Seconds to wait for the upload thread after the FTPS side finishes. Bounds a
# wedged upload instead of letting the task hang to its execution_timeout.
UPLOAD_JOIN_TIMEOUT = 300


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


class FTPStoBlobStreamOperator(BaseOperator):
    """Stream a single file from an FTPS server into Azure Blob Storage.

    No provider ships an FTP/FTPS → Blob transfer, so this subclasses
    `BaseOperator` directly rather than extending an existing operator.

    `ftplib` pushes bytes to a callback while the Azure SDK pulls from a file
    object, so the two are bridged with an `os.pipe()`: `retrbinary` writes one
    end on this thread, `upload` reads the other from a worker thread. Nothing
    touches local disk.

    :param ftp_conn_id: FTPS connection (conn type `FTP`).
    :param wasb_conn_id: Azure Blob connection (conn type `wasb`).
    :param remote_path: absolute path of the source file on the FTPS server.
    :param container_name: destination blob container.
    :param blob_name: destination blob name, including any prefix.
    :param overwrite: replace an existing blob. Keep on so a retry can replace
        the partial blob left by a task that died mid-upload.
    :param create_container: create the container first if it may not exist.
    :param max_bytes: reject a file larger than this before any bytes move.
    :param chunk_size: bytes per `retrbinary` callback.
    :param join_timeout: seconds to wait for the upload thread once the FTPS
        side is done, bounding a wedged upload.
    """

    # Rendered from dag_run.conf at run time, which is why the paths are
    # operator arguments rather than being read from context inside the task.
    # It also makes the resolved values visible in the UI's Rendered Template
    # tab, where a plain callable's locals are invisible.
    template_fields = ("remote_path", "container_name", "blob_name")

    # Keyword-only: these must never be confused with BaseOperator's own
    # positional parameters.
    def __init__(
        self,
        *,
        ftp_conn_id: str,
        wasb_conn_id: str,
        remote_path: str,
        container_name: str,
        blob_name: str,
        overwrite: bool = True,
        create_container: bool = False,
        max_bytes: int = MAX_BYTES,
        chunk_size: int = CHUNK_SIZE,
        join_timeout: int = UPLOAD_JOIN_TIMEOUT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.remote_path = remote_path
        self.container_name = container_name
        self.blob_name = blob_name
        self.overwrite = overwrite
        self.create_container = create_container
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size
        self.join_timeout = join_timeout

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def ftps_hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)

    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    def execute(self, context) -> dict[str, Any]:
        # Imported here, not at module scope: the DAG file is re-parsed on every
        # dag-processor cycle and only a live task needs these.
        import os
        import threading

        with self.ftps_hook as ftps:
            # Size up front for two reasons: it rejects an oversized file before
            # any bytes move, and Azure needs `length` to upload a non-seekable
            # stream.
            expected = ftps.get_size(self.remote_path)
            if expected is None:
                raise ValueError(f"{self.remote_path} not found on the FTPS server")
            if expected > self.max_bytes:
                raise ValueError(
                    f"{self.remote_path} is {expected} bytes, over the {self.max_bytes} limit"
                )

            self.log.info(
                "[ftps_to_blob] streaming %s -> wasb://%s/%s (%s bytes)",
                self.remote_path,
                self.container_name,
                self.blob_name,
                expected,
            )

            # An OS pipe, not a queue or a buffer: it gives backpressure for
            # free. When Azure is slower, the pipe fills and the FTPS write
            # blocks, so the two transfers self-throttle to the slower one.
            read_fd, write_fd = os.pipe()
            sent = 0

            # A thread cannot propagate its exception to the caller, so the
            # upload failure is stashed here and re-raised on the main thread
            # below. Without this the task would pass while the upload silently
            # failed.
            upload_error: list[BaseException] = []
            broken_pipe = False

            def _upload():
                # Runs while retrbinary is still writing; reads until EOF on
                # close. fdopen takes ownership of read_fd, so closing the
                # reader closes the fd exactly once.
                try:
                    with os.fdopen(read_fd, "rb") as reader:
                        self.wasb_hook.upload(
                            container_name=self.container_name,
                            blob_name=self.blob_name,
                            data=reader,
                            # Required for a pipe: without length the SDK would
                            # seek the stream to measure it, which a pipe cannot do.
                            length=expected,
                            create_container=self.create_container,
                            overwrite=self.overwrite,
                            max_concurrency=1,  # >1 requires a seekable source
                        )
                except BaseException as exc:  # surfaced on the main thread below
                    upload_error.append(exc)

            # daemon=True so a wedged upload cannot keep the worker pod alive
            # past the task; the join timeout below is the real guard.
            uploader = threading.Thread(target=_upload, daemon=True)
            uploader.start()

            try:
                # Leaving this `with` closes the write end, which is what raises
                # EOF for the reader thread — the join below therefore has to sit
                # in the finally, outside the with, or it would deadlock waiting
                # on a reader that can never see the stream end.
                with os.fdopen(write_fd, "wb") as writer:

                    def _write(chunk: bytes) -> None:
                        nonlocal sent
                        writer.write(chunk)
                        sent += len(chunk)

                    # retrbinary drives the whole transfer, calling _write per
                    # chunk until the file is exhausted.
                    ftps.get_conn().retrbinary(
                        f"RETR {self.remote_path}", _write, blocksize=self.chunk_size
                    )
            except BrokenPipeError:
                # The reader end died, which means the upload thread already
                # failed — its exception is the real cause and is re-raised
                # below. Swallowing this one keeps "Broken pipe" from masking
                # it; note the close on `with` exit can raise this too, which is
                # why the except wraps the whole block rather than just write().
                self.log.warning(
                    "[ftps_to_blob] pipe closed early — upload side failed first"
                )
                broken_pipe = True
            finally:
                # Closing the write end signals EOF, letting the reader finish.
                # In `finally` so a mid-transfer FTPS error still reaps the
                # thread instead of leaking it.
                uploader.join(timeout=self.join_timeout)

        # Checked in this order deliberately: a real upload exception is more
        # informative than the size mismatch it would also cause.
        if upload_error:
            raise upload_error[0]
        if uploader.is_alive():
            raise TimeoutError(
                f"upload of {self.blob_name} did not finish within {self.join_timeout}s"
            )
        # Only reachable if the reader died without recording why; without this
        # the run would fail on the size mismatch below, which describes the
        # symptom rather than the cause.
        if broken_pipe:
            raise RuntimeError(
                f"upload of {self.blob_name} closed the pipe without reporting an error"
            )
        # Guards against a truncated transfer that raised nothing on either side.
        if sent != expected:
            raise ValueError(
                f"size mismatch for {self.remote_path}: read {sent}, expected {expected}"
            )

        self.log.info("[ftps_to_blob] transferred %s bytes to %s", sent, self.blob_name)
        return {"source": self.remote_path, "blob": self.blob_name, "size": sent}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm the blob exists in the container with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the name and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    blob_name, expected = result["blob"], result["size"]

    # Separate pod, so a fresh client: this is an independent read of service
    # state, not a re-check of the streaming task's own handle.
    hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
    client = hook.get_conn().get_container_client(CONTAINER)
    actual = client.get_blob_client(blob_name).get_blob_properties().size

    if actual != expected:
        raise ValueError(f"{blob_name}: expected {expected} bytes, found {actual}")

    task_log.info("[ftps_to_blob] verified %s — %s bytes", blob_name, actual)
    return {"container": CONTAINER, "blob": blob_name, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-ftps-to-blob-stream",
    description="Stream a file from the FTPS server to Azure Blob Storage",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "ftps", "azure", "wasb", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = FTPStoBlobStreamOperator(
        task_id="stream_transfer",
        ftp_conn_id=FTPS_CONN_ID,
        wasb_conn_id=WASB_CONN_ID,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what makes the resolved paths show up
        # in the UI's Rendered Template tab.
        remote_path=f"{FTPS_DIR}/{{{{ dag_run.conf.get('filename', 'probe.txt') }}}}",
        container_name=CONTAINER,
        blob_name=(
            "{{ dag_run.conf.get('blob_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        overwrite=True,  # a retry must be able to replace a partial blob
        doc_md="""
Opens the FTPS and Azure connections at once and pipes the file through in 8 KiB
chunks.

`FTPStoBlobStreamOperator` subclasses `BaseOperator` directly because the
`microsoft-azure` provider has no FTP/FTPS transfer — only `sftp_to_wasb`,
`s3_to_wasb` and `local_to_wasb`. There is nothing to extend.

The pipe exists because `ftplib` **pushes** to a callback while Azure's `upload`
**pulls** from a file object; it also applies backpressure, so the faster side
waits rather than buffering.

Fails on a size mismatch between what FTPS reported and what was read.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
Reads the blob's properties from Azure and compares its size against the number
of bytes streamed, so a truncated transfer fails the run instead of passing
silently.

Uses the blob name captured by the transfer task rather than rebuilding it from
conf, so the check covers exactly what this run wrote.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
