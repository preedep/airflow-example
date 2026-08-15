"""nix-dag-ftps-to-sftp-stream — stream a file from the FTPS server to the SFTP server."""

import ftplib
import logging
import ssl
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-ftps-to-sftp-stream

Streams a file **FTPS → SFTP** without staging it on disk.

```
FTPS server  ──get──▶  dag (stream)  ──put──▶  SFTP server
 FTPS_DIR/<file>                                SFTP_DIR/<file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt"}
```

Defaults to `probe.txt`.

#### Why one task, not two

The diagram shows get and put as separate arrows, but they are a **single Airflow
task** on purpose. Under KubernetesExecutor every task runs in its own ephemeral
pod, so a file downloaded by a "get" task would not exist for a separate "put"
task — it fails at runtime with `FileNotFoundError`.

Splitting them would also mean writing the payload to disk twice. Instead the
FTPS `retrbinary` callback writes into a pipe that paramiko's `putfo` reads, so
bytes go straight from one socket to the other.

#### Memory

The transfer streams in 8 KiB chunks through an `os.pipe()`, so peak memory is
the chunk size, not the file size. A multi-GB file transfers in constant memory.

`BUFFER_LIMIT` caps a single transfer; a file larger than that fails rather than
running unbounded.

#### Source is not deleted

A successful run leaves the file on the FTPS server, so re-running re-transfers
it. That is deliberate for a demo — deleting is destructive and a failed put
must not lose the only copy. For a real pickup pattern, delete only after
`verify` confirms the size matches.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `ftps_test_001` | FTPS source (conn type `FTP`) |
| Connection | `sftp_test_001` | SFTP destination (conn type `SFTP`) |
| Variable | `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

Set both connections to an address the **worker pods** can resolve. A hostname
that works from a laptop (VPN, mesh network, `/etc/hosts`) often does not resolve
inside the cluster.

#### Why a custom FTPS hook

Stock `FTPSHook.get_conn()` hardcodes `ssl.create_default_context()` with no way
to supply a CA, so the self-signed cert fails with `CERTIFICATE_VERIFY_FAILED`.
`MyFTPSHook` builds the context from `ftps_ca_cert` — **verification stays ON** —
and calls `prot_p()`, which the stock hook omits, so the data channel is
encrypted. The SFTP side needs no such workaround.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

FTPS_CONN_ID = "ftps_test_001"
SFTP_CONN_ID = "sftp_test_001"

# Adjust both to your servers. FTPS_DIR is inside the FTPS user's chroot;
# SFTP_DIR must be writable by the SFTP connection's user.
FTPS_DIR = "/upload"
SFTP_DIR = "/home/airflowsftp/incoming"

# Bytes per retrbinary callback. This is the peak memory of the transfer, not
# the file size — the pipe applies backpressure, so a slow SFTP side stalls the
# FTPS reader rather than letting chunks accumulate.
# Write to <name>.part and rename on success, so a consumer polling the
# destination never sees a partial file. Set to "" to write directly.
TEMP_SUFFIX = ".part"

CHUNK_SIZE = 8192
BUFFER_LIMIT = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


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

        # Host, login, password, port and passive flag all come from the
        # Airflow Connection. Only the TLS context differs, and it is the
        # reason for this override.
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


def stream_ftps_to_sftp(**context):
    """Pipe one file from FTPS to SFTP without staging it on disk.

    FTPS `retrbinary` pushes chunks into a pipe; paramiko's `putfo` pulls from
    the other end in a worker thread. Both sockets are open at once and memory
    stays at one chunk.
    """
    # Imports live in the callable, not at module scope: the DAG file is
    # re-parsed on every dag-processor cycle and only this task needs them.
    import os
    import threading
    import time

    task_log = logging.getLogger("airflow.task")

    filename = (context["dag_run"].conf or {}).get("filename", "probe.txt")
    src = f"{FTPS_DIR}/{filename}"
    dst = f"{SFTP_DIR}/{filename}"
    tmp_dst = f"{dst}{TEMP_SUFFIX}" if TEMP_SUFFIX else dst

    started = time.monotonic()
    ftps_hook = MyFTPSHook(ftp_conn_id=FTPS_CONN_ID)
    sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

    with ftps_hook as ftps:
        expected = ftps.get_size(src)
        if expected is None:
            raise ValueError(f"{src} not found on the FTPS server")
        if expected > BUFFER_LIMIT:
            raise ValueError(f"{src} is {expected} bytes, over the {BUFFER_LIMIT} limit")

        task_log.info("[ftps_to_sftp] streaming %s -> %s (%s bytes)", src, dst, expected)

        # An OS pipe, not a queue or a buffer: it gives backpressure for free.
        # When the SFTP side is slower, the pipe fills and the FTPS write blocks,
        # so the two transfers self-throttle to the slower one.
        # BYTE PATH — nothing touches the worker pod's disk:
        #   FTPS socket -> retrbinary callback -> os.pipe() (a kernel buffer,
        #   not process heap) -> reader thread -> paramiko putfo -> SFTP socket.
        # Both sockets are open at once and only one 8 KiB chunk is in flight.
        read_fd, write_fd = os.pipe()
        sent = 0

        # A thread cannot propagate its exception to the caller, so the put
        # failure is stashed here and re-raised on the main thread below.
        # Without this the task would pass while the upload silently failed.
        put_error: list[BaseException] = []

        def _put():
            # Runs while retrbinary is still writing; reads until EOF on close.
            # fdopen takes ownership of read_fd, so closing the reader closes
            # the fd exactly once — no separate os.close for this end.
            try:
                with os.fdopen(read_fd, "rb") as reader:
                    # putfo streams from the file object; it never needs the
                    # total size up front, which is what allows a pipe source.
                    sftp_hook.get_conn().putfo(reader, tmp_dst)
            except BaseException as exc:  # surfaced on the main thread below
                put_error.append(exc)

        # daemon=True so a wedged upload cannot keep the worker pod alive past
        # the task; the join timeout below is the real guard.
        putter = threading.Thread(target=_put, daemon=True)
        putter.start()

        try:
            # Leaving this `with` closes the write end, which is what raises EOF
            # for the reader thread — the join below therefore has to sit in the
            # finally, outside the with, or it would deadlock waiting on a
            # reader that can never see the stream end.
            with os.fdopen(write_fd, "wb") as writer:

                def _write(chunk: bytes) -> None:
                    nonlocal sent
                    writer.write(chunk)
                    sent += len(chunk)

                # retrbinary drives the whole transfer, calling _write per chunk
                # until the file is exhausted.
                ftps.get_conn().retrbinary(f"RETR {src}", _write, blocksize=CHUNK_SIZE)
        finally:
            # Closing the write end signals EOF, letting the reader finish.
            # In `finally` so a mid-transfer FTPS error still reaps the thread
            # instead of leaking it.
            putter.join(timeout=300)

    # Checked in this order deliberately: a real upload exception is more
    # informative than the size mismatch it would also cause.
    if put_error:
        raise put_error[0]
    if putter.is_alive():
        raise TimeoutError(f"SFTP upload of {dst} did not finish within 300s")
    # Guards against a truncated transfer that raised nothing on either side.
    if sent != expected:
        raise ValueError(f"size mismatch for {filename}: read {sent}, expected {expected}")

    # Renamed only after every check above passes, so the final name never
    # appears unless the transfer is known good. posix_rename, not rename:
    # plain SFTP rename fails with "OSError: Failure" when the target exists,
    # which would break a retry over a previous run's file.
    if TEMP_SUFFIX:
        with sftp_hook.get_managed_conn() as sftp_client:
            sftp_client.posix_rename(tmp_dst, dst)
        task_log.info("[ftps_to_sftp] renamed %s -> %s", tmp_dst, dst)

    task_log.info("%s -> %s", _summary("ftps_to_sftp", sent, time.monotonic() - started), dst)
    return {"source": src, "destination": dst, "size": sent}


def verify_transfer(ti=None, **context):
    """Confirm the file exists on the SFTP side with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the path and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    dst, expected = result["destination"], result["size"]

    # Separate pod, so a fresh connection: this is an independent read of
    # server state, not a re-check of the streaming task's own handle.
    sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    actual = sftp_hook.get_conn().stat(dst).st_size

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[ftps_to_sftp] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-ftps-to-sftp-stream",
    description="Stream a file from the FTPS server to the SFTP server",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "ftps", "sftp", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = PythonOperator(
        task_id="stream_transfer",
        python_callable=stream_ftps_to_sftp,
        doc_md="""
Opens both connections at once and pipes the file through in 8 KiB chunks.

No provider operator covers a direct FTPS→SFTP transfer — the transfer
operators move between a remote host and *local disk*, which would mean two
tasks and a staged file. Hence a `PythonOperator` here.

Fails on a size mismatch between what FTPS reported and what was read.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`stat`s the destination over SFTP and compares its size against the number of
bytes streamed, so a truncated transfer fails the run instead of passing
silently.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
