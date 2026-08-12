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

FTPS_CONN_ID = "ftps_test_001"
SFTP_CONN_ID = "sftp_test_001"

# Adjust both to your servers. FTPS_DIR is inside the FTPS user's chroot;
# SFTP_DIR must be writable by the SFTP connection's user.
FTPS_DIR = "/upload"
SFTP_DIR = "/home/airflowsftp/incoming"

CHUNK_SIZE = 8192
BUFFER_LIMIT = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


class MyFTPSHook(FTPSHook):
    """FTPSHook that trusts a private CA and encrypts data transfers."""

    def get_conn(self) -> ftplib.FTP:
        from airflow.sdk import Variable

        if self.conn is not None:
            return self.conn

        # Host, login, password, port and passive flag all come from the
        # Airflow Connection. Only the TLS context differs, and it is the
        # reason for this override.
        params = self.get_connection(self.ftp_conn_id)

        context = ssl.create_default_context(cadata=Variable.get("ftps_ca_cert"))

        if params.port:
            ftplib.FTP_TLS.port = params.port

        conn = ftplib.FTP_TLS(params.host, params.login, params.password, context=context)
        conn.prot_p()  # stock FTPSHook omits this — without it data is plaintext
        conn.set_pasv(params.extra_dejson.get("passive", True))

        self.conn = conn
        return self.conn


def stream_ftps_to_sftp(**context):
    """Pipe one file from FTPS to SFTP without staging it on disk.

    FTPS `retrbinary` pushes chunks into a pipe; paramiko's `putfo` pulls from
    the other end in a worker thread. Both sockets are open at once and memory
    stays at one chunk.
    """
    import os
    import threading

    task_log = logging.getLogger("airflow.task")

    filename = (context["dag_run"].conf or {}).get("filename", "probe.txt")
    src = f"{FTPS_DIR}/{filename}"
    dst = f"{SFTP_DIR}/{filename}"

    ftps_hook = MyFTPSHook(ftp_conn_id=FTPS_CONN_ID)
    sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

    with ftps_hook as ftps:
        expected = ftps.get_size(src)
        if expected is None:
            raise ValueError(f"{src} not found on the FTPS server")
        if expected > BUFFER_LIMIT:
            raise ValueError(f"{src} is {expected} bytes, over the {BUFFER_LIMIT} limit")

        task_log.info("[ftps_to_sftp] streaming %s -> %s (%s bytes)", src, dst, expected)

        read_fd, write_fd = os.pipe()
        sent = 0
        put_error: list[BaseException] = []

        def _put():
            # Runs while retrbinary is still writing; reads until EOF on close.
            try:
                with os.fdopen(read_fd, "rb") as reader:
                    sftp_hook.get_conn().putfo(reader, dst)
            except BaseException as exc:  # surfaced on the main thread below
                put_error.append(exc)

        putter = threading.Thread(target=_put, daemon=True)
        putter.start()

        try:
            with os.fdopen(write_fd, "wb") as writer:

                def _write(chunk: bytes) -> None:
                    nonlocal sent
                    writer.write(chunk)
                    sent += len(chunk)

                ftps.get_conn().retrbinary(f"RETR {src}", _write, blocksize=CHUNK_SIZE)
        finally:
            # Closing the write end signals EOF, letting the reader finish.
            putter.join(timeout=300)

    if put_error:
        raise put_error[0]
    if putter.is_alive():
        raise TimeoutError(f"SFTP upload of {dst} did not finish within 300s")
    if sent != expected:
        raise ValueError(f"size mismatch for {filename}: read {sent}, expected {expected}")

    task_log.info("[ftps_to_sftp] transferred %s bytes to %s", sent, dst)
    return {"source": src, "destination": dst, "size": sent}


def verify_transfer(ti=None, **context):
    """Confirm the file exists on the SFTP side with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    result = ti.xcom_pull(task_ids="stream_transfer")
    dst, expected = result["destination"], result["size"]

    sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    actual = sftp_hook.get_conn().stat(dst).st_size

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[ftps_to_sftp] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


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

    stream >> verify
