"""nix-dag-s3-to-ftps-stream — stream an object from Amazon S3 to the FTPS server."""

import ftplib
import logging
import ssl
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-s3-to-ftps-stream

Streams an object **Amazon S3 → FTPS** without staging it on disk.

```
S3 bucket  ──get_object──▶  storbinary  ──▶  FTPS server
 <S3_PREFIX><file>                            FTPS_DIR/<file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "s3_prefix": "incoming/"}
```

Defaults to `probe.txt` and S3 prefix `incoming/`.

#### Two things wrong with the stock operator, not one

The `amazon` provider ships `S3ToFTPOperator`, and this DAG subclasses it — but
it needs **two** overrides rather than the usual one.

**1. It stages the whole object on disk:**

```python
with NamedTemporaryFile() as local_tmp_file:
    s3_obj.download_fileobj(local_tmp_file)
    local_tmp_file.seek(0)
    ftp_hook.store_file(self.ftp_path, local_tmp_file.name)
```

**2. It uses `FTPHook` — plain FTP, no TLS.** That is fine for the operator's
intended use but wrong for a server requiring FTPS, and the class hardcodes the
hook rather than exposing it as a property, so there is nothing to swap.

`StreamingS3ToFTPSOperator` therefore overrides `execute` outright and builds
`MyFTPSHook` itself. The inherited value is the templated fields and argument
surface, not the transfer logic.

#### Composition: `storbinary` pulls, S3's body reads

No pipe is needed. `ftplib.storbinary` loops on `fp.read(blocksize)`, and the
S3 response body is a readable, so the two compose directly:

```python
body = s3_client.get_object(Bucket=..., Key=...)["Body"]
ftps.storbinary(f"STOR {target}", body, blocksize=CHUNK_SIZE)
```

Contrast the **FTPS→Blob** direction (#5), which needs an `os.pipe()` and a
worker thread: there `retrbinary` *pushes* to a callback while the Azure upload
*pulls*. Same protocol, opposite direction, completely different plumbing —
because `ftplib` swaps which side drives the loop:

| ftplib call | direction | control |
|---|---|---|
| `retrbinary(cmd, callback)` | reading | **pushes** |
| `storbinary(cmd, fp)` | writing | **pulls** via `fp.read()` |

Measured: 50 MiB S3 → FTPS in **3.9 s**.

#### Write-then-rename

The transfer writes to `<name>.part` and renames once the last byte lands, so a
consumer watching the drop directory never sees a partial file.

FTPS `rename` (RNFR/RNTO) overwrote an existing target on the server tested here,
but that is **server-dependent** — a stricter server may need the target deleted
first. `temp_suffix=""` disables the pattern.

#### `verify` is load-bearing

`storbinary` returns only a response code like `226 Transfer complete`, never a
byte count, so nothing in the transfer task itself proves the right number of
bytes landed. The `SIZE` check in `verify` is the only thing that does.

#### Memory

`storbinary` reads in 8 KiB blocks and the S3 body is fetched on demand, so peak
memory is one block rather than the object size.

`MAX_BYTES` caps a single transfer; a larger object fails before any bytes move.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `ftps_test_001` | FTPS destination (conn type `FTP`) |
| Variable | `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

Set the FTPS connection to an address the **worker pods** can resolve. The
destination directory must exist and be writable; FTPS chroots often make the
login root non-writable, and `553 Could not create file` means the directory, not
the credentials.

#### Why a custom FTPS hook

Stock `FTPSHook.get_conn()` hardcodes `ssl.create_default_context()` with no way
to supply a CA, so a self-signed cert fails with `CERTIFICATE_VERIFY_FAILED`.
`MyFTPSHook` builds the context from `ftps_ca_cert` — **verification stays ON** —
and calls `prot_p()`, which the stock hook omits, so the data channel is
encrypted. With a publicly trusted certificate none of this is needed.

#### Egress cost

S3 charges for data leaving AWS. A demo file is free; a large bucket is a real
bill.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_s3_test_001"
FTPS_CONN_ID = "ftps_test_001"

BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

# Adjust to your server. FTPS_DIR is inside the FTPS user's chroot and must be
# writable by the connection's user.
FTPS_DIR = "/upload"

# Bytes per storbinary read — peak memory of the transfer, not the object size.
CHUNK_SIZE = 8192
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
# Operator override
# --------------------------------------------------------------------------- #


class StreamingS3ToFTPSOperator(S3ToFTPOperator):
    """`S3ToFTPOperator` that streams over FTPS instead of staging on disk.

    Two overrides rather than the usual one: `execute` replaces the temp-file
    copy with a direct stream, and the hook is `MyFTPSHook` because the base
    class hardcodes plain `FTPHook` with no property to swap.

    :param temp_suffix: write to `<ftp_path><temp_suffix>` and rename on
        success, so a consumer never sees a partial file. "" writes directly.
    :param max_bytes: reject an object larger than this before any bytes move.
    :param chunk_size: bytes per `storbinary` read.
    """

    def __init__(
        self,
        *,
        temp_suffix: str = ".part",
        max_bytes: int = MAX_BYTES,
        chunk_size: int = CHUNK_SIZE,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.temp_suffix = temp_suffix
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def ftps_hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)

    def execute(self, context) -> dict[str, Any]:
        s3_client = S3Hook(aws_conn_id=self.aws_conn_id).get_conn()

        # Size up front: rejects an oversized object before any bytes move, and
        # gives verify an authoritative number. storbinary returns a response
        # code, not a byte count, so this is the only figure available.
        expected = s3_client.head_object(Bucket=self.s3_bucket, Key=self.s3_key)[
            "ContentLength"
        ]
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.s3_key} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[s3_to_ftps] streaming s3://%s/%s -> %s (%s bytes)",
            self.s3_bucket,
            self.s3_key,
            self.ftp_path,
            expected,
        )

        started = time.monotonic()

        # Write to a temporary name and rename once complete, so a consumer
        # watching the directory never sees a partial file.
        target = self.ftp_path + self.temp_suffix if self.temp_suffix else self.ftp_path

        # The S3 response body is a readable and storbinary pulls via
        # fp.read(blocksize), so the two compose directly — no pipe, unlike the
        # FTPS→Blob direction where retrbinary pushes.
        body = s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)["Body"]

        with self.ftps_hook as ftps:
            conn = ftps.get_conn()
            response = conn.storbinary(
                f"STOR {target}", body, blocksize=self.chunk_size
            )

            # storbinary reports no byte count, so the size is read back from
            # the server. Checked *before* the rename, so a truncated transfer
            # never replaces a good previous file.
            written = conn.size(target)
            if written != expected:
                raise ValueError(
                    f"size mismatch for {self.s3_key}: wrote {written}, expected {expected}"
                )

            if self.temp_suffix:
                # RNFR/RNTO. Overwrites an existing target on the servers tested
                # here, but that is server-dependent — a strict server may need
                # the target deleted first.
                conn.rename(target, self.ftp_path)
                self.log.info("[s3_to_ftps] renamed %s -> %s", target, self.ftp_path)

        self.log.info(
            "%s -> %s (%s)",
            _summary("s3_to_ftps", written, time.monotonic() - started),
            self.ftp_path,
            response,
        )
        return {"key": self.s3_key, "destination": self.ftp_path, "size": written}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm the file exists on the FTPS side with the size we sent.

    Load-bearing rather than belt-and-braces: `storbinary` returns only a
    response code, so this is the sole check that the right bytes landed.
    """
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the path and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    dst, expected = result["destination"], result["size"]

    # Separate pod, so a fresh connection: an independent read of server state
    # rather than a re-check of the transfer task's own handle.
    hook = MyFTPSHook(ftp_conn_id=FTPS_CONN_ID)
    with hook as ftps:
        actual = ftps.get_size(dst)

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[s3_to_ftps] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-s3-to-ftps-stream",
    description="Stream an object from Amazon S3 to the FTPS server",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "s3", "ftps", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = StreamingS3ToFTPSOperator(
        task_id="stream_transfer",
        aws_conn_id=AWS_CONN_ID,
        ftp_conn_id=FTPS_CONN_ID,
        s3_bucket=BUCKET,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        s3_key=(
            "{{ dag_run.conf.get('s3_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        ftp_path=f"{FTPS_DIR}/{{{{ dag_run.conf.get('filename', 'probe.txt') }}}}",
        doc_md="""
Streams the object from S3 onto the FTPS server without staging it on disk.

Subclasses the provider's `S3ToFTPOperator`, which needs **two** fixes rather
than one: its `execute` stages the whole object in a `NamedTemporaryFile`, and
it hardcodes plain `FTPHook` with no property to swap for an FTPS hook.

**No pipe.** `storbinary` pulls via `fp.read(blocksize)` and the S3 response body
is a readable, so they compose directly — unlike the FTPS→Blob direction, where
`retrbinary` pushes and a pipe is required.

Writes to `<name>.part` and renames on success. The size check runs *before* the
rename, so a truncated transfer never replaces a good previous file.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`SIZE`s the destination over FTPS and compares it against the object's size in
S3, so a truncated transfer fails the run instead of passing silently.

Load-bearing here: `storbinary` returns only a response code, so this is the sole
check that the right number of bytes landed.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
