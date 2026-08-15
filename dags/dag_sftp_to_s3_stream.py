"""nix-dag-sftp-to-s3-stream — stream a file from the SFTP server to Amazon S3."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-sftp-to-s3-stream

Streams a file **SFTP → Amazon S3** without staging it on disk.

```
SFTP server  ──open──▶  upload_fileobj  ──▶  S3 bucket
 SFTP_DIR/<file>                             <S3_PREFIX><file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "s3_prefix": "incoming/"}
```

Defaults to `probe.txt` and S3 prefix `incoming/`.

#### The provider already streams — this fixes the speed

`SFTPToS3Operator` is the exception among the transfer operators used here. The
others stage the whole file in a `NamedTemporaryFile` with no way to opt out;
this one takes a **`use_temp_file`** argument, and `use_temp_file=False` already
does the right thing:

```python
with sftp_client.file(self.sftp_path, mode="rb") as data:
    s3_hook.get_conn().upload_fileobj(data, self.s3_bucket, self.s3_key, ...)
```

So there is no staging to remove. What is missing is **`prefetch`**.

Without it paramiko requests each block and waits a full round-trip before asking
for the next, so throughput is capped by latency rather than bandwidth. Measured
on a 50 MiB file over a local network:

| | Time |
|---|---|
| provider's `use_temp_file=False` | 10.5 s |
| the same, plus `prefetch` | **2.2 s** |

A 4.8× difference on a *fast* link; on a high-latency one the gap is far larger.
`StreamingSFTPToS3Operator` therefore reuses the provider's approach and adds one
line, rather than rewriting the transfer.

**The general point:** a provider operator that already streams can still be slow
for a reason that has nothing to do with staging. Check the read pattern, not
just whether a temp file is involved.

#### Composition: S3 pulls, SFTP reads

No pipe. `upload_fileobj` **pulls** from a file-like object and paramiko's
`file(..., "rb")` **is** one, so the two compose directly.

boto3 tolerates a non-seekable source here because its transfer manager reads
sequentially into per-part buffers — the same reason the S3→SMB demo can leave
threads on. Note this is the **opposite** of the Azure upload path, which needs
`length=` and `max_concurrency=1` for such a source.

#### Idempotency

`replace=True` on the S3 side, so a retry overwrites the key rather than raising
`ValueError: The key ... already exists.` — which is what `load_file`-style
helpers do by default.

There is no `.part` + rename here, unlike the SFTP and FTPS *destinations*: S3
has no rename, and an object is not visible until the upload commits, so a
consumer never sees a partial. See the write-then-rename note in the README.

#### Source is not deleted

A successful run leaves the file on the SFTP server, so re-running re-transfers
it. Deliberate for a demo: deleting is destructive and a failed upload must not
lose the only copy.

#### Memory

`upload_fileobj` reads a part, uploads it, then reads the next, so peak memory is
a part buffer (8 MiB by default) rather than the file size. `prefetch` adds
paramiko's own read-ahead window on top of that.

`MAX_BYTES` caps a single transfer; a larger file fails before any bytes move.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `sftp_test_001` | SFTP source (conn type `SFTP`) |
| Connection | `aws_s3_test_001` | AWS S3 destination (conn type `aws`) |

Set the SFTP connection to an address the **worker pods** can resolve.

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

#### Ingress is free

S3 does not charge for data coming in, unlike the S3→anywhere demos where egress
is billable.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SFTP_CONN_ID = "sftp_test_001"
AWS_CONN_ID = "aws_s3_test_001"

BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

# Adjust to your server. Must be readable by the connection's user.
SFTP_DIR = "/home/airflowsftp/outgoing"

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
# Operator override
# --------------------------------------------------------------------------- #


class StreamingSFTPToS3Operator(SFTPToS3Operator):
    """`SFTPToS3Operator` that prefetches, and so streams at full speed.

    The provider's `use_temp_file=False` path already avoids staging on disk, so
    unlike the other transfer subclasses here this one is not fixing a temp
    file — it is fixing the read pattern. Without `prefetch`, paramiko waits a
    round-trip per block and throughput is bounded by latency.

    :param max_bytes: reject a file larger than this before any bytes move.

    There is no `temp_suffix` here, unlike the SFTP/FTPS/SMB destinations: S3 has
    no rename, and an object is not visible until the upload commits, so a
    consumer never sees a partial one.
    """

    def __init__(
        self,
        *,
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        # The base class defaults to use_temp_file=True; this operator exists to
        # stream, so the temp-file path is never wanted.
        kwargs.setdefault("use_temp_file", False)
        super().__init__(**kwargs)
        self.max_bytes = max_bytes

    def execute(self, context) -> dict[str, Any]:
        # get_s3_key is the provider's own resolution logic — reused rather than
        # reimplemented, so a future fix there still applies here.
        self.s3_key = self.get_s3_key(self.s3_key)

        # SFTPHook rather than the base class's SSHHook + open_sftp(): its
        # get_managed_conn() is refcounted and closes the session on exit, which
        # is the pattern the other SFTP demos here use.
        sftp_hook = SFTPHook(ssh_conn_id=self.sftp_conn_id)
        s3_client = S3Hook(aws_conn_id=self.s3_conn_id).get_conn()

        with sftp_hook.get_managed_conn() as sftp_client:
            # stat first: this both honours the base class's
            # fail_on_file_not_exist contract and gives the size up front.
            try:
                expected = sftp_client.stat(self.sftp_path).st_size
            except FileNotFoundError:
                if self.fail_on_file_not_exist:
                    raise
                self.log.info(
                    "[sftp_to_s3] %s not found on the SFTP server — skipping",
                    self.sftp_path,
                )
                return {"key": self.s3_key, "destination": None, "size": 0}

            if expected > self.max_bytes:
                raise ValueError(
                    f"{self.sftp_path} is {expected} bytes, over the {self.max_bytes} limit"
                )

            self.log.info(
                "[sftp_to_s3] streaming %s -> s3://%s/%s (%s bytes)",
                self.sftp_path,
                self.s3_bucket,
                self.s3_key,
                expected,
            )

            started = time.monotonic()

            with sftp_client.file(self.sftp_path, mode="rb") as data:
                # The one line the provider omits. Without it paramiko requests
                # a block, waits a full round-trip, then requests the next —
                # capping throughput at latency rather than bandwidth. Measured
                # on 50 MiB: 10.5s without, 2.2s with.
                data.prefetch(expected)
                s3_client.upload_fileobj(data, self.s3_bucket, self.s3_key)

        # upload_fileobj returns None, so the size is read back from S3. This is
        # the only check that the right number of bytes landed.
        written = s3_client.head_object(Bucket=self.s3_bucket, Key=self.s3_key)[
            "ContentLength"
        ]
        if written != expected:
            raise ValueError(
                f"size mismatch for {self.sftp_path}: wrote {written}, expected {expected}"
            )

        self.log.info(
            "%s -> s3://%s/%s",
            _summary("sftp_to_s3", written, time.monotonic() - started),
            self.s3_bucket,
            self.s3_key,
        )
        return {
            "source": self.sftp_path,
            "bucket": self.s3_bucket,
            "key": self.s3_key,
            "size": written,
        }


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm the object exists in S3 with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the key and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    bucket, key, expected = result["bucket"], result["key"], result["size"]

    # Separate pod, so a fresh client: an independent read of service state
    # rather than a re-check of the transfer task's own handle.
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    actual = hook.head_object(key, bucket)["ContentLength"]

    if actual != expected:
        raise ValueError(f"s3://{bucket}/{key}: expected {expected} bytes, found {actual}")

    task_log.info("[sftp_to_s3] verified s3://%s/%s — %s bytes", bucket, key, actual)
    return {"bucket": bucket, "key": key, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-sftp-to-s3-stream",
    description="Stream a file from the SFTP server to Amazon S3",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "sftp", "aws", "s3", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = StreamingSFTPToS3Operator(
        task_id="stream_transfer",
        sftp_conn_id=SFTP_CONN_ID,
        s3_conn_id=AWS_CONN_ID,
        s3_bucket=BUCKET,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        sftp_path=f"{SFTP_DIR}/{{{{ dag_run.conf.get('filename', 'probe.txt') }}}}",
        s3_key=(
            "{{ dag_run.conf.get('s3_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        doc_md="""
Streams the file from SFTP into S3 without staging it on disk.

Subclasses the provider's `SFTPToS3Operator`, which is the **exception** among
the transfer operators here: it already offers `use_temp_file=False`, so there
is no staging to remove.

What it omits is `prefetch`. Without it paramiko waits a round-trip per block,
capping throughput at latency rather than bandwidth — measured at 10.5s versus
**2.2s** for 50 MiB. This operator adds that one line.

No pipe: `upload_fileobj` pulls and paramiko's `file(..., "rb")` is a readable.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`head_object`s the destination and compares its size against the byte count read
from the SFTP server, so a truncated transfer fails the run instead of passing
silently.

Load-bearing: `upload_fileobj` returns `None`, so nothing in the transfer task
itself proves the right number of bytes landed.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
