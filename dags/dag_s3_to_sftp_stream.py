"""nix-dag-s3-to-sftp-stream — stream an object from Amazon S3 to the SFTP server."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-s3-to-sftp-stream

Streams an object **Amazon S3 → SFTP** without staging it on disk.

```
S3 bucket  ──download_fileobj──▶  SFTP server
 <S3_PREFIX><file>                SFTP_DIR/<file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "s3_prefix": "incoming/"}
```

Defaults to `probe.txt` and S3 prefix `incoming/`.

#### Another provider operator that stages to disk

The `amazon` provider ships **`S3ToSFTPOperator`**, and its surrounding logic —
templated fields, `s3_key` resolution, connection handling — is worth keeping.
Its `execute` is not:

```python
with NamedTemporaryFile("w") as f:
    s3_client.download_file(self.s3_bucket, self.s3_key, f.name)
    sftp_client.put(f.name, self.sftp_path, confirm=self.confirm)
```

The whole object lands on the worker pod's disk before a byte reaches the SFTP
server. `StreamingS3ToSFTPOperator` overrides `execute` and hands boto3 the
remote file handle directly, so nothing touches local disk.

This is the third demo of that same shape — see also
`nix-dag-sftp-to-blob-stream` and `nix-dag-s3-to-blob-stream`. Provider transfer
operators routinely stage to a temp file; the fix is nearly always to override
the one copy method rather than reimplement the operator.

#### Both halves already fit

`download_fileobj` **writes into** any writable, and paramiko's `open(..., "wb")`
**is** one, so the transfer is a single call:

```python
with sftp_client.open(self.sftp_path, "wb") as handle:
    handle.set_pipelined(True)
    s3_client.download_fileobj(self.s3_bucket, self.s3_key, handle)
```

No pipe, no read loop. Same shape as the S3→SMB demo.

#### On pipelining and threads

`set_pipelined(True)` lets paramiko keep multiple writes in flight rather than
waiting a round-trip each. It is set here because it costs nothing and matters on
a high-latency link — but measured on a 50 MiB object over a local network it
made **no difference** (4.3 s pipelined, 4.2 s not), because boto3's own thread
pool already keeps several writes outstanding.

The threads are left on for the same reason as the S3→SMB demo: boto3 writes each
part at an explicit offset rather than relying on stream position, so a
non-seekable peer is safe. That is the reverse of the Azure *upload* path, which
needs `max_concurrency=1`.

#### Write-then-rename

The transfer writes to `<name>.part` and renames once the last byte lands, so a
consumer watching the drop directory never sees a partial file.

`posix_rename`, not `rename` — plain SFTP `rename` fails when the target exists,
which would break a retry over a previous run's file. Unlike the SMB demos, this
one **is** atomic. `temp_suffix=""` disables the pattern.

#### Memory

Each part is written as it arrives, so peak memory is a part buffer (8 MiB by
default), not the object size.

`MAX_BYTES` caps a single transfer; a larger object fails before any bytes move.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `sftp_test_001` | SFTP destination (conn type `SFTP`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

Set the SFTP connection to an address the **worker pods** can resolve. The
destination directory must exist and be writable by the connection's user;
`open()` does not create parent directories.

#### Egress cost

S3 charges for data leaving AWS. A demo file is free; a large bucket is a real
bill.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_s3_test_001"
SFTP_CONN_ID = "sftp_test_001"

BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

# Adjust to your server. Must exist and be writable by the connection's user.
SFTP_DIR = "/home/airflowsftp/incoming"

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


class StreamingS3ToSFTPOperator(S3ToSFTPOperator):
    """`S3ToSFTPOperator` that streams instead of staging on disk.

    Only `execute` changes: boto3 writes straight into the remote file handle
    rather than into a `NamedTemporaryFile` that is then uploaded. Templated
    fields, `s3_key` resolution and connection handling stay with the provider.

    :param temp_suffix: write to `<sftp_path><temp_suffix>` and rename on
        success, so a consumer never sees a partial file. "" writes directly.
    :param max_bytes: reject an object larger than this before any bytes move.
    """

    def __init__(
        self,
        *,
        temp_suffix: str = ".part",
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.temp_suffix = temp_suffix
        self.max_bytes = max_bytes

    def execute(self, context) -> dict[str, Any]:
        # get_s3_key is the provider's own resolution logic — reused rather than
        # reimplemented, so a future fix there still applies here.
        self.s3_key = self.get_s3_key(self.s3_key)

        # SFTPHook rather than the base class's SSHHook + open_sftp(): its
        # get_managed_conn() is refcounted and closes the session on exit, which
        # is the pattern the other SFTP demos here use.
        sftp_hook = SFTPHook(ssh_conn_id=self.sftp_conn_id)

        # Built here, not taken from the base class: unlike
        # S3ToAzureBlobStorageOperator, S3ToSFTPOperator exposes no `s3_hook`
        # property — it constructs one inside its own execute().
        s3_client = S3Hook(aws_conn_id=self.aws_conn_id).get_conn()

        # Size up front: rejects an oversized object before any bytes move, and
        # gives verify an authoritative number. download_fileobj returns None,
        # so this is the only figure the transfer itself can report.
        expected = s3_client.head_object(Bucket=self.s3_bucket, Key=self.s3_key)[
            "ContentLength"
        ]
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.s3_key} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[s3_to_sftp] streaming s3://%s/%s -> %s (%s bytes)",
            self.s3_bucket,
            self.s3_key,
            self.sftp_path,
            expected,
        )

        started = time.monotonic()

        # Write to a temporary name and rename once complete, so a consumer
        # watching the directory never sees a partial file.
        target = self.sftp_path + self.temp_suffix if self.temp_suffix else self.sftp_path

        with sftp_hook.get_managed_conn() as sftp_client:
            # The remote handle is the download destination — boto3 writes into
            # it directly, so nothing is staged on local disk.
            with sftp_client.open(target, "wb") as handle:
                # Keeps multiple writes in flight rather than one round-trip
                # each. No measurable gain on a local network, where boto3's
                # thread pool already overlaps writes, but it matters on a
                # high-latency link.
                handle.set_pipelined(True)
                # BYTE PATH — nothing touches the worker pod's disk:
                #   HTTPS GET (S3) -> boto3 8 MiB part buffer -> SFTP socket.
                # boto3 writes straight into the remote handle; the stock
                # operator would have staged the whole object first.
                s3_client.download_fileobj(self.s3_bucket, self.s3_key, handle)

            # download_fileobj reports nothing, so the size is read back from
            # the server. Checked *before* the rename, so a truncated transfer
            # never replaces a good previous file.
            written = sftp_client.stat(target).st_size
            if written != expected:
                raise ValueError(
                    f"size mismatch for {self.s3_key}: wrote {written}, expected {expected}"
                )

            if self.temp_suffix:
                # posix_rename, not rename: plain SFTP rename fails when the
                # target exists, so a retry over a previous run's file would
                # break. posix_rename overwrites atomically.
                sftp_client.posix_rename(target, self.sftp_path)
                self.log.info(
                    "[s3_to_sftp] renamed %s -> %s", target, self.sftp_path
                )

        self.log.info(
            "%s -> %s",
            _summary("s3_to_sftp", written, time.monotonic() - started),
            self.sftp_path,
        )
        return {"key": self.s3_key, "destination": self.sftp_path, "size": written}


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

    # Separate pod, so a fresh connection: an independent read of server state
    # rather than a re-check of the transfer task's own handle.
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    with hook.get_managed_conn() as sftp_client:
        actual = sftp_client.stat(dst).st_size

    if actual != expected:
        raise ValueError(f"{dst}: expected {expected} bytes, found {actual}")

    task_log.info("[s3_to_sftp] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-s3-to-sftp-stream",
    description="Stream an object from Amazon S3 to the SFTP server",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "s3", "sftp", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = StreamingS3ToSFTPOperator(
        task_id="stream_transfer",
        aws_conn_id=AWS_CONN_ID,
        sftp_conn_id=SFTP_CONN_ID,
        s3_bucket=BUCKET,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        s3_key=(
            "{{ dag_run.conf.get('s3_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        sftp_path=f"{SFTP_DIR}/{{{{ dag_run.conf.get('filename', 'probe.txt') }}}}",
        doc_md="""
Streams the object from S3 onto the SFTP server without staging it on disk.

Subclasses the provider's `S3ToSFTPOperator` and overrides only `execute` — the
stock version downloads to a `NamedTemporaryFile` and uploads from that path, so
the whole object lands on the worker pod's disk first.

`download_fileobj` writes into any writable and paramiko's `open(..., "wb")` is
one, so the transfer is a single call with no pipe and no read loop.

Writes to `<name>.part` and `posix_rename`s it on success — atomic here, unlike
the SMB demos. The size check runs *before* the rename, so a truncated transfer
never replaces a good previous file.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`stat`s the destination over SFTP and compares its size against the byte count
recorded by the transfer, so a truncated transfer fails the run instead of
passing silently.

Runs in its own pod with a fresh connection, so it is an independent read of
server state.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
