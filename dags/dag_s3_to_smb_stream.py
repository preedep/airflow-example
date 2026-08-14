"""nix-dag-s3-to-smb-stream — stream an object from Amazon S3 to an SMB share."""

import logging
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-s3-to-smb-stream

Streams an object **Amazon S3 → SMB share** without staging it on disk.

```
S3 bucket  ──download_fileobj──▶  SMB share
 <S3_PREFIX><file>                <file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "s3_prefix": "incoming/"}
```

Defaults to `probe.txt` and S3 prefix `incoming/`.

#### One call, because both SDKs already fit

This is the simplest transfer here, and deliberately so. `SambaHook.open_file()`
returns a **writable**, and boto3's `download_fileobj` **writes into** any
writable. The two halves match with nothing in between:

```python
with self.samba_hook.open_file(target, mode="wb") as handle:
    s3_client.download_fileobj(self.bucket_name, self.key, handle)
```

Compare the Blob→SMB demo, which reaches the same place from the other side:
there the *source* pushes (`downloader.readinto(handle)`) because the Azure SDK
has no "download into this stream" entry point of its own. Both avoid a pipe;
only one of them needs the source to drive.

#### Threads are left on, and that is a measured choice

`download_fileobj` uses a thread pool by default, which looks risky against a
non-seekable SMB handle — parallel writers usually imply seeking. boto3 avoids
that by writing each part at an explicit offset rather than relying on the
stream's position, so it works.

Measured on a 50 MiB object over SMB:

| `TransferConfig` | Time |
|---|---|
| default (threads on) | **4.3 s** |
| `use_threads=False` | 15.1 s |

So the default is both correct *and* 3.5× faster here. Worth stating because the
opposite conclusion is easy to reach from first principles — and because it is
the reverse of the Azure upload path, where `max_concurrency=1` really is
required for a non-seekable source.

A small file proves nothing either way: under the multipart threshold boto3 takes
a single-part path and never engages the pool. The number above is from a real
50 MiB transfer.

#### Write-then-rename

The transfer writes to `<name>.part` and renames once the last byte lands, so a
consumer watching the share never sees a partial file.

As in the Blob→SMB demo this is **unlink-then-rename**, not an atomic
`replace()`: Samba refuses SMB's overwrite-rename with `STATUS_ACCESS_DENIED`
even between two files the account owns. There is therefore a brief window where
neither name holds the file. `temp_suffix=""` disables the pattern.

#### Memory

`download_fileobj` writes each part as it arrives, so peak memory is a part
buffer rather than the object size — 8 MiB by default, not the whole file. This
direction has no `max_single_put_size` equivalent to worry about, so it is
constant-memory at any size.

`MAX_BYTES` caps a single transfer; a larger object fails before any bytes move.

#### Paths are relative to the share

`SambaHook` takes the share from the connection's **schema** field and joins the
UNC path itself, so this DAG passes `probe.txt`, not `\\\\host\\share\\probe.txt`.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `smb_test_001` | SMB destination (conn type `samba`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

SMB: **host** an address the worker pods can resolve, **schema** the share name,
**login**/**password** the SMB credentials. The share directory must be writable
by that user, or writes fail with `STATUS_ACCESS_DENIED` while listing still
works.

#### Egress cost

S3 charges for data leaving AWS. A demo file is free; a large bucket is a real
bill.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_s3_test_001"
SMB_CONN_ID = "smb_test_001"

BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

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


class S3ToSMBStreamOperator(BaseOperator):
    """Stream a single object from Amazon S3 to an SMB share.

    No provider ships an S3 → SMB transfer, so this subclasses `BaseOperator`.

    Both halves already fit: `open_file()` returns a writable and boto3's
    `download_fileobj` writes into one, so the transfer is a single call with
    nothing staged on disk and no pipe.

    :param aws_conn_id: AWS connection (conn type `aws`).
    :param samba_conn_id: SMB connection (conn type `samba`).
    :param bucket_name: source S3 bucket.
    :param key: source S3 key, including any prefix.
    :param remote_path: destination path **relative to the share** named in the
        connection's schema field.
    :param temp_suffix: write to `<remote_path><temp_suffix>` and rename on
        success, so a consumer never sees a partial file. "" writes directly.
    :param max_bytes: reject an object larger than this before any bytes move.
    """

    # Rendered from dag_run.conf at run time, which is why the paths are operator
    # arguments rather than being read from context inside the task. It also puts
    # the resolved values in the UI's Rendered Template tab.
    template_fields = ("bucket_name", "key", "remote_path")

    # Keyword-only, so these can never be confused with BaseOperator's own
    # positional parameters.
    def __init__(
        self,
        *,
        aws_conn_id: str,
        samba_conn_id: str,
        bucket_name: str,
        key: str,
        remote_path: str,
        temp_suffix: str = ".part",
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.samba_conn_id = samba_conn_id
        self.bucket_name = bucket_name
        self.key = key
        self.remote_path = remote_path
        self.temp_suffix = temp_suffix
        self.max_bytes = max_bytes

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def s3_hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id)

    @cached_property
    def samba_hook(self) -> SambaHook:
        # The share comes from the connection's schema field; SambaHook joins the
        # UNC path itself, so every path below is relative to the share.
        return SambaHook(samba_conn_id=self.samba_conn_id)

    def execute(self, context) -> dict[str, Any]:
        s3_client = self.s3_hook.get_conn()

        # Size up front: rejects an oversized object before any bytes move, and
        # gives verify an authoritative number. download_fileobj returns None,
        # so this is the only figure the transfer itself can report.
        expected = s3_client.head_object(Bucket=self.bucket_name, Key=self.key)[
            "ContentLength"
        ]
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.key} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[s3_to_smb] streaming s3://%s/%s -> smb:%s (%s bytes)",
            self.bucket_name,
            self.key,
            self.remote_path,
            expected,
        )

        started = time.monotonic()

        # Write to a temporary name and rename once complete, so a consumer
        # watching the share never sees a partial file.
        target = self.remote_path + self.temp_suffix if self.temp_suffix else self.remote_path

        # No TransferConfig override: boto3's thread pool writes each part at an
        # explicit offset rather than relying on the stream position, so a
        # non-seekable SMB handle is safe — and measured 3.5x faster than
        # use_threads=False on a 50 MiB object. Note this is the opposite of the
        # Azure *upload* path, which needs max_concurrency=1 for such a source.
        with self.samba_hook.open_file(target, mode="wb") as handle:
            s3_client.download_fileobj(self.bucket_name, self.key, handle)

        # download_fileobj reports nothing, so the size is read back from the
        # share — this is what catches a truncated write before the rename.
        written = self.samba_hook.stat(target).st_size
        if written != expected:
            raise ValueError(
                f"size mismatch for {self.key}: wrote {written}, expected {expected}"
            )

        if self.temp_suffix:
            # Unlink then rename. Samba refuses SMB's atomic overwrite-rename
            # (FILE_RENAME_INFORMATION with ReplaceIfExists, which `replace`
            # issues) with STATUS_ACCESS_DENIED even when the account owns both
            # files — so the target is removed first. `rename` onto a free name
            # works. Not atomic, unlike SFTP's posix_rename.
            try:
                self.samba_hook.unlink(self.remote_path)
            except Exception:
                # Nothing to replace on a first run.
                pass
            self.samba_hook.rename(target, self.remote_path)
            self.log.info("[s3_to_smb] renamed %s -> %s", target, self.remote_path)

        self.log.info(
            "%s -> %s",
            _summary("s3_to_smb", written, time.monotonic() - started),
            self.remote_path,
        )
        return {"key": self.key, "destination": self.remote_path, "size": written}


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

    task_log.info("[s3_to_smb] verified %s — %s bytes", dst, actual)
    return {"path": dst, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-s3-to-smb-stream",
    description="Stream an object from Amazon S3 to an SMB share",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "s3", "smb", "samba", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = S3ToSMBStreamOperator(
        task_id="stream_transfer",
        aws_conn_id=AWS_CONN_ID,
        samba_conn_id=SMB_CONN_ID,
        bucket_name=BUCKET,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        key=(
            "{{ dag_run.conf.get('s3_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        remote_path="{{ dag_run.conf.get('filename', 'probe.txt') }}",
        doc_md="""
Streams the object from S3 onto the SMB share without staging it on disk.

The simplest transfer here: `open_file()` returns a writable and boto3's
`download_fileobj` writes into one, so it is a single call with no pipe and no
manual read loop.

**Threads are left on deliberately.** boto3 writes each part at an explicit
offset rather than relying on stream position, so a non-seekable SMB handle is
safe — and it measured 3.5x faster than `use_threads=False` on a 50 MiB object.
That is the opposite of the Azure upload path, which needs `max_concurrency=1`.

Writes to `<name>.part` and renames on success.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`stat`s the destination on the share and compares its size against the byte
count recorded by the transfer, so a truncated transfer fails the run instead of
passing silently.

Runs in its own pod with a fresh connection, so it is an independent read of
server state.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
