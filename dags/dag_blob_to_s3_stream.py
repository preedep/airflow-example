"""nix-dag-blob-to-s3-stream — stream a blob from Azure Blob Storage to Amazon S3."""

import logging
import time
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

DAG_DOC_MD = """
### nix-dag-blob-to-s3-stream

Streams a blob **Azure Blob Storage → Amazon S3** without staging it on disk.

```
Blob container  ──download──▶  dag (stream)  ──upload_fileobj──▶  S3 bucket
 <BLOB_PREFIX><file>                                              <S3_PREFIX><file>
```

This is the only **cross-cloud** demo here: two vendors' SDKs, one hop, nothing
on local disk in between.

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "blob_prefix": "incoming/", "s3_prefix": "incoming/"}
```

Defaults to `probe.txt`, blob prefix `incoming/`, S3 prefix `incoming/`.

#### Why no pipe

`WasbHook.download()` returns a `StorageStreamDownloader` — a readable — and
`S3Hook.load_file_obj()` calls boto3's `upload_fileobj`, which **pulls** from a
file-like object. Reader on one side, puller on the other, so the two compose
directly:

```python
downloader = wasb_hook.download(container_name=..., blob_name=...)
s3_hook.load_file_obj(downloader, key=..., bucket_name=..., replace=True)
```

The `os.pipe()` and worker thread the FTPS→Blob demo needs are only required when
both sides push, or both pull.

#### The seek question

`upload_fileobj` does **multipart** uploads, which raises a fair worry: splitting
a stream into parts usually means seeking it, and `StorageStreamDownloader` has
`read` but **no `seek`**.

boto3 handles this. Its transfer manager reads sequentially into per-part buffers
rather than seeking the source, so a non-seekable stream is fine. That is worth
knowing because the equivalent Azure call is *not* so forgiving — the
SFTP→Blob demo has to pass `length=` and `max_concurrency=1` precisely because
the Azure SDK would otherwise seek the source to measure and split it.

| | Azure `upload()` | boto3 `upload_fileobj` |
|---|---|---|
| non-seekable source | needs `length=` + `max_concurrency=1` | works as-is |
| parallelism | seeks to split → must be 1 | reads sequentially into buffers |

#### Memory

The blob is fetched on demand and each part is uploaded as it fills, so peak
memory is a part buffer rather than the file size. `TransferConfig` defaults to
8 MiB parts with up to 10 concurrent threads; a multi-GB blob transfers without
the whole object ever being resident.

`MAX_BYTES` caps a single transfer; a larger blob fails before any bytes move.

#### Idempotency

`replace=True`, so re-running overwrites the key rather than raising
`ValueError: The key ... already exists.` — which is what `load_file_obj` does by
default. That keeps a retry idempotent: a task that died mid-upload must be able
to replace whatever it left behind.

#### Source is not deleted

A successful run leaves the blob in the container, so re-running re-transfers it.
Deliberate for a demo: deleting is destructive and a failed upload must not lose
the only copy. For a real migration, delete only after `verify` confirms the size
matches.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `aws_s3_test_001` | AWS destination (conn type `aws`) |

For the Azure connection, SAS auth goes in **extra** as `sas_token` and **login**
is the storage account name — the hook builds the account URL from it.

For AWS, the access key id goes in **login**, the secret in **password**, and the
region in **extra** as `{"region_name": "..."}`. The region is not optional:
worker pods have no `AWS_DEFAULT_REGION`, so a connection without it fails inside
the cluster even though the same credentials work from a laptop whose CLI profile
sets one.

Neither container nor bucket is part of a connection; both are passed per
operation.

#### Egress cost

Worth stating plainly for a cross-cloud transfer: Azure charges for data leaving
its network. A demo file is free in practice, but the same DAG pointed at a large
container is a real bill. S3 does not charge for ingress.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WASB_CONN_ID = "wasb-nickstorageairflow002"
AWS_CONN_ID = "aws_s3_test_001"

CONTAINER = "data001"
BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


# --------------------------------------------------------------------------- #
# Operator
# --------------------------------------------------------------------------- #


class BlobToS3StreamOperator(BaseOperator):
    """Stream a single blob from Azure Blob Storage to Amazon S3.

    No provider ships a Blob → S3 transfer — the `microsoft-azure` provider has
    `s3_to_wasb`, which points the other way — so this subclasses `BaseOperator`
    directly.

    Both SDKs cooperate: `WasbHook.download()` returns a readable and boto3's
    `upload_fileobj` pulls from one, so no pipe or worker thread is needed and
    nothing touches local disk.

    :param wasb_conn_id: Azure Blob connection (conn type `wasb`).
    :param aws_conn_id: AWS connection (conn type `aws`).
    :param container_name: source blob container.
    :param blob_name: source blob name, including any prefix.
    :param bucket_name: destination S3 bucket.
    :param key: destination S3 key, including any prefix.
    :param replace: overwrite an existing key. Keep on so a retry can replace a
        partial object; `load_file_obj` raises rather than overwriting by default.
    :param max_bytes: reject a blob larger than this before any bytes move.
    """

    # Rendered from dag_run.conf at run time, which is why the paths are operator
    # arguments rather than being read from context inside the task. It also puts
    # the resolved values in the UI's Rendered Template tab.
    template_fields = ("container_name", "blob_name", "bucket_name", "key")

    # Keyword-only, so these can never be confused with BaseOperator's own
    # positional parameters.
    def __init__(
        self,
        *,
        wasb_conn_id: str,
        aws_conn_id: str,
        container_name: str,
        blob_name: str,
        bucket_name: str,
        key: str,
        replace: bool = True,
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.aws_conn_id = aws_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.bucket_name = bucket_name
        self.key = key
        self.replace = replace
        self.max_bytes = max_bytes

    # cached_property, matching the provider convention: the hook is built on
    # first use rather than in __init__, so constructing the operator at DAG
    # parse time never opens a connection.
    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    @cached_property
    def s3_hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id)

    def execute(self, context) -> dict[str, Any]:
        client = self.wasb_hook.get_conn().get_container_client(self.container_name)
        blob_client = client.get_blob_client(self.blob_name)

        # Size up front so an oversized blob fails before any bytes move, and so
        # verify has an authoritative number to compare against. upload_fileobj
        # returns None, so this is the only figure the transfer can report.
        expected = blob_client.get_blob_properties().size
        if expected > self.max_bytes:
            raise ValueError(
                f"{self.blob_name} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[blob_to_s3] streaming wasb://%s/%s -> s3://%s/%s (%s bytes)",
            self.container_name,
            self.blob_name,
            self.bucket_name,
            self.key,
            expected,
        )

        started = time.monotonic()

        # download() returns a StorageStreamDownloader: read() but no seek().
        # boto3's transfer manager reads sequentially into per-part buffers
        # rather than seeking the source, so a non-seekable stream is fine here
        # even though it forces max_concurrency=1 on the Azure upload path.
        downloader = self.wasb_hook.download(
            container_name=self.container_name, blob_name=self.blob_name
        )

        # replace=True: load_file_obj raises ValueError on an existing key
        # otherwise, which would make a retry fail rather than converge.
        # BYTE PATH — nothing touches the worker pod's disk:
        #   HTTPS GET (Azure) -> downloader -> boto3 fills an 8 MiB part buffer
        #   -> HTTPS PUT (S3). Peak memory is a part, not the object.
        self.s3_hook.load_file_obj(
            downloader,
            key=self.key,
            bucket_name=self.bucket_name,
            replace=self.replace,
        )

        self.log.info(
            "%s -> s3://%s/%s",
            _summary("blob_to_s3", expected, time.monotonic() - started),
            self.bucket_name,
            self.key,
        )
        return {
            "blob": self.blob_name,
            "bucket": self.bucket_name,
            "key": self.key,
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
    """Confirm the object exists in S3 with the size we sent.

    This matters here: `upload_fileobj` returns `None`, so nothing in the
    transfer task itself proves the byte count landed intact.
    """
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for the key and byte count rather than
    # recomputing from conf — this verifies what was actually sent.
    result = ti.xcom_pull(task_ids="stream_transfer")
    if not result:
        raise ValueError("stream_transfer pushed no result")

    bucket, key, expected = result["bucket"], result["key"], result["size"]

    # Separate pod, so a fresh client: this is an independent read of service
    # state, not a re-check of the streaming task's own handle.
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    actual = hook.head_object(key, bucket)["ContentLength"]

    if actual != expected:
        raise ValueError(f"s3://{bucket}/{key}: expected {expected} bytes, found {actual}")

    task_log.info("[blob_to_s3] verified s3://%s/%s — %s bytes", bucket, key, actual)
    return {"bucket": bucket, "key": key, "size": actual}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-blob-to-s3-stream",
    description="Stream a blob from Azure Blob Storage to Amazon S3",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "azure", "wasb", "aws", "s3", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = BlobToS3StreamOperator(
        task_id="stream_transfer",
        wasb_conn_id=WASB_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        container_name=CONTAINER,
        bucket_name=BUCKET,
        # Rendered from conf at run time. Templating these — rather than reading
        # dag_run.conf inside the task — is what puts the resolved paths in the
        # UI's Rendered Template tab.
        blob_name=(
            "{{ dag_run.conf.get('blob_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        key=(
            "{{ dag_run.conf.get('s3_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        replace=True,  # a retry must be able to replace a partial object
        doc_md="""
Opens the Azure and AWS connections at once and streams the blob across without
staging it on disk.

`BlobToS3StreamOperator` subclasses `BaseOperator` directly because no provider
ships a Blob → S3 transfer — `s3_to_wasb` exists, but points the other way.

**No pipe needed.** `WasbHook.download()` returns a readable and boto3's
`upload_fileobj` pulls from one. Note the source has `read` but **no `seek`**;
boto3's transfer manager reads sequentially into per-part buffers, so multipart
upload works anyway — unlike the Azure upload path, which needs `length=` and
`max_concurrency=1` for the same kind of stream.

`replace=True` because `load_file_obj` raises on an existing key by default,
which would make a retry fail instead of converge.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
`head_object`s the destination and compares its size against the blob's size, so
a truncated transfer fails the run instead of passing silently.

Load-bearing rather than belt-and-braces: `upload_fileobj` returns `None`, so
this is the only check that the right number of bytes landed.

Runs in its own pod with a fresh client, so it is an independent read of service
state.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
