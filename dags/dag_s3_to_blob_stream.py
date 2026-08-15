"""nix-dag-s3-to-blob-stream — stream an object from Amazon S3 to Azure Blob Storage."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.transfers.s3_to_wasb import (
    S3ToAzureBlobStorageOperator,
)
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-s3-to-blob-stream

Streams an object **Amazon S3 → Azure Blob Storage** without staging it on disk.

```
S3 bucket  ──get_object──▶  dag (stream)  ──upload_blob──▶  Blob container
 <S3_PREFIX><file>                                          <BLOB_PREFIX><file>
```

The cross-cloud pair to `nix-dag-blob-to-s3-stream`, pointing the other way.

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"filename": "probe.txt", "s3_prefix": "incoming/", "blob_prefix": "xcloud"}
```

Defaults to `probe.txt`, S3 prefix `incoming/`, blob prefix `xcloud`.

**Note the missing trailing slash on `blob_prefix`.** The inherited `_create_key`
joins with `f"{prefix}/{file_name}"`, so `"xcloud/"` yields `xcloud//probe.txt` —
a double slash, which object stores treat as a real (and different) key rather
than normalising away. This is the opposite convention to the other DAGs here,
where prefixes *do* end in `/` because they are concatenated directly.

#### This is the one cross-service direction a provider already covers

The other three — FTPS→Blob, Blob→SFTP, Blob→FTPS — have no provider operator at
all, so they subclass `BaseOperator`. This one is different: the
`microsoft-azure` provider ships **`S3ToAzureBlobStorageOperator`**, and it is
worth keeping. Prefix listing, `s3_key` vs `s3_prefix` handling, blob naming,
`replace` semantics and the `TooManyFilesToMoveException` guard are all logic
that would otherwise be rewritten.

Only `move_file` is wrong for streaming. The stock version downloads to a
`NamedTemporaryFile`, then uploads from that path:

```python
with tempfile.NamedTemporaryFile("w") as temp_file:
    s3_client.download_file(self.s3_bucket, source_s3_key, temp_file.name)
    self.wasb_hook.load_file(file_path=temp_file.name, ...)
```

So the whole object lands on the worker pod's disk before a single byte goes to
Azure. `StreamingS3ToAzureBlobStorageOperator` overrides that one method and
pipes `get_object()["Body"]` straight into `upload_blob`.

This is the same shape as the SFTP→Blob demo: **override the one method that is
wrong, inherit everything else.** A future provider fix to prefix handling or
blob naming still applies here.

#### `seekable()` is the subtlety

boto3's response body reports `seek` as an attribute but is **not actually
seekable**:

```
S3_BODY: StreamingChecksumBody | read: True | seek: True | seekable: False
```

`hasattr(body, "seek")` is therefore misleading — the Azure SDK checks
`stream.seekable()`, sees `False`, and takes its sequential upload path. Two
arguments follow from that and are both required:

- **`length=`** — without it the SDK seeks the stream to measure it, which fails.
- **`max_concurrency=1`** — parallel block upload seeks to split the source.

Note the asymmetry with the reverse direction. Sending *to* S3, boto3's
`upload_fileobj` tolerates a non-seekable source with no special arguments;
sending *to* Azure, the SDK needs both. Same pair of clouds, opposite tolerance —
do not carry an assumption from one SDK to the other.

#### Memory

Bytes are fetched from S3 on demand and written to Azure as they arrive, so peak
memory is one block rather than the object size.

The same caveat as the FTPS→Blob demo applies: the Azure SDK does a single
buffered `read()` for objects at or under `max_single_put_size` (64 MiB by
default), switching to streaming block upload above it. So this is
constant-memory for large objects and up-to-64-MiB-buffered for small ones.

`MAX_BYTES` caps a single transfer; a larger object fails before any bytes move.

#### Idempotency

`overwrite=True` on the Azure side, so a retry replaces a partial blob rather
than failing with `ResourceExistsError`.

The inherited `replace` argument is a different thing and worth not confusing:
it filters the *source list*, skipping objects that already exist in the
container, and defaults to `False`. This DAG leaves that default — re-running
transfers only what is missing — while still allowing an individual blob to be
overwritten when it is transferred.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `wasb-nickstorageairflow002` | Azure Blob destination (conn type `wasb`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

Azure: SAS token in **extra** as `sas_token`, **login** is the storage account
name.

Neither bucket nor container belongs to a connection; both are passed per
operation.

#### Egress cost

S3 charges for data leaving AWS; Azure does not charge for ingress. The reverse
of the Blob→S3 demo, and the same warning applies — a demo file is free, a large
bucket is a real bill.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_s3_test_001"
WASB_CONN_ID = "wasb-nickstorageairflow002"

BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"
CONTAINER = "data001"

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


class StreamingS3ToAzureBlobStorageOperator(S3ToAzureBlobStorageOperator):
    """`S3ToAzureBlobStorageOperator` that streams instead of staging on disk.

    Only `move_file` changes. File discovery, `s3_key`/`s3_prefix` handling, blob
    naming, `replace` filtering and the too-many-files guard all stay with the
    provider.
    """

    def move_file(self, file_name: str) -> None:
        # _create_key is the provider's own naming logic — reused rather than
        # reimplemented, so a future fix there still applies here.
        source_s3_key = self._create_key(self.s3_key, self.s3_prefix, file_name)
        destination_blob = self._create_key(self.blob_name, self.blob_prefix, file_name)

        s3_client = self.s3_hook.get_conn()

        # Size up front: rejects an oversized object before any bytes move, and
        # the Azure upload needs `length` for a non-seekable stream anyway.
        expected = s3_client.head_object(Bucket=self.s3_bucket, Key=source_s3_key)[
            "ContentLength"
        ]
        if expected > MAX_BYTES:
            raise ValueError(
                f"{source_s3_key} is {expected} bytes, over the {MAX_BYTES} limit"
            )

        self.log.info(
            "[s3_to_blob] streaming s3://%s/%s -> wasb://%s/%s (%s bytes)",
            self.s3_bucket,
            source_s3_key,
            self.container_name,
            destination_blob,
            expected,
        )

        started = time.monotonic()

        # The response Body is a readable, so it feeds upload_blob directly —
        # nothing touches local disk, unlike the stock move_file.
        body = s3_client.get_object(Bucket=self.s3_bucket, Key=source_s3_key)["Body"]

        blob_client = (
            self.wasb_hook.get_conn()
            .get_container_client(self.container_name)
            .get_blob_client(destination_blob)
        )

        # length and max_concurrency=1 are both required: the S3 body exposes a
        # `seek` attribute but reports seekable() False, and the Azure SDK would
        # otherwise seek it to measure the stream and to split it across threads.
        # BYTE PATH — nothing touches the worker pod's disk:
        #   HTTPS GET (S3) -> response Body -> Azure block -> HTTPS PUT.
        # length= and max_concurrency=1 below are what keep it that way: without
        # them the SDK would seek the stream to measure and split it, which a
        # non-seekable body cannot do.
        blob_client.upload_blob(
            body,
            length=expected,
            overwrite=True,  # a retry must be able to replace a partial blob
            max_concurrency=1,
            **self.wasb_extra_args,
        )

        self.log.info(
            "%s -> %s",
            _summary("s3_to_blob", expected, time.monotonic() - started),
            destination_blob,
        )

        # Recorded so verify can check exactly what this run wrote. The base
        # class returns only file names, which is not enough to check sizes.
        self.transferred.append(
            {"source": source_s3_key, "blob": destination_blob, "size": expected}
        )

    def execute(self, context) -> list[dict[str, Any]]:
        # Initialised here rather than in __init__: execute() runs once per task
        # pod, and this keeps the operator safe to construct at parse time.
        self.transferred: list[dict[str, Any]] = []
        super().execute(context)
        return self.transferred


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm each blob exists in the container with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for names and byte counts rather than re-deriving
    # them from conf — this verifies what was actually transferred.
    transferred = ti.xcom_pull(task_ids="stream_transfer") or []
    if not transferred:
        raise ValueError("stream_transfer reported no files — nothing matched the source")

    hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
    # Separate pod, so a fresh client: an independent read of service state
    # rather than a re-check of the transfer task's own handle.
    client = hook.get_conn().get_container_client(CONTAINER)

    for item in transferred:
        actual = client.get_blob_client(item["blob"]).get_blob_properties().size
        if actual != item["size"]:
            raise ValueError(
                f"{item['blob']}: expected {item['size']} bytes, found {actual}"
            )
        task_log.info("[s3_to_blob] verified %s — %s bytes", item["blob"], actual)

    return {"container": CONTAINER, "count": len(transferred), "files": transferred}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-s3-to-blob-stream",
    description="Stream an object from Amazon S3 to Azure Blob Storage",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "s3", "azure", "wasb", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = StreamingS3ToAzureBlobStorageOperator(
        task_id="stream_transfer",
        aws_conn_id=AWS_CONN_ID,
        wasb_conn_id=WASB_CONN_ID,
        s3_bucket=BUCKET,
        container_name=CONTAINER,
        # s3_key targets one object. The inherited s3_prefix mode would move
        # everything under a prefix, which a demo should not do by default.
        s3_key=(
            "{{ dag_run.conf.get('s3_prefix', 'incoming/') }}"
            "{{ dag_run.conf.get('filename', 'probe.txt') }}"
        ),
        # No trailing slash: the inherited _create_key joins with f"{prefix}/{name}",
        # so "xcloud/" would produce "xcloud//probe.txt". That is the opposite
        # convention to the other DAGs here, where prefixes do end in "/".
        blob_prefix="{{ dag_run.conf.get('blob_prefix', 'xcloud') }}",
        create_container=False,  # the container is expected to exist
        doc_md="""
Streams the object from S3 into Blob Storage without staging it on disk.

Subclasses the provider's `S3ToAzureBlobStorageOperator` and overrides only
`move_file` — the stock version downloads to a `NamedTemporaryFile` first, so
the whole object lands on the worker pod's disk. Prefix handling, blob naming
and `replace` filtering are inherited unchanged.

`length=` and `max_concurrency=1` on the Azure upload are both required: the S3
response body exposes a `seek` attribute but reports `seekable()` as `False`,
and the SDK would otherwise seek it to measure and split the stream.

Pushes `[{source, blob, size}]` to XCom.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
Reads each blob's properties from Azure and compares its size against the byte
count reported by S3, so a truncated transfer fails the run instead of passing
silently.

Runs in its own pod with a fresh client, so it is an independent read of service
state rather than a re-check through the transfer task's handle.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
