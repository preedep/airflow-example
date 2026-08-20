"""nix-dag-cyclic-check-s3-transfer-sftp — poll S3 on a business-hours cycle, stream to SFTP."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)

DAG_DOC_MD = """
### nix-dag-cyclic-check-s3-transfer-sftp

A **cyclic pickup job**: every 5 minutes during business hours it checks an S3
prefix, and if anything is waiting it streams the objects **S3 → SFTP** without
staging them on disk. When the prefix is empty the run short-circuits and ends
`success` with everything downstream skipped.

```
every 5 min, 08:00-19:00 Mon-Fri
        │
        ▼
   check_for_files ──(nothing)──▶ skipped, run ends success
        │
     (found)
        ▼
   stream_to_sftp ──download_fileobj──▶ SFTP server
        │                               SFTP_DIR/<name>
        ▼
   report_cycle
```

#### Schedule

`schedule="*/5 8-18 * * 1-5"` — every 5 minutes from 08:00 through 18:55,
Monday to Friday. The last fire is **18:55**, which is the interval that covers
the run up to the 19:00 boundary; `8-19` would keep firing until 19:55 and run
an hour past the window.

Cron fires in the **scheduler's timezone**. If your operators think in local
time and the deployment runs UTC, set `timezone` on the DAG or convert the hour
range — a window that reads correctly in the source file and fires at the wrong
hour on the cluster is the usual way this goes wrong.

| Setting | Value | Why |
|---|---|---|
| `schedule` | `*/5 8-18 * * 1-5` | the business-hours cycle |
| `max_active_runs` | `1` | **the cyclic guarantee** — a slow transfer never overlaps the next |
| `catchup` | `False` | never backfill; a missed pickup window is gone, not owed |
| `dagrun_timeout` | 30 min | a wedged transfer must not block the cycle indefinitely |

`max_active_runs=1` is what makes this cyclic rather than merely scheduled: a
transfer that overruns 5 minutes queues the next check instead of running beside
it, so the same object is never picked up twice concurrently.

#### Why a short-circuit and not a sensor

The obvious build is `S3KeySensor` → transfer. It is the wrong shape for a
cyclic job:

- A sensor **waits**. This DAG already re-checks every 5 minutes, so waiting
  inside the run duplicates the schedule and holds the slot that
  `max_active_runs=1` limits to one.
- A sensor that finds nothing ends the run **failed** on timeout. An empty
  prefix at 08:05 on a quiet morning is a normal outcome, not an incident — a
  wall of red runs trains everyone to ignore the DAG.

`ShortCircuitOperator` returns a boolean instead: `False` skips everything
downstream and the run ends `success`. The "nothing to do" cycle is visibly
distinct from a broken one in the grid.

#### Idempotency across cycles

Every 5 minutes the same prefix is listed again, so the run **must not**
re-transfer what a previous cycle already moved. Two mechanisms:

- **Archive after transfer** (`ARCHIVE_PREFIX`, default on) — the object is
  copied to the archive prefix and deleted from the source, so the next cycle
  does not see it. S3 has no move; copy-then-delete is the operation.
- **Delete-then-copy on the SFTP side** — the transfer writes `<name>.part` and
  `posix_rename`s it, which overwrites atomically. A retry over a partially
  moved batch converges rather than accumulating.

Set `ARCHIVE_PREFIX = ""` to leave objects in place — then every cycle
re-transfers everything in the prefix, which is fine for a demo and wrong for
production.

The batch is capped at `MAX_FILES_PER_RUN`. A backlog drains over several
cycles instead of one run holding a pod for an hour.

#### Streaming

The transfer subclasses the provider's `S3ToSFTPOperator` and overrides only
`execute`. The stock version stages the whole object on the worker pod's disk:

```python
with NamedTemporaryFile("w") as f:
    s3_client.download_file(self.s3_bucket, self.s3_key, f.name)
    sftp_client.put(f.name, self.sftp_path, confirm=self.confirm)
```

`download_fileobj` **writes into** any writable and paramiko's
`open(..., "wb")` **is** one, so the streaming version is a single call with no
pipe and no read loop:

```python
with sftp_client.open(self.sftp_path, "wb") as handle:
    handle.set_pipelined(True)
    s3_client.download_fileobj(self.s3_bucket, self.s3_key, handle)
```

Peak memory is a part buffer (8 MiB), not the object size. `MAX_BYTES` rejects
an oversized object before any bytes move.

This is the same override as `nix-dag-s3-to-sftp-stream`; that DAG transfers one
named object on a manual trigger, this one discovers a batch on a cycle.

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

#### Cost note

This DAG fires **132 times a day** while unpaused, and S3 charges for both the
`list_objects_v2` calls and the data leaving AWS. Pause it when you are done.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_s3_test_001"
SFTP_CONN_ID = "sftp_test_001"

BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

# The prefix polled each cycle. A prefix is not a directory — S3 has no folders,
# so this is a literal string match and nested keys under it match too.
SOURCE_PREFIX = "incoming/"

# Where an object is moved after a successful transfer, so the next cycle does
# not pick it up again. "" leaves objects in place and re-transfers every cycle.
ARCHIVE_PREFIX = "archive/"

# Adjust to your server. Must exist and be writable by the connection's user.
SFTP_DIR = "/home/airflowsftp/incoming"

# A backlog drains over several cycles rather than one run holding a pod for an
# hour — and keeps the run inside the 5-minute window on a normal day.
MAX_FILES_PER_RUN = 10

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
    """`S3ToSFTPOperator` that streams a *batch* instead of staging on disk.

    Two changes from the provider: `execute` hands boto3 the remote file handle
    rather than a `NamedTemporaryFile`, and it iterates the keys the upstream
    check found instead of a single templated `s3_key`. Connection handling and
    templated fields stay with the provider.

    :param keys_task_id: task whose XCom holds the list of keys to move.
    :param sftp_dir: destination directory; the key's basename is appended.
    :param archive_prefix: copy-then-delete each key here after a successful
        transfer, so the next cycle does not see it. "" leaves it in place.
    :param temp_suffix: write to `<path><temp_suffix>` and rename on success, so
        a consumer never sees a partial file. "" writes directly.
    :param max_bytes: reject an object larger than this before any bytes move.
    """

    def __init__(
        self,
        *,
        keys_task_id: str,
        sftp_dir: str,
        archive_prefix: str = ARCHIVE_PREFIX,
        temp_suffix: str = ".part",
        max_bytes: int = MAX_BYTES,
        **kwargs: Any,
    ) -> None:
        # s3_key/sftp_path are required by the base class but unused: execute is
        # fully overridden and resolves a path per key. Passing the directory
        # keeps any inherited logging honest rather than showing a placeholder.
        kwargs.setdefault("s3_key", "")
        kwargs.setdefault("sftp_path", sftp_dir)
        super().__init__(**kwargs)
        self.keys_task_id = keys_task_id
        self.sftp_dir = sftp_dir
        self.archive_prefix = archive_prefix
        self.temp_suffix = temp_suffix
        self.max_bytes = max_bytes

    def _stream_one(self, s3_client, sftp_client, key: str) -> dict[str, Any]:
        """Stream a single object into the SFTP directory, returning its record."""
        destination = f"{self.sftp_dir}/{key.rsplit('/', 1)[-1]}"

        # Size up front: rejects an oversized object before any bytes move, and
        # gives the size check an authoritative number. download_fileobj returns
        # None, so this is the only figure the transfer itself can report.
        expected = s3_client.head_object(Bucket=self.s3_bucket, Key=key)["ContentLength"]
        if expected > self.max_bytes:
            raise ValueError(
                f"{key} is {expected} bytes, over the {self.max_bytes} limit"
            )

        self.log.info(
            "[cyclic_s3_sftp] streaming s3://%s/%s -> %s (%s bytes)",
            self.s3_bucket,
            key,
            destination,
            expected,
        )

        started = time.monotonic()

        # Write to a temporary name and rename once complete, so a consumer
        # watching the drop directory never sees a partial file.
        target = destination + self.temp_suffix if self.temp_suffix else destination

        # The remote handle is the download destination — boto3 writes into it
        # directly, so nothing is staged on the worker pod's disk.
        with sftp_client.open(target, "wb") as handle:
            # Keeps multiple writes in flight rather than one round-trip each.
            # No measurable gain on a local network, where boto3's thread pool
            # already overlaps writes, but it matters on a high-latency link.
            handle.set_pipelined(True)
            # BYTE PATH — nothing touches the worker pod's disk:
            #   HTTPS GET (S3) -> boto3 8 MiB part buffer -> SFTP socket.
            s3_client.download_fileobj(self.s3_bucket, key, handle)

        # download_fileobj reports nothing, so the size is read back from the
        # server. Checked *before* the rename, so a truncated transfer never
        # replaces a good previous file.
        written = sftp_client.stat(target).st_size
        if written != expected:
            raise ValueError(
                f"size mismatch for {key}: wrote {written}, expected {expected}"
            )

        if self.temp_suffix:
            # posix_rename, not rename: plain SFTP rename fails when the target
            # exists, so a retry over a previous cycle's file would break.
            # posix_rename overwrites atomically.
            sftp_client.posix_rename(target, destination)

        self.log.info(
            "%s -> %s",
            _summary("cyclic_s3_sftp", written, time.monotonic() - started),
            destination,
        )
        return {"key": key, "destination": destination, "size": written}

    def _archive(self, s3_hook: S3Hook, key: str) -> str:
        """Move a transferred object out of the polled prefix.

        S3 has no move operation — copy then delete is the move. Done only after
        the size check passed, so a failed transfer leaves the object in place
        for the next cycle to retry.
        """
        archived_key = f"{self.archive_prefix.rstrip('/')}/{key.rsplit('/', 1)[-1]}"

        s3_hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=archived_key,
            source_bucket_name=self.s3_bucket,
            dest_bucket_name=self.s3_bucket,
        )
        s3_hook.delete_objects(bucket=self.s3_bucket, keys=[key])

        self.log.info("[cyclic_s3_sftp] archived %s -> %s", key, archived_key)
        return archived_key

    def execute(self, context) -> dict[str, Any]:
        keys = context["ti"].xcom_pull(task_ids=self.keys_task_id, key="pending_keys")
        if not keys:
            # The short-circuit only lets this task run after pushing a non-empty
            # list, so an empty pull means the contract broke (task renamed, XCom
            # cleared) rather than "nothing to do". Fail rather than no-op.
            raise ValueError(f"{self.keys_task_id} pushed no keys")

        # SFTPHook rather than the base class's SSHHook + open_sftp(): its
        # get_managed_conn() is refcounted and closes the session on exit. One
        # session for the whole batch, not one per file.
        sftp_hook = SFTPHook(ssh_conn_id=self.sftp_conn_id)

        # Built here, not taken from the base class: unlike
        # S3ToAzureBlobStorageOperator, S3ToSFTPOperator exposes no `s3_hook`
        # property — it constructs one inside its own execute().
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_client = s3_hook.get_conn()

        transferred: list[dict[str, Any]] = []
        with sftp_hook.get_managed_conn() as sftp_client:
            for key in keys:
                record = self._stream_one(s3_client, sftp_client, key)
                # Archive per file rather than after the batch: if file 7 of 10
                # fails, the first six stay archived and the retry resumes at
                # the remainder instead of re-sending everything.
                if self.archive_prefix:
                    record["archived_to"] = self._archive(s3_hook, key)
                transferred.append(record)

        total = sum(item["size"] for item in transferred)
        self.log.info(
            "[cyclic_s3_sftp] batch complete: %d file(s), %s",
            len(transferred),
            _human(total),
        )
        return {"count": len(transferred), "total_bytes": total, "files": transferred}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def check_for_files(ti=None, **context):
    """List the polled prefix; return False to skip the cycle when it is empty.

    The return value drives `ShortCircuitOperator`: False skips everything
    downstream and the run ends `success`, so an empty prefix is a normal quiet
    cycle rather than a failed one.
    """
    task_log = logging.getLogger("airflow.task")

    conf = context["dag_run"].conf or {}
    prefix = conf.get("prefix", SOURCE_PREFIX)
    suffix = conf.get("suffix", "")

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # iter_file_metadata passes Prefix to S3 and paginates, so the filtering
    # happens service-side; only the already-narrowed page contents are scanned
    # locally for the suffix.
    keys = [
        item["Key"]
        for item in hook.iter_file_metadata(prefix, BUCKET)
        # A "directory marker" — a zero-byte key ending in "/" that the console
        # creates for an empty folder — is not a file and must not be transferred.
        if not item["Key"].endswith("/")
        and (not suffix or item["Key"].lower().endswith(suffix.lower()))
    ]

    if not keys:
        task_log.info(
            "[cyclic_s3_sftp] nothing at s3://%s/%s — skipping this cycle", BUCKET, prefix
        )
        return False

    # sorted() makes the batch deterministic, so a retry picks the same files in
    # the same order as the attempt it is replacing.
    batch = sorted(keys)[:MAX_FILES_PER_RUN]
    if len(keys) > MAX_FILES_PER_RUN:
        task_log.info(
            "[cyclic_s3_sftp] %d file(s) waiting, taking %d this cycle — "
            "the rest drain on the next one",
            len(keys),
            MAX_FILES_PER_RUN,
        )

    task_log.info("[cyclic_s3_sftp] picked up %d file(s): %s", len(batch), batch)

    # Hand the exact keys downstream so the transfer does not re-list and
    # possibly see a different set — objects land and are archived continuously.
    ti.xcom_push(key="pending_keys", value=batch)
    return True


def report_cycle(ti=None, **context):
    """Log what this cycle moved, and confirm each file landed at the size we sent."""
    task_log = logging.getLogger("airflow.task")

    result = ti.xcom_pull(task_ids="stream_to_sftp")
    if not result:
        raise ValueError("stream_to_sftp pushed no result")

    # Separate pod, so a fresh connection: an independent read of server state
    # rather than a re-check of the transfer task's own handle.
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    with hook.get_managed_conn() as sftp_client:
        for item in result["files"]:
            actual = sftp_client.stat(item["destination"]).st_size
            if actual != item["size"]:
                raise ValueError(
                    f"{item['destination']}: expected {item['size']} bytes, found {actual}"
                )
            task_log.info(
                "[cyclic_s3_sftp] verified %s — %s bytes", item["destination"], actual
            )

    task_log.info(
        "[cyclic_s3_sftp] cycle moved %d file(s), %s",
        result["count"],
        _human(result["total_bytes"]),
    )
    return {"count": result["count"], "total_bytes": result["total_bytes"]}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-cyclic-check-s3-transfer-sftp",
    description="Every 5 min on weekdays 08:00-19:00, stream any waiting S3 object to SFTP",
    # Every 5 minutes from 08:00 through 18:55, Mon-Fri. The hour range stops at
    # 18 on purpose: "8-19" would keep firing until 19:55, an hour past the
    # window. Fires in the scheduler's timezone.
    schedule="*/5 8-18 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,  # a missed pickup window is gone, not owed
    max_active_runs=1,  # the cyclic guarantee — a slow transfer never overlaps the next check
    dagrun_timeout=timedelta(minutes=30),  # a wedged transfer must not block the cycle
    tags=["demo", "cyclic", "aws", "s3", "sftp", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    check = ShortCircuitOperator(
        task_id="check_for_files",
        python_callable=check_for_files,
        doc_md="""
Lists the polled S3 prefix and decides whether this cycle has work.

Returns `False` when the prefix is empty, which **skips** the transfer and the
report — the run ends `success`, so a quiet cycle is visibly different from a
broken one. A sensor would instead wait out its timeout and mark the run failed.

The prefix is applied **server-side** by `iter_file_metadata`, which paginates,
so a large bucket is not listed in full on every poke. Zero-byte directory
markers are filtered out; an optional `suffix` conf narrows further.

Pushes the batch to XCom as `pending_keys`, capped at `MAX_FILES_PER_RUN` so a
backlog drains over several cycles rather than one long run.
""",
    )

    stream = StreamingS3ToSFTPOperator(
        task_id="stream_to_sftp",
        aws_conn_id=AWS_CONN_ID,
        sftp_conn_id=SFTP_CONN_ID,
        s3_bucket=BUCKET,
        keys_task_id="check_for_files",
        sftp_dir=SFTP_DIR,
        doc_md="""
Streams each object the check found onto the SFTP server without staging it on
disk.

Subclasses the provider's `S3ToSFTPOperator` and overrides only `execute` — the
stock version downloads to a `NamedTemporaryFile` and uploads from that path, so
the whole object lands on the worker pod's disk first. `download_fileobj` writes
into any writable and paramiko's `open(..., "wb")` is one, so the transfer is a
single call with no pipe and no read loop.

Each file is written as `<name>.part` and `posix_rename`d on success, so a
consumer never sees a partial file. The size check runs *before* the rename, so a
truncated transfer never replaces a good previous file.

Archives each key to `ARCHIVE_PREFIX` immediately after its own transfer, not
after the batch — a failure partway leaves the already-moved files archived and
the retry resumes at the remainder.

One SFTP session is opened for the whole batch, not one per file.
""",
    )

    report = PythonOperator(
        task_id="report_cycle",
        python_callable=report_cycle,
        doc_md="""
`stat`s every destination over SFTP and compares its size against the byte count
recorded by the transfer, so a truncated file fails the run instead of passing
silently.

Runs in its own pod with a fresh connection, so it is an independent read of
server state.
""",
    )

    # Both edges are data dependencies, not just ordering: the transfer consumes
    # pending_keys and the report consumes the transfer's result.
    check >> stream >> report
