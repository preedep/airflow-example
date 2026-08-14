"""nix-dag-s3-prefix-suffix-sensor — wait for an S3 object matching a prefix AND suffix."""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-s3-prefix-suffix-sensor

Watches an S3 bucket for objects matching **both** a prefix and a suffix, then
reports what it found.

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"prefix": "incoming/", "suffix": ".csv"}
```

Defaults to prefix `probe/` and suffix `.txt`.

#### Why subclass, when the provider already matches wildcards

`S3KeySensor` is closer to the job than its Azure counterpart. It already
supports `wildcard_match=True`, so `incoming/*.csv` works out of the box, and
`iter_file_metadata` pushes the prefix to S3 and paginates — the matching itself
needs no help.

The gap is **what the sensor hands downstream**. With `wildcard_match`, its
`_check_key` returns `True` on the *first* match without recording which keys
matched, and `S3KeySensor` never calls `xcom_push` at all. A downstream task is
left re-listing the bucket, which can return a different set than the one that
satisfied the sensor — new objects land, lifecycle rules delete others.

`S3PrefixSuffixSensor` overrides `poke()` to collect **every** match and push the
sorted list to XCom as `matched_keys`, so the reporting task acts on exactly what
fired the sensor.

Compare `nix-dag-wasb-prefix-suffix-sensor`, which subclasses for a different
reason: the Azure provider has no suffix matching at all, so that one adds the
capability. Here the capability exists and only the hand-off is missing.

#### Matching

| Setting | Effect |
|---|---|
| `prefix` | server-side filter, e.g. `incoming/2026-08-` |
| `suffix` | matched on the key, case-insensitive by default |

The prefix goes to S3, so the service filters before returning and only matching
keys come back; the suffix is applied locally to that reduced list. An empty
`suffix` matches everything, degrading to a prefix-only watch.

**A prefix is not a directory.** S3 has no folders — `incoming/` is a literal
string prefix, so `incoming/2026/a.csv` matches `incoming/` just as
`incoming/a.csv` does. There is no `delimiter` to opt out of recursion the way
the Azure demo needs.

#### Sensor settings

`mode="reschedule"`, `poke_interval=30`, `timeout=1800`. Reschedule frees the
worker slot between pokes — under KubernetesExecutor `poke` mode would hold a pod
idle for the whole wait. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

`deferrable=True` would be lighter still, but it needs a triggerer process
running; reschedule works on any deployment.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS credentials, conn type `aws` |

Put the access key in **login**, the secret in **password**, and set the region
in **extra** as `{"region_name": "ap-southeast-1"}`. The region matters: worker
pods have no `AWS_DEFAULT_REGION`, so a connection without it fails where a
laptop with a configured profile succeeds.

The bucket is not part of the connection; it is passed per operation.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_s3_test_001"
BUCKET = "nix-s3-demo-743702012710-ap-southeast-1-an"

# Referenced by the DAG docs; the live defaults are the Jinja fallbacks on the
# sensor's prefix/suffix fields, since a callable never sees these constants.
DEFAULT_PREFIX = "probe/"
DEFAULT_SUFFIX = ".txt"


# --------------------------------------------------------------------------- #
# Sensor override
# --------------------------------------------------------------------------- #


class S3PrefixSuffixSensor(S3KeySensor):
    """Wait for S3 objects whose key starts with `prefix` and ends with `suffix`.

    Extends the provider's key sensor rather than replacing it: connection
    handling, `verify`, and region resolution are all inherited. Only `poke` is
    overridden, to collect every match and hand the list downstream — the stock
    sensor returns on the first hit and pushes nothing.
    """

    # Redeclared to add "prefix" and "suffix". template_fields is not merged with
    # the base class's, so dropping the inherited names would silently stop Jinja
    # rendering on bucket_name and friends.
    template_fields = (
        "aws_conn_id",
        "bucket_name",
        "prefix",
        "suffix",
        "region_name",
        "verify",
    )

    # Keyword-only, so the added arguments can never be confused with the base
    # sensor's own parameters.
    def __init__(
        self,
        *,
        prefix: str = "",
        suffix: str = "",
        case_sensitive: bool = False,
        **kwargs: Any,
    ) -> None:
        # bucket_key is required by the base class but unused here — poke() is
        # fully overridden and matches on prefix/suffix instead. Passing the
        # prefix keeps any inherited logging honest rather than showing a
        # placeholder key that nothing looks for.
        kwargs.setdefault("bucket_key", prefix or "*")
        super().__init__(**kwargs)
        self.prefix = prefix
        self.suffix = suffix
        self.case_sensitive = case_sensitive

    def _matches(self, key: str) -> bool:
        # Empty suffix means "no suffix filter", degrading to a prefix-only
        # watch rather than matching nothing.
        if not self.suffix:
            return True
        if self.case_sensitive:
            return key.endswith(self.suffix)
        return key.lower().endswith(self.suffix.lower())

    def poke(self, context) -> bool:
        # self.hook is the provider's cached_property, so this reuses the base
        # class's connection handling rather than building a hook by hand.
        #
        # iter_file_metadata passes Prefix to S3 and paginates, so the filtering
        # happens service-side; only the already-narrowed page contents are
        # scanned locally for the suffix.
        matched = [
            item["Key"]
            for item in self.hook.iter_file_metadata(self.prefix, self.bucket_name)
            if self._matches(item["Key"])
        ]

        self.log.info(
            "[s3_sensor] prefix=%r suffix=%r — %d matching key(s)",
            self.prefix,
            self.suffix,
            len(matched),
        )

        if not matched:
            return False

        # Hand the exact matches downstream so it does not have to re-list and
        # possibly see a different set. sorted() keeps the XCom stable across
        # runs that match the same objects.
        context["ti"].xcom_push(key="matched_keys", value=sorted(matched))
        return True


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def report_matches(ti=None, **context):
    """Log the objects the sensor matched, with their sizes."""
    task_log = logging.getLogger("airflow.task")

    # Defensive: the sensor only returns True after pushing a non-empty list, so
    # an empty pull means the contract broke (task renamed, XCom cleared) rather
    # than "nothing matched". Fail instead of reporting a misleading zero.
    keys = ti.xcom_pull(task_ids="wait_for_object", key="matched_keys") or []
    if not keys:
        raise ValueError("sensor succeeded but pushed no keys")

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # One head_object per key — fine for a demo-sized match set, but this is the
    # part to batch if the sensor can match thousands of objects.
    results = []
    for key in keys:
        size = hook.head_object(key, BUCKET)["ContentLength"]
        results.append({"key": key, "size": size})
        task_log.info("[s3_sensor] %s — %s bytes", key, size)

    return {"bucket": BUCKET, "count": len(results), "objects": results}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-s3-prefix-suffix-sensor",
    description="Wait for an S3 object matching a prefix and suffix, then report it",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "s3", "sensor"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    wait_for_object = S3PrefixSuffixSensor(
        task_id="wait_for_object",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=BUCKET,
        prefix="{{ dag_run.conf.get('prefix', 'probe/') }}",
        suffix="{{ dag_run.conf.get('suffix', '.txt') }}",
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=30,
        timeout=60 * 30,  # 30 minutes
        doc_md="""
Polls the bucket for objects matching both the prefix and the suffix.

The prefix is applied **server-side** by S3 via `iter_file_metadata`, which
paginates; only the suffix is checked locally, so a large bucket is not listed
in full on every poke.

Pushes every matching key to XCom as `matched_keys` — the stock `S3KeySensor`
returns on the first match and pushes nothing, which is the reason this
subclass exists. Returns `False` and keeps waiting when nothing matches.
""",
    )

    report = PythonOperator(
        task_id="report_matches",
        python_callable=report_matches,
        doc_md="""
Reads `matched_keys` from XCom and fetches each object's size with
`head_object`.

Uses the keys the sensor captured rather than re-listing, so the report matches
exactly what satisfied the sensor even if the bucket changed in between.
""",
    )

    # report consumes the sensor's matched_keys XCom — a data dependency.
    wait_for_object >> report
