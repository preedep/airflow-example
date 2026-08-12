"""nix-dag-wasb-prefix-suffix-sensor — wait for a blob matching a prefix AND suffix."""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-wasb-prefix-suffix-sensor

Watches an Azure Blob Storage container for a blob matching **both** a prefix and a
suffix, then reports what it found.

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"prefix": "incoming/", "suffix": ".csv"}
```

Defaults to prefix `""` (whole container) and suffix `.txt`.

#### Why a custom sensor

The provider ships `WasbBlobSensor` (exact blob name) and `WasbPrefixSensor`
(prefix only). Neither can express *"any `.csv` under `incoming/`"*, which is the
common landing-zone pattern — the extension filters on a suffix as well.

`WasbPrefixSensorWithSuffix` subclasses `WasbPrefixSensor` rather than
`BaseSensorOperator`, so it inherits the provider's connection handling,
`check_options`, and templated fields. Only `poke()` is overridden.

**The prefix is still pushed to Azure.** It is passed to `get_blobs_list`, so the
service filters server-side and only matching names come back; the suffix is
matched locally on that reduced list. Filtering everything client-side would list
the whole container on every poke.

#### Matching

| Setting | Effect |
|---|---|
| `prefix` | server-side filter, e.g. `incoming/2026-08-` |
| `suffix` | matched on the blob name, case-insensitive by default |
| `delimiter=""` | recurse into "subdirectories" (the provider default `/` does not) |

An empty `suffix` matches everything, making this behave like the stock
`WasbPrefixSensor`.

#### Sensor settings

`mode="reschedule"`, `poke_interval=60`, `timeout=3600`. Reschedule frees the worker
slot between pokes — under KubernetesExecutor `poke` mode would hold a pod idle for
the whole wait. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

#### What it returns

The sensor pushes the list of matching blob names to XCom (`matched_blobs`), so the
downstream task knows exactly which files triggered it rather than re-listing and
possibly seeing a different set.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob, conn type `wasb` |

For SAS auth the token goes in the connection's **extra** as `sas_token`, and
`login` is the **storage account name** — the hook builds the account URL from it.
The container is not part of the connection; it is passed per operation.
"""

WASB_CONN_ID = "wasb-nickstorageairflow002"
CONTAINER = "data001"

DEFAULT_PREFIX = ""
DEFAULT_SUFFIX = ".txt"


class WasbPrefixSensorWithSuffix(WasbPrefixSensor):
    """Wait for a blob whose name starts with `prefix` and ends with `suffix`.

    Extends the provider's prefix sensor instead of replacing it: the prefix is
    still applied server-side, and only the suffix check is done locally.
    """

    template_fields = ("container_name", "prefix", "suffix")

    def __init__(
        self,
        *,
        suffix: str = "",
        case_sensitive: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.suffix = suffix
        self.case_sensitive = case_sensitive

    def _matches(self, blob_name: str) -> bool:
        if not self.suffix:
            return True
        if self.case_sensitive:
            return blob_name.endswith(self.suffix)
        return blob_name.lower().endswith(self.suffix.lower())

    def poke(self, context) -> bool:
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)

        # delimiter="" recurses; the provider's default "/" stops at the first level.
        options = {"delimiter": "", **self.check_options}
        blobs = hook.get_blobs_list(
            container_name=self.container_name, prefix=self.prefix, **options
        )

        matched = [b for b in blobs if self._matches(b)]

        self.log.info(
            "[wasb_sensor] prefix=%r suffix=%r — %d blob(s) under prefix, %d matching",
            self.prefix,
            self.suffix,
            len(blobs),
            len(matched),
        )

        if not matched:
            return False

        # Hand the exact matches downstream so it does not have to re-list.
        context["ti"].xcom_push(key="matched_blobs", value=sorted(matched))
        return True


def report_matches(ti=None, **context):
    """Log the blobs the sensor matched, with their sizes."""
    task_log = logging.getLogger("airflow.task")

    names = ti.xcom_pull(task_ids="wait_for_blob", key="matched_blobs") or []
    if not names:
        raise ValueError("sensor succeeded but pushed no blob names")

    hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
    client = hook.get_conn().get_container_client(CONTAINER)

    results = []
    for name in names:
        props = client.get_blob_client(name).get_blob_properties()
        results.append({"name": name, "size": props.size})
        task_log.info("[wasb_sensor] %s — %s bytes", name, props.size)

    return {"container": CONTAINER, "count": len(results), "blobs": results}


with DAG(
    dag_id="nix-dag-wasb-prefix-suffix-sensor",
    description="Wait for an Azure blob matching a prefix and suffix, then report it",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "azure", "wasb", "sensor"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    wait_for_blob = WasbPrefixSensorWithSuffix(
        task_id="wait_for_blob",
        wasb_conn_id=WASB_CONN_ID,
        container_name=CONTAINER,
        prefix="{{ dag_run.conf.get('prefix', '') }}",
        suffix="{{ dag_run.conf.get('suffix', '.txt') }}",
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=60,  # 1 minute
        timeout=60 * 60,  # 1 hour
        doc_md="""
Polls the container for blobs matching both the prefix and the suffix.

The prefix is applied **server-side** by Azure; only the suffix is checked
locally, so a large container is not listed in full on every poke.

Pushes the matching names to XCom as `matched_blobs`. Returns `False` and keeps
waiting when nothing matches — a `550`-style "not found" is not an error here.
""",
    )

    report = PythonOperator(
        task_id="report_matches",
        python_callable=report_matches,
        doc_md="""
Reads `matched_blobs` from XCom and fetches each blob's size.

Uses the names the sensor captured rather than re-listing, so the report matches
exactly what satisfied the sensor even if the container changed in between.
""",
    )

    wait_for_blob >> report
