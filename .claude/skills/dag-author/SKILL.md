---
name: dag-author
description: Write a new Airflow 3 DAG or modify an existing one in this repo as a standalone single file. Use when the user asks to create a DAG, add a pipeline, add tasks, or port an Airflow 2 DAG.
---

# Authoring a DAG

Target is Airflow 3.2.1 on KubernetesExecutor. Follow the pattern below rather than
generic Airflow examples from the web, which are mostly 2.x.

Read `.claude/references/g1pro.md` §6–§8 for the environment rules behind this.

## 1. One file, standalone

**Each demo DAG is a single self-contained file** at `dags/dag_<name>.py`. No
subfolders, no shared `dag_utils` module, no cross-DAG imports — this is a deliberate
constraint so each demo reads and deploys on its own. If two DAGs need the same small
helper, duplicate it rather than coupling them.

Task callables are module-level functions in the same file.

## 2. Write the DAG

**`doc_md` is mandatory** — on the DAG and on every task. It renders as Markdown in
the UI and is enforced by `tests/test_dag_integrity.py`. Keep the DAG-level Markdown
in a `DAG_DOC_MD` constant placed **after the imports**; putting it before them makes
ruff report every import as `E402`.

```python
"""nix-dag-mydemo — example pipeline."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-mydemo

What it does, in a sentence.

#### Trigger
Manual only. Pass conf: `{"endpoint": "https://..."}`

#### Requires
| Kind | Name | Purpose |
|---|---|---|
| Connection | `my_conn` | ... |
"""


def fetch_data(**context):
    import requests                                    # import inside the callable

    task_log = logging.getLogger("airflow.task")
    endpoint = (context["dag_run"].conf or {}).get("endpoint", "https://api.example.com/v1")

    task_log.info("[mydemo] fetching %s", endpoint)
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()                        # raise → task fails visibly
    return response.json()                             # return value → XCom


def process_data(ti=None, **context):
    task_log = logging.getLogger("airflow.task")
    data = ti.xcom_pull(task_ids="fetch_data")
    task_log.info("[mydemo] processing: %s", data)
    return {"status": "ok"}


with DAG(
    dag_id="nix-dag-mydemo",
    description="Example demo pipeline",
    schedule=None,                   # None = manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "mydemo"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        doc_md="Fetches the payload and pushes it to XCom.",
    )
    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        doc_md="Pulls the payload from XCom and processes it.",
    )

    fetch >> process
```

Conventions: import heavy modules inside the callable; log via
`logging.getLogger("airflow.task")` with a `[<name>]` prefix; always set a `timeout`
on network calls; raise rather than return an error.

## 3. Pick the operator before writing Python

**Check the provider for an operator that already does the job.** Reach for
`PythonOperator` only when nothing fits. A provider operator brings templated
fields, retries, logging, and XCom handling you would otherwise re-implement.

```bash
# What does this provider ship?
.venv/bin/python -c "
import pkgutil, airflow.providers.ftp as p
[print(m.name) for m in pkgutil.walk_packages(p.__path__, p.__name__ + '.')]"
```

Easy to miss: `apache-airflow-providers-ftp` has **`FTPSFileTransmitOperator`** for
put/get — writing a `PythonOperator` that calls `hook.store_file()` duplicates it.

Needing a custom hook is **not** a reason to abandon the operator. Subclass and
override the `hook` property; the operator's logic stays intact:

```python
class MyFTPSFileTransmitOperator(FTPSFileTransmitOperator):
    @cached_property
    def hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)
```

## 3a. Tasks do not share a filesystem

Every task runs in its **own ephemeral pod** under KubernetesExecutor. A file
written to local disk by one task does not exist for the next — it fails with
`FileNotFoundError`, and only at runtime, so a parse check will not catch it.

Options, in order of preference:

1. **Read from shared storage.** `/mnt/external-storage/airflow-dags` on the host is
   `/opt/airflow/dags` in every pod. Put fixtures in `dags/files/`.
2. **Do it in one task.** Build the payload in memory and pass a `BytesIO` buffer
   to the hook rather than writing a temp file.
3. **Pass a reference, not a file.** Write to object storage, push the key to XCom.

Never assume `/tmp` persists between tasks.

## 4. Sensors

Always `mode="reschedule"` with an explicit `poke_interval` and `timeout`.
`mode="poke"` holds a worker pod for the entire wait — on KubernetesExecutor that is a
pod sitting idle for hours.

```python
from airflow.providers.ftp.sensors.ftp import FTPSSensor

wait = FTPSSensor(
    task_id="wait_for_file",
    ftp_conn_id="ftps_test_001",
    path="/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}",
    mode="reschedule",
    poke_interval=60,
    timeout=60 * 60,
)
```

A sensor without a `timeout` waits forever and blocks `max_active_runs`.
`dags/dag_ftps_sensor.py` is a working example.

## 5. Rules that break naive code

**No deprecated APIs — enforced, not advisory.** The 2.x paths still *work* in 3.2.1
as warning shims, so they parse fine and even run. The integrity tests fail the build
on them anyway. Do not reach for one because "it imports".

```python
from airflow.providers.standard.operators.python import PythonOperator   # ✅
from airflow.providers.standard.operators.bash import BashOperator       # ✅
from airflow.providers.standard.sensors.python import PythonSensor       # ✅
from airflow.sdk import Variable, Connection                             # ✅

from airflow.operators.python import PythonOperator                      # ❌ deprecated shim
from airflow.sensors.python import PythonSensor                          # ❌ deprecated shim
from airflow.utils.dates import days_ago                                 # ❌ deprecated
from airflow.utils.operator_helpers import determine_kwargs              # ❌ → airflow.sdk.bases.decorator
from airflow.models import Variable                                      # ❌ fails in worker pods
```

Rule of thumb: anything under `airflow.operators.*`, `airflow.sensors.*`, or
`airflow.hooks.*` moved to `airflow.providers.standard.*` in 3.x. `airflow.models`
is still fine for `DagBag` etc., but `Variable`/`Connection` come from `airflow.sdk`.

**`schedule=`, not `schedule_interval=`.** Removed in 3.x.

**`catchup=False`.** A past `start_date` with catchup on floods the cluster with
backfill runs on first unpause.

**Package availability.** ~30 providers are in the image — `standard`, `ftp`, `sftp`,
`ssh`, `cncf-kubernetes`, `postgres`, `mysql`, `http`, `smtp`, `amazon`, `google`,
`snowflake`, `slack` and more, plus `requests`. Full list in `CLAUDE.md`. Anything
outside it needs an image rebuild and cannot be pip-installed into a worker.

The local `.venv` carries only a subset, so add a provider with
`uv add "apache-airflow-providers-<name>==<server version>"` before parse-checking a
DAG that imports it.

**In-cluster addresses.** Tasks run inside k3s. Use
`postgres.postgres.svc.cluster.local:5432`, not `nixhome-linux-g1pro:30080`.

**Secrets.** `Variable.get("name")` inside the callable, never at module scope and
never hardcoded. Set them per `.claude/references/g1pro.md` §9.

**Idempotency.** A retry must converge, not accumulate. Delete-then-insert by
partition rather than blind append.

## 6. Heavy or non-Python work

This is a KubernetesExecutor cluster — use `KubernetesPodOperator`
(`airflow.providers.cncf.kubernetes.operators.pod`) rather than fattening the worker.
Pin the image to an **x86_64** tag, never `:latest`, and set memory limits — an
unbounded pod OOM-kills as `Negsignal.SIGKILL`.

## 7. Validate before deploying

`.venv` is pinned to Python 3.13 + Airflow 3.2.1 to match the server image.

```bash
.venv/bin/python dags/dag_<name>.py       # exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

`tests/test_dag_integrity.py` picks up new DAGs automatically — it globs
`dags/dag_*.py`, so there is no per-DAG test to add. It enforces the rules in §5
plus the standalone-file requirement.

A clean local parse is necessary but not sufficient: the venv carries only the
common providers, so it cannot prove a third-party import exists in the server
image. The authoritative check is `mcp__airflow__get_import_errors` after deploy —
see the `dag-deploy` skill.
