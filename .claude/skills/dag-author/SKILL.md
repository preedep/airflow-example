---
name: dag-author
description: Write a new Airflow 3 DAG or modify an existing one in this repo, using the self-contained subfolder + dag_utils pattern verified on the g1pro cluster. Use when the user asks to create a DAG, add a pipeline, add tasks, or port an Airflow 2 DAG.
---

# Authoring a DAG

Target is Airflow 3.2.1 on KubernetesExecutor. The pattern below mirrors
`investment/dag_nix_dag_fx_alert.py`, which runs successfully on this cluster today —
follow it rather than generic Airflow examples from the web, which are mostly 2.x.

Read `.claude/references/g1pro.md` §6–§8 for the environment rules behind this.

## 1. Create the subfolder

Every DAG project is a self-contained folder under `dags/`:

```
dags/mydemo/
  __init__.py          empty file — required
  dag_utils.py         task callables + default_args
  dag_my_pipeline.py   the DAG
```

Do not put a bare `.py` at the top of `dags/`, and do not try to share a helper module
across subfolders — they are not packages on `sys.path`.

## 2. Write `dag_utils.py`

Task logic lives here as plain functions. This is what makes it testable without
Airflow, and it keeps the DAG file to structure only.

```python
"""Shared helpers for mydemo DAGs."""

import logging

log = logging.getLogger(__name__)


def default_args(owner="nix", retries=1, retry_delay_sec=30, **kwargs):
    args = {"owner": owner, "retries": retries, "retry_delay_sec": retry_delay_sec}
    args.update(kwargs)
    return args


def fetch_data(endpoint, **context):
    import requests                                    # import inside the callable

    task_log = logging.getLogger("airflow.task")
    task_log.info("[mydemo] fetching %s", endpoint)

    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()                        # raise → task fails visibly
    return response.json()                             # return value → XCom


def process_data(ti=None, **context):
    task_log = logging.getLogger("airflow.task")
    data = ti.xcom_pull(task_ids="fetch_data")
    task_log.info("[mydemo] processing: %s", data)
    return {"status": "ok"}
```

Conventions: import heavy modules inside the function; log via
`logging.getLogger("airflow.task")` with a `[<project>]` prefix; always set a `timeout`
on network calls; raise rather than return an error.

## 3. Write the DAG file

```python
"""
nix-dag-mydemo — example pipeline.
Lives at: /opt/airflow/dags/mydemo/
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_DAGS_DIR = Path(__file__).parent.resolve()
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from dag_utils import default_args, fetch_data, process_data

with DAG(
    dag_id="nix-dag-mydemo",
    description="Example demo pipeline",
    schedule="0 */6 * * *",          # None = manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "mydemo"],
    default_args=default_args(owner="nix"),
) as dag:
    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        op_kwargs={"endpoint": "https://api.example.com/v1/data"},
    )

    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    fetch >> process
```

The `_DAGS_DIR` block is mandatory. Without it the dag-processor raises
`ModuleNotFoundError: No module named 'dag_utils'`.

## 4. Rules that break naive code

**Import paths (3.x).**
```python
from airflow.providers.standard.operators.python import PythonOperator   # ✅
from airflow.providers.standard.operators.bash import BashOperator       # ✅
from airflow.sdk import Variable                                         # ✅
from airflow.operators.python import PythonOperator                      # ❌ 2.x
from airflow.models import Variable                                      # ❌ fails in worker pods
```

**`schedule=`, not `schedule_interval=`.** Removed in 3.x.

**`catchup=False`.** A past `start_date` with catchup on floods the cluster with
backfill runs on first unpause.

**Package availability.** Only these providers are in the image: `standard`,
`cncf-kubernetes`, `postgres`, `common-sql`, `common-io`, `http`, `smtp`, `amazon`,
`microsoft-azure`, `databricks` — plus `requests`. Anything else needs an image
rebuild and cannot be pip-installed into a worker. Check before importing.

**In-cluster addresses.** Tasks run inside k3s. Use
`postgres.postgres.svc.cluster.local:5432`, not `nixhome-linux-g1pro:30080`.

**Secrets.** `Variable.get("name")` inside the callable, never at module scope and
never hardcoded. Set them per `.claude/references/g1pro.md` §9.

**Idempotency.** A retry must converge, not accumulate. Delete-then-insert by
partition rather than blind append.

## 5. Heavy or non-Python work

This is a KubernetesExecutor cluster — use `KubernetesPodOperator`
(`airflow.providers.cncf.kubernetes.operators.pod`) rather than fattening the worker.
Pin the image to an **x86_64** tag, never `:latest`, and set memory limits — an
unbounded pod OOM-kills as `Negsignal.SIGKILL`.

## 6. Validate before deploying

`.venv` is pinned to Python 3.13 + Airflow 3.2.1 to match the server image.

```bash
.venv/bin/python dags/mydemo/dag_my_pipeline.py   # exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

`tests/test_dag_integrity.py` picks up new DAGs automatically — it globs
`dags/*/dag_*.py`, so there is no per-DAG test to add. It enforces the rules in §4
plus the `__init__.py` and `sys.path` requirements.

A clean local parse is necessary but not sufficient: the venv carries only the
common providers, so it cannot prove a third-party import exists in the server
image. The authoritative check is `mcp__airflow__get_import_errors` after deploy —
see the `dag-deploy` skill.
