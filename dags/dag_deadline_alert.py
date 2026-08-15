"""nix-dag-deadline-alert — fire a callback when a DAG run misses its deadline."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
)

DAG_DOC_MD = """
### nix-dag-deadline-alert

Fires a callback when a DAG run **takes too long**, without failing it.

```
run queued ──┬── 90s ──▶ deadline missed ──▶ alert callback
             └── task sleeps 150s ──▶ run still finishes normally
```

The task sleeps past the deadline on purpose, so a manual trigger demonstrates
the alert every time.

#### Deadline alerts are not timeouts

This is the distinction the DAG exists to show. Airflow already had two ways to
stop slow work; a deadline alert is a third thing that **stops nothing**.

| Mechanism | On breach | Run outcome |
|---|---|---|
| `execution_timeout` | raises `AirflowTaskTimeout` in the task | task fails |
| `dagrun_timeout` | kills the run | run fails |
| **`deadline`** | calls your callback | **run continues untouched** |

Use a timeout to bound cost. Use a deadline to tell somebody the SLA is at risk
while the work carries on — the pipeline may still succeed, just late, and
killing it would make things worse rather than better.

Compare `nix-dag-cyclic`, which uses both timeouts and has to *infer* a timeout
from the exception type because there is no timeout callback. A deadline alert
is the callback that never existed for that case.

#### Anatomy

```python
DAG(
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=90),
        callback=AsyncCallback(on_deadline_missed, kwargs={"sla_name": "demo-90s"}),
    ),
)
```

| Piece | Meaning |
|---|---|
| `reference` | what the clock starts from |
| `interval` | how long after that reference the deadline falls |
| `callback` | what runs when the deadline passes with the run unfinished |

#### Choosing a reference

| Reference | Clock starts at |
|---|---|
| `DAGRUN_QUEUED_AT` | the run entering the queue |
| `DAGRUN_LOGICAL_DATE` | the run's logical date |
| `AVERAGE_RUNTIME(max_runs=N)` | the mean of the last N runs |
| `FIXED_DATETIME(dt)` | a literal timestamp |

Pick `DAGRUN_QUEUED_AT` when wall-clock from submission is what matters — queue
time included. Pick `DAGRUN_LOGICAL_DATE` when the SLA is "done by 06:00",
whenever the run actually started. `AVERAGE_RUNTIME` catches "slower than usual"
with no fixed number to choose, and `FIXED_DATETIME` covers a one-off cutoff.

This DAG uses `DAGRUN_QUEUED_AT`, which is the honest choice for "the run is
taking too long" — a run stuck in the queue is late for the person waiting on it,
even though it has not started.

`AVERAGE_RUNTIME` is the interesting one for real pipelines: it adapts as the
workload grows, so the alert does not need re-tuning every quarter.

#### Sync vs async callbacks

| | Runs on | Use for |
|---|---|---|
| `AsyncCallback` | the triggerer's event loop | quick, non-blocking work — a POST, a log line |
| `SyncCallback` | a worker, via an executor | anything blocking or heavy |

`AsyncCallback` is used here because the callback only logs. **It must not
block** — a `time.sleep` or a synchronous database call inside an async callback
stalls the triggerer for every DAG on the cluster, not just this one. Reach for
`SyncCallback` the moment the work is more than trivial.

**`AsyncCallback` requires an actual `async def`.** It checks at construction
time, so a plain function fails the DAG at parse:

```
AttributeError: Provided callback <function ...> is not awaitable.
```

That is a helpful failure — it happens in the parse check rather than at 3am when
the deadline first fires.

#### The callback must be importable *by the triggerer* — the real trap

This is the part that bites, and it is not obvious from the API.

The callable is stored as a **path string**, not a closure. Passing the function
object looks natural:

```python
callback=AsyncCallback(on_deadline_missed)          # ← fails at run time
```

but Airflow derives the path from the function's `__module__`, and inside the
dag-processor that is a **hash-mangled name**. The triggerer then tries to import
it and cannot:

```
ModuleNotFoundError: No module named
'unusual_prefix_bc6401d69fc56ffddb73444a4c243dae33e95a2d_dag_deadline_alert'
```

The deadline fires correctly; only the callback fails, and it fails in the
*triggerer* log rather than anywhere near the DAG run — which reports `success`.
Nothing in the UI suggests the alert never ran.

So pass an explicit string:

```python
callback=AsyncCallback("dag_deadline_alert.on_deadline_missed")
```

**The string form is necessary but not sufficient.** The DAG folder is mounted
on the triggerer but is *not* on its `sys.path`, so the import still fails —
with the honest path this time:

```
Failed to import the callable on the triggerer: dag_deadline_alert.on_deadline_missed
ModuleNotFoundError: No module named 'dag_deadline_alert'
```

Both halves were verified on a live cluster. With `PYTHONPATH` unset the import
fails; with `PYTHONPATH=<dag folder>` the same path resolves and the callable is
confirmed a coroutine. So the deployment needs one of:

- **`PYTHONPATH=<dag folder>`** on the triggerer (for `AsyncCallback`) or on the
  workers (for `SyncCallback`), or
- the callback moved into an **installed package** or the plugins folder, which
  is the more robust choice for anything beyond a demo.

**Why this is worth knowing:** the deadline fires on time and the DAG run reports
`success`. The only evidence of failure is in the *triggerer* log. An alert that
never pages anybody looks exactly like an alert that was never needed.

`context` is a **reserved** kwarg — passing it raises `ValueError`. Airflow
supplies the context itself; `kwargs` is for your own extras.

#### Requires

Nothing. No connections, no Variables — the DAG only sleeps and logs.

**A triggerer must be running** for `AsyncCallback` to fire. Without one the
deadline is recorded and the callback never executes, which looks like the
feature silently not working.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# The run breaches the deadline on purpose: the task sleeps well past it, so a
# manual trigger always demonstrates the alert.
DEADLINE_SECONDS = 90
TASK_SLEEP_SECONDS = 150

# Import path the *triggerer* uses to find the callback. It must be resolvable
# there, which is a stricter requirement than "defined in this file" — see the
# doc_md. The DAG folder is mounted on the triggerer but is not on its
# sys.path, so this only works with the sitecustomize/PYTHONPATH note below.
CALLBACK_PATH = "dag_deadline_alert.on_deadline_missed"


# --------------------------------------------------------------------------- #
# Deadline callback
# --------------------------------------------------------------------------- #


async def on_deadline_missed(context, sla_name: str = "unnamed") -> None:
    """Report a missed deadline. Runs on the triggerer, so it must not block.

    `async def` is not optional: `AsyncCallback` calls `inspect.iscoroutinefunction`
    at construction and raises `AttributeError: Provided callback ... is not
    awaitable.` for a plain `def`. Use `SyncCallback` for a non-async callable.

    Module-level on purpose: the callback is serialized as an import path, so a
    nested function or lambda would not survive. `context` is supplied by
    Airflow and is a reserved kwarg — everything else comes from the
    `AsyncCallback(kwargs=...)` dict.
    """
    task_log = logging.getLogger("airflow.task")

    # dag_run may be absent depending on how the callback is invoked, so this
    # reads defensively rather than assuming the full task context.
    dag_run = context.get("dag_run") if hasattr(context, "get") else None
    run_id = getattr(dag_run, "run_id", "<unknown>")
    dag_id = getattr(dag_run, "dag_id", "<unknown>")

    # A real implementation would page here — Slack, PagerDuty, an HTTP POST.
    # Keep it non-blocking: this runs inside the triggerer's event loop.
    task_log.warning(
        "[deadline_alert][SLA-BREACH] %s run %s exceeded %s (%ss) — still running",
        dag_id,
        run_id,
        sla_name,
        DEADLINE_SECONDS,
    )


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def report_outcome(**context):
    """Runs after the slow task, showing the run was never interrupted."""
    task_log = logging.getLogger("airflow.task")
    task_log.info("[deadline_alert] downstream ran normally — the deadline did not stop anything")
    return {"status": "completed despite missing the deadline"}


def slow_work(**context):
    """Stand in for real work that overruns the deadline."""
    import time

    task_log = logging.getLogger("airflow.task")
    task_log.info(
        "[deadline_alert] working for %ss — deadline is %ss after queueing",
        TASK_SLEEP_SECONDS,
        DEADLINE_SECONDS,
    )

    # The sleep is the whole point: it guarantees the deadline passes while the
    # run is still going, which is what triggers the callback.
    time.sleep(TASK_SLEEP_SECONDS)

    task_log.info("[deadline_alert] finished — note the run still SUCCEEDS")
    return {"slept": TASK_SLEEP_SECONDS, "deadline": DEADLINE_SECONDS}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-deadline-alert",
    description="Fire a callback when a run misses its deadline, without failing it",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "deadline", "alert", "sla"],
    default_args={"owner": "nix", "retries": 0},
    # No execution_timeout or dagrun_timeout here, deliberately: adding one
    # would kill the run before the deadline could demonstrate that it does not.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=DEADLINE_SECONDS),
        # A STRING path, not the function object. Passing `on_deadline_missed`
        # directly serializes the DAG file's hash-mangled processor module name
        # (unusual_prefix_<sha>_dag_deadline_alert), which does not exist as an
        # importable module anywhere — see the doc_md section on this.
        callback=AsyncCallback(
            CALLBACK_PATH,
            kwargs={"sla_name": f"demo-{DEADLINE_SECONDS}s"},
        ),
    ),
    doc_md=DAG_DOC_MD,
) as dag:
    work = PythonOperator(
        task_id="slow_work",
        python_callable=slow_work,
        doc_md=f"""
Sleeps {TASK_SLEEP_SECONDS}s, comfortably past the {DEADLINE_SECONDS}s deadline,
so the alert fires on every manual trigger.

**Watch what does *not* happen:** the deadline passes, the callback logs
`[deadline_alert][SLA-BREACH]`, and this task keeps running to completion. The
run ends `success`.

That is the difference from `execution_timeout`, which would raise
`AirflowTaskTimeout` and fail the task instead.
""",
    )

    report = PythonOperator(
        task_id="report",
        python_callable=report_outcome,
        doc_md="""
Runs normally after the slow task, demonstrating that a missed deadline does not
interrupt the pipeline — downstream work proceeds as usual.

Grep the triggerer logs for `[deadline_alert][SLA-BREACH]` to see the alert that
fired while this run was still in flight.
""",
    )

    work >> report
