"""nix-dag-cyclic — Control-M style cyclic job: runs every 5 minutes, never overlapping."""

import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowTaskTimeout

DAG_DOC_MD = """
### nix-dag-cyclic

A **cyclic job** in the Control-M sense: it runs on a fixed interval and a single
instance is ever active. The work here is a `sleep` standing in for a real task.

```
every 5 min  ──▶  simulate_work (4 min)  ──▶  report
                  └─ next run waits if this one is still going
```

#### Settings

| Setting | Value | Why |
|---|---|---|
| `schedule` | `*/5 * * * *` | fire every 5 minutes |
| `max_active_runs` | `1` | **the cyclic guarantee** — no two runs at once |
| `catchup` | `False` | do not backfill every interval since `start_date` |
| `dagrun_timeout` | 9 min | kill a wedged run instead of blocking the cycle forever |
| task duration | 4 min | leaves ~1 min headroom inside the 5 min window |

`max_active_runs=1` is what makes this cyclic rather than merely scheduled. Without
it, a run that overruns its interval would execute concurrently with the next one.

#### How this differs from Control-M cyclic

Worth understanding before relying on it:

- **Control-M** measures the interval from when the previous run *ends*. Airflow's
  cron fires on wall-clock time regardless of what the last run did.
- If a run overruns, Airflow **queues** the next interval rather than skipping it.
  Once the slow run finishes, the queued run starts immediately — so you can see
  two runs back to back with no 5-minute gap. `max_active_runs=1` prevents overlap,
  not catch-up.
- To *skip* missed intervals instead of queueing them, add
  `max_consecutive_failed_dag_runs` or a short `dagrun_timeout` (used here), or
  gate the work behind a task that exits early when it is already too late to be
  useful.
- For a true "wait N minutes after the previous run finishes", use
  `schedule=timedelta(minutes=5)` — that measures from the previous run, closer to
  Control-M's model, but it drifts relative to the clock.

#### Timeout handling

**Airflow has no dedicated timeout callback.** A timeout arrives as a *failure*, so
the callbacks below have to tell the two apart:

| Layer | Setting | Callback | Fires when |
|---|---|---|---|
| Task | `execution_timeout=6min` | `on_task_timeout_or_failure` | one task overruns |
| DAG run | `dagrun_timeout=9min` | `on_dagrun_failure` | the whole run overruns |

The task callback checks `isinstance(context["exception"], AirflowTaskTimeout)` and
logs `[cyclic][TIMEOUT]` rather than `[cyclic][FAILED]`.

Both layers are needed. A `dagrun_timeout` kills the run **without** failing an
individual task, so the task callback may never fire — the DAG-level callback is the
backstop. It infers a timeout by comparing the run's duration against
`dagrun_timeout`, since the context carries no explicit "timed out" flag.

Grep the logs for `[cyclic][TIMEOUT]` to find blocked cycles.

#### Why the sleep is chunked

`simulate_work` sleeps in 30-second increments and logs progress, rather than one
`time.sleep(240)`. A single long sleep looks indistinguishable from a hung task in
the UI, and nothing is emitted if it is killed partway.

#### Watch it behave

Unpause and let it run for ~15 minutes, then look at the grid view: runs should be
strictly sequential, never side by side. Triggering manually while a run is active
shows the new run sitting in `queued` until the active one finishes.

#### Cost note

This DAG runs **288 times a day** while unpaused and holds a worker pod for 4
minutes each time. Pause it when you are done demonstrating it.
"""

SLEEP_SECONDS = 4 * 60
CHUNK_SECONDS = 30

# Task-level cap. Lower than dagrun_timeout so a single slow task is caught here,
# with a clearer message than a whole-run timeout gives.
TASK_TIMEOUT = timedelta(minutes=6)


def on_task_timeout_or_failure(context):
    """Task `on_failure_callback` — distinguishes a timeout from an ordinary failure.

    Airflow has no dedicated timeout callback: a timeout arrives here as a failure
    whose exception is AirflowTaskTimeout. Anything else is a normal error.
    """
    task_log = logging.getLogger("airflow.task")

    ti = context["task_instance"]
    exc = context.get("exception")
    timed_out = isinstance(exc, AirflowTaskTimeout)

    if timed_out:
        task_log.error(
            "[cyclic][TIMEOUT] task %s exceeded execution_timeout=%s "
            "(run_id=%s, try %s) — the cycle is running slower than its window",
            ti.task_id,
            TASK_TIMEOUT,
            ti.run_id,
            ti.try_number,
        )
    else:
        task_log.error(
            "[cyclic][FAILED] task %s failed: %s (run_id=%s, try %s)",
            ti.task_id,
            exc,
            ti.run_id,
            ti.try_number,
        )


def on_dagrun_failure(context):
    """DAG `on_failure_callback` — fires when the run fails, including dagrun_timeout.

    A dagrun_timeout kills the run without failing an individual task, so the
    task-level callback above may never fire. This is the backstop that logs it.
    """
    log = logging.getLogger("airflow.task")

    dag_run = context["dag_run"]
    started = dag_run.start_date
    ran_for = (dag_run.end_date - started).total_seconds() if started and dag_run.end_date else None

    log.error(
        "[cyclic][DAGRUN FAILED] run_id=%s state=%s ran_for=%ss dagrun_timeout=%s",
        dag_run.run_id,
        dag_run.state,
        round(ran_for) if ran_for is not None else "unknown",
        context["dag"].dagrun_timeout,
    )

    # A run that lasted about as long as the timeout was almost certainly killed by
    # it, rather than failing on its own.
    timeout_s = context["dag"].dagrun_timeout.total_seconds()
    if ran_for is not None and ran_for >= timeout_s - 30:
        log.error(
            "[cyclic][TIMEOUT] run_id=%s hit dagrun_timeout after %ss — "
            "next cycle was blocked until this run was killed",
            dag_run.run_id,
            round(ran_for),
        )


def simulate_work(**context):
    """Stand-in for real work: sleep, logging progress so the task is not opaque."""
    task_log = logging.getLogger("airflow.task")

    run_id = context["dag_run"].run_id
    started = time.monotonic()

    task_log.info("[cyclic] start run_id=%s, simulating %ss of work", run_id, SLEEP_SECONDS)

    elapsed = 0
    while elapsed < SLEEP_SECONDS:
        time.sleep(min(CHUNK_SECONDS, SLEEP_SECONDS - elapsed))
        elapsed = int(time.monotonic() - started)
        task_log.info("[cyclic] working... %ss / %ss", elapsed, SLEEP_SECONDS)

    task_log.info("[cyclic] finished after %ss", elapsed)
    return {"run_id": run_id, "duration_seconds": elapsed}


def report_cycle(ti=None, **context):
    """Log how this run sat relative to its schedule — the interesting part of a cycle."""
    task_log = logging.getLogger("airflow.task")

    result = ti.xcom_pull(task_ids="simulate_work")
    dag_run = context["dag_run"]

    # logical_date is None for manually triggered runs in Airflow 3.
    scheduled_for = dag_run.logical_date or dag_run.run_after
    lag = (dag_run.start_date - scheduled_for).total_seconds() if dag_run.start_date else None

    task_log.info(
        "[cyclic] run_id=%s scheduled=%s started_late_by=%ss work=%ss",
        result["run_id"],
        scheduled_for,
        round(lag) if lag is not None else "unknown",
        result["duration_seconds"],
    )

    # A lag well over the 5 min interval means the previous run was still going and
    # this one queued behind it — the cyclic guarantee doing its job.
    return {"scheduled_for": str(scheduled_for), "start_lag_seconds": lag}


with DAG(
    dag_id="nix-dag-cyclic",
    description="Control-M style cyclic job — every 5 minutes, one run at a time",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,  # never backfill: this is a cycle, not a history
    max_active_runs=1,  # the cyclic guarantee — no overlapping runs
    dagrun_timeout=timedelta(minutes=9),  # a wedged run must not block the cycle
    on_failure_callback=on_dagrun_failure,  # fires on dagrun_timeout too
    tags=["demo", "cyclic", "scheduling"],
    default_args={
        "owner": "nix",
        "retries": 0,  # a retry would push the run past its window
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": on_task_timeout_or_failure,
    },
    doc_md=DAG_DOC_MD,
) as dag:
    work = PythonOperator(
        task_id="simulate_work",
        python_callable=simulate_work,
        execution_timeout=TASK_TIMEOUT,
        doc_md="""
Sleeps for 4 minutes to simulate a long-running job, logging every 30 seconds so
the task is visibly progressing rather than looking hung.

Deliberately longer than half the 5-minute interval: it makes overlap possible,
which is what `max_active_runs=1` is there to prevent.

Capped by `execution_timeout=6min`. On breach the task raises
`AirflowTaskTimeout`, and `on_task_timeout_or_failure` logs it as a **TIMEOUT**
rather than a generic failure.
""",
    )

    report = PythonOperator(
        task_id="report_cycle",
        python_callable=report_cycle,
        doc_md="""
Logs how late the run started relative to its scheduled time.

A lag much larger than the interval means this run queued behind the previous
one — visible evidence that `max_active_runs=1` serialised the cycle.
""",
    )

    work >> report
