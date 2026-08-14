# `dag_cyclic.py`


`nix-dag-cyclic` — a **cyclic job** in the Control-M sense: fires every 5 minutes,
with only one run ever active. The task sleeps 4 minutes to stand in for real work.

Unlike the others this one is **scheduled**, so just unpause it:

```bash
airflow dags unpause nix-dag-cyclic
```

**Pattern: `max_active_runs=1` is what makes a schedule cyclic.** Without it, a run
that overruns its interval executes concurrently with the next one. With it, the
next run waits.

```python
with DAG(
    schedule="*/5 * * * *",
    max_active_runs=1,                      # no two runs at once
    catchup=False,                          # a cycle, not a history
    dagrun_timeout=timedelta(minutes=9),    # a wedged run must not block the cycle
):
```

`dagrun_timeout` matters more than it looks: with `max_active_runs=1`, one hung run
blocks the cycle indefinitely. The timeout bounds that.

**Pattern: there is no timeout callback — a timeout arrives as a failure.** To log
one distinctly you have to identify it yourself, at two levels:

```python
def on_task_timeout_or_failure(context):
    if isinstance(context.get("exception"), AirflowTaskTimeout):
        log.error("[cyclic][TIMEOUT] %s exceeded execution_timeout", ...)
    else:
        log.error("[cyclic][FAILED] %s failed: %s", ...)
```

| Layer | Setting | Callback fires when |
|---|---|---|
| Task | `execution_timeout=6min` | one task overruns → raises `AirflowTaskTimeout` |
| DAG run | `dagrun_timeout=9min` | the whole run overruns |

Both are needed. A `dagrun_timeout` kills the run **without** failing an individual
task, so the task callback may never fire — the DAG-level `on_failure_callback` is
the backstop. It infers a timeout by comparing the run's duration against
`dagrun_timeout`, because the context carries no explicit "timed out" flag.

Import `AirflowTaskTimeout` from **`airflow.sdk.exceptions`**; the
`airflow.exceptions` path is deprecated in 3.x.

Grep logs for `[cyclic][TIMEOUT]` to find blocked cycles.

**Where Airflow differs from Control-M**, worth knowing before relying on it:

- Control-M measures the gap from when the previous run *ends*; Airflow's cron fires
  on wall-clock time regardless.
- If a run overruns, Airflow **queues** the missed interval rather than skipping it —
  so the next run may start immediately after, with no gap. `max_active_runs=1`
  prevents overlap, not catch-up.
- For "wait N minutes after the previous run finishes", use
  `schedule=timedelta(minutes=5)` instead. That measures from the previous run —
  closer to Control-M — but drifts relative to the clock.

Watch the grid view for ~15 minutes: runs should be strictly sequential. Triggering
manually while one is active shows the new run `queued` until the active one ends.

> Runs **288 times a day** while unpaused, holding a worker for 4 minutes each time.
> Pause it when you are done.

---

[← back to the DAG index](../README.md)
