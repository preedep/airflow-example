# `dag_deadline_alert.py`

📄 **[Source: `dag_deadline_alert.py`](../dag_deadline_alert.py)**

`nix-dag-deadline-alert` — fires a callback when a DAG run takes too long,
**without failing it**.

```
run queued ──┬── 90s ──▶ deadline missed ──▶ alert callback
             └── task sleeps 150s ──▶ run still finishes normally
```

```bash
airflow dags trigger nix-dag-deadline-alert
```

No connections or Variables needed — the DAG only sleeps and logs. The task
overruns the deadline on purpose, so every manual trigger demonstrates it.

## Deadline alerts are not timeouts

This is the distinction the DAG exists to show. Airflow already had two ways to
*stop* slow work; a deadline alert is a third thing that **stops nothing**.

| Mechanism | On breach | Run outcome |
|---|---|---|
| `execution_timeout` | raises `AirflowTaskTimeout` in the task | task fails |
| `dagrun_timeout` | kills the run | run fails |
| **`deadline`** | calls your callback | **run continues untouched** |

Use a timeout to bound cost. Use a deadline to tell somebody the SLA is at risk
while the work carries on — the pipeline may still succeed, just late, and
killing it would often make things worse.

Compare [#18](dag_cyclic.md), which uses both timeouts and has to *infer* a
timeout from the exception type, because there is no timeout callback. A deadline
alert is the callback that never existed for that case.

## Anatomy

```python
DAG(
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=90),
        callback=AsyncCallback(CALLBACK_PATH, kwargs={"sla_name": "demo-90s"}),
    ),
)
```

| Piece | Meaning |
|---|---|
| `reference` | what the clock starts from |
| `interval` | how long after that reference the deadline falls |
| `callback` | what runs when the deadline passes with the run unfinished |

| Reference | Clock starts at |
|---|---|
| `DAGRUN_QUEUED_AT` | the run entering the queue |
| `DAGRUN_LOGICAL_DATE` | the run's logical date |
| `AVERAGE_RUNTIME(max_runs=N)` | the mean of the last N runs |
| `FIXED_DATETIME(dt)` | a literal timestamp |

`AVERAGE_RUNTIME` is the interesting one for real pipelines: it catches "much
slower than usual" without a fixed number that needs re-tuning as the workload
grows.

## The callback-import trap

This is the part worth reading before you use the feature, and it cost two failed
runs here to pin down. **There are two separate problems**, and fixing only the
first leaves the alert silently broken.

### 1. Passing the function object serializes a mangled name

```python
callback=AsyncCallback(on_deadline_missed)      # ← looks fine, fails at run time
```

The callable is stored as a **path string** derived from `__module__`, and inside
the dag-processor that is a hash-mangled name:

```
ModuleNotFoundError: No module named
'unusual_prefix_bc6401d69fc56ffddb73444a4c243dae33e95a2d_dag_deadline_alert'
```

Pass an explicit string instead:

```python
callback=AsyncCallback("dag_deadline_alert.on_deadline_missed")
```

### 2. The triggerer cannot import the DAG folder

With the honest path, the error becomes honest too — and still fails:

```
Failed to import the callable on the triggerer: dag_deadline_alert.on_deadline_missed
ModuleNotFoundError: No module named 'dag_deadline_alert'
```

The DAG folder is *mounted* on the triggerer but is **not on its `sys.path`**.
Verified both ways on a live cluster: with `PYTHONPATH` unset the import fails;
with `PYTHONPATH=<dag folder>` the same path resolves and the callable is
confirmed to be a coroutine.

So the deployment needs one of:

- **`PYTHONPATH=<dag folder>`** on the triggerer (for `AsyncCallback`) or the
  workers (for `SyncCallback`), or
- the callback moved into an **installed package** or the plugins folder — the
  better choice for anything beyond a demo.

### Why this is easy to miss

The deadline fires on time. The DAG run reports **`success`**. The task logs look
perfect. The only evidence is in the *triggerer* log, which nobody reads unless
they already suspect a problem.

An alert that never pages anybody is indistinguishable from an alert that was
never needed — so verify the callback actually ran the first time you wire one
up.

## `AsyncCallback` requires an actual `async def`

It checks at construction, so a plain function fails the DAG at **parse** time:

```
AttributeError: Provided callback <function ...> is not awaitable.
```

That is a helpful failure — caught by the parse check rather than at 3am when the
deadline first fires. Use `SyncCallback` for a non-async callable.

| | Runs on | Use for |
|---|---|---|
| `AsyncCallback` | the triggerer's event loop | quick, non-blocking work |
| `SyncCallback` | a worker, via an executor | anything blocking or heavy |

**An `AsyncCallback` must not block.** A `time.sleep` or a synchronous DB call
inside one stalls the triggerer for every DAG on the cluster, not just this one.

## Other constraints

- `context` is a **reserved** kwarg — passing it raises `ValueError`. Airflow
  supplies the context; `kwargs` is for your own extras.
- **A triggerer must be running** for `AsyncCallback` to fire at all. Without
  one the deadline is recorded and nothing happens.
- This DAG deliberately sets **no** `execution_timeout` or `dagrun_timeout` —
  either would kill the run before the deadline could demonstrate that it does
  not.

## What a successful demo looks like

The run succeeds, the downstream task runs normally, and the alert appears in the
triggerer log while the run is still in flight:

```
[deadline_alert] working for 150s — deadline is 90s after queueing
[deadline_alert][SLA-BREACH] nix-dag-deadline-alert run ... exceeded demo-90s (90s) — still running
[deadline_alert] finished — note the run still SUCCEEDS
[deadline_alert] downstream ran normally — the deadline did not stop anything
```

On this cluster the `SLA-BREACH` line does **not** appear, because `PYTHONPATH`
is unset on the triggerer — see the trap above. The deadline itself fires
correctly; only the callback import fails.

---

[← back to the DAG index](../README.md)
