---
name: dag-debug
description: Diagnose a failing, stuck, or missing Airflow DAG on the g1pro k3s cluster using the Airflow MCP server. Use when a DAG does not appear in the UI, a task is failing or retrying, a run is queued but not starting, or the user asks why a pipeline did not run.
---

# Debugging Airflow on g1pro

Use the **Airflow MCP server** (`http://nixhome-linux-g1pro:30700/mcp`) for
everything inside Airflow. Drop to `kubectl` only when the evidence points at
infrastructure — pod scheduling, OOM, node resources.

MCP tool inventory and usage notes: `.claude/references/g1pro.md` §9b.
Environment and troubleshooting table: `.claude/references/g1pro.md` §12.

Work top-down. Each step rules out the ones below it — do not read task logs before
confirming the DAG parsed.

## 1. Did it parse?

```
mcp__airflow__get_import_errors()
```

An entry here is the whole answer. Get the traceback with
`mcp__airflow__get_import_error(import_error_id=...)`, fix, re-rsync, re-check.

Most common causes on this cluster:

| Traceback | Cause |
|---|---|
| `ModuleNotFoundError` on a sibling module | demo DAGs must be standalone single files |
| `ModuleNotFoundError` on a 3rd-party lib | not in the Airflow image — cannot pip-install into a worker |
| `ImportError` from `airflow.operators.*` | 2.x path; use `airflow.providers.standard.*` |
| `ImportError` from `airflow.models import Variable` | use `airflow.sdk` |
| `TypeError: unexpected keyword 'schedule_interval'` | removed in 3.x, use `schedule` |

Reproduce locally with `.venv/bin/python dags/dag_<name>.py` — except for the
missing-package cases, which only reproduce on the server.

## 2. Does the scheduler know it?

```
mcp__airflow__fetch_dags(dag_id_pattern="<dag_id>")
mcp__airflow__get_dag_details(dag_id="<dag_id>")
```

Always filter — the cluster hosts ~106 DAGs including Airflow's bundled examples.

- **Missing, no import error** → the file never landed, or the processor has not
  parsed it. Confirm on disk:
  `ssh nickmsft@nixhome-linux-g1pro "ls -la /mnt/external-storage/airflow-dags/"`
  Then force a reparse: `mcp__airflow__reparse_dag_file(file_token=...)` — the token
  comes from `fetch_dags`, not a path.
- **Present but never runs** → check `is_paused`. New DAGs are paused by default and
  this is the single most common "my DAG isn't running".
- **`is_stale: true`** → the source file was removed from the DAG folder.

## 3. Did a run get created?

```
mcp__airflow__get_dag_runs(dag_id=..., limit=5)
```

No runs at all, DAG unpaused:
- `start_date` is in the future
- `catchup=False` and the first interval has not closed — a `0 */6 * * *` DAG
  deployed at 07:00 creates nothing until 12:00
- asset-scheduled with no upstream event — `mcp__airflow__get_dataset_events`

Runs stuck in `queued`:
- `max_active_runs` held by an earlier run that never finished
- pool exhausted — `mcp__airflow__get_pools`
- KubernetesExecutor cannot place the worker pod — this is the infrastructure case,
  go to §5

## 4. Which task failed, and why?

```
mcp__airflow__list_task_instances(dag_id=..., dag_run_id=...)
mcp__airflow__get_log(dag_id=..., dag_run_id=..., task_id=..., task_try_number=1)
```

`get_log` needs all four arguments. It works after the worker pod is gone — unlike
`kubectl logs`, which loses everything when the ephemeral pod exits.

**Read try 1 first.** Later tries often fail differently because of partial state the
first attempt left behind, and that difference is itself the finding: the task is not
idempotent. Compare with `mcp__airflow__list_task_instance_tries`.

| Symptom | Cause |
|---|---|
| `Negsignal.SIGKILL` | OOM — raise the pod memory request |
| `Invalid auth token: Signature verification failed` | JWT secret mismatch; components disagree on `airflow-secrets` |
| Stuck `running`, no log output | worker pod evicted — check `mcp__airflow__get_event_logs` |
| `up_for_retry` looping to failure | external dependency down, or non-idempotent task |
| `KeyError` on a Variable | Variable not set on the server (`mcp__airflow__list_variables`) |
| Works locally, fails on server | ARM vs x86_64 image, or a missing Connection/Variable |
| XCom value too large | returning a payload instead of a reference; write to storage, return the path |

## 5. Infrastructure — kubectl

Only once §1–§4 point outside Airflow.

```bash
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"

kubectl -n airflow get pods                              # component health
kubectl -n airflow describe pod <worker-pod>             # scheduling failures, OOMKilled
kubectl -n airflow logs -f deploy/airflow-dag-processor  # parser detail
kubectl -n airflow logs -f deploy/airflow-scheduler      # scheduling issues
kubectl -n airflow get pods -w                           # watch workers appear
```

If `kubectl` or `ssh` times out, Tailscale is down on the Mac mini — restart it rather
than working around it.

## 6. Re-run

```
mcp__airflow__clear_task_instances(...)      # re-runs the task and its downstream
mcp__airflow__set_task_instances_state(...)  # mark state without executing
```

Clearing executes real work against real systems on a shared cluster. Confirm with
the user before clearing anything with external side effects, and prefer clearing one
task over a whole run.

## Reporting

Say what actually failed and quote the relevant log line. When the root cause is
infrastructure — OOM, evicted pod, a missing Variable, a package absent from the
image — say so plainly rather than reshaping the DAG to work around it.
