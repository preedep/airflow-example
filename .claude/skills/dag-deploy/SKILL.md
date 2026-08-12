---
name: dag-deploy
description: Deploy DAG files from this repo to the shared DAG folder on g1pro via rsync and verify they parsed. Use when the user asks to deploy, push, ship, or sync a DAG to the server, or to check whether a DAG landed.
---

# Deploying a DAG to g1pro

Deployment is **rsync to `/mnt/external-storage/airflow-dags/` on g1pro**. That path
is a k8s hostPath mounted into every Airflow component at `/opt/airflow/dags/`, mode
777, so `nickmsft` writes to it directly. The dag-processor picks up changes
automatically — no pod restart, no redeploy, no sudo.

Git is version control only. Committing deploys nothing.

**Two interfaces, two jobs:** files move with `rsync` (MCP cannot write files);
everything after that — verify, unpause, trigger, inspect — goes through the
**Airflow MCP server**, not `kubectl exec`. Reach for `kubectl` only when the problem
is infrastructure rather than Airflow.

Environment detail: `.claude/references/g1pro.md` — §5 for deployment, §9b for the
full MCP tool inventory and usage notes.

## Preflight

```bash
ssh nickmsft@nixhome-linux-g1pro true && echo OK          # Tailscale up?
.venv/bin/python dags/<project>/dag_<name>.py             # parse — exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

The venv is pinned to Python 3.13 + Airflow 3.2.1 to match the server image.

If SSH fails, Tailscale is down — start it and retry rather than working around it.

## Deploy

```bash
rsync -av --exclude='__pycache__' --exclude='*.pyc' \
  ./dags/<project>/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/<project>/
```

Sync the whole subfolder, not individual files — `dag_utils.py` and `__init__.py`
must land alongside the DAG. Always exclude `__pycache__`; stale ARM-built `.pyc`
files in the shared folder cause confusing parse behavior.

Note the trailing slashes on both paths — without them rsync nests the directory.

The shared folder holds other people's DAGs. Only ever write inside your own project
subfolder; never rsync to the DAG root and never use `--delete`.

## Verify — via MCP

```bash
# File landed
ssh nickmsft@nixhome-linux-g1pro "ls -la /mnt/external-storage/airflow-dags/<project>/"
```

Then, in order:

1. `mcp__airflow__get_import_errors` — **check this first**; an entry here invalidates
   everything below. Clean looks like `{'import_errors': [], 'total_entries': 0.0}`.
2. `mcp__airflow__fetch_dags(dag_id_pattern="<dag_id>")` — registered? Also shows
   `is_paused` and `last_parsed_time`. Always pass a filter: the cluster hosts ~106
   DAGs including Airflow's bundled examples.
3. `mcp__airflow__get_dag_details(dag_id=...)` — schedule, tags, owners as intended.

Step 1 is the one that matters. A local parse passes on ARM against locally installed
packages; the server image may not have the import. This is the authoritative check.

If the DAG is absent with no import error, the processor has not picked it up yet.
Force a reparse with `mcp__airflow__reparse_dag_file(file_token=...)` — note it takes
the **`file_token`** from a `fetch_dags` result, not a path.

## Enable and run — via MCP

New DAGs land **paused**.

```
mcp__airflow__unpause_dag(dag_id="nix-dag-mydemo")
mcp__airflow__post_dag_run(dag_id="nix-dag-mydemo")
mcp__airflow__get_dag_runs(dag_id="nix-dag-mydemo", limit=1)
```

Confirm with the user before either of these: unpausing a DAG with `catchup=True` and
a past start_date (it immediately queues one run per missed interval), and triggering
a DAG that writes to external systems (real work, shared cluster).

## Watch the run

```
mcp__airflow__list_task_instances(dag_id=..., dag_run_id=...)
mcp__airflow__get_log(dag_id=..., dag_run_id=..., task_id=..., task_try_number=1)
```

`get_log` works after the worker pod is gone — prefer it over `kubectl logs`, since
KubernetesExecutor pods are ephemeral and vanish with their logs when the task ends.

To watch pods appear in real time (infrastructure view only):

```bash
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"
kubectl -n airflow get pods -w
```

## Rollback

Re-rsync the previous version of the folder, or delete the file on the server:

```bash
ssh nickmsft@nixhome-linux-g1pro "rm /mnt/external-storage/airflow-dags/<project>/dag_<name>.py"
```

Pause the DAG before removing its file if runs may be in flight. Deleting the file
removes the DAG from the UI but leaves its run history in the metadata DB.

## Quick reference

```bash
# 1. Ship the files (rsync only — MCP cannot write files)
rsync -av --exclude='__pycache__' ./dags/mydemo/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/mydemo/
```

```
# 2. Verify, enable, run (MCP)
mcp__airflow__get_import_errors()
mcp__airflow__fetch_dags(dag_id_pattern="nix-dag-mydemo")
mcp__airflow__unpause_dag(dag_id="nix-dag-mydemo")
mcp__airflow__post_dag_run(dag_id="nix-dag-mydemo")
```

CLI fallback, only if the MCP server is unreachable:

```bash
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags list-import-errors
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags unpause nix-dag-mydemo
```

UI: `http://nixhome-linux-g1pro:30080` · MCP: `http://nixhome-linux-g1pro:30700/mcp`
