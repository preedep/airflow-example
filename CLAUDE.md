# airflow-example

Demo repository for Apache Airflow 3.x DAG authoring. DAGs are developed on the Mac
mini and deployed by **rsync to the shared DAG folder on `g1pro`** — there is no
git-sync and no CI. Pushing to this repo does not deploy anything.

## Target Environment

| Item | Value |
|---|---|
| Airflow | 3.2.1, KubernetesExecutor, k3s namespace `airflow` |
| UI | `http://nixhome-linux-g1pro:30080` (admin) |
| REST API | `http://nixhome-linux-g1pro:30080/api/v2` |
| Airflow MCP | `http://nixhome-linux-g1pro:30700/mcp` |
| DAG folder (server) | `/mnt/external-storage/airflow-dags/` — hostPath, mode 777 |
| DAG folder (in-pod) | `/opt/airflow/dags/` |
| SSH | `nickmsft@nixhome-linux-g1pro`, key auth, **no password** |
| kubeconfig | `export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"` |

Full environment detail — hosts, storage, other services, credentials, troubleshooting
table — is in `.claude/references/g1pro.md`. **Read it before touching deployment.**

**Use the Airflow MCP server to inspect and control Airflow** — import errors, DAG
state, runs, logs, triggering, Variables. It returns structured data and needs no
kubeconfig. `kubectl exec` into the webserver is a fallback, not the default. Full
tool inventory, usage notes, and known issues: `.claude/references/g1pro.md` §9b.
MCP cannot write files, so deployment itself is still `rsync`.

Two gotchas worth knowing up front: always filter `fetch_dags` (the cluster has ~106
DAGs, mostly bundled examples), and `get_health` 404s on 3.2.1 — use `get_version` as
the liveness check.

Requires Tailscale up. `sudo` on g1pro needs an interactive password; DAG deployment
never needs sudo.

## Repository Layout

Mirrors the server's DAG folder so `rsync` is a straight copy.

```
dags/
  <project>/            self-contained subfolder — one per DAG group
    __init__.py         empty marker, required
    dag_utils.py        task callables + default_args, LOCAL to this folder
    dag_<name>.py       the DAG definition
tests/                  parse and integrity tests (run locally)
.claude/
  skills/               dag-author, dag-deploy, dag-debug
  commands/             slash commands
  references/           g1pro.md and other deep docs
```

`dag_utils.py` is **not shared across subfolders**. Two projects may each define their
own with different contents. Do not create a common top-level helper module — the
subfolders are not packages on `sys.path`.

## DAG Conventions

- One self-contained subfolder per project, always with an empty `__init__.py`.
- `dag_id` uses the `nix-dag-<name>` convention, e.g. `nix-dag-fx-alert`.
- File named `dag_<snake_name>.py`.
- **Classic style** — `with DAG(...)` plus operators. Not the `@dag` decorator; the
  verified working DAGs on this cluster use the context-manager form.
- Task logic lives in `dag_utils.py` as plain callables and is wired in with
  `PythonOperator`. Keeps the logic testable without Airflow.
- Every DAG sets `dag_id`, `description`, `schedule`, `start_date`, `catchup=False`,
  `tags`, and `default_args`.
- `start_date` is a static `datetime(...)` — never `datetime.now()` or `days_ago`.
- No secrets in code. Use Airflow Variables, read inside the callable.

### Required sys.path preamble

Subfolders are not on `sys.path`, so a DAG file must insert its own directory before
importing its siblings. Omitting this is the most common failure — it raises
`ModuleNotFoundError: No module named 'dag_utils'` in the dag-processor.

```python
import sys
from pathlib import Path

_DAGS_DIR = Path(__file__).parent.resolve()
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from dag_utils import default_args, my_callable
```

## Airflow 3.x Rules

These parse fine and fail at runtime — get them right:

```python
from airflow.sdk import Variable                                       # ✅
from airflow.models import Variable                                    # ❌ fails in worker pods

from airflow.providers.standard.operators.python import PythonOperator # ✅ 3.x
from airflow.operators.python import PythonOperator                    # ❌ 2.x path

schedule="0 */6 * * *"                                                 # ✅
schedule_interval="@daily"                                             # ❌ removed
```

- `catchup=False` always, unless a backfill is genuinely intended and confirmed.
- Tasks run in **ephemeral worker pods** talking to the execution API server, not the
  metadata DB. No direct DB access from task code.
- **Imports go inside the callable**, not at module scope — the DAG file is re-parsed
  every processor cycle, and a top-level `import requests` costs every cycle.
- Nothing at module scope may do I/O: no `Variable.get`, no HTTP, no DB queries.

### Available packages

Verified on the server image 2026-08-12 (`pip list` in the webserver pod) — this is
broader than `g1pro.md` §8 claims:

`standard` · `cncf-kubernetes` · `postgres` · `mysql` · `common-sql` · `common-io` ·
`common-compat` · `common-messaging` · `http` · `smtp` · `ssh` · `sftp` · `ftp` ·
`amazon` · `google` · `microsoft-azure` · `microsoft-psrp` · `databricks` ·
`snowflake` · `elasticsearch` · `redis` · `celery` · `docker` · `git` · `grpc` ·
`hashicorp` · `odbc` · `openlineage` · `slack` · `fab`

Plus `requests` 2.33.1 and `pendulum` 3.2.0.

Anything outside this list requires **rebuilding the Airflow image** — it cannot be
pip-installed into a running worker. Check before writing an import; re-verify with
`kubectl -n airflow exec deploy/airflow-webserver -- python -m pip list` if unsure.

### In-cluster addressing

A task runs inside the cluster, so it must use in-cluster service addresses, not the
NodePort URLs used from the Mac mini:

```python
LITELLM = "http://litellm.ai-gateway.svc.cluster.local:4000/v1"
PG      = "postgres.postgres.svc.cluster.local:5432"
```

## Deploy

```bash
rsync -av --exclude='__pycache__' --exclude='*.pyc' ./dags/<project>/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/<project>/
```

The dag-processor picks up changes automatically — no pod restart, no sudo. New DAGs
land **paused**. Then verify with `mcp__airflow__get_import_errors` and
`mcp__airflow__fetch_dags(dag_id_pattern=...)`. See the `dag-deploy` skill for the
full sequence.

The DAG folder is shared with other projects. Only ever rsync into your own project
subfolder — never the root, never with `--delete`.

## Local Validation

`.venv` is pinned to **Python 3.13 + Airflow 3.2.1**, matching the server image, so a
local parse exercises the same import paths the dag-processor will.

```bash
uv sync                                       # first time / after dep changes
.venv/bin/python dags/<project>/dag_<name>.py # parse — exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

`tests/test_dag_integrity.py` runs over every `dags/*/dag_*.py` automatically — no
per-DAG test to write. It asserts: subfolder has `__init__.py`, the file imports and
defines a DAG, the `sys.path` block is present when `dag_utils` is imported, no 2.x
import paths or `schedule_interval=`, no top-level I/O, and `catchup=False` + tags +
description are set.

The venv matches the server's Python and Airflow versions but not its full provider
set. A clean local parse still does not prove a third-party import exists in the
server image — `mcp__airflow__get_import_errors` after deploy remains the
authoritative check.

## Code Style

- Explicit over clever. Small functions. Write less code.
- Comments only where the *why* is non-obvious. No docstring walls.
- Callables log through `logging.getLogger("airflow.task")` with a `[<project>]` prefix.
- Callables raise on failure so the task fails visibly; never swallow an exception.
