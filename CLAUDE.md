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

```
dags/
  dag_<name>.py         one standalone file per demo DAG
scripts/
  deploy-dag.sh         parse-check + test + rsync to g1pro
tests/                  parse and integrity tests (run locally)
.claude/
  skills/               dag-author, dag-deploy, dag-debug
  commands/             slash commands
  references/           g1pro.md and other deep docs
```

## DAG Conventions

**Each demo DAG is a single standalone file.** No subfolders, no shared `dag_utils`
module, no cross-DAG imports. This is a deliberate constraint: a demo should be
readable and deployable on its own. Duplicating a small helper between two DAGs is
preferred over coupling them.

- File named `dag_<snake_name>.py`, directly under `dags/`.
- `dag_id` uses the `nix-dag-<name>` convention, e.g. `nix-dag-ftps-sensor`.
- **Classic style** — `with DAG(...)` plus operators. Not the `@dag` decorator; the
  verified working DAGs on this cluster use the context-manager form.
- Task callables are module-level functions in the same file, wired in with
  `PythonOperator`.
- Every DAG sets `dag_id`, `description`, `schedule`, `start_date`, `catchup=False`,
  `tags`, and `default_args`.
- **`doc_md` is required — on the DAG and on every task.** It renders as Markdown in
  the UI (Graph → task → Documentation) and is the only in-product explanation an
  operator gets. A module docstring does *not* count: Airflow only surfaces it if
  passed explicitly as `doc_md=__doc__`, and it renders as plain text.
  Put the DAG-level Markdown in a `DAG_DOC_MD` constant **below the imports** —
  above them it makes ruff flag every import as `E402`.
- `start_date` is a static `datetime(...)` — never `datetime.now()` or `days_ago`.
- No secrets in code. Use Airflow Variables, read inside the callable.

## Airflow 3.x Rules

**No deprecated APIs.** Airflow 3.2.1 still accepts most 2.x import paths — they are
shims that warn on attribute access rather than failing. Using them is a build
failure here, not a warning: `tests/test_dag_integrity.py` enforces it two ways, an
AST scan for deprecated module paths plus a subprocess parse with `FutureWarning` and
`DeprecationWarning` raised as errors.

Both are needed. The AST scan catches deprecated imports inside functions and
branches that never execute during a parse; the runtime parse catches deprecated
*attribute* access and removed kwargs that no import-path grep can see.

Two traps worth knowing if you touch that test:

- `airflow.operators.python` and friends **import silently** — the warning fires only
  when you access an attribute. A plain import check proves nothing.
- Airflow's `DeprecatedImportWarning` subclasses **`FutureWarning`**, not
  `DeprecationWarning`, and is re-emitted through logging (`py.warnings`), so
  `warnings.catch_warnings` records nothing. Filtering on `DeprecationWarning` alone
  silently passes everything.

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
./scripts/deploy-dag.sh dag_<name>     # or --all
```

Runs connectivity check → parse check → integrity tests → rsync, and refuses to ship
anything that fails. It never uses `--delete`; the DAG folder is shared with other
projects.

The dag-processor picks up changes automatically — no pod restart, no sudo, ~30s to
register. New DAGs land **paused**. Verify with `mcp__airflow__get_import_errors`
then `mcp__airflow__get_dag(dag_id=...)`; use the exact `dag_id`, since
`fetch_dags(dag_id_pattern=...)` does not substring-match reliably. See the
`dag-deploy` skill.

## Local Validation

`.venv` is pinned to **Python 3.13 + Airflow 3.2.1**, matching the server image, so a
local parse exercises the same import paths the dag-processor will.

```bash
uv sync                                   # first time / after dep changes
.venv/bin/python dags/dag_<name>.py       # parse — exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

`tests/test_dag_integrity.py` runs over every `dags/dag_*.py` automatically — no
per-DAG test to write. It asserts:

- the file imports and defines a DAG
- it is standalone (no `dag_utils` import)
- **no deprecated Airflow 2.x modules** (AST scan, covers unexecuted code paths)
- **parses clean with `FutureWarning`/`DeprecationWarning` as errors** (subprocess)
- no `schedule_interval=`, no 2.x import paths
- no top-level I/O
- `catchup=False`, plus tags and description set
- **`doc_md` present on the DAG and on every task**

The venv matches the server's Python and Airflow versions but not its full provider
set. A clean local parse still does not prove a third-party import exists in the
server image — `mcp__airflow__get_import_errors` after deploy remains the
authoritative check.

## Code Style

- Explicit over clever. Small functions. Write less code.
- Comments only where the *why* is non-obvious. No docstring walls.
- Callables log through `logging.getLogger("airflow.task")` with a `[<dag_name>]` prefix.
- Callables raise on failure so the task fails visibly; never swallow an exception.
