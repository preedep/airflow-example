# g1pro — Integration Reference for `airflow-demo`

Reference document for AI coding agents working in the **`airflow-demo`** project.
It describes how to reach the shared home-lab server (`g1pro`), transfer files to it, and deploy Airflow DAGs.

> **Scope:** this file documents the *target environment*, not the `airflow-demo` codebase itself.
> Everything below was verified against the live server on **2026-08-12** unless marked `NOT DEPLOYED`.

> **Credentials live in `g1pro.secrets.md`** — same directory, gitignored, local only. This file is
> safe to commit. When adding a new credential, put the value there and reference it from here.

---

## 1. Hosts & Network

All machines are on a **Tailscale** tailnet. Prefer Tailscale hostnames — they work from the home LAN and remotely. No port forwarding, no public IP.

| Machine | Role | Hostname | LAN IP | CPU |
|---|---|---|---|---|
| Mac mini | Developer workstation (write + run code here) | — | — | ARM (Apple Silicon) |
| **g1pro** | Server — k3s, Airflow, all workloads | `nixhome-linux-g1pro` | `192.168.101.19` | x86_64 |
| pi5 | Auxiliary node | `raspi5` | `192.168.101.30` | ARM64 |

**Rule of thumb:** code is written and commands are run on the **Mac mini**; workloads run on **g1pro**.

```bash
# Verify connectivity before anything else
ssh nickmsft@nixhome-linux-g1pro true && echo OK
```

If this fails, Tailscale is probably down. Start it and retry.

---

## 2. SFTP / SSH Access

**This is the recommended file transfer method.** It is already active — no extra service, single port, key-based auth.

| Field | Value |
|---|---|
| Host | `nixhome-linux-g1pro` (Tailscale) or `192.168.101.19` (LAN) |
| Port | `22` |
| User | `nickmsft` |
| Password | **none — SSH public-key auth only** |
| Shell | `/usr/bin/zsh` |
| Home | `/home/nickmsft` |
| Server module | `openssh-sftp-server` (OpenSSH built-in subsystem) |

> **There is no SFTP password.** Authentication is via the SSH key already present on the Mac mini
> (`~/.ssh/`). Do not attempt password authentication — it is not enabled. If a key is missing,
> copy one with `ssh-copy-id nickmsft@nixhome-linux-g1pro`.

### Usage

```bash
# Interactive session
sftp nickmsft@nixhome-linux-g1pro

# Copy a single file
scp ./file.txt nickmsft@nixhome-linux-g1pro:/mnt/external-storage/

# Sync a directory (preferred — idempotent, only transfers changes)
rsync -av ./localdir/ nickmsft@nixhome-linux-g1pro:/mnt/external-storage/localdir/

# Run a remote command
ssh nickmsft@nixhome-linux-g1pro "ls -la /mnt/external-storage/"
```

### Sudo caveat

`sudo` on g1pro **requires a password** (it is not passwordless). Any script needing root must use
`ssh -t` so the prompt reaches the user's terminal. An AI agent cannot supply this password
non-interactively — surface the command to the user instead.

Note that **DAG deployment does not need sudo** (see §5) — `/mnt/external-storage/airflow-dags/`
is world-writable.

---

## 3. FTPS Access

> ### ⚠️ NOT DEPLOYED
> As of **2026-08-12**, vsftpd is **not installed** on g1pro. `systemctl is-active vsftpd` returns
> `inactive`, the package is absent, and `/mnt/external-storage/ftp` does not exist.
> The deploy script exists but has **not been run** — it needs an interactive sudo password.
>
> **Use SFTP (§2) instead.** The values below describe the *intended* configuration and will be
> correct only after deployment. Do not write code that depends on FTPS until this section's
> warning is removed.

**To deploy** (run from the Mac mini in the `home-second-brain` repo — prompts for g1pro sudo password):

```bash
FTP_PASSWORD='<choose-a-password>' ./scripts/deploy-ftps.sh
```

### Intended configuration (post-deployment)

| Field | Value |
|---|---|
| Host | `nixhome-linux-g1pro` or `192.168.101.19` |
| Control port | `21` |
| Mode | **Explicit FTPS (FTPES)** — TLS required for login *and* data |
| User | `ftpuser` |
| Password | set via `FTP_PASSWORD` at deploy time — **not stored in any repo** |
| Passive ports | `30000-30100/tcp` |
| Chroot root | `/mnt/external-storage/ftp` — **read-only** |
| Writable dir | `/mnt/external-storage/ftp/upload` |
| TLS cert | self-signed, `/etc/ssl/private/vsftpd.pem` |
| Cert copy on Mac | `~/.certs/g1pro-ftps.crt` |

**Two constraints that break naive code:**

1. **Upload into `/upload/`, never `/`.** The chroot root is root-owned and mode `555` — vsftpd
   refuses to run with a writable chroot root. Writing to `/` returns `553 Could not create file`.
2. **The cert is self-signed.** Clients must pass `--insecure` (curl) / accept-on-first-use
   (FileZilla), or verify explicitly with `--cacert ~/.certs/g1pro-ftps.crt`.

### Usage (once deployed)

```bash
# Upload
curl --ftp-ssl --insecure -T ./file.txt \
  ftp://nixhome-linux-g1pro/upload/ --user 'ftpuser:<password>'

# List
curl --ftp-ssl --insecure --list-only \
  ftp://nixhome-linux-g1pro/upload/ --user 'ftpuser:<password>'

# Download
curl --ftp-ssl --insecure -O \
  ftp://nixhome-linux-g1pro/upload/file.txt --user 'ftpuser:<password>'
```

**Python (`ftplib`, explicit TLS):**

```python
from ftplib import FTP_TLS
import ssl

ctx = ssl._create_unverified_context()  # self-signed cert
ftps = FTP_TLS(context=ctx)
ftps.connect("nixhome-linux-g1pro", 21, timeout=30)
ftps.login("ftpuser", "<password>")
ftps.prot_p()                            # encrypt the data channel — required
ftps.cwd("/upload")                      # never write to "/"

with open("file.txt", "rb") as fh:
    ftps.storbinary("STOR file.txt", fh)

print(ftps.nlst())
ftps.quit()
```

---

## 4. Airflow

| Field | Value |
|---|---|
| **Web UI** | `http://nixhome-linux-g1pro:30080` |
| Via gateway | `http://nixhome-linux-g1pro:30800/airflow/` |
| Username | `admin` |
| Password | see `g1pro.secrets.md` (gitignored, local only) |
| Version | **3.2.1** |
| Executor | **KubernetesExecutor** — each task runs in an ephemeral worker pod |
| Namespace | `airflow` (k3s) |
| Metadata DB | PostgreSQL, schema `airflow_db` |
| DAG folder (server) | `/mnt/external-storage/airflow-dags/` |
| DAG folder (in-pod) | `/opt/airflow/dags/` |
| REST API | `http://nixhome-linux-g1pro:30080/api/v2` |
| MCP server | `http://nixhome-linux-g1pro:30700/mcp` — **live, 68 tools, see §9b** |

### Components

| Deployment | Replicas | Role |
|---|---|---|
| `airflow-scheduler` | 2 | Schedules tasks, spawns worker pods |
| `airflow-webserver` | 1 | UI / API server (NodePort 30080) |
| `airflow-dag-processor` | 1 | Parses DAG files from the shared folder |
| `airflow-triggerer` | 2 | Deferrable operators |

### Airflow 3.x command changes

| Old (2.x) | New (3.x) |
|---|---|
| `airflow webserver` | `airflow api-server` |
| `airflow db upgrade` | `airflow db migrate` |

---

## 5. How to Deploy a DAG

DAGs are deployed by **copying files into `/mnt/external-storage/airflow-dags/` on g1pro**. That
directory is a k8s `hostPath` volume mounted into every Airflow component at `/opt/airflow/dags/`.
The `dag-processor` picks up changes automatically — **no pod restart, no redeploy, no sudo.**

> The directory is mode `777`, so `nickmsft` writes to it directly over SSH.

### Deploy

```bash
# Single DAG file
scp ./dags/my_dag.py \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/

# A whole self-contained subfolder (preferred)
rsync -av ./dags/mydemo/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/mydemo/

# Exclude local noise
rsync -av --exclude='__pycache__' --exclude='*.pyc' \
  ./dags/mydemo/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/mydemo/
```

### Verify it parsed

```bash
# Confirm the file landed
ssh nickmsft@nixhome-linux-g1pro "ls -la /mnt/external-storage/airflow-dags/mydemo/"

# Check for import errors (most common failure) — the direct way
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags list-import-errors

# Or read the parser log
kubectl -n airflow logs deploy/airflow-dag-processor --tail=50 | grep -i error

# List registered DAGs
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags list | grep mydemo

# Force an immediate reparse instead of waiting
kubectl -n airflow exec deploy/airflow-webserver -- \
  airflow dags reserialize
```

Then open `http://nixhome-linux-g1pro:30080` — the DAG appears **paused** by default. Unpause it in
the UI, or:

```bash
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags unpause <dag_id>
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags trigger <dag_id>
```

### Watch a run

```bash
# Ephemeral worker pods appear and disappear per task
kubectl -n airflow get pods -w

# Task logs
kubectl -n airflow logs <worker-pod-name>
```

---

## 6. DAG Folder Convention

Each DAG lives in a **self-contained subfolder** under the DAG root:

```
/mnt/external-storage/airflow-dags/
└── mydemo/
    ├── __init__.py          # empty marker file — required
    ├── dag_utils.py         # local helpers (task callables, default_args)
    └── dag_my_pipeline.py   # the DAG definition
```

`dag_utils.py` is **local to each subfolder** — it is not shared across subfolders. Two subfolders
may both define `dag_utils.py` with different contents.

### Importing local helpers

Because subfolders are not Python packages on `sys.path`, a DAG file must insert its own directory
before importing siblings:

```python
import sys
from pathlib import Path

_DAGS_DIR = Path(__file__).parent.resolve()
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from dag_utils import default_args, my_task_callable
```

Omitting this causes `ModuleNotFoundError: No module named 'dag_utils'` in the dag-processor.

### Two co-existing styles

| Style | Example folder | Approach |
|---|---|---|
| **Classic Airflow** | `dags/investment/` | `DAG()` context manager + operators directly |
| **dag-factory (YAML)** | `dags/APxxxx/` | `dagfactory.load_yaml_dags()` + YAML task configs |

For a new demo project, **use the classic style** — it is more explicit and easier to debug.

---

## 7. DAG Example (verified working pattern)

This mirrors `nix-dag-fx-alert`, which runs successfully on this cluster today.

**`mydemo/__init__.py`** — empty file.

**`mydemo/dag_utils.py`**

```python
"""Shared helpers for mydemo DAGs."""

import logging

log = logging.getLogger(__name__)


def default_args(owner="nix", retries=1, retry_delay_sec=30, **kwargs):
    args = {"owner": owner, "retries": retries, "retry_delay_sec": retry_delay_sec}
    args.update(kwargs)
    return args


def fetch_data(endpoint, **context):
    """PythonOperator callable. Raises on failure so the task fails in Airflow."""
    import requests

    task_log = logging.getLogger("airflow.task")
    task_log.info("[mydemo] fetching %s", endpoint)

    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()
    data = response.json()

    task_log.info("[mydemo] received %d keys", len(data))
    return data          # returned value is pushed to XCom automatically


def process_data(**context):
    """Pull the previous task's XCom and act on it."""
    task_log = logging.getLogger("airflow.task")
    data = context["ti"].xcom_pull(task_ids="fetch_data")
    task_log.info("[mydemo] processing: %s", data)
    return {"status": "ok"}
```

**`mydemo/dag_my_pipeline.py`**

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
    schedule="0 */6 * * *",          # every 6 hours; None = manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,                    # do NOT backfill missed runs
    tags=["demo", "mydemo"],
    default_args=default_args(owner="nix"),
) as dag:
    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        op_kwargs={"endpoint": "https://api.frankfurter.dev/v1/latest?base=USD&symbols=THB"},
    )

    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    fetch >> process
```

Deploy it:

```bash
rsync -av --exclude='__pycache__' ./dags/mydemo/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/mydemo/
```

---

## 8. Airflow 3.x Rules That Break Naive DAG Code

These are the traps specific to **Airflow 3.x + KubernetesExecutor**. Get these wrong and the DAG
parses fine but fails at runtime.

### Import `Variable` from `airflow.sdk`

Tasks run in **ephemeral worker pods** that talk to the execution API server, not the metadata DB.

```python
from airflow.sdk import Variable        # ✅ correct
# from airflow.models import Variable   # ❌ fails in worker pods
```

### Operator import paths moved

```python
from airflow.providers.standard.operators.python import PythonOperator   # ✅ 3.x
from airflow.providers.standard.operators.bash import BashOperator       # ✅ 3.x
# from airflow.operators.python import PythonOperator                    # ❌ 2.x path
```

### `schedule`, not `schedule_interval`

```python
schedule="0 */6 * * *"        # ✅
# schedule_interval="@daily"  # ❌ removed in 3.x
```

### Always set `catchup=False`

`start_date` in the past + `catchup=True` (the default) floods the cluster with backfill runs.

### Imports go inside the callable

Heavy imports at module scope slow every DAG parse cycle. Put them inside the task function
(see `fetch_data` above importing `requests` locally).

### Available providers

> **Updated 2026-08-12** — re-verified via `pip list` in the webserver pod. The image ships
> considerably more than originally listed here. Authoritative list is in the project `CLAUDE.md`.

`standard` · `cncf-kubernetes` · `postgres` · `mysql` · `common-sql` · `common-io` ·
`common-compat` · `common-messaging` · `http` · `smtp` · `ssh` · `sftp` · `ftp` · `amazon` ·
`google` · `microsoft-azure` · `microsoft-psrp` · `databricks` · `snowflake` · `elasticsearch` ·
`redis` · `celery` · `docker` · `git` · `grpc` · `hashicorp` · `odbc` · `openlineage` · `slack` ·
`fab`

**`requests` 2.33.1 and `pendulum` 3.2.0 are available.** Anything outside this list requires
rebuilding the Airflow image — it cannot be `pip install`ed into a running worker. Re-check with:

```bash
kubectl -n airflow exec deploy/airflow-webserver -- python -m pip list
```

---

## 9. Airflow Variables & Connections

Secrets belong in Airflow Variables, **never in DAG code or the repo**.

```bash
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"

# Set
kubectl -n airflow exec deploy/airflow-webserver -- \
  airflow variables set my_api_token "<token>"

# Get / list
kubectl -n airflow exec deploy/airflow-webserver -- airflow variables get my_api_token
kubectl -n airflow exec deploy/airflow-webserver -- airflow variables list
```

Read in a task:

```python
from airflow.sdk import Variable
token = Variable.get("my_api_token")
```

If `airflow-demo` needs to reach the FTPS server from inside a DAG, store the password as a
Variable (e.g. `ftps_password`) — do not hardcode it.

---

## 9b. Airflow MCP Server (recommended for AI agents)

**Status: LIVE and verified working (2026-08-12).** An AI agent can manage Airflow directly through
MCP tool calls instead of shelling out to `kubectl exec` — this is the preferred path for anything
except deploying DAG *files* (which still needs `rsync`, §5).

| Field | Value |
|---|---|
| Endpoint (from Mac mini) | `http://nixhome-linux-g1pro:30700/mcp` |
| In-cluster | `http://airflow-mcp.airflow-mcp.svc.cluster.local:8000/mcp` |
| Transport | HTTP streamable (SSE responses) |
| Server | `mcp-apache-airflow` **v3.4.2** |
| Auth to endpoint | **none** — no token required on the MCP port itself |
| Auth to Airflow | basic auth, injected server-side from `airflow-mcp-secret` |
| Airflow API version | `v2` |
| Tools exposed | **68** |
| Namespace | `airflow-mcp`, NodePort `30700` |

> **Note:** `CLAUDE.md` says the package is `v0.2.10`. The image tag is `airflow-mcp:0.2.10`, but the
> server reports itself as **`mcp-apache-airflow` v3.4.2** at runtime. Trust the runtime value.

### Client configuration

**Claude Code** — `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "airflow": {
      "type": "http",
      "url": "http://nixhome-linux-g1pro:30700/mcp"
    }
  }
}
```

No API key is needed — credentials live in the server's Kubernetes secret, not the client.

### Tools available (68 total)

| Group | Tools |
|---|---|
| **DAGs** | `fetch_dags`, `get_dag`, `get_dag_details`, `get_dag_source`, `pause_dag`, `unpause_dag`, `patch_dag`, `patch_dags`, `delete_dag`, `reparse_dag_file`, `get_dag_stats` |
| **DAG runs** | `post_dag_run` *(trigger)*, `get_dag_runs`, `get_dag_runs_batch`, `get_dag_run`, `update_dag_run_state`, `delete_dag_run`, `clear_dag_run`, `set_dag_run_note` |
| **Tasks** | `get_dag_tasks`, `get_task`, `get_tasks`, `get_task_instance`, `list_task_instances`, `update_task_instance`, `clear_task_instances`, `set_task_instances_state`, `list_task_instance_tries` |
| **Logs / debug** | `get_log`, `get_import_errors`, `get_import_error`, `get_event_logs`, `get_event_log` |
| **Variables** | `list_variables`, `get_variable`, `create_variable`, `update_variable`, `delete_variable` |
| **Connections** | `list_connections`, `get_connection`, `create_connection`, `update_connection`, `delete_connection`, `test_connection` |
| **XCom** | `get_xcom_entries`, `get_xcom_entry` |
| **Datasets** | `get_datasets`, `get_dataset`, `get_dataset_events`, `create_dataset_event`, `get_upstream_dataset_events`, + 6 queued-event tools |
| **Pools** | `get_pools`, `get_pool`, `post_pool`, `patch_pool`, `delete_pool` |
| **Cluster info** | `get_version`, `get_config`, `get_value`, `get_plugins`, `get_providers`, `get_health` ⚠️ |

### Typical agent workflow

After `rsync`-ing DAG files (§5), everything else is an MCP call:

| Goal | Tool |
|---|---|
| Did my DAG parse? | `get_import_errors` |
| Is it registered? | `fetch_dags` or `get_dag` |
| Enable it | `unpause_dag` |
| Run it now | `post_dag_run` |
| Did the run succeed? | `get_dag_runs` → `list_task_instances` |
| Why did the task fail? | `get_log` |
| Set a secret | `create_variable` / `update_variable` |
| Re-run a failed task | `clear_task_instances` |

### Usage notes for this project

- **Always filter `fetch_dags`.** The cluster hosts ~106 DAGs, most of them Airflow's bundled
  examples. Use `dag_id_pattern="nix-dag-"` or `tags=[...]`; an unfiltered call buries your DAG.
- **`reparse_dag_file` takes a `file_token`, not a path.** Get it from the `file_token` field of a
  `fetch_dags` / `get_dag` result.
- **`get_log` needs all four args** — `dag_id`, `dag_run_id`, `task_id`, `task_try_number`. It
  works after the worker pod is gone, unlike `kubectl logs` on an ephemeral pod.
- **Read try 1 first.** A task that fails differently on retry is not idempotent — that difference
  is the finding.
- **Some tools carry 2.x parameters.** `post_dag_run` / `get_dag_runs` still accept
  `execution_date`; on 3.x prefer `logical_date`. Note `logical_date` is **null** for manually
  triggered and asset-triggered runs.
- **Shared cluster.** Write tools (`post_dag_run`, `clear_task_instances`, `pause_dag`,
  `delete_dag`) do real work on DAGs that may not be yours. Confirm with the user before
  triggering or clearing anything with external side effects, and never touch another project's
  DAG.

### Known issues

| Symptom | Cause | Workaround |
|---|---|---|
| `get_health` → `404 API route not found` | Tool targets a legacy route removed in Airflow 3.2.1 | Use `get_version` instead — it works and proves connectivity *(independently confirmed 2026-08-12)* |
| Pod has **79 restarts** | Liveness probe `timeoutSeconds: 1` is too tight; a busy pod misses the TCP check and gets killed | Cosmetic — the pod self-recovers and serves fine. To fix properly, raise `timeoutSeconds` to `5` in `k8s/airflow-mcp/deployment.yaml` |
| Session errors on repeat calls | HTTP transport needs the `mcp-session-id` header from `initialize` | Any real MCP client handles this automatically |

### Verify from the shell

```bash
# Handshake — should return serverInfo
curl -s -X POST http://nixhome-linux-g1pro:30700/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"probe","version":"1.0"}}}'

# Pod status / logs
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"
kubectl -n airflow-mcp get pods
kubectl -n airflow-mcp logs -f deploy/airflow-mcp
```

---

## 10. Other Services on g1pro

Useful if `airflow-demo` needs a database, an LLM endpoint, or log/metric inspection.

| Service | External URL (from Mac mini) | In-cluster address |
|---|---|---|
| Airflow | `http://nixhome-linux-g1pro:30080` | `airflow-webserver.airflow.svc.cluster.local:8080` |
| Envoy Gateway | `http://nixhome-linux-g1pro:30800` | — |
| LiteLLM (AI gateway) | `http://nixhome-linux-g1pro:30400` | `litellm.ai-gateway.svc.cluster.local:4000` |
| Grafana | `http://nixhome-linux-g1pro:30030` | — |
| Prometheus | `http://nixhome-linux-g1pro:30090` | — |
| Kibana (logs) | `http://nixhome-linux-g1pro:30601` | — |
| Open WebUI | `http://nixhome-linux-g1pro:30500` | — |
| PostgreSQL | *ClusterIP only* | `postgres.postgres.svc.cluster.local:5432` |
| Elasticsearch | *ClusterIP only* | `elasticsearch.elk.svc.cluster.local:9200` |
| Airflow MCP | `http://nixhome-linux-g1pro:30700/mcp` (§9b) | `airflow-mcp.airflow-mcp.svc.cluster.local:8000` |

**A DAG task runs inside the cluster** — it must use the in-cluster address, not the NodePort URL.

```python
# ✅ from inside a DAG task
LITELLM = "http://litellm.ai-gateway.svc.cluster.local:4000/v1"
PG = "postgres.postgres.svc.cluster.local:5432"
```

Credentials: see `g1pro.secrets.md` (gitignored, local only). Kibana, Open WebUI and Prometheus
have no auth.

Port-forward for ClusterIP-only services from the Mac mini:

```bash
kubectl -n postgres port-forward svc/postgres 5432:5432
```

---

## 11. Persistent Storage

All stateful data lives under `/mnt/external-storage/` on g1pro (NVMe, ~739 GB free).

| Path | Purpose | Writable by `nickmsft` over SSH |
|---|---|---|
| `/mnt/external-storage/airflow-dags` | **DAG files — deploy target** | ✅ yes (mode 777) |
| `/mnt/external-storage/airflow-logs` | Task logs | uid 50000 |
| `/mnt/external-storage/ftp` | FTPS root *(after deployment)* | root-owned; `upload/` writable by `ftpuser` |
| `/mnt/external-storage/postgres` | PostgreSQL data | uid 999 |
| `/mnt/external-storage/elasticsearch` | ES indices | uid 1000 |

---

## 12. Troubleshooting

```bash
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"

kubectl -n airflow get pods                                  # component health
kubectl -n airflow exec deploy/airflow-webserver -- \
  airflow dags list-import-errors                            # DAG parse errors (start here)
kubectl -n airflow logs -f deploy/airflow-dag-processor      # parser detail
kubectl -n airflow logs -f deploy/airflow-scheduler          # scheduling issues
kubectl -n airflow get pods -w                               # watch worker pods
```

| Symptom | Cause | Fix |
|---|---|---|
| DAG not in UI | Parse error, or file not synced | Check `dag-processor` logs; confirm the file exists on g1pro |
| `ModuleNotFoundError: dag_utils` | Missing `sys.path` insert | Add the `_DAGS_DIR` block (§6) |
| `ModuleNotFoundError` on a 3rd-party lib | Not in the image | Use an installed provider, or rebuild the image |
| Worker pod `Error`, no logs | Bad image or resource limits | `kubectl -n airflow describe pod <name>` |
| `Invalid auth token: Signature verification failed` | JWT secret mismatch | Redeploy Airflow — all components must share `airflow-secrets` |
| Import works locally, fails in Airflow | 2.x import paths | Use `airflow.providers.standard.*` (§8) |
| Hundreds of runs on first unpause | `catchup` defaulted to True | Set `catchup=False`, delete the extra runs |
| `kubectl` / `ssh` times out | Tailscale down | Restart Tailscale on the Mac mini |

---

## 13. Quick Reference

```bash
# --- Connectivity ---
ssh nickmsft@nixhome-linux-g1pro true
export KUBECONFIG="$HOME/.kube/config:$HOME/.kube/nixhome-config"

# --- Deploy a DAG ---
rsync -av --exclude='__pycache__' ./dags/mydemo/ \
  nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/mydemo/

# --- Verify ---
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags list-import-errors
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags list | grep mydemo

# --- Run ---
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags unpause nix-dag-mydemo
kubectl -n airflow exec deploy/airflow-webserver -- airflow dags trigger nix-dag-mydemo
kubectl -n airflow get pods -w

# --- File transfer (SFTP — works today) ---
scp ./file.txt nickmsft@nixhome-linux-g1pro:/mnt/external-storage/
```

| What | Where |
|---|---|
| Airflow UI | `http://nixhome-linux-g1pro:30080` — `admin` / see `g1pro.secrets.md` |
| Airflow MCP | `http://nixhome-linux-g1pro:30700/mcp` — **live, no auth, 68 tools** (§9b) |
| DAG deploy target | `nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/` |
| SFTP | `nickmsft@nixhome-linux-g1pro` — **SSH key, no password** |
| FTPS | `ftpuser@nixhome-linux-g1pro:21` — **NOT DEPLOYED YET** (§3) |

---

*Verified against the live g1pro server on 2026-08-12. Airflow 3.2.1, KubernetesExecutor, k3s.*
