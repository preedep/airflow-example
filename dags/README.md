# Demo DAGs

Apache Airflow **3.x** examples covering file-transfer patterns: uploading to an
FTPS server, waiting on a file with a sensor, and streaming between two servers.

Each DAG is a **standalone single file** — no shared helper module, no subfolders,
no cross-DAG imports. Copy one out of this repo and it still works.

Written for a **KubernetesExecutor** deployment, where every task runs in its own
ephemeral pod. Most of the design decisions here follow from that; see
[Two constraints](#two-constraints-that-shape-every-dag-here).

---

## Run them in this order

The DAGs build on each other. Run top to bottom the first time.

| # | DAG | Demonstrates | Depends on |
|---|---|---|---|
| 1 | `dag_ftps_simple_transfer.py` | provider operator, file upload | — |
| 2 | `dag_ftps_sensor.py` | sensor in reschedule mode | a file uploaded by #1 |
| 3 | `dag_ftps_to_sftp_stream_transfer.py` | streaming between two servers | a file uploaded by #1 |

**Start with #1.** It uploads a file to the FTPS server. Both #2 and #3 expect a
file to already be there, so running them first means the sensor waits out its
timeout and the stream transfer fails with "not found".

---

## Prerequisites

Create these before running anything. Names are what the DAGs reference — change
them in one place at the top of each file if yours differ.

### Airflow Connections

| Conn ID | Type | Fields |
|---|---|---|
| `ftps_test_001` | **`FTP`** | host, login, password, port `21` |
| `sftp_test_001` | `SFTP` | host, login, password, port `22` |

> **There is no `FTPS` connection type.** The `ftp` provider registers
> `conn_type="ftp"` for both `FTPHook` and `FTPSHook`. TLS is selected in code by
> which hook you import, not by the connection. Choosing `FTP` in the UI is correct.

### Airflow Variable

| Key | Value |
|---|---|
| `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

Only needed if your FTPS server uses a **self-signed or private-CA** certificate.
With a publicly trusted cert you can drop `MyFTPSHook` and use the stock
`FTPSHook`/`FTPSFileTransmitOperator` directly.

### Host addressing

Use an address the **worker pods** can resolve. A hostname that works from your
laptop (VPN, `/etc/hosts`, mesh network) often does not resolve inside the cluster
— use the IP or an in-cluster DNS name in the connection.

### Providers

```
apache-airflow-providers-ftp
apache-airflow-providers-sftp     # for #3
```

Both must be in the **Airflow image**, not just your local venv — they cannot be
`pip install`ed into a running worker.

### Fixture file

`files/probe.txt` ships with the repo and is uploaded by #1. The DAG folder is
mounted into every pod, so `dags/files/` is readable at
`<AIRFLOW_HOME>/dags/files/` at runtime.

---

## 1. `dag_ftps_simple_transfer.py`

`nix-dag-ftps-simple-transfer` — uploads a file from the DAG folder to FTPS.

```
dags/files/probe.txt  ──put──▶  FTPS /upload/probe.txt
```

Trigger from the UI, or:

```bash
airflow dags trigger nix-dag-ftps-simple-transfer
```

Optional conf: `{"filename": "other.txt"}` — the file must exist in `dags/files/`.

**Pattern: prefer a provider operator over `PythonOperator`.** This uses
`FTPSFileTransmitOperator` from the `ftp` provider rather than a hand-written
callable. You get templated fields, logging, and retry handling for free.

**Pattern: needing a custom hook is not a reason to abandon the operator.**
Subclass it and override the `hook` property — two lines, and the operator's own
logic stays intact:

```python
class MyFTPSFileTransmitOperator(FTPSFileTransmitOperator):
    @cached_property
    def hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)
```

---

## 2. `dag_ftps_sensor.py`

`nix-dag-ftps-sensor` — waits for a file to appear, then reports its size and mtime.

```
FTPS /upload/<file>  ──poll every 60s, up to 1h──▶  report
```

```bash
airflow dags trigger nix-dag-ftps-sensor --conf '{"filename":"probe.txt"}'
```

**Pattern: always `mode="reschedule"` with an explicit `poke_interval` and
`timeout`.**

```python
FTPSSensor(
    task_id="wait_for_file",
    path="/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}",
    mode="reschedule",   # frees the worker slot between pokes
    poke_interval=60,
    timeout=60 * 60,
)
```

`mode="poke"` holds a worker for the entire wait — on KubernetesExecutor that is a
pod sitting idle for an hour. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

To see it actually wait, trigger with a filename that does not exist yet, then run
#1 to upload it and watch the sensor pick it up on its next poke.

---

## 3. `dag_ftps_to_sftp_stream_transfer.py`

`nix-dag-ftps-to-sftp-stream` — streams a file between two servers.

```
FTPS  ──get──▶  stream  ──put──▶  SFTP
```

```bash
airflow dags trigger nix-dag-ftps-to-sftp-stream
```

**Pattern: get and put are one task, not two.** Separate tasks land in separate
pods, so a file downloaded by "get" would not exist for "put".

Bytes move through an `os.pipe()` from FTPS `retrbinary` straight into paramiko's
`putfo` on a worker thread. Both sockets are open at once, nothing touches disk,
and peak memory is one 8 KiB chunk regardless of file size — a multi-GB file
transfers in constant memory. `BUFFER_LIMIT` caps a single transfer rather than
letting it run unbounded.

This is one of the few cases where `PythonOperator` is right: the provider transfer
operators move between a remote host and **local disk**, so none of them does a
direct server-to-server hop.

**The source is not deleted** after a successful transfer, so re-running re-sends
the same file. That is deliberate — a failed put must not lose the only copy. For a
real pickup pattern, delete only after the verify step confirms the size matches.

---

## Deploying

Deployment is environment-specific. In this repo:

```bash
./scripts/deploy-dag.sh dag_ftps_sensor      # one DAG
./scripts/deploy-dag.sh --all                # all of them
```

It checks connectivity, parses the DAG, runs the integrity tests, then copies the
files — refusing to ship anything that fails. It also syncs `dags/files/`.

If your deployment uses git-sync, a baked image, or object storage instead, the
only requirement is that `dags/files/` lands beside the DAGs.

New DAGs land **paused**:

```bash
airflow dags unpause <dag_id>
```

Allow time for the DAG processor to pick up a new file before assuming it failed.

---

## Local validation

```bash
uv sync
.venv/bin/python dags/dag_<name>.py     # parse — exit 0, no output
.venv/bin/python -m pytest tests/
.venv/bin/ruff check dags/
```

`tests/test_dag_integrity.py` applies to every `dags/dag_*.py` automatically. It
enforces the project's constraints: no deprecated Airflow 2.x APIs, `doc_md` on the
DAG and every task, `catchup=False`, no top-level I/O, standalone files.

A clean local parse does not prove a third-party import exists in the **server**
image. Check import errors after deploying.

---

## Two constraints that shape every DAG here

### Tasks do not share a filesystem

Under KubernetesExecutor every task runs in its own ephemeral pod. A file written
to local disk by one task does not exist for the next — it fails at runtime with
`FileNotFoundError`, and a parse check will not catch it.

Options, in order of preference:

1. **Read from shared storage.** The DAG folder is mounted into every pod, so
   `dags/files/` is readable at runtime.
2. **Do it in one task.** Build the payload in memory and pass a buffer to the hook.
3. **Pass a reference, not a file.** Write to object storage, push the key to XCom.

Never assume `/tmp` persists between tasks.

### Self-signed certificates need a custom hook

Stock `FTPSHook.get_conn()` hardcodes `ssl.create_default_context()` with no
parameter for a CA, so a self-signed cert fails with `CERTIFICATE_VERIFY_FAILED`.
No connection setting can override it.

Each FTPS DAG here subclasses the hook:

```python
class MyFTPSHook(FTPSHook):
    def get_conn(self):
        params = self.get_connection(self.ftp_conn_id)
        context = ssl.create_default_context(cadata=Variable.get("ftps_ca_cert"))
        conn = ftplib.FTP_TLS(params.host, params.login, params.password, context=context)
        conn.prot_p()      # stock hook omits this — without it data is plaintext
        conn.set_pasv(params.extra_dejson.get("passive", True))
        return conn
```

Two things worth noting: verification stays **on** — the CA is trusted, not
bypassed, so this is not `verify=False`. And `prot_p()` is missing from the stock
hook, meaning the control channel is encrypted but the **data** channel is not.

With a publicly trusted certificate, none of this is needed.

---

## Airflow 3.x notes

These examples are 3.x-only. The most common porting mistakes:

```python
from airflow.providers.standard.operators.python import PythonOperator   # ✅ 3.x
from airflow.operators.python import PythonOperator                      # ❌ 2.x shim

from airflow.sdk import Variable                                         # ✅ 3.x
from airflow.models import Variable                                      # ❌ fails in workers

schedule=None                                                            # ✅
schedule_interval=None                                                   # ❌ removed
```

The 2.x import paths still work as deprecation shims — they parse and even run, so
nothing obvious tells you they are wrong. The integrity tests fail the build on
them.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `CERTIFICATE_VERIFY_FAILED` | using stock `FTPSHook` with a self-signed cert, or `ftps_ca_cert` not set |
| `FileNotFoundError: /tmp/...` | assumed two tasks share a disk |
| `550` from FTPS | file not there — run #1 first |
| `553 Could not create file` | wrote to a read-only directory; FTPS chroots often make the root non-writable |
| Data transfer hangs after login | passive-mode port range not reachable from the worker |
| DAG missing from UI, no import error | processor has not parsed it yet |
| DAG present but never runs | still paused |
| Works locally, fails on the server | provider missing from the server image, or a connection/variable not set |

Check import errors first — an entry there invalidates everything else:

```bash
airflow dags list-import-errors
```
