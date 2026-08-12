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
| 4 | `dag_wasb_prefix_suffix_sensor.py` | extending a provider sensor | a blob in the container |
| 5 | `dag_cyclic.py` | non-overlapping scheduled runs | — |

**Start with #1.** It uploads a file to the FTPS server. Both #2 and #3 expect a
file to already be there, so running them first means the sensor waits out its
timeout and the stream transfer fails with "not found".

**#4 is independent** of the FTPS/SFTP chain — it needs only an Azure Blob
connection and a blob to find.

**#5 needs no connection at all** and is the only scheduled DAG here; the rest are
manual-trigger only.

---

## Prerequisites

Create these before running anything. Names are what the DAGs reference — change
them in one place at the top of each file if yours differ.

### Airflow Connections

| Conn ID | Type | Fields |
|---|---|---|
| `ftps_test_001` | **`FTP`** | host, login, password, port `21` |
| `sftp_test_001` | `SFTP` | host, login, password, port `22` |
| `wasb-nickstorageairflow002` | `wasb` | login = storage account; SAS in extra (#4 only) |

For SAS auth, put the token in the connection's **extra** as
`{"sas_token": "?sp=...&sig=..."}` and set **login to the storage account name** —
`WasbHook` builds the account URL from it. The container is not part of the
connection; it is passed per operation.

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

## 4. `dag_wasb_prefix_suffix_sensor.py`

`nix-dag-wasb-prefix-suffix-sensor` — waits for an Azure blob matching **both** a
prefix and a suffix.

```bash
airflow dags trigger nix-dag-wasb-prefix-suffix-sensor \
  --conf '{"prefix":"incoming/","suffix":".csv"}'
```

**Pattern: extend a provider sensor rather than writing one from scratch.** The
provider ships `WasbBlobSensor` (exact name) and `WasbPrefixSensor` (prefix only);
neither expresses *"any `.csv` under `incoming/`"*. Subclassing `WasbPrefixSensor`
and overriding `poke()` inherits its connection handling and templated fields:

```python
class WasbPrefixSensorWithSuffix(WasbPrefixSensor):
    template_fields = ("container_name", "prefix", "suffix")

    def poke(self, context) -> bool:
        blobs = hook.get_blobs_list(container_name=..., prefix=self.prefix, delimiter="")
        matched = [b for b in blobs if b.lower().endswith(self.suffix.lower())]
        ...
```

**Push the filter as far server-side as it goes.** The prefix is passed to Azure so
it filters before returning; only the suffix is matched locally. Filtering entirely
client-side would list the whole container on every poke.

Note `get_blobs_list` defaults to `delimiter="/"`, which stops at the first level —
pass `delimiter=""` to recurse.

The sensor pushes matching names to XCom (`matched_blobs`) so the downstream task
acts on exactly what satisfied it, rather than re-listing and possibly seeing a
different set.

---

## 5. `dag_cyclic.py`

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
