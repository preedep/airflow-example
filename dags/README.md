# Demo DAGs

Apache Airflow **3.x** examples covering file-transfer patterns: uploading to an
FTPS server, waiting on a file with a sensor, streaming between two servers, and
streaming into Azure Blob Storage.

They also work through a progression in **how much of a provider you reuse** —
using an operator as shipped (#1), extending a sensor (#7), overriding one method
of a transfer operator (#4), and writing an operator from scratch when the
provider has nothing (#5, #6, #8). The
[class table](#classes-defined-in-these-dags) below maps each one to its file.

Each DAG is a **standalone single file** — no shared helper module, no subfolders,
no cross-DAG imports. Copy one out of this repo and it still works. That is why
`MyFTPSHook` appears in more than one file: duplicating a small helper is
preferred here over coupling two demos together.

Written for a **KubernetesExecutor** deployment, where every task runs in its own
ephemeral pod. Most of the design decisions here follow from that; see
[Two constraints](#two-constraints-that-shape-every-dag-here).

### Where things live

This file is the **index**: what each example is, what order to run them in, and
what to set up first. Each DAG's full write-up — the patterns it demonstrates,
the traps it hit, its log output — is a separate file under `docs/`.

```
dags/
  README.md              you are here — the index
  dag_<name>.py          the DAG itself
  docs/dag_<name>.md     that DAG's full detail
  files/                 fixtures a DAG reads at runtime
```

**Deployment is not covered here** — copy these files into your DAG folder
however you already ship DAGs (git-sync, a baked image, object storage, rsync).
The only requirement is that `dags/files/` lands beside the DAG files, since #1
reads a fixture from it at runtime. `docs/` does not need to ship; each DAG's
`doc_md` carries the same explanation to the UI.

The same explanation is also on each DAG's `doc_md`, which renders in the Airflow
UI under **Graph → task → Documentation** — usually the fastest place to read it
while a run is in front of you.

---

## Run them in this order

The DAGs build on each other. Run top to bottom the first time.

| # | DAG | Demonstrates | Depends on |
|---|---|---|---|
| 1 | `dag_ftps_simple_transfer.py` | provider operator, file upload | — |
| 2 | `dag_ftps_sensor.py` | sensor in reschedule mode | a file uploaded by #1 |
| 3 | `dag_ftps_to_sftp_stream_transfer.py` | streaming between two servers | a file uploaded by #1 |
| 4 | `dag_sftp_to_blob_stream.py` | streaming into object storage | a file in the SFTP source directory |
| 5 | `dag_ftps_to_blob_stream.py` | bridging a push API to a pull API | a file uploaded by #1 |
| 6 | `dag_blob_to_sftp_stream.py` | streaming back out of object storage | a blob in the container (#4 or #5 writes one) |
| 7 | `dag_wasb_prefix_suffix_sensor.py` | extending a provider sensor | a blob in the container |
| 8 | `dag_blob_to_ftps_stream.py` | same protocol, opposite control flow | a blob in the container (#4 or #5 writes one) |
| 9 | `dag_s3_prefix_suffix_sensor.py` | the same sensor pattern on S3 | an object in the S3 bucket |
| 10 | `dag_cyclic.py` | non-overlapping scheduled runs | — |

**Start with #1.** It uploads a file to the FTPS server. Both #2 and #3 expect a
file to already be there, so running them first means the sensor waits out its
timeout and the stream transfer fails with "not found".

**#4/#5 → #6/#7 chain**: #4 and #5 both write a blob into the container; #6
streams one back out to SFTP and #7 finds it there. #4 needs a file in its SFTP source directory first — see
[SFTP source directory](#sftp-source-directory-4); #3 delivers into the SFTP
user's `incoming/`, so pointing #4 at that path chains all three. #5 reads from
the FTPS server, so it only needs #1 to have run.

**#4, #5, #6 and #8 are the same problem with different plumbing** — read them
together. Which side controls the loop decides the design: a source that hands
back a readable composes straight into a destination that pulls (#4, #6, #8),
while a source that pushes to a callback needs a pipe in between (#5). That is a
property of the *call*, not the protocol — #5 and #8 both speak FTPS, and only
#5 needs the pipe. The comparison table across all four directions is in
[`docs/dag_blob_to_sftp_stream.md`](docs/dag_blob_to_sftp_stream.md).

**#10 needs no connection at all** and is the only scheduled DAG here; the rest are
manual-trigger only.

---

## Classes defined in these DAGs

Every custom class lives **inside the DAG file that uses it** — there is no shared
module. Use this table to find a worked example of the pattern you need.

| Class | In | Extends | Why it exists |
|---|---|---|---|
| `MyFTPSHook` | [#1, #2, #3, #5, #8](docs/dag_ftps_simple_transfer.md) | `FTPSHook` | Trust a private CA and call `prot_p()`; the stock hook can do neither |
| `MyFTPSFileTransmitOperator` | [#1](docs/dag_ftps_simple_transfer.md) | `FTPSFileTransmitOperator` | Swap in the custom hook, keep the operator |
| `MyFTPSSensor` | [#2](docs/dag_ftps_sensor.md) | `FTPSSensor` | Same swap, for the sensor |
| `StreamingSFTPToWasbOperator` | [#4](docs/dag_sftp_to_blob_stream.md) | `SFTPToWasbOperator` | Override one method so files stream instead of staging on disk |
| `WasbPrefixSensorWithSuffix` | [#7](docs/dag_wasb_prefix_suffix_sensor.md) | `WasbPrefixSensor` | Add suffix matching the provider does not offer |
| `FTPStoBlobStreamOperator` | [#5](docs/dag_ftps_to_blob_stream.md) | `BaseOperator` | No provider FTPS → Blob transfer exists |
| `BlobToSFTPStreamOperator` | [#6](docs/dag_blob_to_sftp_stream.md) | `BaseOperator` | No provider Blob → SFTP transfer exists |
| `BlobToFTPSStreamOperator` | [#8](docs/dag_blob_to_ftps_stream.md) | `BaseOperator` | No provider Blob → FTPS transfer exists |
| `S3PrefixSuffixSensor` | [#9](docs/dag_s3_prefix_suffix_sensor.md) | `S3KeySensor` | Collect every match and push it to XCom; the stock sensor pushes nothing |

They fall into three tiers, and the right one is always the **lowest** that works:

| Tier | When | Examples |
|---|---|---|
| Subclass an **operator/sensor**, override a property | the operator is right, one dependency is wrong | `MyFTPSFileTransmitOperator`, `MyFTPSSensor` |
| Subclass an **operator**, override one method | the operator is right, one *behaviour* is wrong | `StreamingSFTPToWasbOperator`, `WasbPrefixSensorWithSuffix` |
| Subclass **`BaseOperator`** | no provider operator exists at all | the three `*StreamOperator` classes |

Check the provider before writing a `BaseOperator` subclass — the `microsoft-azure`
provider ships `sftp_to_wasb`, `s3_to_wasb`, `local_to_wasb` and
`oracle_to_azure_data_lake`, but nothing in the FTP/FTPS direction and nothing
*out of* Blob Storage. Those gaps are why three operators here start from scratch.

> `MyFTPSHook` appears in five files. That duplication is deliberate — each DAG
> is a standalone single file, as noted at the top.

---

## Prerequisites

Create these before running anything. Names are what the DAGs reference — change
them in one place at the top of each file if yours differ.

### Airflow Connections

| Conn ID | Type | Fields |
|---|---|---|
| `ftps_test_001` | **`FTP`** | host, login, password, port `21` |
| `sftp_test_001` | `SFTP` | host, login, password, port `22` |
| `wasb-nickstorageairflow002` | `wasb` | login = storage account; SAS in extra (#4, #5, #6, #7, #8) |
| `aws_s3_test_001` | `aws` | login = access key id, password = secret; `{"region_name": "..."}` in extra (#9) |

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
apache-airflow-providers-ftp                 # for #1, #2, #3, #5, #8
apache-airflow-providers-sftp                # for #3, #4, #6
apache-airflow-providers-microsoft-azure     # for #4, #5, #6, #7, #8
apache-airflow-providers-amazon              # for #9
```

Both must be in the **Airflow image**, not just your local venv — they cannot be
`pip install`ed into a running worker.

### Fixture file

`files/probe.txt` ships with the repo and is uploaded by #1. The DAG folder is
mounted into every pod, so `dags/files/` is readable at
`<AIRFLOW_HOME>/dags/files/` at runtime.

### SFTP source directory (#4)

#4 reads from a directory on the **SFTP server** — not from `dags/files/`. Create
it and drop a file in before the first run:

```bash
sftp <sftp-user>@<sftp-host>
sftp> mkdir outgoing
sftp> put probe.txt outgoing/
```

The directory must exist and the connection's user must be able to read it. Note
that #4's default source is the SFTP user's `outgoing/`, while #3 *delivers* into
`incoming/` — point #4 at `incoming/` if you want to chain them directly.

---

## 1. `dag_ftps_simple_transfer.py`

`nix-dag-ftps-simple-transfer` — uploads a file from the DAG folder to FTPS.

**Teaches:** Prefer a provider operator over `PythonOperator`; subclass it to swap in a custom hook.

→ **[Full detail: `docs/dag_ftps_simple_transfer.md`](docs/dag_ftps_simple_transfer.md)**

---

## 2. `dag_ftps_sensor.py`

`nix-dag-ftps-sensor` — waits for a file to appear, then reports its size and mtime.

**Teaches:** Always `mode="reschedule"` with an explicit `poke_interval` and `timeout`.

→ **[Full detail: `docs/dag_ftps_sensor.md`](docs/dag_ftps_sensor.md)**

---

## 3. `dag_ftps_to_sftp_stream_transfer.py`

`nix-dag-ftps-to-sftp-stream` — streams a file between two servers.

**Teaches:** Get and put are one task, not two — separate tasks land in separate pods.

→ **[Full detail: `docs/dag_ftps_to_sftp_stream_transfer.md`](docs/dag_ftps_to_sftp_stream_transfer.md)**

---

## 4. `dag_sftp_to_blob_stream.py`

`nix-dag-sftp-to-blob-stream` — streams a file from SFTP into a blob container.

**Teaches:** Override the one provider method that is wrong, inherit the rest. Three runtime traps a parse check cannot catch.

→ **[Full detail: `docs/dag_sftp_to_blob_stream.md`](docs/dag_sftp_to_blob_stream.md)**

---

## 5. `dag_ftps_to_blob_stream.py`

`nix-dag-ftps-to-blob-stream` — streams a file from FTPS into a blob container.

**Teaches:** When no provider operator exists, write one. Bridging a **push** API to a **pull** API with `os.pipe()`.

→ **[Full detail: `docs/dag_ftps_to_blob_stream.md`](docs/dag_ftps_to_blob_stream.md)**

---

## 6. `dag_blob_to_sftp_stream.py`

`nix-dag-blob-to-sftp-stream` — streams a blob back out to the SFTP server.

**Teaches:** Two readables compose directly — no pipe needed. Reach for one only when both sides push or both pull.

→ **[Full detail: `docs/dag_blob_to_sftp_stream.md`](docs/dag_blob_to_sftp_stream.md)**

---

## 7. `dag_wasb_prefix_suffix_sensor.py`

`nix-dag-wasb-prefix-suffix-sensor` — waits for a blob matching a prefix **and** suffix.

**Teaches:** Extend a provider sensor rather than writing one; push the filter server-side.

→ **[Full detail: `docs/dag_wasb_prefix_suffix_sensor.md`](docs/dag_wasb_prefix_suffix_sensor.md)**

---

## 8. `dag_blob_to_ftps_stream.py`

`nix-dag-blob-to-ftps-stream` — streams a blob out to the FTPS server.

**Teaches:** Control flow is a property of the *call*, not the protocol — same server as #5, no pipe required.

→ **[Full detail: `docs/dag_blob_to_ftps_stream.md`](docs/dag_blob_to_ftps_stream.md)**

---

## 9. `dag_s3_prefix_suffix_sensor.py`

`nix-dag-s3-prefix-suffix-sensor` — waits for an S3 object matching a prefix **and** suffix.

**Teaches:** subclass to fix the *hand-off*, not the matching — `S3KeySensor` already
does wildcards, but tells you nothing about what it matched.

→ **[Full detail: `docs/dag_s3_prefix_suffix_sensor.md`](docs/dag_s3_prefix_suffix_sensor.md)**

---

## 10. `dag_cyclic.py`

`nix-dag-cyclic` — a cyclic job: fires every 5 minutes, one run at a time.

**Teaches:** `max_active_runs=1` is what makes a schedule cyclic; a timeout arrives as a failure, not a callback.

→ **[Full detail: `docs/dag_cyclic.md`](docs/dag_cyclic.md)**

---

## What a parse check cannot catch

Every trap documented in these examples parsed cleanly. They only failed against
a live server:

| Bug | Where it surfaces |
|---|---|
| a path that is a file where a directory is expected | first `listdir` |
| a hook whose connection was already closed | first call on the dead client |
| a kwarg the SDK forwards to its HTTP transport | inside the request |
| a provider missing from the runtime image | DAG import |

So treat a clean parse as a fast filter, not proof. The first real run against
the target systems is the actual test.

For threaded code — the `os.pipe()` bridge in #5 — a parse proves nothing at all:
a deadlock or a swallowed exception looks identical to working code until it runs.
Exercise those against fake hooks: feed `execute()` a fake that pushes known bytes,
assert on what the fake destination received, and add one case per failure mode
(source missing, oversize, upload raises, truncated read).

That is how #5's `BrokenPipeError` masking was found — the happy path passed on
the first try, and only the "upload raises" case revealed that the real error was
being hidden behind a broken pipe. Any test that only transfers a file
successfully would have missed it.

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
| `ResourceExistsError` on upload | blob already there and `wasb_overwrite_object=False` |
| `unsupported operation: seek` on upload | streaming a non-seekable source with `max_concurrency` > 1 |
| `FileNotFoundError` from `listdir_attr`, file exists | #4 `source_path` is a bare filename — needs a directory or `*` |
| `OSError: Socket is closed` mid-transfer | used `get_conn()` after a hook method closed the managed session |
| `Session.request() got an unexpected keyword argument` | passed a client-config kwarg (e.g. `max_block_size`) to `upload()` |
| `No such file` writing over SFTP | destination directory does not exist — `putfo` will not create it |
| size mismatch reporting `wrote None` | `putfo(confirm=False)` returns empty `SFTPAttributes`; nothing to compare |
| DAG missing from UI, no import error | the DAG processor has not parsed it yet — it scans on a cycle |
| DAG present but never runs | still paused; these are all manual-trigger except #10 |
| Parses locally, fails in a worker | provider missing from your runtime image, or a connection/Variable not set |

Check import errors first — an entry there invalidates everything else:

```bash
airflow dags list-import-errors
```
