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
| 1 | [`dag_ftps_simple_transfer.py`](dag_ftps_simple_transfer.py) | provider operator, file upload | — |
| 2 | [`dag_ftps_sensor.py`](dag_ftps_sensor.py) | sensor in reschedule mode | a file uploaded by #1 |
| 3 | [`dag_ftps_to_sftp_stream_transfer.py`](dag_ftps_to_sftp_stream_transfer.py) | streaming between two servers | a file uploaded by #1 |
| 4 | [`dag_sftp_to_blob_stream.py`](dag_sftp_to_blob_stream.py) | streaming into object storage | a file in the SFTP source directory |
| 5 | [`dag_ftps_to_blob_stream.py`](dag_ftps_to_blob_stream.py) | bridging a push API to a pull API | a file uploaded by #1 |
| 6 | [`dag_blob_to_sftp_stream.py`](dag_blob_to_sftp_stream.py) | streaming back out of object storage | a blob in the container (#4 or #5 writes one) |
| 7 | [`dag_wasb_prefix_suffix_sensor.py`](dag_wasb_prefix_suffix_sensor.py) | extending a provider sensor | a blob in the container |
| 8 | [`dag_blob_to_ftps_stream.py`](dag_blob_to_ftps_stream.py) | same protocol, opposite control flow | a blob in the container (#4 or #5 writes one) |
| 9 | [`dag_s3_prefix_suffix_sensor.py`](dag_s3_prefix_suffix_sensor.py) | the same sensor pattern on S3 | an object in the S3 bucket |
| 10 | [`dag_blob_to_s3_stream.py`](dag_blob_to_s3_stream.py) | cross-cloud streaming, Azure to AWS | a blob in the container (#4 or #5 writes one) |
| 11 | [`dag_s3_to_blob_stream.py`](dag_s3_to_blob_stream.py) | cross-cloud the other way, via a provider operator | an object in the S3 bucket (#10 writes one) |
| 12 | [`dag_blob_to_smb_stream.py`](dag_blob_to_smb_stream.py) | a destination that is *writable*, not readable | a blob in the container (#4 or #5 writes one) |
| 13 | [`dag_s3_to_smb_stream.py`](dag_s3_to_smb_stream.py) | one call, because both SDKs already fit | an object in the S3 bucket (#10 writes one) |
| 14 | [`dag_s3_to_sftp_stream.py`](dag_s3_to_sftp_stream.py) | a third provider operator that stages to disk | an object in the S3 bucket (#10 writes one) |
| 15 | [`dag_s3_to_ftps_stream.py`](dag_s3_to_ftps_stream.py) | two overrides, not one — logic *and* hook | an object in the S3 bucket (#10 writes one) |
| 16 | [`dag_sftp_to_s3_stream.py`](dag_sftp_to_s3_stream.py) | a provider that streams but forgets `prefetch` | a file in the SFTP source directory |
| 17 | [`dag_deadline_alert.py`](dag_deadline_alert.py) | alert on a slow run without failing it | — |
| 18 | [`dag_sftp_sensor.py`](dag_sftp_sensor.py) | the sensor that needs no subclass | a file in the SFTP source directory |
| 19 | [`dag_cyclic.py`](dag_cyclic.py) | non-overlapping scheduled runs | — |

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

**#17 and #19 need no connection at all** and is the only scheduled DAG here; the rest are
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
| `BlobToS3StreamOperator` | [#10](docs/dag_blob_to_s3_stream.md) | `BaseOperator` | No provider Blob → S3 transfer exists; `s3_to_wasb` points the other way |
| `StreamingS3ToAzureBlobStorageOperator` | [#11](docs/dag_s3_to_blob_stream.md) | `S3ToAzureBlobStorageOperator` | Override one method so objects stream instead of staging on disk |
| `BlobToSMBStreamOperator` | [#12](docs/dag_blob_to_smb_stream.md) | `BaseOperator` | No provider Blob → SMB transfer exists |
| `S3ToSMBStreamOperator` | [#13](docs/dag_s3_to_smb_stream.md) | `BaseOperator` | No provider S3 → SMB transfer exists |
| `StreamingS3ToSFTPOperator` | [#14](docs/dag_s3_to_sftp_stream.md) | `S3ToSFTPOperator` | Override `execute` so objects stream instead of staging on disk |
| `StreamingS3ToFTPSOperator` | [#15](docs/dag_s3_to_ftps_stream.md) | `S3ToFTPOperator` | Stream *and* swap the hardcoded plain-FTP hook for FTPS |
| `StreamingSFTPToS3Operator` | [#16](docs/dag_sftp_to_s3_stream.md) | `SFTPToS3Operator` | Add the `prefetch` the provider omits — 4.8x on 50 MiB |
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
| `wasb-nickstorageairflow002` | `wasb` | login = storage account; SAS in extra (#4, #5, #6, #7, #8, #10, #11, #12) |
| `smb_test_001` | `samba` | host, **schema = share name**, login, password (#12, #13) |
| `aws_s3_test_001` | `aws` | login = access key id, password = secret; `{"region_name": "..."}` in extra (#9, #10, #11, #13, #14, #15, #16) |

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
apache-airflow-providers-ftp                 # for #1, #2, #3, #5, #8, #15
apache-airflow-providers-sftp                # for #3, #4, #6, #14, #16, #18
apache-airflow-providers-microsoft-azure     # for #4, #5, #6, #7, #8
apache-airflow-providers-amazon              # for #9, #10, #11, #13, #14, #15
apache-airflow-providers-samba               # for #12, #13
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

## 1. [`dag_ftps_simple_transfer.py`](dag_ftps_simple_transfer.py)

`nix-dag-ftps-simple-transfer` — uploads a file from the DAG folder to FTPS.

**Teaches:** Prefer a provider operator over `PythonOperator`; subclass it to swap in a custom hook.

→ **[Full detail: `docs/dag_ftps_simple_transfer.md`](docs/dag_ftps_simple_transfer.md)**  ·  📄 **[Source: `dag_ftps_simple_transfer.py`](dag_ftps_simple_transfer.py)**

---

## 2. [`dag_ftps_sensor.py`](dag_ftps_sensor.py)

`nix-dag-ftps-sensor` — waits for a file to appear, then reports its size and mtime.

**Teaches:** Always `mode="reschedule"` with an explicit `poke_interval` and `timeout`.

→ **[Full detail: `docs/dag_ftps_sensor.md`](docs/dag_ftps_sensor.md)**  ·  📄 **[Source: `dag_ftps_sensor.py`](dag_ftps_sensor.py)**

---

## 3. [`dag_ftps_to_sftp_stream_transfer.py`](dag_ftps_to_sftp_stream_transfer.py)

`nix-dag-ftps-to-sftp-stream` — streams a file between two servers.

**Teaches:** Get and put are one task, not two — separate tasks land in separate pods.

→ **[Full detail: `docs/dag_ftps_to_sftp_stream_transfer.md`](docs/dag_ftps_to_sftp_stream_transfer.md)**  ·  📄 **[Source: `dag_ftps_to_sftp_stream_transfer.py`](dag_ftps_to_sftp_stream_transfer.py)**

---

## 4. [`dag_sftp_to_blob_stream.py`](dag_sftp_to_blob_stream.py)

`nix-dag-sftp-to-blob-stream` — streams a file from SFTP into a blob container.

**Teaches:** Override the one provider method that is wrong, inherit the rest. Three runtime traps a parse check cannot catch.

→ **[Full detail: `docs/dag_sftp_to_blob_stream.md`](docs/dag_sftp_to_blob_stream.md)**  ·  📄 **[Source: `dag_sftp_to_blob_stream.py`](dag_sftp_to_blob_stream.py)**

---

## 5. [`dag_ftps_to_blob_stream.py`](dag_ftps_to_blob_stream.py)

`nix-dag-ftps-to-blob-stream` — streams a file from FTPS into a blob container.

**Teaches:** When no provider operator exists, write one. Bridging a **push** API to a **pull** API with `os.pipe()`.

→ **[Full detail: `docs/dag_ftps_to_blob_stream.md`](docs/dag_ftps_to_blob_stream.md)**  ·  📄 **[Source: `dag_ftps_to_blob_stream.py`](dag_ftps_to_blob_stream.py)**

---

## 6. [`dag_blob_to_sftp_stream.py`](dag_blob_to_sftp_stream.py)

`nix-dag-blob-to-sftp-stream` — streams a blob back out to the SFTP server.

**Teaches:** Two readables compose directly — no pipe needed. Reach for one only when both sides push or both pull.

→ **[Full detail: `docs/dag_blob_to_sftp_stream.md`](docs/dag_blob_to_sftp_stream.md)**  ·  📄 **[Source: `dag_blob_to_sftp_stream.py`](dag_blob_to_sftp_stream.py)**

---

## 7. [`dag_wasb_prefix_suffix_sensor.py`](dag_wasb_prefix_suffix_sensor.py)

`nix-dag-wasb-prefix-suffix-sensor` — waits for a blob matching a prefix **and** suffix.

**Teaches:** Extend a provider sensor rather than writing one; push the filter server-side.

→ **[Full detail: `docs/dag_wasb_prefix_suffix_sensor.md`](docs/dag_wasb_prefix_suffix_sensor.md)**  ·  📄 **[Source: `dag_wasb_prefix_suffix_sensor.py`](dag_wasb_prefix_suffix_sensor.py)**

---

## 8. [`dag_blob_to_ftps_stream.py`](dag_blob_to_ftps_stream.py)

`nix-dag-blob-to-ftps-stream` — streams a blob out to the FTPS server.

**Teaches:** Control flow is a property of the *call*, not the protocol — same server as #5, no pipe required.

→ **[Full detail: `docs/dag_blob_to_ftps_stream.md`](docs/dag_blob_to_ftps_stream.md)**  ·  📄 **[Source: `dag_blob_to_ftps_stream.py`](dag_blob_to_ftps_stream.py)**

---

## 9. [`dag_s3_prefix_suffix_sensor.py`](dag_s3_prefix_suffix_sensor.py)

`nix-dag-s3-prefix-suffix-sensor` — waits for an S3 object matching a prefix **and** suffix.

**Teaches:** subclass to fix the *hand-off*, not the matching — `S3KeySensor` already
does wildcards, but tells you nothing about what it matched.

→ **[Full detail: `docs/dag_s3_prefix_suffix_sensor.md`](docs/dag_s3_prefix_suffix_sensor.md)**  ·  📄 **[Source: `dag_s3_prefix_suffix_sensor.py`](dag_s3_prefix_suffix_sensor.py)**

---

## 10. [`dag_blob_to_s3_stream.py`](dag_blob_to_s3_stream.py)

`nix-dag-blob-to-s3-stream` — streams a blob from Azure Blob Storage to Amazon S3.

**Teaches:** the only cross-cloud hop here — two vendors' SDKs, no pipe, and why
boto3 tolerates a non-seekable source where the Azure SDK does not.

→ **[Full detail: `docs/dag_blob_to_s3_stream.md`](docs/dag_blob_to_s3_stream.md)**  ·  📄 **[Source: `dag_blob_to_s3_stream.py`](dag_blob_to_s3_stream.py)**

---

## 11. [`dag_s3_to_blob_stream.py`](dag_s3_to_blob_stream.py)

`nix-dag-s3-to-blob-stream` — streams an object from Amazon S3 to Azure Blob Storage.

**Teaches:** the one cross-service direction a provider already covers — override
`move_file` and inherit the rest. Also why `hasattr(body, "seek")` lies.

→ **[Full detail: `docs/dag_s3_to_blob_stream.md`](docs/dag_s3_to_blob_stream.md)**  ·  📄 **[Source: `dag_s3_to_blob_stream.py`](dag_s3_to_blob_stream.py)**

---

## 12. [`dag_blob_to_smb_stream.py`](dag_blob_to_smb_stream.py)

`nix-dag-blob-to-smb-stream` — streams a blob from Azure Blob Storage to an SMB share.

**Teaches:** what to do when the destination is a *writable* rather than a
readable — the source pushes with `readinto`, and still no pipe. Also why Samba
refuses SMB's atomic overwrite-rename.

→ **[Full detail: `docs/dag_blob_to_smb_stream.md`](docs/dag_blob_to_smb_stream.md)**  ·  📄 **[Source: `dag_blob_to_smb_stream.py`](dag_blob_to_smb_stream.py)**

---

## 13. [`dag_s3_to_smb_stream.py`](dag_s3_to_smb_stream.py)

`nix-dag-s3-to-smb-stream` — streams an object from Amazon S3 to an SMB share.

**Teaches:** the simplest transfer here — both SDKs already fit, so it is one
call. Also that boto3's threads are safe against a non-seekable destination, and
3.5x faster.

→ **[Full detail: `docs/dag_s3_to_smb_stream.md`](docs/dag_s3_to_smb_stream.md)**  ·  📄 **[Source: `dag_s3_to_smb_stream.py`](dag_s3_to_smb_stream.py)**

---

## 14. [`dag_s3_to_sftp_stream.py`](dag_s3_to_sftp_stream.py)

`nix-dag-s3-to-sftp-stream` — streams an object from Amazon S3 to the SFTP server.

**Teaches:** a third provider operator that stages to a temp file, fixed the same
way. Fastest path in the set at ~20 MiB/s.

→ **[Full detail: `docs/dag_s3_to_sftp_stream.md`](docs/dag_s3_to_sftp_stream.md)**  ·  📄 **[Source: `dag_s3_to_sftp_stream.py`](dag_s3_to_sftp_stream.py)**

---

## 15. [`dag_s3_to_ftps_stream.py`](dag_s3_to_ftps_stream.py)

`nix-dag-s3-to-ftps-stream` — streams an object from Amazon S3 to the FTPS server.

**Teaches:** when a provider operator needs **two** fixes — the staging logic
*and* a hardcoded hook that cannot do TLS.

→ **[Full detail: `docs/dag_s3_to_ftps_stream.md`](docs/dag_s3_to_ftps_stream.md)**  ·  📄 **[Source: `dag_s3_to_ftps_stream.py`](dag_s3_to_ftps_stream.py)**

---

## 16. [`dag_sftp_to_s3_stream.py`](dag_sftp_to_s3_stream.py)

`nix-dag-sftp-to-s3-stream` — streams a file from the SFTP server to Amazon S3.

**Teaches:** the exception among provider transfer operators — this one already
streams, but omits `prefetch`, which costs 4.8x.

→ **[Full detail: `docs/dag_sftp_to_s3_stream.md`](docs/dag_sftp_to_s3_stream.md)**  ·  📄 **[Source: `dag_sftp_to_s3_stream.py`](dag_sftp_to_s3_stream.py)**

---

## 17. [`dag_deadline_alert.py`](dag_deadline_alert.py)

`nix-dag-deadline-alert` — fires a callback when a run misses its deadline,
without failing it.

**Teaches:** Airflow 3 deadline alerts, how they differ from timeouts, and the
callback-import trap that makes them silently never fire.

→ **[Full detail: `docs/dag_deadline_alert.md`](docs/dag_deadline_alert.md)**  ·  📄 **[Source: `dag_deadline_alert.py`](dag_deadline_alert.py)**

---

## 18. [`dag_sftp_sensor.py`](dag_sftp_sensor.py)

`nix-dag-sftp-sensor` — waits for files matching a glob on the SFTP server.

**Teaches:** the one sensor here that needs **no** subclass — `SFTPSensor` already
does pattern matching *and* the downstream hand-off. Check before extending.

→ **[Full detail: `docs/dag_sftp_sensor.md`](docs/dag_sftp_sensor.md)**  ·  📄 **[Source: `dag_sftp_sensor.py`](dag_sftp_sensor.py)**

---

## 19. [`dag_cyclic.py`](dag_cyclic.py)

`nix-dag-cyclic` — a cyclic job: fires every 5 minutes, one run at a time.

**Teaches:** `max_active_runs=1` is what makes a schedule cyclic; a timeout arrives as a failure, not a callback.

→ **[Full detail: `docs/dag_cyclic.md`](docs/dag_cyclic.md)**  ·  📄 **[Source: `dag_cyclic.py`](dag_cyclic.py)**

---

## Running them

All except #19 are manual-trigger. From the UI use **Trigger DAG w/ config**; from
the CLI:

```bash
airflow dags unpause <dag_id>          # new DAGs land paused
airflow dags trigger <dag_id> --conf '{...}'
```

Every DAG has usable defaults, so `--conf` is optional — the commands below show
the default fixture on the left and a larger file on the right.

| # | Trigger |
|---|---|
| 1 | `airflow dags trigger nix-dag-ftps-simple-transfer` |
| 2 | `airflow dags trigger nix-dag-ftps-sensor --conf '{"filename":"probe.txt"}'` |
| 3 | `airflow dags trigger nix-dag-ftps-to-sftp-stream --conf '{"filename":"probe.txt"}'` |
| 4 | `airflow dags trigger nix-dag-sftp-to-blob-stream --conf '{"source_path":"<sftp-dir>/probe.*","blob_prefix":"incoming/"}'` |
| 5 | `airflow dags trigger nix-dag-ftps-to-blob-stream --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'` |
| 6 | `airflow dags trigger nix-dag-blob-to-sftp-stream --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'` |
| 7 | `airflow dags trigger nix-dag-wasb-prefix-suffix-sensor --conf '{"prefix":"incoming/","suffix":".csv"}'` |
| 8 | `airflow dags trigger nix-dag-blob-to-ftps-stream --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'` |
| 9 | `airflow dags trigger nix-dag-s3-prefix-suffix-sensor --conf '{"prefix":"probe/","suffix":".txt"}'` |
| 10 | `airflow dags trigger nix-dag-blob-to-s3-stream --conf '{"filename":"probe.txt","blob_prefix":"incoming/","s3_prefix":"incoming/"}'` |
| 11 | `airflow dags trigger nix-dag-s3-to-blob-stream --conf '{"filename":"probe.txt","s3_prefix":"incoming/","blob_prefix":"xcloud"}'` |
| 12 | `airflow dags trigger nix-dag-blob-to-smb-stream --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'` |
| 13 | `airflow dags trigger nix-dag-s3-to-smb-stream --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'` |
| 14 | `airflow dags trigger nix-dag-s3-to-sftp-stream --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'` |
| 15 | `airflow dags trigger nix-dag-s3-to-ftps-stream --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'` |
| 16 | `airflow dags trigger nix-dag-sftp-to-s3-stream --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'` |
| 17 | `airflow dags trigger nix-dag-deadline-alert` |
| 18 | `airflow dags trigger nix-dag-sftp-sensor --conf '{"pattern":"*.csv"}'` |
| 19 | `airflow dags unpause nix-dag-cyclic` — scheduled, not triggered |

### Testing with a large file

The bundled `probe.txt` is 118 bytes, which proves the wiring but says nothing
about throughput. To exercise the streaming path, put a larger file on the source
system and pass its name — nothing in the DAGs is size-specific.

```bash
# a 50 MiB fixture; use random data, not zeros — compressible filler lets TLS
# compression inflate the apparent throughput
dd if=/dev/urandom of=large50.bin bs=1m count=50

sftp <user>@<host>   # then: put large50.bin outgoing/
# and/or upload it to the FTPS server and the blob container
```

Then trigger with that filename:

```bash
airflow dags trigger nix-dag-ftps-to-sftp-stream  --conf '{"filename":"large50.bin"}'
airflow dags trigger nix-dag-sftp-to-blob-stream  --conf '{"source_path":"<sftp-dir>/large50.*","blob_prefix":"large/"}'
airflow dags trigger nix-dag-ftps-to-blob-stream  --conf '{"filename":"large50.bin","blob_prefix":"large/"}'
airflow dags trigger nix-dag-blob-to-sftp-stream  --conf '{"filename":"large50.bin","blob_prefix":"large/"}'
airflow dags trigger nix-dag-blob-to-ftps-stream  --conf '{"filename":"large50.bin","blob_prefix":"large/"}'
airflow dags trigger nix-dag-blob-to-s3-stream    --conf '{"filename":"large50.bin","blob_prefix":"large/","s3_prefix":"large/"}'
```

Two things that catch people out:

- **#4 needs a wildcard**, `large50.*` and not `large50.bin` — a bare filename
  hits the `listdir`-on-a-file trap described in
  [its page](docs/dag_sftp_to_blob_stream.md).
- **`blob_prefix` must match where the blob actually is.** The Blob-source DAGs
  read `<blob_prefix><filename>`, so a file uploaded under `large/` needs
  `"blob_prefix":"large/"`, not the `incoming/` default.

Each transfer logs a summary line, which is where the throughput comparison
lives:

```
[blob_to_ftps] done: 50.0 MiB in 3.7s (13.7 MiB/s) -> /upload/large50.bin
```

Expect the pipe-based #5 to be markedly slower than the direct-compose paths: it
moves 8 KiB at a time, while #4 uses 4 MiB blocks and #10 uses 8 MiB parts. That
difference is the point of the
[chunking table](#how-the-streaming-dags-chunk).

**A 50 MiB file does not exercise #5's block-upload path.** Azure switches from a
single buffered `read()` to streaming block upload above `max_single_put_size`
(64 MiB default), so a file *over* that threshold — say 100 MiB — is what proves
constant-memory behaviour there. Every other DAG streams at any size.

---

## Write-then-rename, and where it does not apply

The DAGs with a **filesystem-like destination** — SFTP, FTPS or SMB (#3, #6, #8,
#12, #13, #14, #15) — write to `<name>.part` and rename once the last byte lands. A consumer polling the drop
directory therefore never sees a partial file, and a failed run leaves an obvious
`.part` rather than a truncated file indistinguishable from a good one.

The rename is a metadata operation, so no data is re-transferred. Pass
`temp_suffix=""` to write directly to the final name.

The object-store DAGs (#4, #5, #9, #10, #11) deliberately **do not** do this:

| Destination | Rename | Cost | Atomic? |
|---|---|---|---|
| SFTP | `posix_rename()` | free | yes |
| FTPS | `rename()` (RNFR/RNTO) | free | yes, where supported |
| SMB via Samba | unlink + `rename()` | free | **no** — brief gap |
| Azure Blob | none — only `start_copy_from_url` | full server-side copy | — |
| S3 | none — `copy_object` + delete | full server-side copy | — |

Object stores have no rename, so the equivalent would double the data written and
the bill — and they do not expose a partially-uploaded object in the first place,
which is the problem the pattern solves.

**Use `posix_rename` on SFTP, not `rename`.** Plain `rename` fails when the
target exists:

```
OSError: Failure
```

which breaks a retry over a previous run's file. `posix_rename` overwrites
atomically. FTPS `rename` overwrote on the server tested here, but that is
server-dependent — a stricter server may need the target deleted first.

**SMB is the awkward one.** It has an atomic overwrite-rename
(`FILE_RENAME_INFORMATION` with `ReplaceIfExists`, issued by
`smbclient.replace`), but Samba refuses it with `STATUS_ACCESS_DENIED` even
between two files the account just created and can delete outright — so #12
unlinks the target first and then renames, giving up atomicity to keep the
pattern. See [its page](docs/dag_blob_to_smb_stream.md).

---

## How the streaming DAGs chunk

"Streaming" here always means **chunked**: one side reads a chunk, the other
writes it, and only then is the next fetched. Peak memory is one chunk, so a
2 GiB file costs the same as a 2 KiB one. Where a pipe is involved it also
applies backpressure — a slow destination stalls the source rather than letting
chunks pile up.

The chunk size comes from whichever call drives the loop, so it differs per DAG:

| DAG | Chunked by | Size | Configurable? |
|---|---|---|---|
| #3 FTPS → SFTP | `retrbinary` → pipe → `putfo` | 8 KiB | `CHUNK_SIZE` constant |
| #4 SFTP → Blob | Azure block upload | 4 MiB | SDK default; set on the client, not the call |
| #5 FTPS → Blob | `retrbinary` → pipe → Azure upload | 8 KiB | `chunk_size` argument |
| #6 Blob → SFTP | `putfo` | 32 KiB | **no** — paramiko hardcodes it |
| #8 Blob → FTPS | `storbinary` | 8 KiB | `chunk_size` argument |
| #10 Blob → S3 | boto3 multipart | 8 MiB parts | `TransferConfig` |
| #11 S3 → Blob | Azure block upload | 4 MiB | client config |
| #12 Blob → SMB | `readinto` (source pushes) | SDK default | no |
| #13 S3 → SMB | boto3 multipart, threaded | 8 MiB parts | `TransferConfig` |
| #14 S3 → SFTP | boto3 multipart, threaded | 8 MiB parts | `TransferConfig` |
| #15 S3 → FTPS | `storbinary` | 8 KiB | `chunk_size` argument |
| #16 SFTP → S3 | boto3 multipart + paramiko `prefetch` | 8 MiB parts | `TransferConfig` |

Only the DAGs whose underlying call accepts a `blocksize` expose a `chunk_size`
argument. #6 deliberately does not: `putfo` hardcodes `reader.read(32768)`, so an
argument would be a knob that does nothing.

**One exception to constant memory.** #5 uploads via the Azure SDK, which does a
single `stream.read(length)` for blobs at or under `max_single_put_size` (64 MiB
by default) — buffering the whole object. Above that threshold it switches to
block upload and streams properly. So #5 is constant-memory for large files and
up-to-64-MiB-buffered for small ones; every other DAG streams at any size.

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
