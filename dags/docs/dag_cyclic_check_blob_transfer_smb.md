# `dag_cyclic_check_blob_transfer_smb.py`

📄 **[Source: `dag_cyclic_check_blob_transfer_smb.py`](../dag_cyclic_check_blob_transfer_smb.py)**


`nix-dag-cyclic-check-blob-transfer-smb` — a **cyclic pickup job**. Every 5 minutes
during business hours it checks a blob prefix; if anything is waiting it streams it
onto an SMB share, and if not the run ends quietly.

```
every 5 min, 08:00-19:00 Mon-Fri
        │
        ▼
   check_for_blobs ──(nothing)──▶ skipped, run ends success
        │
     (found)
        ▼
   stream_to_smb ──readinto──▶ SMB share
        │
        ▼
   report_cycle
```

It is scheduled, so just unpause it:

```bash
airflow dags unpause nix-dag-cyclic-check-blob-transfer-smb
```

This is [#22](dag_cyclic_check_s3_transfer_sftp.md) with both endpoints swapped —
same cyclic skeleton, [#12](dag_blob_to_smb_stream.md)'s transfer. **Read the two
side by side.** The scheduling half ports over unchanged; the storage half does not,
and the differences are the interesting part:

| | #22 (S3 → SFTP) | #23 (Blob → SMB) |
|---|---|---|
| Listing trap | zero-byte directory markers | `get_blobs_list` is *hierarchical* |
| Archive copy | `copy_object` — synchronous | `start_copy_from_url` — **asynchronous** |
| Rename | `posix_rename`, atomic | unlink-then-rename, **not** atomic |
| Composition | destination pulls | destination is a **writable** |

---

## Pattern: a cyclic poll wants a short-circuit, not a sensor

Unchanged from [#22](dag_cyclic_check_s3_transfer_sftp.md), and the reasoning is
worth repeating because the provider's own sensor (`WasbPrefixSensor`, used in
[#7](dag_wasb_prefix_suffix_sensor.md)) makes the wrong build look obvious.

A sensor **waits**. This DAG already re-checks every 5 minutes, so waiting inside
the run duplicates the schedule and holds the one slot `max_active_runs=1` allows.
And a sensor that finds nothing ends the run **failed** on timeout — an empty prefix
at 08:05 on a quiet morning is a normal outcome, not an incident.

`ShortCircuitOperator` returns `False` instead: downstream is skipped and the run
ends `success`, so a quiet cycle is visibly distinct from a broken one.

---

## Trap: `get_blobs_list` is a hierarchical listing

This one costs an afternoon if you take the obvious method name at face value:

```python
def get_blobs_list(self, container_name, prefix=None, include=None, delimiter="/"):
    blobs = container.walk_blobs(name_starts_with=prefix, delimiter=delimiter, ...)
```

`walk_blobs` with `delimiter="/"` is the *hierarchical* listing. It returns virtual
directory prefixes as entries alongside real blobs, so a container laid out as
`incoming/2026/a.csv` yields `incoming/2026/` — and handing that to the transfer
tries to download a directory.

Use **`get_blobs_list_recursive`**, which calls `list_blobs` and returns real blob
names only. Its `endswith` argument does the suffix filter in the same pass:

```python
blobs = hook.get_blobs_list_recursive(
    container_name=CONTAINER, prefix=prefix, endswith=suffix
)
```

The S3 demo has the same *class* of problem with a different shape — the zero-byte
key ending in `/` that the console creates for an "empty folder". Both providers let
you list something that is not a file; neither warns you.

---

## Trap: the archive copy is asynchronous

The idempotency requirement is identical to #22 — poll the same prefix 132 times a
day and, without marking work as done, re-send everything forever. Neither service
has a move, so copy-then-delete is the move.

But a naive port of the S3 version **loses data here**:

```python
copy = destination.start_copy_from_url(source.url)
source.delete_blob()                                  # ← races the copy
```

`start_copy_from_url` returns as soon as the service *accepts* the job. Its result
carries `copy_status`, which is `'success'` only if the copy completed
synchronously — otherwise `'pending'`. Deleting the source at that point can destroy
the blob before it has been read.

So `_archive` polls before deleting:

```python
status = copy.get("copy_status")
while status == "pending":
    if time.monotonic() > deadline:
        destination.abort_copy(copy["copy_id"])
        raise TimeoutError(...)                       # source left in place
    time.sleep(COPY_POLL_SECONDS)
    status = destination.get_blob_properties().copy.status

if status != "success":
    raise ValueError(...)                             # source left in place
source.delete_blob()
```

Small blobs usually do complete synchronously, so the loop is normally skipped
entirely — which is exactly why this is dangerous. It passes every demo-sized test
and loses a large file in production.

On timeout the copy is **aborted** rather than abandoned, so a half-copied blob does
not sit accruing storage. Every failure path leaves the source untouched, so the
next cycle retries it.

---

## Trap: the archive must not be inside the polled prefix

Shared with #22 in principle, but easier to hit here because both prefixes live in
one container:

```python
if ARCHIVE_PREFIX:
    blobs = [name for name in blobs if not name.startswith(ARCHIVE_PREFIX)]
```

Without it, an archive nested under the polled prefix means the cycle re-transfers
its own output — forever, every 5 minutes.

---

## The streaming half

Unchanged from [#12](dag_blob_to_smb_stream.md), including its two oddities.

**The composition is inverted.** Every other transfer here pairs a readable source
with a destination that pulls. `SambaHook.open_file()` returns a file object opened
for *writing*, so nothing is there to pull. Azure's `StorageStreamDownloader`
supplies the missing half with `readinto(stream)`, which pushes into any writable:

```python
with self.samba_hook.open_file(target, mode="wb") as handle:
    written = downloader.readinto(handle)
```

Still no pipe — one side drives the loop and the other is a plain file object. A
pipe is only needed when both sides push, or both pull. `readinto` returns the byte
count, which is what the size check compares.

**Delete-then-rename, not `replace()`.** Samba refuses SMB's atomic overwrite-rename
(`FILE_RENAME_INFORMATION` with `ReplaceIfExists`) with `STATUS_ACCESS_DENIED`, even
between two files the account just created and can delete outright. `rename` onto a
*free* name works, so the target is unlinked first. There is a brief window where
neither name holds the final file — so unlike SFTP's `posix_rename` in #22, this is
**not** atomic. It still prevents a consumer seeing a *partial* file.

Peak memory is one SDK chunk. A download has no `max_single_put_size` equivalent, so
this direction is constant-memory at any size — unlike the Azure *upload* paths in
#5 and #11.

Batch-specific changes versus #12: `execute` iterates the names the short-circuit
found rather than one templated `blob_name`, and each blob is archived immediately
after its own transfer, so a failure partway leaves the already-moved files archived
and the retry resumes at the remainder.

---

## Schedule

```python
schedule="*/5 8-18 * * 1-5"     # 08:00 through 18:55, Mon-Fri
```

`8-19` looks like the obvious spelling of "08:00 to 19:00" and is wrong: cron hour
ranges are inclusive, so it keeps firing at 19:00, 19:05 … 19:55 — a full hour past
the window. The last interval that *starts* inside the window is 18:55.

Verified rather than assumed:

```
first: 2026-08-20 08:00:00   last: 2026-08-20 18:55:00   count/day: 132
weekend fires: 0             next after Fri: 2026-08-24 08:00:00
```

Cron fires in the **scheduler's timezone**. If your operators think in local time
and the deployment runs UTC, set `timezone` on the DAG or shift the hour range.

`max_active_runs=1` is what makes this cyclic: a transfer that overruns 5 minutes
queues the next check instead of running beside it, so the same blob is never picked
up twice concurrently. `dagrun_timeout` bounds a wedged run, which would otherwise
block the cycle indefinitely.

---

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `smb_test_001` | SMB destination (conn type `samba`) |

For the SMB connection: **host** is an address the worker pods can resolve,
**schema** is the share name, **login**/**password** are the SMB credentials.
`SambaHook` joins the UNC path itself, so paths are relative to the share.

The share directory must be writable by the SMB user; a directory owned by another
account yields `STATUS_ACCESS_DENIED`, which is a server-side filesystem permission
rather than an Airflow problem — the share can list fine and still refuse writes.

For SAS auth on the Azure side the token goes in **extra** as `sas_token`, and
`login` is the storage account name.

To force a cycle outside the schedule, or to narrow what it picks up:

```bash
airflow dags trigger nix-dag-cyclic-check-blob-transfer-smb \
  --conf '{"prefix":"incoming/","suffix":".csv"}'
```

---

## Cost note

132 runs a day while unpaused, each making at least one list call, plus egress for
data leaving the region. Pause it when you are done.
