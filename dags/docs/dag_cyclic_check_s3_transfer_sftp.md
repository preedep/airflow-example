# `dag_cyclic_check_s3_transfer_sftp.py`

📄 **[Source: `dag_cyclic_check_s3_transfer_sftp.py`](../dag_cyclic_check_s3_transfer_sftp.py)**


`nix-dag-cyclic-check-s3-transfer-sftp` — a **cyclic pickup job**. Every 5 minutes
during business hours it checks an S3 prefix; if anything is waiting it streams it
out to the SFTP server, and if not the run ends quietly.

```
every 5 min, 08:00-19:00 Mon-Fri
        │
        ▼
   check_for_files ──(nothing)──▶ skipped, run ends success
        │
     (found)
        ▼
   stream_to_sftp ──download_fileobj──▶ SFTP server
        │
        ▼
   report_cycle
```

This is [#21](dag_cyclic.md) and [#14](dag_s3_to_sftp_stream.md) combined: the
cyclic scheduling of one, the streaming override of the other. It is scheduled, so
just unpause it:

```bash
airflow dags unpause nix-dag-cyclic-check-s3-transfer-sftp
```

---

## Pattern: a cyclic poll wants a short-circuit, not a sensor

The obvious build is `S3KeySensor` → transfer. Both of this repo's sensor
demos ([#9](dag_s3_prefix_suffix_sensor.md), [#18](dag_sftp_sensor.md)) do exactly
that, and it is the wrong shape here.

A sensor **waits**. This DAG already re-checks every 5 minutes, so waiting inside
the run duplicates the schedule and holds the one slot `max_active_runs=1` allows.
Worse, a sensor that finds nothing ends the run **failed** on timeout — and an
empty prefix at 08:05 on a quiet morning is a normal outcome, not an incident. A
column of red runs in the grid trains everyone to stop reading it.

`ShortCircuitOperator` returns a boolean instead:

```python
def check_for_files(ti=None, **context):
    keys = [...]
    if not keys:
        return False                          # skip downstream, run ends success
    ti.xcom_push(key="pending_keys", value=batch)
    return True
```

`False` skips everything downstream and the run ends `success`, so a quiet cycle is
visibly distinct from a broken one.

**The rule of thumb:** a sensor is for a run that *should* wait — a one-shot
pipeline whose whole purpose is blocked until the file arrives. A cycle already
re-checks on its own, so it should ask once and leave.

---

## Pattern: the schedule's last hour is off by one

```python
schedule="*/5 8-18 * * 1-5"     # 08:00 through 18:55, Mon-Fri
```

`8-19` looks like the obvious spelling of "08:00 to 19:00" and is wrong: cron hour
ranges are inclusive, so it keeps firing at 19:00, 19:05 … 19:55 — a full hour past
the window. The last interval that *starts* inside the window is 18:55, so the
range stops at 18.

Verified rather than assumed:

```
first: 2026-08-20 08:00:00   last: 2026-08-20 18:55:00   count/day: 132
weekend fires: 0             next after Fri: 2026-08-24 08:00:00
```

Cron fires in the **scheduler's timezone**. If your operators think in local time
and the deployment runs UTC, set `timezone` on the DAG or shift the hour range — a
window that reads correctly in the source file and fires at the wrong hour on the
cluster is the usual way this goes wrong.

---

## Pattern: a repeating transfer owes you idempotency

A one-shot transfer moves one named object and stops. A cycle lists the *same
prefix* every 5 minutes, so without something to mark work as done it re-sends
everything, forever — 132 times a day.

S3 has no move operation, so copy-then-delete is the move:

```python
s3_hook.copy_object(source_bucket_key=key, dest_bucket_key=archived_key, ...)
s3_hook.delete_objects(bucket=self.s3_bucket, keys=[key])
```

Two details that matter on retry:

- **Archive per file, not per batch.** If file 7 of 10 fails, the first six stay
  archived and the retry resumes at the remainder rather than re-sending all ten.
- **Archive only after the size check passes.** A failed transfer leaves the object
  in the source prefix for the next cycle to pick up, which is what you want — the
  cycle *is* the retry mechanism.

`MAX_FILES_PER_RUN` caps the batch, so a backlog drains over several cycles instead
of one run holding a pod for an hour and blocking the next check.

`sorted()` before slicing makes the batch deterministic: a retry picks the same
files in the same order as the attempt it replaces.

Set `ARCHIVE_PREFIX = ""` to leave objects in place. Every cycle then re-transfers
everything — fine for watching a demo, wrong in production.

---

## Trap: the directory marker that is not a file

The S3 console creates a zero-byte key ending in `/` when you make an "empty
folder". It comes back from a list like any other key, and transferring it produces
a zero-byte file on the SFTP side with a name ending in nothing:

```python
if not item["Key"].endswith("/")
```

There are no folders in S3 — `incoming/` is a literal string prefix, so nested keys
under it match too. That is worth knowing before pointing this at a bucket with
structure underneath the polled prefix.

---

## The streaming override

Unchanged in substance from [#14](dag_s3_to_sftp_stream.md) — the provider's
`S3ToSFTPOperator` stages the whole object on the worker pod's disk:

```python
with NamedTemporaryFile("w") as f:
    s3_client.download_file(self.s3_bucket, self.s3_key, f.name)
    sftp_client.put(f.name, self.sftp_path, confirm=self.confirm)
```

`download_fileobj` **writes into** any writable and paramiko's `open(..., "wb")`
**is** one, so the streaming version is a single call with no pipe and no read loop:

```python
with sftp_client.open(target, "wb") as handle:
    handle.set_pipelined(True)
    s3_client.download_fileobj(self.s3_bucket, key, handle)
```

Peak memory is a part buffer (8 MiB), not the object size.

Two changes from #14, both because this one moves a **batch**:

- `execute` iterates the keys the short-circuit found rather than one templated
  `s3_key`. The keys come from XCom, not from re-listing — objects land and are
  archived continuously, so a re-list could return a different set than the one the
  check approved.
- **One SFTP session for the whole batch**, opened once outside the loop. Ten files
  is ten transfers, not ten handshakes.

Write-then-`posix_rename` per file is kept: a consumer watching the drop directory
never sees a partial file, and `posix_rename` overwrites atomically where plain
SFTP `rename` fails when the target exists.

---

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `sftp_test_001` | SFTP destination (conn type `SFTP`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

The SFTP destination directory must exist and be writable by the connection's user;
`open()` does not create parent directories.

To force a cycle outside the schedule, or to narrow what it picks up:

```bash
airflow dags trigger nix-dag-cyclic-check-s3-transfer-sftp \
  --conf '{"prefix":"incoming/","suffix":".csv"}'
```

---

## Cost note

132 runs a day while unpaused, each making at least one `list_objects_v2` call, plus
S3 egress charges for everything transferred out of AWS. Pause it when you are done.
