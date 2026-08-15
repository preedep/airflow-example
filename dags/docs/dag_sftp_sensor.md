# `dag_sftp_sensor.py`

📄 **[Source: `dag_sftp_sensor.py`](../dag_sftp_sensor.py)**

`nix-dag-sftp-sensor` — waits for files matching a glob on the SFTP server, then
reports what it found.

```bash
airflow dags trigger nix-dag-sftp-sensor --conf '{"pattern":"*.csv"}'
```

Defaults to `probe.*`. Two verified runs:

```
[sftp_sensor] nix-dag-sftp-sensor matched 1 file(s): ['/home/.../outgoing/probe.txt']
[sftp_sensor] /home/.../outgoing/probe.txt — 118 bytes

[sftp_sensor] nix-dag-sftp-sensor matched 1 file(s): ['/home/.../outgoing/sample.csv']
```

## The sensor that needs no subclass

The other three sensor demos all extend their provider class:

| Demo | Why it subclasses |
|---|---|
| [#2 FTPS](dag_ftps_sensor.md) | swap in a hook that trusts a private CA |
| [#7 WASB](dag_wasb_prefix_suffix_sensor.md) | the provider has **no** suffix matching |
| [#9 S3](dag_s3_prefix_suffix_sensor.md) | the provider never tells you what matched |

`SFTPSensor` needs none of that, and seeing why is the point of this demo. It is
the most capable of the four out of the box:

- **`file_pattern`** takes an `fnmatch` glob, so `probe.*` or `*.csv` covers
  prefix *and* suffix in one argument — the capability #7 has to add.
- **`python_callable`** runs on the successful poke and pushes `files_found` to
  XCom — the hand-off #9 has to add.
- **`newer_than`** filters by modification time, which none of the others offer.

So this is the shortest of the four: a stock sensor, a plain callable, no custom
class anywhere.

**Check what the provider already does before extending it.** Three of these four
sensors needed help and one did not — and the only way to know which is to read
the class.

## `python_callable` is not a callback

Easy to misread. The callable does not fire "when the sensor succeeds" as a
notification; its return value is packaged into the sensor's XCom *alongside* the
file list:

```python
PokeReturnValue(
    is_done=True,
    xcom_value={"files_found": [...], "decorator_return_value": <your return>},
)
```

So a downstream task reads `files_found` out of that dict:

```python
pushed = ti.xcom_pull(task_ids="wait_for_files") or {}
names = pushed.get("files_found")
```

Note the extra nesting compared with #9, which pushes `matched_keys` directly.

## Two ways `op_kwargs` bites

**1. An empty `op_kwargs` silently drops the file list.** The provider does
`self.op_kwargs["files_found"] = files_found` — it assigns *into* the dict rather
than creating one, so with `op_kwargs=None` the callable never receives the
matches. Passing a non-empty dict is load-bearing.

**2. Every key is passed straight through**, so the callable must accept them
all. This DAG's first run failed exactly here:

```
TypeError: on_files_found() got an unexpected keyword argument 'source'
```

A parse check cannot catch either problem — the sensor only calls the callable on
a **successful poke**, so both surface the first time something actually matches.
That may be long after deployment, on a DAG that has looked healthy while
waiting.

## Matching is one directory deep

`get_files_by_pattern` lists `path` and `fnmatch`es each **filename**:

```python
for file in self.list_directory_with_attr(path):
    if fnmatch(file.filename, fnmatch_pattern):
```

No recursion, and no server-side filtering — the whole directory is listed on
every poke and matched locally.

That is the opposite of the object-store sensors, where the prefix goes to the
service and only matching keys come back. SFTP has no equivalent, so a large
directory is listed in full every `poke_interval`. Fine for a drop directory,
poor for a tree with thousands of entries.

| Setting | Effect |
|---|---|
| `file_pattern` | `fnmatch` glob on the filename, e.g. `*.csv` |
| `newer_than` | only files modified at or after this time |
| *(no pattern)* | `path` is treated as a single file and `isfile` checked |

## A provider deprecation worth knowing

`apache-airflow-providers-sftp` 5.7.3 imports the deprecated
`airflow.utils.timezone` shims at module scope, so merely importing `SFTPSensor`
emits `DeprecatedImportWarning`. Verified identical on the server image, so it is
not a local-venv artifact — it is upstream's to fix, and no DAG can avoid it
short of not using the provider.

This repo's integrity test previously failed on it. The test now ignores
deprecations raised **inside provider code** while still failing on deprecated
imports or kwargs in a DAG file itself, verified both ways with a throwaway probe
DAG.

## Sensor settings

`mode="reschedule"`, `poke_interval=30`, `timeout=1800`. Reschedule frees the
worker slot between pokes — under KubernetesExecutor `poke` mode would hold a pod
idle for the whole wait. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

`SFTPSensor` also supports `deferrable=True`, lighter still, but it needs a
triggerer. Reschedule works anywhere.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `sftp_test_001` | SFTP server (conn type `SFTP`) |

Set the connection to an address the **worker pods** can resolve. The watched
directory must exist — a missing path raises rather than waiting, since there is
nothing to poll.

---

[← back to the DAG index](../README.md)
