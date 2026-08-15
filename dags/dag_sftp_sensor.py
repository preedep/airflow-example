"""nix-dag-sftp-sensor — wait for files matching a pattern on the SFTP server."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-sftp-sensor

Waits for files matching a pattern on the SFTP server, then reports what it
found.

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"pattern": "*.csv"}
```

Defaults to `probe.*` in the source directory.

#### The sensor that needs no subclass

The other three sensor demos all extend their provider class:

| Demo | Why it subclasses |
|---|---|
| `nix-dag-ftps-sensor` | swap in a hook that trusts a private CA |
| `nix-dag-wasb-prefix-suffix-sensor` | the provider has **no** suffix matching |
| `nix-dag-s3-prefix-suffix-sensor` | the provider never tells you what matched |

`SFTPSensor` needs none of that, and seeing why is the point of this demo. It is
the most capable of the four out of the box:

- **`file_pattern`** takes an `fnmatch` glob, so `probe.*` or `*.csv` covers
  prefix *and* suffix in one argument — the capability the Azure demo has to add.
- **`python_callable`** runs on the successful poke and pushes `files_found` to
  XCom — the hand-off the S3 demo has to add.
- **`newer_than`** filters by modification time, which none of the others offer.

So this DAG is the shortest of the four: a stock sensor, a plain callable, and no
custom class anywhere. **Check what the provider already does before extending
it** — three of these four sensors needed help, and one did not.

#### `python_callable` is not a callback

It is easy to misread. The callable does not fire "when the sensor succeeds" as a
notification; its return value is packaged into the sensor's XCom alongside the
file list:

```python
PokeReturnValue(
    is_done=True,
    xcom_value={"files_found": [...], "decorator_return_value": <your return>},
)
```

So the downstream task reads `files_found` out of that dict, not from a separate
key. Note the extra nesting compared with the S3 demo, which pushes
`matched_keys` directly.

Two things about `op_kwargs` bite:

- The callable receives `files_found` **only if `op_kwargs` is non-empty** — the
  provider does `self.op_kwargs["files_found"] = files_found`, which is skipped
  when `op_kwargs` is `None`. Passing a dict is therefore load-bearing.
- Whatever is in that dict is passed **straight through** to the callable, so
  the signature must accept every key. A filler key the callable does not
  declare fails the task at run time:

  ```
  TypeError: on_files_found() got an unexpected keyword argument 'source'
  ```

  A parse check cannot catch that — the sensor only calls the callable on a
  *successful* poke, so it surfaces the first time something actually matches.

#### Matching is one directory deep

`get_files_by_pattern` lists `path` and `fnmatch`es each **filename**:

```python
for file in self.list_directory_with_attr(path):
    if fnmatch(file.filename, fnmatch_pattern):
```

There is no recursion and no server-side filtering — the directory is listed in
full on every poke, then matched locally. That is fine for a drop directory and
poor for a tree with thousands of entries.

Contrast the object-store sensors, where the prefix is pushed to the service and
only matching keys come back. SFTP has no equivalent, so a large directory is
listed every `poke_interval`.

| Setting | Effect |
|---|---|
| `file_pattern` | `fnmatch` glob on the filename, e.g. `*.csv` |
| `newer_than` | only files modified at or after this time |
| *(no pattern)* | `path` is treated as a single file and `isfile` checked |

#### Sensor settings

`mode="reschedule"`, `poke_interval=30`, `timeout=1800`. Reschedule frees the
worker slot between pokes — under KubernetesExecutor `poke` mode would hold a pod
idle for the whole wait. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

`SFTPSensor` also supports `deferrable=True`, which is lighter still, but needs a
triggerer. Reschedule works anywhere.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `sftp_test_001` | SFTP server (conn type `SFTP`) |

Set the connection to an address the **worker pods** can resolve. The watched
directory must exist — a missing path raises rather than waiting, since there is
nothing to poll.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SFTP_CONN_ID = "sftp_test_001"

# Adjust to your server. Must exist and be readable by the connection's user.
SFTP_DIR = "/home/airflowsftp/outgoing"

# Referenced by the docs; the live default is the Jinja fallback on the sensor's
# file_pattern field, since a callable never sees this constant.
DEFAULT_PATTERN = "probe.*"


# --------------------------------------------------------------------------- #
# Sensor callable
# --------------------------------------------------------------------------- #


def on_files_found(source: str = "", files_found: list[str] | None = None) -> dict:
    """Run on the successful poke; the return value lands in the sensor's XCom.

    Two things about the signature are load-bearing:

    - `files_found` arrives only because the sensor is given a **non-empty**
      `op_kwargs`. The provider assigns into that dict rather than creating one,
      so an empty `op_kwargs` silently drops the file list.
    - Every key in `op_kwargs` is passed straight through, so the callable must
      accept them all. A filler key the signature does not declare fails the
      task with `TypeError: got an unexpected keyword argument`.
    """
    task_log = logging.getLogger("airflow.task")
    names = files_found or []

    task_log.info("[sftp_sensor] %s matched %d file(s): %s", source, len(names), names)
    # sorted() keeps the XCom stable across runs that match the same set.
    return {"count": len(names), "files": sorted(names)}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def report_matches(ti=None, **context):
    """Report each matched file with its size and modification time."""
    task_log = logging.getLogger("airflow.task")

    # The sensor's XCom is a dict with the file list under "files_found", not the
    # bare list — see the doc_md note on PokeReturnValue.
    pushed = ti.xcom_pull(task_ids="wait_for_files") or {}
    names = pushed.get("files_found") if isinstance(pushed, dict) else None

    # Defensive: the sensor only returns True with a non-empty match, so an empty
    # pull means the contract broke (task renamed, XCom cleared) rather than
    # "nothing matched". Fail instead of reporting a misleading zero.
    if not names:
        raise ValueError(f"sensor succeeded but pushed no files: {pushed!r}")

    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    results = []
    with hook.get_managed_conn() as sftp_client:
        for path in sorted(names):
            attrs = sftp_client.stat(path)
            results.append({"path": path, "size": attrs.st_size})
            task_log.info("[sftp_sensor] %s — %s bytes", path, attrs.st_size)

    return {"directory": SFTP_DIR, "count": len(results), "files": results}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-sftp-sensor",
    description="Wait for files matching a pattern on the SFTP server, then report them",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "sftp", "sensor"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    wait_for_files = SFTPSensor(
        task_id="wait_for_files",
        sftp_conn_id=SFTP_CONN_ID,
        path=SFTP_DIR,
        # fnmatch glob, so this covers prefix and suffix in one argument — no
        # subclass needed, unlike the Azure and S3 sensor demos.
        file_pattern=f"{{{{ dag_run.conf.get('pattern', '{DEFAULT_PATTERN}') }}}}",
        python_callable=on_files_found,
        # Load-bearing: the provider assigns files_found *into* this dict, so a
        # None/empty op_kwargs means the callable never sees the file list.
        op_kwargs={"source": "nix-dag-sftp-sensor"},
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=30,
        timeout=60 * 30,  # 30 minutes
        doc_md="""
Polls the directory for files matching the `fnmatch` pattern.

**No subclass.** `SFTPSensor` already does prefix+suffix matching via
`file_pattern` and already hands the match list downstream via
`python_callable` — the two things the Azure and S3 sensor demos have to add.

The listing is **client-side and one directory deep**: the whole directory is
listed on every poke and matched locally, with no server-side filter and no
recursion. Fine for a drop directory, poor for a large tree.

Returns `False` and keeps waiting when nothing matches.
""",
    )

    report = PythonOperator(
        task_id="report_matches",
        python_callable=report_matches,
        doc_md="""
Reads the matched paths from the sensor's XCom and `stat`s each one.

Note the XCom shape: `PokeReturnValue` wraps the list as
`{"files_found": [...], "decorator_return_value": ...}`, so this reads
`files_found` out of a dict rather than pulling a bare list.

Uses the paths the sensor captured rather than re-listing, so the report matches
exactly what satisfied the sensor even if the directory changed in between.
""",
    )

    # report consumes the sensor's XCom — a data dependency, not just order.
    wait_for_files >> report
