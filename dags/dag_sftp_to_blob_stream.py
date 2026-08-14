"""nix-dag-sftp-to-blob-stream — stream a file from the SFTP server to Azure Blob Storage."""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-sftp-to-blob-stream

Streams a file **SFTP → Azure Blob Storage** without staging it on disk.

```
SFTP server  ──open──▶  dag (stream)  ──upload──▶  Blob container
 SFTP_DIR/<file>                                   <blob_prefix><file>
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"source_path": "/home/airflowsftp/outgoing/*.csv", "blob_prefix": "incoming/"}
```

Defaults to `SFTP_DIR/probe.*` and prefix `incoming/`.

#### `source_path` must be a directory or a wildcard — never a bare filename

This one fails confusingly. With no `*` in the path, the inherited
`get_tree_behavior` passes it unchanged to `SFTPHook.get_tree_map`, which
`listdir`s it. `listdir` on a *regular file* returns SFTP status 2, surfacing as:

```
FileNotFoundError: [Errno 2] No such file
  ... get_tree_map -> walktree -> list_directory_with_attr -> listdir_attr
```

The file is right there and readable — the path is simply being treated as a
directory. Read the traceback for `walktree`/`listdir_attr`: that means "not a
directory", not "not found". Use `.../probe.*` for a single file, `.../*.csv` for
a set, or a bare directory for everything in it.

#### Use `get_managed_conn()`, never `get_conn()`

`SFTPHook`'s public methods are wrapped in `handle_connection_management`, which
enters `get_managed_conn()` and **closes the session on exit**. The inherited
`get_sftp_files_map()` lists the source that way, so by the time the copy starts
that connection is gone. Reaching for `get_conn()` then returns the closed client
and the first call fails with:

```
OSError: Socket is closed
  ... copy_files_to_wasb -> sftp_client.stat -> _send_packet -> channel.send
```

`get_managed_conn()` is refcounted, so opening it once around the whole loop
holds one session open for every file rather than reconnecting per file.

#### Why a subclass, not the stock operator

The provider ships `SFTPToWasbOperator`, and everything except the copy itself is
worth keeping — wildcard expansion, blob naming, `move_object`, templated fields.
Its `copy_files_to_wasb`, though, downloads each file to a `NamedTemporaryFile`
first and only then uploads it, so the whole file lands on the worker pod's disk.

`StreamingSFTPToWasbOperator` overrides that one method. It opens the remote file
over SFTP and hands the file object straight to `WasbHook.upload`, which reads it
in chunks and block-uploads as it goes. Nothing touches local disk, and peak
memory is a chunk rather than the file size.

Overriding a method beats reimplementing the operator: a future provider fix to
wildcard handling or blob naming still applies here.

#### Memory and disk

| | stock operator | this DAG |
|---|---|---|
| local disk | full file size | none |
| peak memory | one block | one block |

The block size is the Azure SDK's 4 MiB default. It is a property of the client,
not of a single upload — passing `max_block_size` to the upload call raises
`TypeError: Session.request() got an unexpected keyword argument`.

`remote.prefetch(size)` pipelines the SFTP reads so the reader is not stalled one
round-trip per block — without it, a high-latency link makes the transfer crawl
regardless of bandwidth.

`MAX_BYTES` caps a single file; anything larger fails before the transfer starts
rather than running unbounded.

#### Overwrite behaviour

`wasb_overwrite_object=True`, so re-running replaces the blob instead of failing
with `ResourceExistsError`. That keeps a retry idempotent — a task that died
mid-upload leaves a partial blob, and the retry must be able to replace it.

#### Source is not deleted

`move_object=False`, so a successful run leaves the file on the SFTP server and
re-running re-transfers it. Deliberate for a demo: deleting is destructive and a
failed upload must not lose the only copy. Set `move_object=True` for a real
pickup pattern, once `verify` is trusted to catch a bad transfer.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `sftp_test_001` | SFTP source (conn type `SFTP`) |
| Connection | `wasb-nickstorageairflow002` | Azure Blob destination (conn type `wasb`) |

Set the SFTP connection to an address the **worker pods** can resolve. A hostname
that works from a laptop (VPN, mesh network, `/etc/hosts`) often does not resolve
inside the cluster.

For SAS auth the token goes in the Azure connection's **extra** as `sas_token`,
and `login` is the **storage account name** — the hook builds the account URL
from it. The container is not part of the connection; it is passed per operation.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SFTP_CONN_ID = "sftp_test_001"
WASB_CONN_ID = "wasb-nickstorageairflow002"

CONTAINER = "data001"

# Adjust to your server. SFTP_DIR must be readable by the SFTP connection's user.
SFTP_DIR = "/home/airflowsftp/outgoing"

# The default source. This is a *wildcard* path, not a bare filename, and that is
# load-bearing: with no "*" the operator passes the path straight to
# `SFTPHook.get_tree_map`, which calls `listdir` on it. `listdir` on a regular
# file returns SFTP status 2, so a bare file path fails with a misleading
# `FileNotFoundError` even when the file is plainly there.
#
# `sftp_source_path` therefore has to be a directory or a wildcard. `probe.*`
# selects the single fixture while keeping the path in the wildcard branch.
DEFAULT_SOURCE = f"{SFTP_DIR}/probe.*"

# Peak memory of a transfer is one block, not the file size — the Azure client
# reads a block from the SFTP handle, uploads it, then reads the next, so chunks
# never accumulate. The block size is the SDK's own 4 MiB default and is set on
# the client, not per upload; see the note in copy_files_to_wasb.
MAX_BYTES = 2 * 1024**3  # 2 GiB — fail rather than transfer unbounded


# --------------------------------------------------------------------------- #
# Operator override
# --------------------------------------------------------------------------- #


class StreamingSFTPToWasbOperator(SFTPToWasbOperator):
    """`SFTPToWasbOperator` that pipes each file instead of staging it on disk.

    Only `copy_files_to_wasb` changes; discovery, wildcard expansion, blob naming
    and the `move_object` delete all stay with the provider.
    """

    def copy_files_to_wasb(self, sftp_files: list) -> list[str]:
        # Built here rather than cached on the instance: execute() runs once per
        # task pod, so there is nothing to reuse across calls.
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        uploaded_files = []
        # Recorded per file so the verify task can compare against what the
        # source actually reported, not against a re-read that may have changed.
        transferred: list[dict[str, Any]] = []

        # `get_managed_conn()`, not `get_conn()`. Every decorated SFTPHook method
        # wraps itself in this context manager and closes the session on exit, so
        # by the time the inherited `get_sftp_files_map()` has listed the source
        # the connection it used is already shut. Calling `get_conn()` afterwards
        # hands back that dead client and the first `stat` fails with
        # `OSError: Socket is closed`.
        #
        # The context manager is refcounted, so opening it once here keeps a
        # single session alive across every file instead of reconnecting per file.
        with self.sftp_hook.get_managed_conn() as sftp_client:
            for file in sftp_files:
                # stat first: an oversized file should fail before a single byte
                # moves, not halfway through an upload that has to be cleaned up.
                size = sftp_client.stat(file.sftp_file_path).st_size
                if size > MAX_BYTES:
                    raise ValueError(
                        f"{file.sftp_file_path} is {size} bytes, over the {MAX_BYTES} limit"
                    )

                self.log.info(
                    "[sftp_to_blob] streaming %s -> wasb://%s/%s (%s bytes)",
                    file.sftp_file_path,
                    self.container_name,
                    file.blob_name,
                    size,
                )

                # The remote file object is the upload source. upload() reads it
                # in max_block_size chunks and stages each as a block, so neither
                # side ever holds the whole file.
                with sftp_client.open(file.sftp_file_path, "rb") as remote:
                    # Without prefetch paramiko waits a full round-trip per read,
                    # which caps throughput at chunk/latency regardless of bandwidth.
                    remote.prefetch(size)
                    # No max_block_size here. It looks like an upload argument but
                    # is read from the *client's* StorageConfiguration, so
                    # upload_blob() forwards it as an unknown kwarg all the way to
                    # Session.request() and fails with `unexpected keyword
                    # argument 'max_block_size'`. The SDK's own 4 MiB default is
                    # the block size we wanted anyway; changing it means
                    # configuring the BlobServiceClient, which WasbHook builds
                    # internally.
                    wasb_hook.upload(
                        container_name=self.container_name,
                        blob_name=file.blob_name,
                        data=remote,
                        # Supplying length lets the SDK skip seeking the stream to
                        # measure it — a pipe-like SFTP handle would fail that seek.
                        length=size,
                        create_container=self.create_container,
                        max_concurrency=1,  # >1 requires a seekable source
                        **self.load_options,
                    )

                uploaded_files.append(file.sftp_file_path)
                transferred.append(
                    {"source": file.sftp_file_path, "blob": file.blob_name, "size": size}
                )

        # Returned as the operator's XCom so verify knows exactly which blobs
        # this run wrote; the base class returns only source paths, which is not
        # enough to check the destination.
        self.transferred = transferred
        return uploaded_files

    def execute(self, context) -> list[dict[str, Any]]:
        super().execute(context)
        return self.transferred


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def verify_transfer(ti=None, **context):
    """Confirm every blob exists in the container with the size we sent."""
    task_log = logging.getLogger("airflow.task")

    # Trusts the upstream XCom for names and byte counts rather than re-deriving
    # them from conf — this verifies what was actually transferred.
    transferred = ti.xcom_pull(task_ids="stream_transfer") or []
    if not transferred:
        raise ValueError("stream_transfer reported no files — nothing matched the source path")

    hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
    # Separate pod, so a fresh client: this is an independent read of service
    # state, not a re-check of the transfer task's own handle.
    client = hook.get_conn().get_container_client(CONTAINER)

    for item in transferred:
        actual = client.get_blob_client(item["blob"]).get_blob_properties().size
        if actual != item["size"]:
            raise ValueError(
                f"{item['blob']}: expected {item['size']} bytes, found {actual}"
            )
        task_log.info("[sftp_to_blob] verified %s — %s bytes", item["blob"], actual)

    return {"container": CONTAINER, "count": len(transferred), "files": transferred}


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-sftp-to-blob-stream",
    description="Stream a file from the SFTP server to Azure Blob Storage",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "sftp", "azure", "wasb", "transfer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    stream = StreamingSFTPToWasbOperator(
        task_id="stream_transfer",
        sftp_conn_id=SFTP_CONN_ID,
        wasb_conn_id=WASB_CONN_ID,
        sftp_source_path=f"{{{{ dag_run.conf.get('source_path', '{DEFAULT_SOURCE}') }}}}",
        container_name=CONTAINER,
        blob_prefix="{{ dag_run.conf.get('blob_prefix', 'incoming/') }}",
        wasb_overwrite_object=True,  # a retry must be able to replace a partial blob
        move_object=False,  # leave the source file in place
        create_container=False,  # the container is expected to exist
        doc_md="""
Opens each source file over SFTP and pipes it straight into Azure Blob Storage.

Subclasses the provider's `SFTPToWasbOperator` and overrides only
`copy_files_to_wasb` — the stock version stages every file in a
`NamedTemporaryFile` on the worker pod first. Wildcard expansion, blob naming and
`move_object` are inherited unchanged.

`sftp_source_path` takes one optional `*` wildcard, so a single task can transfer
a whole matching set. Pushes `[{source, blob, size}]` to XCom.
""",
    )

    verify = PythonOperator(
        task_id="verify_transfer",
        python_callable=verify_transfer,
        doc_md="""
Reads each blob's properties from Azure and compares its size against the byte
count reported by the SFTP source, so a truncated transfer fails the run instead
of passing silently.

Uses the names captured by the transfer task rather than re-listing the
container, so the check covers exactly what this run wrote.
""",
    )

    # verify reads the stream task's XCom, so the dependency is data, not just order.
    stream >> verify
