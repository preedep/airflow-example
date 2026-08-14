# `dag_sftp_to_blob_stream.py`

📄 **[Source: `dag_sftp_to_blob_stream.py`](../dag_sftp_to_blob_stream.py)**


`nix-dag-sftp-to-blob-stream` — streams a file from SFTP into an Azure Blob
container without staging it on disk.

```
SFTP  ──open──▶  stream  ──upload──▶  Blob container
```

```bash
airflow dags trigger nix-dag-sftp-to-blob-stream \
  --conf '{"source_path":"/home/airflowsftp/outgoing/probe.*","blob_prefix":"incoming/"}'
```

`source_path` must be a **directory or a wildcard**, never a bare filename — see
the trap below. `.../outgoing/*.csv` transfers a whole matching set in one run.

**Pattern: override the one method that is wrong, keep the rest of the operator.**
The provider *does* ship `SFTPToWasbOperator` — but its `copy_files_to_wasb`
downloads each file to a `NamedTemporaryFile` before uploading it, so the full file
lands on the worker pod's disk. Only that method needs replacing:

```python
class StreamingSFTPToWasbOperator(SFTPToWasbOperator):
    def copy_files_to_wasb(self, sftp_files):
        # get_managed_conn(), not get_conn() — see trap 2
        with self.sftp_hook.get_managed_conn() as sftp_client:
            for file in sftp_files:
                size = sftp_client.stat(file.sftp_file_path).st_size
                with sftp_client.open(file.sftp_file_path, "rb") as remote:
                    remote.prefetch(size)       # else one round-trip per read
                    wasb_hook.upload(
                        container_name=..., blob_name=..., data=remote,
                        length=size,
                        max_concurrency=1,      # >1 needs a seekable source
                    )
```

Wildcard expansion, blob naming, templated fields and `move_object` are all
inherited unchanged — a future provider fix to any of them still applies here.

**`WasbHook.upload` takes a file object, not just a path.** It reads a 4 MiB block,
uploads it, then reads the next, so peak memory is one block regardless of file
size. Contrast #3, which needed an `os.pipe()` because neither side would accept a
stream — here the destination pulls, so no pipe or thread is needed.

`length=size` is load-bearing: it lets the SDK skip seeking the stream to measure
it, which an SFTP handle would fail. So is `max_concurrency=1` — parallel block
upload seeks the source to split it, and the default would break on a
non-seekable read.

### Three traps, each hidden behind the last

All three parse cleanly, pass the integrity tests, and only fail against a live
server. Each one's error message points somewhere other than the real cause.

**1. A bare filename in `source_path` fails as "file not found".** With no `*`, the
inherited `get_tree_behavior` passes the path straight to `SFTPHook.get_tree_map`,
which `listdir`s it. `listdir` on a regular file returns SFTP status 2:

```
FileNotFoundError: [Errno 2] No such file
  ... get_tree_map -> walktree -> list_directory_with_attr -> listdir_attr
```

The file is right there and readable. `walktree`/`listdir_attr` in the traceback
means *"not a directory"*, not *"not found"*.

**2. `get_conn()` hands back a closed socket.** Every decorated `SFTPHook` method
wraps itself in `handle_connection_management`, which enters `get_managed_conn()`
and **closes the session on exit**. The inherited `get_sftp_files_map()` lists the
source that way, so the connection is already shut before the copy starts:

```
OSError: Socket is closed
  ... copy_files_to_wasb -> sftp_client.stat -> _send_packet -> channel.send
```

Use `get_managed_conn()`. It is refcounted, so opening it once around the whole
loop holds one session for every file instead of reconnecting per file.

**3. `max_block_size` is not an upload argument.** It reads as one, but it comes
from the *client's* `StorageConfiguration`. `upload_blob()` forwards unrecognised
kwargs down the pipeline until they reach the HTTP transport:

```
TypeError: Session.request() got an unexpected keyword argument 'max_block_size'
```

The SDK's 4 MiB default is the block size you want anyway; changing it means
configuring the `BlobServiceClient`, which `WasbHook` builds internally.
`max_concurrency`, by contrast, *is* a real per-call parameter.

**`wasb_overwrite_object=True`** so a retry can replace a partial blob left by a
task that died mid-upload. **The source is not deleted** (`move_object=False`), so
re-running re-sends; flip it once you trust the verify step.

---

[← back to the DAG index](../README.md)
