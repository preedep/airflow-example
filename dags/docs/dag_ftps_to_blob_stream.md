# `dag_ftps_to_blob_stream.py`

📄 **[Source: `dag_ftps_to_blob_stream.py`](../dag_ftps_to_blob_stream.py)**


`nix-dag-ftps-to-blob-stream` — streams a file from FTPS into an Azure Blob
container without staging it on disk.

```
FTPS  ──retrbinary──▶  os.pipe()  ──upload──▶  Blob container
```

```bash
airflow dags trigger nix-dag-ftps-to-blob-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'
```

Needs a file on the FTPS server, so run #1 first. A successful run logs the two
ends of the transfer and the independent check:

```
[ftps_to_blob] streaming /upload/probe.txt -> wasb://data001/incoming/probe.txt (205 bytes)
[ftps_to_blob] transferred 205 bytes to incoming/probe.txt
[ftps_to_blob] verified incoming/probe.txt — 205 bytes
```

The byte count appears three times on purpose — reported by FTPS, counted through
the pipe, then read back from Azure in a separate pod. A truncated transfer breaks
the chain and fails the run instead of passing quietly.

**Pattern: when no provider operator exists, write one.** #4 could subclass
`SFTPToWasbOperator`; there is no FTP/FTPS equivalent here. The `microsoft-azure`
provider ships only `sftp_to_wasb`, `s3_to_wasb`, `local_to_wasb` and
`oracle_to_azure_data_lake`. Check before assuming symmetry — "there's an SFTP
one, so there's an FTP one" is wrong.

So `FTPStoBlobStreamOperator` subclasses `BaseOperator` (from **`airflow.sdk`** in
3.x) directly. That is worth doing over a `PythonOperator` for three reasons:

```python
class FTPStoBlobStreamOperator(BaseOperator):
    template_fields = ("remote_path", "container_name", "blob_name")

    def __init__(self, *, ftp_conn_id, wasb_conn_id, remote_path,
                 container_name, blob_name, overwrite=True, **kwargs):
        super().__init__(**kwargs)
        ...

    @cached_property                    # built on use, not at parse time
    def ftps_hook(self): return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)

    def execute(self, context): ...
```

- **Templated fields** render from `dag_run.conf`, so the resolved paths show up
  in the UI's *Rendered Template* tab. A callable that reads `dag_run.conf`
  internally shows nothing there — you cannot see what path a past run used.
  Here `remote_path` is authored as
  `/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}` and renders to
  `/upload/probe.txt`, which is what the tab shows after the run.
- **Configuration is constructor arguments**, so a second task streaming a
  different file is one more operator call, not a second copy of the function.
- **Hooks are `cached_property`**, so building the operator at parse time opens
  no connection.

### Push versus pull: why #5 needs a pipe

This is the interesting difference between #4 and #5, and it is decided entirely
by which side controls the loop:

| | source API | destination API | bridge |
|---|---|---|---|
| #4 SFTP → Blob | `open()` returns a readable | `upload()` reads | none needed |
| #5 FTPS → Blob | `retrbinary()` **pushes** to a callback | `upload()` **pulls** | `os.pipe()` |

`ftplib` never hands back a file object — it calls a callback per chunk. Azure's
`upload()` wants something to `read()`. Two pushers and no puller, so a pipe sits
between them, with `retrbinary` writing one end and `upload` reading the other
from a worker thread:

```python
read_fd, write_fd = os.pipe()

def _upload():
    with os.fdopen(read_fd, "rb") as reader:
        wasb_hook.upload(container_name=..., blob_name=..., data=reader,
                         length=expected,      # required — a pipe cannot be seeked
                         max_concurrency=1)

uploader = threading.Thread(target=_upload, daemon=True); uploader.start()
try:
    with os.fdopen(write_fd, "wb") as writer:
        ftps.get_conn().retrbinary(f"RETR {src}", writer.write, blocksize=8192)
finally:
    uploader.join(timeout=300)   # outside the `with` — closing it is what signals EOF
```

Three details that are easy to get wrong:

- **`join()` must be outside the `with`.** Closing the write end is what raises
  EOF for the reader. Joining inside would wait forever on a reader that can
  never see the stream end.
- **A thread cannot raise into its caller.** The upload exception is stashed in a
  list and re-raised on the main thread; without that the task passes green while
  the upload silently failed.
- **`length=` is mandatory for a pipe.** Without it the SDK tries to seek the
  stream to measure it, which a pipe cannot do.
- **`BrokenPipeError` must be swallowed, not raised.** If the upload fails early
  the reader closes, and the next FTPS write — or the `with` block's own close —
  raises `BrokenPipeError`, which would mask the real cause. Catch it, then
  re-raise the stashed upload exception so the log says *"azure exploded"* rather
  than *"Broken pipe"*.

The pipe also gives **backpressure** for free: if Azure is slower than the FTPS
read, the pipe fills, the write blocks, and the two sides self-throttle to the
slower one rather than buffering the difference.

**One honest caveat about memory.** The transfer itself moves 8 KiB at a time, but
the Azure SDK does a **single-shot upload** for blobs at or under
`max_single_put_size` (64 MiB by default), and that path does one
`stream.read(length)` — buffering the whole file. Above that threshold it switches
to block upload and streams in constant memory. So: constant-memory for large
files, up-to-64-MiB-buffered for small ones.

---

[← back to the DAG index](../README.md)
