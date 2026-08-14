# `dag_blob_to_ftps_stream.py`

📄 **[Source: `dag_blob_to_ftps_stream.py`](../dag_blob_to_ftps_stream.py)**


`nix-dag-blob-to-ftps-stream` — streams a blob out of Azure Blob Storage to the
FTPS server.

```
Blob container  ──download──▶  stream  ──storbinary──▶  FTPS
```

```bash
airflow dags trigger nix-dag-blob-to-ftps-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'
```

Needs a blob in the container, so run #4 or #5 first. A successful run logs:

```
[blob_to_ftps] streaming wasb://data001/incoming/probe.txt -> /upload/probe.txt (118 bytes)
[blob_to_ftps] transferred 118 bytes to /upload/probe.txt (226 Transfer complete.)
[blob_to_ftps] verified /upload/probe.txt — 118 bytes
```

Note the middle line reports `226 Transfer complete.` — that is `storbinary`'s
whole return value. The byte count beside it is the blob's size, measured before
the transfer, not a confirmation from the server.

**Pattern: control flow is a property of the call, not the protocol.** This is #5
in reverse — same FTPS server, same Azure SDK — yet it needs no pipe and no
thread, because `ftplib` swaps which side drives the loop depending on direction:

| ftplib call | direction | control |
|---|---|---|
| `retrbinary(cmd, callback)` | reading | **pushes** to your callback |
| `storbinary(cmd, fp)` | writing | **pulls** via `fp.read(blocksize)` |

`storbinary` loops on `fp.read(blocksize)` — exactly the readable that
`WasbHook.download()` returns — so the two compose in one line:

```python
downloader = self.wasb_hook.download(container_name=..., blob_name=...)
with self.ftps_hook as ftps:
    ftps.get_conn().storbinary(f"STOR {remote_path}", downloader, blocksize=8192)
```

So across all four transfer directions, exactly one needs a pipe:

| Direction | source | destination | bridge |
|---|---|---|---|
| #4 SFTP → Blob | readable | `upload()` pulls | none |
| #5 FTPS → Blob | `retrbinary` **pushes** | `upload()` **pulls** | `os.pipe()` + thread |
| #6 Blob → SFTP | readable | `putfo()` pulls | none |
| #8 Blob → FTPS | readable | `storbinary()` pulls | none |

Check the specific call before assuming you need a pipe. "FTPS needs a pipe"
would be the wrong lesson from #5 — *`retrbinary`* needs one.

**`verify` is load-bearing here, not belt-and-braces.** The other transfer demos
get a byte count back from the write side: `putfo(confirm=True)` stats the file,
and the SFTP→Blob path counts bytes through the pipe. `storbinary` returns only a
response code like `226 Transfer complete`, so nothing in the transfer task
itself proves the right number of bytes landed — the `SIZE` check in `verify` is
the only thing that does.

`STOR` truncates an existing file, so a retry replaces rather than appends.

---

[← back to the DAG index](../README.md)
