# `dag_blob_to_sftp_stream.py`


`nix-dag-blob-to-sftp-stream` — streams a blob back out of Azure Blob Storage to
the SFTP server.

```
Blob container  ──download──▶  stream  ──putfo──▶  SFTP
```

```bash
airflow dags trigger nix-dag-blob-to-sftp-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'
```

Needs a blob in the container, so run #4 or #5 first. A successful run logs:

```
[blob_to_sftp] streaming wasb://data001/incoming/probe.txt -> .../incoming/probe.txt (118 bytes)
[blob_to_sftp] transferred 118 bytes to .../incoming/probe.txt
[blob_to_sftp] verified .../incoming/probe.txt — 118 bytes
```

### The pipe is not always needed — read #4, #5 and #6 together

These three demos differ only in **which side controls the loop**, and that alone
decides whether you need a pipe:

| Direction | source | destination | bridge |
|---|---|---|---|
| #4 SFTP → Blob | `open()` returns a readable | `upload()` **pulls** | none |
| #5 FTPS → Blob | `retrbinary()` **pushes** | `upload()` **pulls** | `os.pipe()` + thread |
| #6 Blob → SFTP | `download()` returns a readable | `putfo()` **pulls** | none |

`WasbHook.download()` returns a `StorageStreamDownloader`, whose `read(size)`
returns bytes and an empty result at EOF — the readable contract paramiko's
`putfo` expects. A reader on one side and a puller on the other compose directly:

```python
downloader = self.wasb_hook.download(container_name=..., blob_name=...)
with self.sftp_hook.get_managed_conn() as sftp_client:
    attrs = sftp_client.putfo(downloader, remote_path,
                              file_size=expected, confirm=True)
```

**Reach for a pipe only when both sides push, or both pull** — that is #5 and
nothing else here. Adding one to #6 would be a thread and a pair of file
descriptors buying nothing.

`confirm=True` makes paramiko `stat` the file afterwards and compare sizes, so a
short write raises inside the transfer task rather than surfacing downstream.

One paramiko detail worth knowing if you turn that off: with `confirm=False`,
`putfo` returns an **empty `SFTPAttributes` whose `st_size` is `None`**, not
`None` itself. Code that compares `attrs.st_size` against the expected size will
report a bogus "wrote None" mismatch unless it treats that case as "nothing to
compare".

---

[← back to the DAG index](../README.md)
