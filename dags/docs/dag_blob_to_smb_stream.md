# `dag_blob_to_smb_stream.py`

📄 **[Source: `dag_blob_to_smb_stream.py`](../dag_blob_to_smb_stream.py)**

`nix-dag-blob-to-smb-stream` — streams a blob from Azure Blob Storage to an SMB
share without staging it on disk.

```
Blob container  ──download──▶  stream  ──readinto──▶  SMB share
```

```bash
airflow dags trigger nix-dag-blob-to-smb-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/"}'
```

Needs a blob in the container, so run #4 or #5 first. A successful run logs:

```
[blob_to_smb] streaming wasb://data001/incoming/probe.txt -> smb:probe.txt (118 bytes)
[blob_to_smb] renamed probe.txt.part -> probe.txt
[blob_to_smb] done: 118 B in 0.2s -> probe.txt
[blob_to_smb] verified probe.txt — 118 bytes
```

## The destination is a *writable*, which inverts the composition

Every other transfer demo pairs a **readable source** with a destination that
**pulls**. SMB is the other way round: `SambaHook.open_file()` hands back a file
object opened for *writing*, so there is nothing to pull from.

The Azure downloader supplies the missing half — `StorageStreamDownloader` has
`readinto(stream)`, which pushes its content into any writable:

```python
with self.samba_hook.open_file(target, mode="wb") as handle:
    written = downloader.readinto(handle)
```

That completes the set of shapes across these demos:

| Source | Destination | Bridge |
|---|---|---|
| readable (`open`, `download`) | pulls (`upload`, `putfo`, `storbinary`) | none |
| **pushes** (`retrbinary`) | pulls (`upload`) | `os.pipe()` + thread |
| readable with `readinto` | **writable** (`open_file`) | none — the source pushes |

Still no pipe. One side drives the loop and the other is a plain file object; a
pipe is only needed when **both** sides push, or both pull.

`readinto` also returns the byte count written, which is what the size check
compares against — useful, since nothing else in the SMB path reports one.

## Samba refuses SMB's atomic overwrite-rename

The write-then-rename pattern works here, but not the way it does on SFTP.

SMB has an atomic overwrite-rename — `FILE_RENAME_INFORMATION` with
`ReplaceIfExists` — which is what `smbclient.replace` issues. Samba refuses it:

```
SMBOSError: [NtStatus 0xc0000022] STATUS_ACCESS_DENIED
```

**This is not a permissions problem**, which is the trap. It fails between two
files the account just created, and deleting that same target immediately
afterwards succeeds. `rename` onto a *free* name also works. So the DAG unlinks
the target first, then renames:

```python
try:
    self.samba_hook.unlink(self.remote_path)
except Exception:
    pass                      # nothing to replace on a first run
self.samba_hook.rename(target, self.remote_path)
```

The trade-off, stated plainly: there is a brief window where neither name holds
the final file, so this is **not atomic** the way SFTP's `posix_rename` is. It
still prevents a consumer seeing a *partial* file, which is what the pattern is
for.

| Destination | Overwrite-rename | Atomic? |
|---|---|---|
| SFTP | `posix_rename` | yes |
| FTPS | `rename` (server-dependent) | yes, where supported |
| SMB via Samba | unlink + `rename` | **no** — brief gap |

## How the failure surfaced

The first real run **failed on the rename, not the transfer** — the log showed
`probe.txt.part` written and closed, then `STATUS_ACCESS_DENIED` on the target.
The leftover `.part` on the share made it obvious which half had worked.

Worth noting as an argument for the pattern itself: a DAG writing straight to the
final name would have looked fine here and quietly done the wrong thing on
retries.

## Paths are relative to the share

`SambaHook` reads the share from the connection's **schema** field and joins the
UNC path itself, so the DAG passes `probe.txt` — not
`\\host\share\probe.txt`. Set `schema` to the share name on the connection.

## Memory

`readinto` writes in the SDK's own chunks and the blob is fetched on demand, so
peak memory is one chunk rather than the file size.

Unlike the *upload* paths in #5 and #11, there is no `max_single_put_size`
equivalent on a download, so this direction is constant-memory at any size.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `smb_test_001` | SMB destination (conn type `samba`) |

For the SMB connection: **host** must be an address the worker pods can resolve,
**schema** is the share name, **login**/**password** are the SMB credentials. Set
`share_type` in extra to `windows` for a Windows-style share; the default is
`posix`.

**The share directory must be writable by the SMB user.** A directory owned by
another account gives the same `STATUS_ACCESS_DENIED` as above, but for a
different reason — the share lists fine and still refuses writes. Check
ownership on the server before assuming the connection is wrong:

```bash
ls -ld /srv/samba/<share>      # should be owned by the SMB user
```

---

[← back to the DAG index](../README.md)
