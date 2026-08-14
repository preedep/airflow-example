# `dag_s3_to_smb_stream.py`

📄 **[Source: `dag_s3_to_smb_stream.py`](../dag_s3_to_smb_stream.py)**

`nix-dag-s3-to-smb-stream` — streams an object from Amazon S3 to an SMB share
without staging it on disk.

```
S3 bucket  ──download_fileobj──▶  SMB share
```

```bash
airflow dags trigger nix-dag-s3-to-smb-stream \
  --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'
```

Needs an object in the bucket, so run #10 first. Two verified runs:

```
[s3_to_smb] streaming s3://<bucket>/incoming/probe.txt -> smb:probe.txt (118 bytes)
[s3_to_smb] renamed probe.txt.part -> probe.txt
[s3_to_smb] done: 118 B in 0.2s -> probe.txt
[s3_to_smb] verified probe.txt — 118 bytes

[s3_to_smb] streaming s3://<bucket>/large/large50.bin -> smb:large50.bin (52428800 bytes)
[s3_to_smb] renamed large50.bin.part -> large50.bin
[s3_to_smb] done: 50.0 MiB in 13.7s (3.6 MiB/s) -> large50.bin
[s3_to_smb] verified large50.bin — 52428800 bytes
```

## One call, because both SDKs already fit

This is the simplest transfer in the set. `SambaHook.open_file()` returns a
**writable**, and boto3's `download_fileobj` **writes into** any writable, so the
two halves match with nothing in between:

```python
with self.samba_hook.open_file(target, mode="wb") as handle:
    s3_client.download_fileobj(self.bucket_name, self.key, handle)
```

No pipe, no manual read loop, no `readinto`. Compare [#12](dag_blob_to_smb_stream.md),
which reaches the same destination from the other side: there the *source* has to
push (`downloader.readinto(handle)`) because the Azure SDK has no
"download into this stream" entry point. Same writable destination, different
amount of work — decided entirely by what the source SDK offers.

## Threads are left on, and that is measured

`download_fileobj` uses a thread pool by default, which looks unsafe against a
non-seekable SMB handle — parallel writers usually imply seeking. boto3 avoids
that by writing each part at an **explicit offset** rather than relying on the
stream's position.

Measured on a 50 MiB object over SMB before writing the DAG:

| `TransferConfig` | Time |
|---|---|
| default (threads on) | **4.3 s** |
| `use_threads=False` | 15.1 s |

So the default is both correct and 3.5× faster. Worth stating because the
opposite conclusion is easy to reach from first principles, and because it is the
**reverse** of the Azure upload path, where `max_concurrency=1` genuinely is
required for a non-seekable source:

| | boto3 download → writable | Azure `upload_blob` ← readable |
|---|---|---|
| non-seekable peer | threads fine (explicit offsets) | needs `max_concurrency=1` |

A small file proves nothing either way: under the multipart threshold boto3 takes
a single-part path and never engages the pool. The numbers above are from a real
50 MiB transfer.

## Write-then-rename

The transfer writes to `<name>.part` and renames once the last byte lands, so a
consumer watching the share never sees a partial file.

As in #12 this is **unlink-then-rename**, not `replace()` — Samba refuses SMB's
atomic overwrite-rename with `STATUS_ACCESS_DENIED` even between two files the
account owns. There is therefore a brief window where neither name holds the
file, unlike SFTP's genuinely atomic `posix_rename`.

**The size check happens before the rename**, deliberately: a truncated transfer
raises without unlinking the existing target, so a bad run cannot destroy a good
previous file.

`download_fileobj` returns `None` — no byte count — so the size is read back from
the share with `stat` and compared against `head_object`. That check is the only
thing standing between a short write and a silently corrupt destination.

## Memory

Each part is written as it arrives, so peak memory is a part buffer (8 MiB by
default), not the object size. This direction has no `max_single_put_size`
equivalent, so it is constant-memory at any size.

`MAX_BYTES` caps a single transfer; a larger object fails before any bytes move.

## Paths are relative to the share

`SambaHook` reads the share from the connection's **schema** field and joins the
UNC path itself, so the DAG passes `probe.txt`, not `\\host\share\probe.txt`.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `smb_test_001` | SMB destination (conn type `samba`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

SMB: **host** an address the worker pods can resolve, **schema** the share name,
**login**/**password** the credentials. The share directory must be writable by
that user, or writes fail with `STATUS_ACCESS_DENIED` while listing still works.

## Egress cost

S3 charges for data leaving AWS. A demo file is free; a large bucket is a real
bill.

---

[← back to the DAG index](../README.md)
