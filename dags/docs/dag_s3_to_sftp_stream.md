# `dag_s3_to_sftp_stream.py`

📄 **[Source: `dag_s3_to_sftp_stream.py`](../dag_s3_to_sftp_stream.py)**

`nix-dag-s3-to-sftp-stream` — streams an object from Amazon S3 to the SFTP server
without staging it on disk.

```
S3 bucket  ──download_fileobj──▶  SFTP server
```

```bash
airflow dags trigger nix-dag-s3-to-sftp-stream \
  --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'
```

Needs an object in the bucket, so run #10 first. Two verified runs:

```
[s3_to_sftp] done: 118 B in 0.7s -> /home/.../incoming/probe.txt
[s3_to_sftp] done: 50.0 MiB in 2.5s (19.9 MiB/s) -> /home/.../incoming/large50.bin
```

At ~20 MiB/s this is the **fastest path in the set** — worth comparing against
#5's 3.3 MiB/s, which moves 8 KiB at a time through an `os.pipe()`.

## A third provider operator that stages to disk

The `amazon` provider ships `S3ToSFTPOperator`, and its `execute` does this:

```python
with NamedTemporaryFile("w") as f:
    s3_client.download_file(self.s3_bucket, self.s3_key, f.name)
    sftp_client.put(f.name, self.sftp_path, confirm=self.confirm)
```

The whole object lands on the worker pod's disk before a byte reaches the SFTP
server. `StreamingS3ToSFTPOperator` overrides `execute` and hands boto3 the
remote file handle directly.

That is now the third demo of the same shape — see also
[#4](dag_sftp_to_blob_stream.md) and [#11](dag_s3_to_blob_stream.md). **Provider
transfer operators routinely stage to a temp file**, and the fix is nearly always
to override the one copy method rather than reimplement the operator.

## Both halves already fit

`download_fileobj` writes into any writable, and paramiko's `open(..., "wb")`
*is* one, so the transfer is a single call:

```python
with sftp_client.open(target, "wb") as handle:
    handle.set_pipelined(True)
    s3_client.download_fileobj(self.s3_bucket, self.s3_key, handle)
```

No pipe, no read loop — the same shape as [#13](dag_s3_to_smb_stream.md).

## One assumption that did not hold

`S3ToAzureBlobStorageOperator` exposes an `s3_hook` property, so it was natural
to reach for `self.s3_hook` here too. `S3ToSFTPOperator` has **no such
property** — it constructs `S3Hook` inside its own `execute`. Caught before
deploying; the override builds the hook itself.

Worth generalising: sibling operators in the same provider do not share a
consistent surface. Check the specific class rather than assuming.

## On pipelining

`set_pipelined(True)` lets paramiko keep multiple writes in flight rather than
waiting a round-trip each. It is set here because it costs nothing and matters on
a high-latency link — but measured on a 50 MiB object over a local network it
made **no difference**:

| | Time |
|---|---|
| `set_pipelined(True)` | 4.3 s |
| default | 4.2 s |

The reason is that boto3's own thread pool already keeps several writes
outstanding, so paramiko's pipelining has nothing left to overlap. Stated here
rather than claiming a speedup that was not observed.

Threads are left on for the same reason as [#13](dag_s3_to_smb_stream.md): boto3
writes each part at an explicit offset rather than relying on stream position.

## Write-then-rename

Writes to `<name>.part` and renames on success, so a consumer never sees a
partial file. `posix_rename`, not `rename` — plain SFTP `rename` fails when the
target exists, which would break a retry.

Unlike the SMB demos, this one **is** atomic.

**The size check runs before the rename**, deliberately: a truncated transfer
raises without replacing a good previous file. `download_fileobj` returns `None`,
so the size is read back from the server with `stat` and compared against
`head_object`.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `sftp_test_001` | SFTP destination (conn type `SFTP`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

The SFTP destination directory must exist and be writable; `open()` does not
create parent directories.

---

[← back to the DAG index](../README.md)
