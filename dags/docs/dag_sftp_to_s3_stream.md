# `dag_sftp_to_s3_stream.py`

📄 **[Source: `dag_sftp_to_s3_stream.py`](../dag_sftp_to_s3_stream.py)**

`nix-dag-sftp-to-s3-stream` — streams a file from the SFTP server to Amazon S3
without staging it on disk.

```
SFTP server  ──open──▶  upload_fileobj  ──▶  S3 bucket
```

```bash
airflow dags trigger nix-dag-sftp-to-s3-stream \
  --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'
```

Two verified runs:

```
[sftp_to_s3] done: 118 B in 0.3s -> s3://<bucket>/incoming/probe.txt
[sftp_to_s3] done: 50.0 MiB in 6.5s (7.7 MiB/s) -> s3://<bucket>/large/large50.bin
```

## The provider already streams — this fixes the speed

`SFTPToS3Operator` is the **exception** among the transfer operators used in
these demos. The others stage the whole file in a `NamedTemporaryFile` with no
way to opt out; this one takes a `use_temp_file` argument, and
`use_temp_file=False` already does the right thing:

```python
with sftp_client.file(self.sftp_path, mode="rb") as data:
    s3_hook.get_conn().upload_fileobj(data, self.s3_bucket, self.s3_key, ...)
```

So there is no staging to remove. What is missing is **`prefetch`**.

Without it paramiko requests a block, waits a full round-trip, then requests the
next — so throughput is bounded by latency rather than bandwidth. Measured on a
50 MiB file over a local network before writing the DAG:

| | Time |
|---|---|
| provider's `use_temp_file=False` | 10.5 s |
| the same, plus `prefetch` | **2.2 s** |

**4.8× on a fast link**, and the gap widens as latency grows. The subclass reuses
the provider's approach and adds one line:

```python
with sftp_client.file(self.sftp_path, mode="rb") as data:
    data.prefetch(expected)          # ← the line the provider omits
    s3_client.upload_fileobj(data, self.s3_bucket, self.s3_key)
```

**The general lesson:** a provider operator that already streams can still be
slow for a reason that has nothing to do with staging. Check the *read pattern*,
not just whether a temp file is involved.

## Composition: S3 pulls, SFTP reads

No pipe. `upload_fileobj` pulls from a file-like object and paramiko's
`file(..., "rb")` is one.

boto3 tolerates a non-seekable source because its transfer manager reads
sequentially into per-part buffers — the same reason [#13](dag_s3_to_smb_stream.md)
can leave threads on. This is the **opposite** of the Azure upload path, which
needs `length=` and `max_concurrency=1` for such a source.

## No `.part` rename here

Unlike the SFTP, FTPS and SMB *destinations*, there is no write-then-rename step:
S3 has no rename, and an object is not visible until the upload commits, so a
consumer never sees a partial one. See the write-then-rename section in the
[README](../README.md).

## `verify` is load-bearing

`upload_fileobj` returns `None` — no byte count. The size is read back from S3
with `head_object` and compared against the SFTP `stat`, which is the only check
that the right number of bytes landed.

## The inherited not-found contract

`SFTPToS3Operator` takes `fail_on_file_not_exist`, and the override honours it:
raise when `True`, log and return a zero-size result when `False`. Both branches
are covered by the fake-driven tests.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `sftp_test_001` | SFTP source (conn type `SFTP`) |
| Connection | `aws_s3_test_001` | AWS S3 destination (conn type `aws`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

## Ingress is free

S3 does not charge for data coming in, unlike the S3→anywhere demos where egress
is billable.

---

[← back to the DAG index](../README.md)
