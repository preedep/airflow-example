# `dag_s3_to_ftps_stream.py`

📄 **[Source: `dag_s3_to_ftps_stream.py`](../dag_s3_to_ftps_stream.py)**

`nix-dag-s3-to-ftps-stream` — streams an object from Amazon S3 to the FTPS server
without staging it on disk.

```
S3 bucket  ──get_object──▶  storbinary  ──▶  FTPS server
```

```bash
airflow dags trigger nix-dag-s3-to-ftps-stream \
  --conf '{"filename":"probe.txt","s3_prefix":"incoming/"}'
```

Needs an object in the bucket, so run #10 first. Two verified runs:

```
[s3_to_ftps] done: 118 B in 0.1s -> /upload/probe.txt (226 Transfer complete.)
[s3_to_ftps] done: 50.0 MiB in 2.4s (20.7 MiB/s) -> /upload/large50.bin (226 Transfer complete.)
```

## Two things wrong with the stock operator, not one

The `amazon` provider ships `S3ToFTPOperator`, and this DAG subclasses it — but
unlike every other subclass here it needs **two** overrides.

**1. It stages the whole object on disk:**

```python
with NamedTemporaryFile() as local_tmp_file:
    s3_obj.download_fileobj(local_tmp_file)
    local_tmp_file.seek(0)
    ftp_hook.store_file(self.ftp_path, local_tmp_file.name)
```

**2. It hardcodes `FTPHook` — plain FTP, no TLS.** Fine for the operator's
intended use, wrong for a server requiring FTPS, and there is no `hook` property
to swap. Compare [#1](dag_ftps_simple_transfer.md), where
`FTPSFileTransmitOperator` *does* expose one and a two-line override is enough.

So `StreamingS3ToFTPSOperator` overrides `execute` outright and adds its own
`MyFTPSHook`. What is inherited is the templated fields and argument surface —
not the transfer logic.

**The general lesson:** check whether the dependency you need to replace is
exposed. A hardcoded hook turns a small override into a full rewrite of the
method.

## Composition: `storbinary` pulls, S3's body reads

No pipe. `ftplib.storbinary` loops on `fp.read(blocksize)` and the S3 response
body is a readable, so they compose directly:

```python
body = s3_client.get_object(Bucket=..., Key=...)["Body"]
ftps.storbinary(f"STOR {target}", body, blocksize=CHUNK_SIZE)
```

Contrast [#5](dag_ftps_to_blob_stream.md), the FTPS→Blob direction, which needs
an `os.pipe()` and a worker thread. Same protocol, opposite direction, entirely
different plumbing — because `ftplib` swaps which side drives the loop:

| ftplib call | direction | control |
|---|---|---|
| `retrbinary(cmd, callback)` | reading | **pushes** to your callback |
| `storbinary(cmd, fp)` | writing | **pulls** via `fp.read()` |

That asymmetry is worth internalising: "FTPS needs a pipe" is the wrong lesson
from #5. *`retrbinary`* needs one.

## `verify` is load-bearing

`storbinary` returns only a response code — `226 Transfer complete.` — never a
byte count. Nothing in the transfer task itself proves the right number of bytes
landed, so the `SIZE` check in `verify` is the only thing that does.

The in-task size check runs **before** the rename for the same reason: a
truncated transfer must not replace a good previous file.

## Write-then-rename

Writes to `<name>.part` and renames on success (RNFR/RNTO), so a consumer
watching the drop directory never sees a partial file.

Whether `rename` overwrites an existing target is **server-dependent**. It does
on the server tested here; a stricter server may need the target deleted first —
the same split as SFTP's `rename` vs `posix_rename`.

## Memory

`storbinary` reads in 8 KiB blocks and the S3 body is fetched on demand, so peak
memory is one block rather than the object size.

Note this is 8 KiB where the S3→SFTP and S3→SMB demos use boto3's 8 MiB parts —
yet throughput is comparable (20.7 vs 19.9 MiB/s), because the bottleneck here is
the network rather than the block size.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `ftps_test_001` | FTPS destination (conn type `FTP`) |
| Variable | `ftps_ca_cert` | PEM of the FTPS server's CA certificate |

The destination directory must exist and be writable. FTPS chroots often make the
login root non-writable — `553 Could not create file` means the directory, not
the credentials.

## Why a custom FTPS hook

Stock `FTPSHook.get_conn()` hardcodes `ssl.create_default_context()` with no
parameter for a CA, so a self-signed cert fails with
`CERTIFICATE_VERIFY_FAILED`. `MyFTPSHook` builds the context from `ftps_ca_cert`
— **verification stays on** — and calls `prot_p()`, which the stock hook omits,
so the data channel is encrypted. With a publicly trusted certificate none of
this is needed.

---

[← back to the DAG index](../README.md)
