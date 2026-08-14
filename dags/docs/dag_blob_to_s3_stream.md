# `dag_blob_to_s3_stream.py`

📄 **[Source: `dag_blob_to_s3_stream.py`](../dag_blob_to_s3_stream.py)**

`nix-dag-blob-to-s3-stream` — streams a blob from Azure Blob Storage to Amazon S3
without staging it on disk.

```
Blob container  ──download──▶  stream  ──upload_fileobj──▶  S3 bucket
```

The only **cross-cloud** demo here: two vendors' SDKs, one hop, nothing on local
disk in between.

```bash
airflow dags trigger nix-dag-blob-to-s3-stream \
  --conf '{"filename":"probe.txt","blob_prefix":"incoming/","s3_prefix":"incoming/"}'
```

Needs a blob in the container, so run #4 or #5 first. A successful run logs:

```
[blob_to_s3] streaming wasb://data001/incoming/probe.txt -> s3://<bucket>/incoming/probe.txt (118 bytes)
[blob_to_s3] transferred 118 bytes to s3://<bucket>/incoming/probe.txt
[blob_to_s3] verified s3://<bucket>/incoming/probe.txt — 118 bytes
```

## No pipe: both SDKs cooperate

`WasbHook.download()` returns a `StorageStreamDownloader` — a readable — and
`S3Hook.load_file_obj()` calls boto3's `upload_fileobj`, which **pulls** from a
file-like object. Reader on one side, puller on the other, so they compose in two
lines:

```python
downloader = self.wasb_hook.download(container_name=..., blob_name=...)
self.s3_hook.load_file_obj(downloader, key=..., bucket_name=..., replace=True)
```

The `os.pipe()` and worker thread that #5 needs are only required when both sides
push, or both pull.

## The seek question

`upload_fileobj` does **multipart** uploads, which raises a fair worry: splitting
a stream into parts usually means seeking it, and `StorageStreamDownloader` has
`read` but **no `seek`**. Verified against the live services:

```
DOWNLOADER: StorageStreamDownloader | read: True | seek: False
UPLOAD_OK size: 118
```

boto3 handles it. Its transfer manager reads sequentially into per-part buffers
rather than seeking the source, so a non-seekable stream is fine.

That is worth knowing because **the equivalent Azure call is not so forgiving**.
The SFTP→Blob demo (#4) has to pass `length=` and `max_concurrency=1` precisely
because the Azure SDK would otherwise seek the source to measure and split it:

| | Azure `upload()` | boto3 `upload_fileobj` |
|---|---|---|
| non-seekable source | needs `length=` + `max_concurrency=1` | works as-is |
| parallelism | seeks to split → must be 1 | reads sequentially into buffers |

Same shape of operation, opposite tolerance. Do not carry an assumption from one
cloud's SDK to the other.

## `replace=True` is not optional

`load_file_obj` **raises on an existing key** by default:

```python
if not replace and self.check_for_key(key, bucket_name):
    raise ValueError(f"The key {key} already exists.")
```

That default makes a retry fail rather than converge — a task that died
mid-upload leaves an object behind, and the retry must be able to replace it. So
the operator passes `replace=True`.

Note this differs from the Azure demos, where the equivalent knob is
`overwrite=True` on `upload()`, and from FTPS, where `STOR` truncates silently
with no flag at all.

## Memory

The blob is fetched on demand and each part is uploaded as it fills, so peak
memory is a part buffer rather than the file size. boto3's `TransferConfig`
defaults to 8 MiB parts with up to 10 concurrent threads, so a multi-GB blob
transfers without the whole object ever being resident.

`MAX_BYTES` caps a single transfer; a larger blob fails before any bytes move.

## `verify` is load-bearing

`upload_fileobj` returns `None` — no byte count, no ETag surfaced by the hook. So
nothing in the transfer task itself proves the right number of bytes landed; the
`head_object` check in `verify_transfer` is the only thing that does.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `wasb-nickstorageairflow002` | Azure Blob source (conn type `wasb`) |
| Connection | `aws_s3_test_001` | AWS destination (conn type `aws`) |

Azure: SAS token in **extra** as `sas_token`, **login** is the storage account
name. AWS: access key in **login**, secret in **password**, region in **extra**
as `{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

Neither container nor bucket belongs to a connection; both are passed per
operation.

## Egress cost

Worth stating plainly for a cross-cloud transfer: **Azure charges for data
leaving its network.** A demo file is free in practice, but the same DAG pointed
at a large container is a real bill. S3 does not charge for ingress.

---

[← back to the DAG index](../README.md)
