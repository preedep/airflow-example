# `dag_s3_to_blob_stream.py`

📄 **[Source: `dag_s3_to_blob_stream.py`](../dag_s3_to_blob_stream.py)**

`nix-dag-s3-to-blob-stream` — streams an object from Amazon S3 to Azure Blob
Storage without staging it on disk. The cross-cloud pair to
[#10](dag_blob_to_s3_stream.md), pointing the other way.

```
S3 bucket  ──get_object──▶  stream  ──upload_blob──▶  Blob container
```

```bash
airflow dags trigger nix-dag-s3-to-blob-stream \
  --conf '{"filename":"probe.txt","s3_prefix":"incoming/","blob_prefix":"xcloud"}'
```

Needs an object in the bucket, so run #10 first. A successful run logs:

```
[s3_to_blob] streaming s3://<bucket>/incoming/probe.txt -> wasb://data001/xcloud/probe.txt (118 bytes)
[s3_to_blob] done: 118 B in 0.3s (n/a) -> xcloud/probe.txt
[s3_to_blob] verified xcloud/probe.txt — 118 bytes
```

## The one direction a provider already covers

The other cross-service demos — FTPS→Blob, Blob→SFTP, Blob→FTPS, Blob→S3 — have
no provider operator at all, so they subclass `BaseOperator`. This one is
different: the `microsoft-azure` provider ships **`S3ToAzureBlobStorageOperator`**,
and most of it is worth keeping — prefix listing, `s3_key` vs `s3_prefix`
handling, blob naming, `replace` filtering, and a `TooManyFilesToMoveException`
guard.

Only `move_file` is wrong for streaming:

```python
with tempfile.NamedTemporaryFile("w") as temp_file:
    s3_client.download_file(self.s3_bucket, source_s3_key, temp_file.name)
    self.wasb_hook.load_file(file_path=temp_file.name, ...)
```

The whole object lands on the worker pod's disk before a byte goes to Azure.
`StreamingS3ToAzureBlobStorageOperator` overrides that one method and pipes
`get_object()["Body"]` straight into `upload_blob`.

Same shape as [#4](dag_sftp_to_blob_stream.md): **override the one method that is
wrong, inherit everything else.** A future provider fix to prefix handling or
blob naming still applies here.

## `hasattr(body, "seek")` lies

boto3's response body has a `seek` *attribute* but is not actually seekable.
Verified against the live services:

```
S3_BODY: StreamingChecksumBody | read: True | seek: True | seekable: False
```

So an `hasattr` check would tell you the stream is seekable when it is not. The
Azure SDK checks `stream.seekable()`, sees `False`, and takes its sequential
path — which means two arguments are required:

- **`length=`** — without it the SDK seeks the stream to measure it.
- **`max_concurrency=1`** — parallel block upload seeks to split the source.

**Note the asymmetry with the reverse direction.** Sending *to* S3 (#10), boto3's
`upload_fileobj` tolerates a non-seekable source with no special arguments;
sending *to* Azure, the SDK needs both:

| | boto3 `upload_fileobj` | Azure `upload_blob` |
|---|---|---|
| non-seekable source | works as-is | needs `length=` + `max_concurrency=1` |

Same pair of clouds, opposite tolerance. Do not carry an assumption from one
SDK to the other.

## `blob_prefix` takes no trailing slash

This one bit on the first real run — the blob landed as `xcloud//probe.txt`. The
inherited `_create_key` joins unconditionally:

```python
if prefix and file_name:
    return f"{prefix}/{file_name}"
```

So `"xcloud/"` becomes `xcloud//probe.txt`. Object stores treat a double slash as
a **distinct key**, not a typo to normalise away, so the file is quietly written
to the wrong place.

That is the opposite convention to every other DAG here, where prefixes *do* end
in `/` because they are concatenated directly. Worth checking whenever a demo
mixes hand-built and provider-built keys.

## Memory

Bytes are fetched from S3 on demand and written to Azure as they arrive, so peak
memory is one block rather than the object size.

The same caveat as [#5](dag_ftps_to_blob_stream.md) applies: the Azure SDK does a
single buffered `read()` for objects at or under `max_single_put_size` (64 MiB by
default), switching to streaming block upload above it. Constant-memory for large
objects, up-to-64-MiB-buffered for small ones.

## Two different "replace" knobs

Worth not confusing:

| Setting | Scope | Default here |
|---|---|---|
| `overwrite=True` on `upload_blob` | one blob — lets a retry replace a partial | on |
| inherited `replace` | the *source list* — skips objects already in the container | `False` |

So a re-run transfers only what is missing, while any blob it does transfer can
be overwritten.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS S3 source (conn type `aws`) |
| Connection | `wasb-nickstorageairflow002` | Azure Blob destination (conn type `wasb`) |

AWS: access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`. Azure: SAS token in **extra** as `sas_token`, **login** is
the storage account name.

## Egress cost

S3 charges for data leaving AWS; Azure does not charge for ingress — the mirror
of #10. A demo file is free; a large bucket is a real bill.

---

[← back to the DAG index](../README.md)
