# Measured throughput

Real numbers from **24 runs** of a 50 MiB fixture (52,428,800 bytes), taken from
the `done:` line each transfer DAG logs. Nothing here is estimated.

```
[blob_to_ftps] done: 50.0 MiB in 3.7s (13.7 MiB/s) -> /upload/large50.bin
```

## Read this before the table

These are **one cluster, one network, one 50 MiB file** — useful for comparing
the DAGs against each other, not as a benchmark of the protocols themselves.
Three things move the numbers more than the code does:

- **Contention.** The "concurrent" column is from running all 12 transfers at
  once, competing for the same NIC and worker pods.
- **Where the endpoint lives.** SFTP, FTPS and SMB are on the LAN; S3 is in
  `ap-southeast-1` and Azure Blob is remote. A cross-cloud hop pays both.
- **Run-to-run variance is large.** S3→SFTP measured 19.9 MiB/s alone and
  1.1 MiB/s under load — an 18× spread on identical code.

A single number per direction would be misleading, so both are shown.

## Results

| Direction | Solo (best of) | All 12 concurrent |
|---|---|---|
| FTPS → SFTP | **37.3 MiB/s** | 31.6 MiB/s |
| S3 → FTPS | 20.7 MiB/s | **32.7 MiB/s** |
| S3 → SFTP | 19.9 MiB/s | 1.1 MiB/s |
| Blob → SMB | 17.7 MiB/s | *(failed — see below)* |
| SFTP → S3 | 7.7 MiB/s | 16.3 MiB/s |
| Blob → FTPS | 13.7 MiB/s | 11.0 MiB/s |
| Blob → SFTP | — | 12.8 MiB/s |
| Blob → S3 | 9.3 MiB/s | 4.9 MiB/s |
| S3 → Blob | — | 8.1 MiB/s |
| FTPS → Blob | — | 3.8 MiB/s |
| S3 → SMB | 3.6 MiB/s | 0.4 MiB/s |
| SFTP → Blob | 2.4 MiB/s | 1.9 MiB/s |

Two directions were only ever measured under load, so their solo figures are
blank rather than guessed.

## What the numbers actually show

**LAN-to-LAN is fastest.** FTPS→SFTP tops the table at 37.3 MiB/s because both
endpoints are on the same local network — no internet leg at all.

**Anything touching Blob is slower than anything touching S3**, in both
directions, on this network. Compare SFTP→S3 at 7.7 against SFTP→Blob at 2.4,
and Blob→SFTP at 12.8 against S3→SFTP at 19.9. That is a property of the link to
each service from here, not of the code — the two DAGs are near-identical in
structure.

**Chunk size is not the bottleneck.** S3→FTPS moves 8 KiB at a time and is one of
the fastest; SFTP→Blob uses 4 MiB blocks and is the slowest. The network
dominates, which is why the earlier decision to leave boto3's threading on
mattered more than any chunk-size tuning.

**Concurrency is not uniformly bad.** Two directions got *faster* in the
concurrent batch (S3→FTPS, SFTP→S3), which is the giveaway that these runs are
noisy and that scheduling order matters as much as bandwidth. Treat any single
measurement here as ±50%.

## The one failure

`Blob → SMB` failed in the concurrent batch:

```
SMBOSError: [NtStatus 0xc0000043] The process cannot access the file
because it is being used by another process: '/airflow/large50.bin.part'
```

Not a code defect. Both SMB DAGs derive the destination from `filename`, so both
resolved to `large50.bin.part` on the same share, and SMB holds an exclusive lock
for the duration of a write. Timings confirm the overlap exactly — `s3_to_smb`
held the name from 14:41:39 to 14:43:41, and `blob_to_smb` tried to open it at
14:42:33.

Re-run alone it completed in 2.8 s at 17.7 MiB/s.

Worth noting the write-then-rename pattern behaved **correctly under contention**:
the share ended with one intact 52,428,800-byte file and no orphaned `.part`. A
partial file was never visible to a consumer.

The underlying issue is not concurrency but that **two pipelines write the same
destination path**. Run sequentially the same collision is silent — the second
run simply overwrites the first, with no error at all. The lock error is the
louder, better failure.

## Reproducing

```bash
dd if=/dev/urandom of=large50.bin bs=1m count=50   # random, not zeros
```

Random data matters: compressible filler lets TLS compression inflate the
apparent rate.

Stage it at whichever source the DAG reads, then trigger with `filename` in the
conf — see [Testing with a large file](../README.md) in the main README. Every
transfer DAG logs its own `done:` line, so re-measuring is just reading the logs.

---

[← back to the DAG index](../README.md)
