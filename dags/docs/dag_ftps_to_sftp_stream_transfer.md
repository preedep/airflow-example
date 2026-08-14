# `dag_ftps_to_sftp_stream_transfer.py`


`nix-dag-ftps-to-sftp-stream` — streams a file between two servers.

```
FTPS  ──get──▶  stream  ──put──▶  SFTP
```

```bash
airflow dags trigger nix-dag-ftps-to-sftp-stream
```

**Pattern: get and put are one task, not two.** Separate tasks land in separate
pods, so a file downloaded by "get" would not exist for "put".

Bytes move through an `os.pipe()` from FTPS `retrbinary` straight into paramiko's
`putfo` on a worker thread. Both sockets are open at once, nothing touches disk,
and peak memory is one 8 KiB chunk regardless of file size — a multi-GB file
transfers in constant memory. `BUFFER_LIMIT` caps a single transfer rather than
letting it run unbounded.

This is one of the few cases where `PythonOperator` is right: the provider transfer
operators move between a remote host and **local disk**, so none of them does a
direct server-to-server hop.

**The source is not deleted** after a successful transfer, so re-running re-sends
the same file. That is deliberate — a failed put must not lose the only copy. For a
real pickup pattern, delete only after the verify step confirms the size matches.

---

[← back to the DAG index](../README.md)
