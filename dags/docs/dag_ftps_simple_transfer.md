# `dag_ftps_simple_transfer.py`

📄 **[Source: `dag_ftps_simple_transfer.py`](../dag_ftps_simple_transfer.py)**


`nix-dag-ftps-simple-transfer` — uploads a file from the DAG folder to FTPS.

```
dags/files/probe.txt  ──put──▶  FTPS /upload/probe.txt
```

Trigger from the UI, or:

```bash
airflow dags trigger nix-dag-ftps-simple-transfer
```

Optional conf: `{"filename": "other.txt"}` — the file must exist in `dags/files/`.

**Pattern: prefer a provider operator over `PythonOperator`.** This uses
`FTPSFileTransmitOperator` from the `ftp` provider rather than a hand-written
callable. You get templated fields, logging, and retry handling for free.

**Pattern: needing a custom hook is not a reason to abandon the operator.**
Subclass it and override the `hook` property — two lines, and the operator's own
logic stays intact:

```python
class MyFTPSFileTransmitOperator(FTPSFileTransmitOperator):
    @cached_property
    def hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)
```

---

[← back to the DAG index](../README.md)
