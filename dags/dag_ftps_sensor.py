"""nix-dag-ftps-sensor — wait for a file to land on the g1pro FTPS server."""


import ftplib
import logging
import ssl
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.providers.ftp.sensors.ftp import FTPSSensor
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-ftps-sensor

Waits for a file to appear on the **g1pro FTPS server**, then reports its size
and modification time.

#### Trigger

Manual only (`schedule=None`). Pass the filename in the run conf:

```json
{"filename": "myfile.txt"}
```

Defaults to `probe.txt` when no conf is given. Only `/upload` is writable —
vsftpd chroots `ftpuser` and the chroot root is mode `555`.

#### Sensor behaviour

| Setting | Value | Why |
|---|---|---|
| `mode` | `reschedule` | releases the worker pod between pokes |
| `poke_interval` | 60s | |
| `timeout` | 1h | fails the task rather than blocking `max_active_runs` forever |

#### Why a custom hook

The server presents a **self-signed certificate**. Stock `FTPSHook.get_conn()`
hardcodes `ssl.create_default_context()` with no way to supply a CA, so it
fails with `CERTIFICATE_VERIFY_FAILED`.

`MyFTPSHook` overrides `get_conn()` to build the context from the
`ftps_ca_cert` Variable — **certificate verification stays ON**, the CA is
trusted rather than bypassed. It also calls `prot_p()`, which the stock hook
omits; without it the data channel is transferred in plaintext.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `ftps_test_001` | host, login, password, port (conn type `FTP`) |
| Variable | `ftps_ca_cert` | PEM of `/etc/ssl/private/vsftpd.pem` |

There is no `FTPS` connection type — the `ftp` provider registers `conn_type="ftp"`
for both hooks. TLS is selected by importing `FTPSHook`, not by the connection.
"""

FTPS_CONN_ID = "ftps_test_001"

# vsftpd chroots ftpuser; the chroot root is read-only (mode 555) and only
# /upload is writable. See g1pro.md §3.
REMOTE_PATH = "/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}"


class MyFTPSHook(FTPSHook):
    """FTPSHook that trusts the g1pro self-signed CA and encrypts data transfers."""

    def get_conn(self) -> ftplib.FTP:
        from airflow.sdk import Variable

        if self.conn is not None:
            return self.conn

        # Host, login, password, port and passive flag all come from the
        # Airflow Connection (ftps_test_001) — same as the stock hook. Only
        # the TLS context differs, and it is the reason for this override.
        params = self.get_connection(self.ftp_conn_id)

        context = ssl.create_default_context(cadata=Variable.get("ftps_ca_cert"))

        if params.port:
            ftplib.FTP_TLS.port = params.port

        conn = ftplib.FTP_TLS(params.host, params.login, params.password, context=context)
        conn.prot_p()  # stock FTPSHook omits this — without it data is plaintext
        conn.set_pasv(params.extra_dejson.get("passive", True))

        self.conn = conn
        return self.conn


class MyFTPSSensor(FTPSSensor):
    def _create_hook(self) -> FTPSHook:
        return MyFTPSHook(ftp_conn_id=self.ftp_conn_id)


def report_arrival(**context):
    """Log size and mtime of the file the sensor found."""
    task_log = logging.getLogger("airflow.task")

    conf = context["dag_run"].conf or {}
    path = f"/upload/{conf.get('filename', 'probe.txt')}"

    with MyFTPSHook(ftp_conn_id=FTPS_CONN_ID) as hook:
        size = hook.get_size(path)
        mod_time = hook.get_mod_time(path)

    task_log.info("[ftps_sensor] %s — %s bytes, modified %s", path, size, mod_time)
    return {"path": path, "size": size, "mod_time": str(mod_time)}


with DAG(
    dag_id="nix-dag-ftps-sensor",
    description="Wait for a file on the g1pro FTPS server, then report it",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "ftps", "sensor"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    wait_for_file = MyFTPSSensor(
        task_id="wait_for_file",
        ftp_conn_id=FTPS_CONN_ID,
        path=REMOTE_PATH,
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=60,  # 1 minute
        timeout=60 * 60,  # 1 hour
        doc_md="""
Polls `/upload/<filename>` over FTPS until the file exists.

`reschedule` mode releases the worker pod between pokes — with `poke` the pod
would sit idle for up to an hour holding a slot.

Fails after 1 hour if the file never arrives. `550` from the server means
"not found" and is treated as *keep waiting*, not an error.
""",
    )

    report = PythonOperator(
        task_id="report_arrival",
        python_callable=report_arrival,
        doc_md="""
Reconnects over FTPS and logs the file's **size** and **modification time**.

Returns a dict (`path`, `size`, `mod_time`) which is pushed to XCom.
""",
    )

    wait_for_file >> report
