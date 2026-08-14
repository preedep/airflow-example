# `dag_ftps_sensor.py`

📄 **[Source: `dag_ftps_sensor.py`](../dag_ftps_sensor.py)**


`nix-dag-ftps-sensor` — waits for a file to appear, then reports its size and mtime.

```
FTPS /upload/<file>  ──poll every 60s, up to 1h──▶  report
```

```bash
airflow dags trigger nix-dag-ftps-sensor --conf '{"filename":"probe.txt"}'
```

**Pattern: always `mode="reschedule"` with an explicit `poke_interval` and
`timeout`.**

```python
FTPSSensor(
    task_id="wait_for_file",
    path="/upload/{{ dag_run.conf.get('filename', 'probe.txt') }}",
    mode="reschedule",   # frees the worker slot between pokes
    poke_interval=60,
    timeout=60 * 60,
)
```

`mode="poke"` holds a worker for the entire wait — on KubernetesExecutor that is a
pod sitting idle for an hour. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

To see it actually wait, trigger with a filename that does not exist yet, then run
#1 to upload it and watch the sensor pick it up on its next poke.

---

[← back to the DAG index](../README.md)
