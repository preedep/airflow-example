---
description: Deploy a DAG subfolder to g1pro via rsync and verify it parsed
---

Deploy the DAG project subfolder named in `$ARGUMENTS` to the g1pro cluster.

Follow the `dag-deploy` skill. In summary:

1. **Preflight** — `ssh nickmsft@nixhome-linux-g1pro true`, then parse-check every
   DAG file with `.venv/bin/python dags/$ARGUMENTS/dag_*.py` (exit 0, no output),
   then `.venv/bin/python -m pytest tests/`. Stop and report if any fail; do not
   deploy a file that does not parse locally.

2. **Ship** —
   ```bash
   rsync -av --exclude='__pycache__' --exclude='*.pyc' \
     ./dags/$ARGUMENTS/ \
     nickmsft@nixhome-linux-g1pro:/mnt/external-storage/airflow-dags/$ARGUMENTS/
   ```
   Only into this subfolder. Never the DAG root, never `--delete`.

3. **Verify** — `mcp__airflow__get_import_errors()` first, then
   `mcp__airflow__fetch_dags(dag_id_pattern=...)` for the DAG(s) in this folder.

Report what landed, whether it parsed, and its paused state. Do **not** unpause or
trigger — leave that to the user unless they explicitly asked for it in
`$ARGUMENTS`.
