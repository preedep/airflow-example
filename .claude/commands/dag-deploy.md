---
description: Deploy a DAG file to g1pro with scripts/deploy-dag.sh and verify it parsed
---

Deploy the DAG named in `$ARGUMENTS` (e.g. `dag_ftps_sensor`, or `--all`) to g1pro.

1. **Run the deploy script** — it handles connectivity check, parse check, integrity
   tests, and rsync, and refuses to ship anything that fails:

   ```bash
   ./scripts/deploy-dag.sh $ARGUMENTS
   ```

   If it exits non-zero, stop and report the failure. Do not work around it by
   rsyncing manually.

2. **Verify server-side** — the script does not do this:
   - `mcp__airflow__get_import_errors()` — must be empty
   - `mcp__airflow__get_dag(dag_id="<dag_id>")` — confirms registration, shows
     `is_paused`, `has_import_errors`, tags, and schedule

   Use `get_dag` with the exact `dag_id`, not `fetch_dags(dag_id_pattern=...)` —
   the pattern filter does not substring-match reliably.

   The dag-processor takes up to ~30s to pick up a new file. If the DAG is absent
   with no import error, wait one cycle and re-check before investigating.

Report what landed, whether it parsed, and its paused state. Do **not** unpause or
trigger unless `$ARGUMENTS` explicitly asked for it.
