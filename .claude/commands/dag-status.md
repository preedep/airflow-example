---
description: Show status of this project's DAGs on g1pro — import errors, paused state, last run
---

Report the current state of this project's DAGs on the g1pro cluster using the
Airflow MCP server. Read-only — do not trigger, pause, unpause, or clear anything.

Argument (optional): a `dag_id` or pattern. If `$ARGUMENTS` is empty, default to
pattern `nix-dag-` so bundled Airflow examples and other projects' DAGs are excluded.

1. `mcp__airflow__get_import_errors()` — report any entry prominently; that is the
   headline if present.
2. `mcp__airflow__fetch_dags(dag_id_pattern=...)` — always filter; the cluster has
   ~106 DAGs.
3. For each matched DAG, `mcp__airflow__get_dag_runs(dag_id=..., limit=1)`.

Present one compact table: dag_id, paused, schedule, last run state, last run time.
Then one line calling out anything that needs attention — import errors, a DAG that
is unpaused but has never run, or a most-recent run in `failed`. If everything is
healthy, say so in a sentence rather than restating the table.
