---
description: Fetch and summarize logs for the most recent run of a DAG
---

Show what happened in the latest run of the DAG named in `$ARGUMENTS`.

1. `mcp__airflow__get_dag_runs(dag_id="$ARGUMENTS", limit=1)` — get the newest run.
2. `mcp__airflow__list_task_instances(dag_id=..., dag_run_id=...)` — task states.
3. For each task not in `success`, `mcp__airflow__get_log(dag_id=..., dag_run_id=...,
   task_id=..., task_try_number=1)`.

Read **try 1** first. If a task retried, also read the final try and note any
difference — a task that fails differently on retry is not idempotent, and that is
worth reporting on its own.

Output: the run's overall state and timing, then per failed task the task_id and the
actual error line quoted from the log — not a paraphrase. Close with the most likely
root cause and the specific fix. Cross-reference the symptom table in the `dag-debug`
skill; if the cause is infrastructure (OOM, evicted pod, missing Variable, package
absent from the image), say so plainly rather than proposing DAG changes to work
around it.

If the DAG has no runs, say so and check `is_paused` via
`mcp__airflow__fetch_dags(dag_id_pattern="$ARGUMENTS")` — paused-and-never-run is the
most common explanation.
