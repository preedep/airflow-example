# airflow-example

Working **Apache Airflow 3.x** DAG examples, with the project scaffolding that keeps
them honest: pinned local environment, integrity tests, and a deploy script that
refuses to ship a broken DAG.

The examples cover file-transfer patterns — FTPS upload, waiting on a file with a
sensor, streaming between two servers, and extending a provider sensor for Azure
Blob Storage.

## Repository layout

```
dags/
  README.md              how to run the examples — start here
  dag_<name>.py          one standalone file per example
  files/                 fixtures a DAG reads at runtime
scripts/
  deploy-dag.sh          parse-check + test + ship
tests/
  test_dag_integrity.py  repo-wide checks, applied to every DAG
```

**[→ dags/README.md](dags/README.md)** explains what each DAG demonstrates, the order
to run them in, and the connections and variables to create first.

## Quick start

```bash
uv sync                                  # Python 3.13 + Airflow 3.2.1
.venv/bin/python -m pytest tests/
.venv/bin/ruff check .
```

The virtualenv is pinned to the same Python and Airflow versions as the target
deployment, so a local parse exercises the same import paths the DAG processor will.

To run a DAG you need an Airflow deployment and the connections listed in
[dags/README.md](dags/README.md#prerequisites).

## Conventions

These are enforced by `tests/test_dag_integrity.py`, not just documented:

| Rule | Why |
|---|---|
| One standalone file per DAG | copy it out and it still works — no shared helper module |
| No deprecated Airflow 2.x APIs | the 2.x shims still import and run, so nothing else catches them |
| `doc_md` on the DAG and every task | renders in the UI; the only in-product explanation an operator gets |
| `catchup=False`, tags, description | a past `start_date` with catchup on floods the scheduler |
| No top-level I/O | the DAG file is re-parsed every processor cycle |

Add a DAG and the tests pick it up automatically — there is no per-DAG test to write.

## Two constraints worth knowing up front

**Tasks do not share a filesystem.** On KubernetesExecutor every task runs in its own
ephemeral pod, so a file written to `/tmp` by one task does not exist for the next.
It fails at runtime with `FileNotFoundError`, and a parse check will not catch it.

**Prefer a provider operator over `PythonOperator`.** Check what the provider ships
first. If you need different behaviour, subclass and override one method rather than
rewriting the operator — the examples do this for both a hook and a sensor.

Both are covered in detail in [dags/README.md](dags/README.md).

## Airflow version

Targets **Airflow 3.2.1**. Most Airflow examples online are 2.x, and the 2.x import
paths still work as deprecation shims — they parse, they run, and nothing obvious
tells you they are wrong. The integrity tests fail the build on them.
