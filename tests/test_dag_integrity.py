"""Repo-wide DAG checks. Applies to every subfolder under dags/.

Catches the failures that show up as import errors on g1pro:
missing __init__.py, missing sys.path block, 2.x import paths, catchup left on.
"""

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parent.parent / "dags"

DAG_FILES = sorted(DAGS_DIR.glob("*/dag_*.py"))
PROJECT_DIRS = sorted(p for p in DAGS_DIR.iterdir() if p.is_dir() and not p.name.startswith("."))

BANNED_IMPORTS = {
    "airflow.operators.python": "airflow.providers.standard.operators.python",
    "airflow.operators.bash": "airflow.providers.standard.operators.bash",
    "airflow.models.Variable": "airflow.sdk",
}


@pytest.mark.parametrize("project", PROJECT_DIRS, ids=lambda p: p.name)
def test_subfolder_has_init(project):
    assert (project / "__init__.py").exists(), (
        f"{project.name}/ needs an empty __init__.py — required by the DAG folder convention"
    )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_dag_file_parses(dag_file):
    """Full import — catches bad import paths and top-level exceptions, not just syntax."""
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(dag_file), include_examples=False)
    assert not bag.import_errors, f"{dag_file.name}: {bag.import_errors}"
    assert bag.dags, f"{dag_file.name} defined no DAG — missing `with DAG(...)`?"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_has_syspath_block(dag_file):
    """Sibling imports fail in the dag-processor without the _DAGS_DIR insert."""
    source = dag_file.read_text()
    if "from dag_utils import" not in source and "import dag_utils" not in source:
        pytest.skip("no sibling import")
    assert "sys.path.insert" in source, (
        f"{dag_file.name} imports dag_utils but has no sys.path insert — "
        "will raise ModuleNotFoundError in the dag-processor"
    )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_no_airflow2_imports(dag_file):
    source = dag_file.read_text()
    for banned, replacement in BANNED_IMPORTS.items():
        assert banned not in source, f"{dag_file.name}: 2.x path {banned} — use {replacement}"
    assert "schedule_interval" not in source, (
        f"{dag_file.name}: schedule_interval= was removed in Airflow 3 — use schedule="
    )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_no_toplevel_io(dag_file):
    """Module scope is re-executed every parse cycle — no I/O there."""
    tree = ast.parse(dag_file.read_text())
    for node in tree.body:
        if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
            continue
        call = ast.unparse(node.value)
        assert not any(bad in call for bad in ("Variable.get", "requests.", "boto3.")), (
            f"{dag_file.name}: top-level I/O `{call}` — move it inside a task callable"
        )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_dag_attributes(dag_file):
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(dag_file), include_examples=False)
    for dag_id, dag in bag.dags.items():
        assert dag.catchup is False, f"{dag_id}: set catchup=False — a past start_date floods k3s"
        assert dag.tags, f"{dag_id}: add tags"
        assert dag.description, f"{dag_id}: add a description"
