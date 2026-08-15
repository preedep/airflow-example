"""Repo-wide DAG checks. Applies to every dag_*.py under dags/.

Demo DAGs are standalone single files — no subfolders, no shared helper module.
Catches the failures that show up as import errors on g1pro:
2.x import paths, catchup left on, top-level I/O.
"""

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parent.parent / "dags"

DAG_FILES = sorted(DAGS_DIR.glob("dag_*.py"))

BANNED_IMPORTS = {
    "airflow.operators.python": "airflow.providers.standard.operators.python",
    "airflow.operators.bash": "airflow.providers.standard.operators.bash",
    "airflow.models.Variable": "airflow.sdk",
}

# Modules that still import cleanly in 3.2.1 but warn on attribute access.
# A silent import means grep alone is not enough — hence both the AST check
# below and the warnings-as-errors parse.
DEPRECATED_MODULES = {
    "airflow.operators.python": "airflow.providers.standard.operators.python",
    "airflow.operators.bash": "airflow.providers.standard.operators.bash",
    "airflow.operators.empty": "airflow.providers.standard.operators.empty",
    "airflow.sensors.python": "airflow.providers.standard.sensors.python",
    "airflow.sensors.bash": "airflow.providers.standard.sensors.bash",
    "airflow.sensors.filesystem": "airflow.providers.standard.sensors.filesystem",
    "airflow.hooks.filesystem": "airflow.providers.standard.hooks.filesystem",
    "airflow.hooks.subprocess": "airflow.providers.standard.hooks.subprocess",
    "airflow.utils.operator_helpers": "airflow.sdk.bases.decorator",
    "airflow.utils.dates": "pendulum / datetime",
    "airflow.models": "airflow.sdk (for Variable, Connection)",
}


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_dag_file_parses(dag_file):
    """Full import — catches bad import paths and top-level exceptions, not just syntax."""
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(dag_file), include_examples=False)
    assert not bag.import_errors, f"{dag_file.name}: {bag.import_errors}"
    assert bag.dags, f"{dag_file.name} defined no DAG — missing `with DAG(...)`?"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_no_deprecated_imports(dag_file):
    """AST check: no import may resolve to a deprecated module.

    Catches deprecated paths on code that never executes during a parse —
    inside a callable, a branch, an except handler.
    """
    tree = ast.parse(dag_file.read_text())
    bad = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            mod = node.module
            if mod in DEPRECATED_MODULES:
                # `from airflow.models import DagBag` is fine; only some names moved.
                if mod == "airflow.models":
                    moved = {"Variable", "Connection"}
                    hit = moved.intersection(a.name for a in node.names)
                    if not hit:
                        continue
                    bad.append(f"line {node.lineno}: from {mod} import {', '.join(sorted(hit))}")
                else:
                    bad.append(f"line {node.lineno}: from {mod} import ...")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in DEPRECATED_MODULES:
                    bad.append(f"line {node.lineno}: import {alias.name}")

    assert not bad, "{} uses deprecated Airflow 2.x modules:\n  {}\nUse instead: {}".format(
        dag_file.name,
        "\n  ".join(bad),
        ", ".join(sorted({DEPRECATED_MODULES[m] for m in DEPRECATED_MODULES if m in str(bad)})),
    )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_no_deprecation_warnings_on_parse(dag_file):
    """Parsing must emit no deprecation warning attributable to this file.

    Complements the AST check: catches deprecated *attribute* access and
    removed kwargs, which import-path grepping cannot see. Warnings raised
    from site-packages (e.g. dagfactory, provider internals) are ignored —
    only warnings whose source is this DAG file fail the test.

    Note Airflow's `DeprecatedImportWarning` subclasses **FutureWarning**, not
    DeprecationWarning, and its deprecations are re-emitted through logging.
    Filtering on DeprecationWarning alone silently catches nothing.
    """
    import subprocess
    import sys

    # Run in a subprocess: these warnings fire once per module import, so an
    # in-process check passes spuriously when another test imported first.
    # Deprecations raised *inside provider code* are ignored — they are
    # upstream's to fix and a DAG cannot avoid them short of not using the
    # provider. Everything else is still an error, so a deprecated import or
    # kwarg in the DAG file itself fails the test.
    #
    # Known offender: apache-airflow-providers-sftp 5.7.3 imports the deprecated
    # airflow.utils.timezone shims at module scope. Verified identical on the
    # server image, so this is not a local-venv artifact.
    probe = (
        "import warnings, sys\n"
        "warnings.simplefilter('error', FutureWarning)\n"
        "warnings.simplefilter('error', DeprecationWarning)\n"
        # Ignore must come after simplefilter: later filters take precedence.
        "warnings.filterwarnings('ignore', category=FutureWarning,\n"
        "                        module=r'airflow\\.providers\\..*')\n"
        "warnings.filterwarnings('ignore', category=DeprecationWarning,\n"
        "                        module=r'airflow\\.providers\\..*')\n"
        "from airflow.models import DagBag\n"
        f"b = DagBag(dag_folder={str(dag_file)!r}, include_examples=False)\n"
        "sys.exit(1 if b.import_errors else 0)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, (
        f"{dag_file.name} fails to parse with deprecation warnings as errors:\n"
        f"{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}"
    )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_is_standalone(dag_file):
    """Demo DAGs must not depend on a sibling helper module."""
    source = dag_file.read_text()
    assert "dag_utils" not in source, (
        f"{dag_file.name} imports dag_utils — demo DAGs are standalone single files"
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


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_doc_md_present(dag_file):
    """Every DAG and every task must carry doc_md — it renders in the UI.

    A module docstring is not a substitute: Airflow only picks that up when it
    is passed as `doc_md=__doc__`, and it renders as plain text, not Markdown.
    """
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(dag_file), include_examples=False)
    for dag_id, dag in bag.dags.items():
        assert dag.doc_md and dag.doc_md.strip(), (
            f"{dag_id}: set doc_md on the DAG — required by this project"
        )
        missing = [t.task_id for t in dag.tasks if not (t.doc_md or "").strip()]
        assert not missing, f"{dag_id}: tasks without doc_md: {', '.join(sorted(missing))}"
