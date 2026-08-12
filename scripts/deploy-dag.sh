#!/bin/sh
# Deploy a DAG file to the shared Airflow DAG folder on g1pro.
#
#   ./scripts/deploy-dag.sh dag_ftps_sensor
#   ./scripts/deploy-dag.sh dags/dag_ftps_sensor.py
#   ./scripts/deploy-dag.sh --all
#
# Parse-checks locally first, then rsyncs. Verification of the result is a
# separate step — use the Airflow MCP server (get_import_errors / fetch_dags).
#
# POSIX sh: this runs from the Mac but is kept portable per project convention.

set -eu

REMOTE_USER="nickmsft"
REMOTE_HOST="nixhome-linux-g1pro"
REMOTE_DIR="/mnt/external-storage/airflow-dags"

REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
DAGS_DIR="$REPO_ROOT/dags"
VENV_PY="$REPO_ROOT/.venv/bin/python"

die() { printf '%s\n' "error: $*" >&2; exit 1; }
info() { printf '==> %s\n' "$*"; }

[ $# -ge 1 ] || die "usage: $0 <dag_name|path|--all> [--no-test]"

RUN_TESTS=1
TARGET=""
for arg in "$@"; do
    case "$arg" in
        --no-test) RUN_TESTS=0 ;;
        --all)     TARGET="--all" ;;
        -*)        die "unknown option: $arg" ;;
        *)         TARGET="$arg" ;;
    esac
done
[ -n "$TARGET" ] || die "no DAG specified"

# Resolve target to a newline-separated list of files under dags/.
if [ "$TARGET" = "--all" ]; then
    FILES=$(find "$DAGS_DIR" -maxdepth 1 -name 'dag_*.py' | sort)
    [ -n "$FILES" ] || die "no dag_*.py files in $DAGS_DIR"
else
    # Accept "dag_x", "dag_x.py", or "dags/dag_x.py".
    name=$(basename "$TARGET" .py)
    FILES="$DAGS_DIR/$name.py"
    [ -f "$FILES" ] || die "not found: $FILES"
fi

[ -x "$VENV_PY" ] || die ".venv not found — run 'uv sync' first"

# --- Preflight -------------------------------------------------------------

info "Checking connectivity to $REMOTE_HOST"
ssh -o ConnectTimeout=10 -o BatchMode=yes "$REMOTE_USER@$REMOTE_HOST" true \
    || die "cannot reach $REMOTE_HOST — is Tailscale up?"

info "Parse-checking"
for f in $FILES; do
    printf '    %s ... ' "$(basename "$f")"
    if out=$("$VENV_PY" "$f" 2>&1); then
        printf 'ok\n'
    else
        printf 'FAILED\n'
        printf '%s\n' "$out" >&2
        die "parse failed — not deploying"
    fi
done

if [ "$RUN_TESTS" -eq 1 ]; then
    info "Running integrity tests"
    "$VENV_PY" -m pytest "$REPO_ROOT/tests" -q || die "tests failed — not deploying"
fi

# --- Ship ------------------------------------------------------------------
# One file at a time, never --delete: the folder is shared with other projects.

info "Deploying to $REMOTE_HOST:$REMOTE_DIR"
for f in $FILES; do
    rsync -av --exclude='__pycache__' --exclude='*.pyc' \
        "$f" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/"
done

info "Deployed. Verify with the Airflow MCP server:"
printf '    mcp__airflow__get_import_errors()\n'
printf '    mcp__airflow__get_dag(dag_id="<exact-dag-id>")\n'
printf '\n  The dag-processor needs ~30s to register a new file.\n'
printf '  New DAGs land paused — unpause with mcp__airflow__unpause_dag(dag_id=...).\n'
