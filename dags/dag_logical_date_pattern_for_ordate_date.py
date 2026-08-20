"""nix-dag-logical-date-pattern-for-odate — map an Airflow logical date to a Control-M ODATE."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-logical-date-pattern-for-odate

Converts an Airflow **logical date** into a Control-M **ODATE**, and pulls the
year, month and day out of it — in a task callable, and in a Jinja template.

Runs daily at 02:00 so there is a real interval to reason about. Nothing is
transferred; the point is the date arithmetic.

#### ODATE is not the same idea as `logical_date`

Both name "the date this run stands for", and for a daily job they disagree by
one day. That off-by-one is the whole reason this DAG exists.

| | Control-M ODATE | Airflow `logical_date` |
|---|---|---|
| Means | the **scheduling day** the job belongs to | the **start of the interval** the run covers |
| A daily job firing 2026-08-21 02:00 | `20260821` | `2026-08-20 02:00` |
| Timezone | the server's local business day | always **UTC** |
| Manual run | operator supplies it | **`None`** |

Verified on this Airflow, with `schedule="0 2 * * *"`:

```
data_interval_start : 2026-08-20 02:00:00+00:00   <- logical_date
data_interval_end   : 2026-08-21 02:00:00+00:00
run_after           : 2026-08-21 02:00:00+00:00   <- when it actually fires
```

The run that **fires on the 21st** has a `logical_date` of the **20th**, because
it covers the interval that *began* on the 20th. Control-M would call that same
run ODATE `20260821`.

So there are two defensible mappings, and picking the wrong one silently shifts
every downstream partition by a day:

| You want | Use | Why |
|---|---|---|
| the day the data *covers* | `logical_date` | matches Airflow's own `ds`, and backfills correctly |
| the day the job *runs* (true ODATE) | `data_interval_end` | equals Control-M's ODATE, daily |

`ODATE_SOURCE` selects which. **Default is `data_interval_end`**, because this
DAG is about matching Control-M; switch it to `logical_date` if you are
partitioning by the covered day.

The rule that resolves it: Control-M's ODATE answers *"which scheduling day is
this?"*, Airflow's `logical_date` answers *"which interval is this?"*. They only
coincide when the interval is named after its end.

#### Trap: `logical_date` is `None` on a manual run

In Airflow 3 `DagRun.logical_date` is nullable, and a run you trigger by hand
has no logical date at all:

```python
logical_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
```

So `context["logical_date"].strftime(...)` raises `AttributeError` on exactly
the run you use to test it. `run_after` — when the run was actually queued — is
the sane fallback, and is what a manual Control-M order does anyway:

```python
odate_dt = dag_run.logical_date or dag_run.run_after
```

#### Trap: ODATE is a *local* business date, `logical_date` is UTC

`logical_date` is always UTC. A business day is whatever the operations centre
says it is, so for any site east or west of UTC the two disagree for part of
each day:

```
logical_date (UTC)      : 2026-08-20 19:30:00+00:00  -> ODATE 20260820
same instant in Bangkok : 2026-08-21 02:30:00+07:00  -> ODATE 20260821
```

A 19:30 UTC run is already **tomorrow** in Bangkok. Set `BUSINESS_TIMEZONE` to
the timezone Control-M schedules in and convert before formatting, or every
evening run lands in the wrong day's partition. `"UTC"` disables the conversion.

#### The parts

`get_odate_parts` returns the pieces most jobs actually need:

| Key | Example | Note |
|---|---|---|
| `odate` | `20260821` | `YYYYMMDD`, Control-M's own format |
| `year` | `2026` | int |
| `month` | `8` | int — **not** zero-padded |
| `day` | `21` | int |
| `yyyy` / `mm` / `dd` | `2026` / `08` / `21` | strings, zero-padded |
| `odate_dashed` | `2026-08-21` | for SQL and partition paths |

Both forms are returned on purpose. `month` is an int for arithmetic;
`mm` is the zero-padded string, because `f"{year}{month}{day}"` on 8 August
yields `202688`, not `20260808` — a real and quiet bug.

#### Control-M variable → Jinja, side by side

The mapping most people want first, with the substitutions that actually hold up
on Airflow 3:

| Control-M | Jinja | Result |
|---|---|---|
| `%%$ODATE` / `%%ODATE` | `{{ data_interval_end &#124; ds_nodash }}` | `20260821` |
| `%%$YEAR` | `{{ ds[:4] }}` | `2026` |
| `%%MONTH` | `{{ ds[5:7] }}` | `08` |
| `%%DAY` | `{{ ds[8:10] }}` | `20` |
| `%%PREV` | `{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}` | `20260819` |
| `%%SUBSTR %%PREV 1 4` | `{{ macros.ds_add(ds, -1)[:4] }}` | `2026` |
| `%%SUBSTR %%PREV 5 2` | `{{ macros.ds_add(ds, -1)[5:7] }}` | `08` |
| `%%SUBSTR %%PREV 7 2` | `{{ macros.ds_add(ds, -1)[8:10] }}` | `19` |

**Why these are not spelled `logical_date.strftime(...)`.** That form is the
obvious translation and it breaks on a manual run:

```
scheduled  -> 2026
manual     -> UndefinedError: 'None' has no attribute 'strftime'
```

`logical_date` is nullable in Airflow 3 (see the trap below), and Jinja has no
`or` that saves you — `{{ logical_date.strftime('%Y') }}` raises before any
default applies. `ds` is a **string** that Airflow always renders, so slicing it
cannot fail that way. `ds[:4]` is also cheaper than parsing a datetime to
re-format it.

`%%PREV` is Control-M's *previous scheduling date*, so `ds_add(ds, -1)` is the
honest equivalent: it is calendar arithmetic on the business date, not
`logical_date - timedelta(days=1)`, which subtracts 24 hours from a UTC instant
and lands on the wrong day across a DST boundary in a non-UTC business timezone.

`macros.timedelta` does exist, so `{{ (logical_date - macros.timedelta(days=1)) }}`
renders on a scheduled run — it just inherits both problems above.

One caveat on the first row: `%%ODATE` maps to `data_interval_end`, **not**
`ds_nodash`, for the off-by-one reason above. `ds_nodash` is the logical date, so
for a daily job it is the day *before* the ODATE Control-M would show. The
`%%$YEAR`/`%%MONTH`/`%%DAY` rows use `ds` for the same reason they are strings —
if you need those parts of the *ODATE* rather than of the logical date, take them
from `get_odate_parts` below, which resolves the source once and consistently.

#### Doing it in a template instead

A callable is not needed to format a date. In Jinja, `ds_nodash` **is** the
`YYYYMMDD` of the logical date:

```
{{ ds_nodash }}                                  -> 20260820
{{ data_interval_end | ds_nodash }}              -> 20260821   (true ODATE)
{{ data_interval_end.in_timezone("Asia/Bangkok") | ds_nodash }}
{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }} -> 20260820
```

`show_odate_in_template` prints these so the two routes can be compared in one
run's logs. Templating is the better choice when the value is just being passed
to a command; a callable earns its place when the date drives real logic.

#### Trigger

Scheduled daily at 02:00, and safe to trigger by hand — the manual run
exercises the `None` fallback. To pin a specific ODATE without waiting:

```json
{"odate": "20260821"}
```

#### Requires

Nothing — no connections, no Variables.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# Which end of the data interval is the ODATE.
#
#   "data_interval_end" — the day the job RUNS. Equals Control-M's ODATE for a
#                         daily job, which is why it is the default here.
#   "logical_date"      — the day the data COVERS. Matches Airflow's own `ds`.
#
# For a daily schedule these differ by one day. Choosing wrong shifts every
# downstream partition silently, so it is a named constant rather than a literal.
ODATE_SOURCE = "data_interval_end"

# The timezone Control-M schedules in. logical_date is always UTC, so without
# converting, an evening run lands in the previous business day. "UTC" is a
# no-op for sites that genuinely schedule in UTC.
BUSINESS_TIMEZONE = "Asia/Bangkok"

ODATE_FORMAT = "%Y%m%d"


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def _resolve_odate_datetime(dag_run, context):
    """Pick the datetime that ODATE is derived from, honouring conf and the fallback.

    Returns (datetime, source) so the caller can log *which* rule applied — the
    difference between the two sources is a day, and a log line that does not say
    which one was used is not much help at 03:00.
    """
    # An explicit conf wins: it is how you reproduce a specific business day,
    # the same way an operator re-orders a Control-M job with a given ODATE.
    forced = (dag_run.conf or {}).get("odate")
    if forced:
        return datetime.strptime(forced, ODATE_FORMAT), "dag_run.conf['odate']"

    if ODATE_SOURCE == "data_interval_end":
        # data_interval_end is None for a manual run too, so it falls through the
        # same chain rather than being trusted on its own.
        candidate = context.get("data_interval_end")
        if candidate:
            return candidate, "data_interval_end"

    # logical_date is nullable in Airflow 3 — None on every manually triggered
    # run — so run_after (when the run was queued) is the fallback.
    if dag_run.logical_date:
        return dag_run.logical_date, "logical_date"

    return dag_run.run_after, "run_after (manual run: logical_date is None)"


def get_odate_parts(ti=None, **context):
    """Derive the Control-M ODATE and its year/month/day parts from the run's dates."""
    task_log = logging.getLogger("airflow.task")

    dag_run = context["dag_run"]
    odate_dt, source = _resolve_odate_datetime(dag_run, context)

    # Convert before formatting, never after. ODATE is a local business date and
    # logical_date is UTC, so an evening run is already tomorrow in Bangkok.
    # A conf-supplied ODATE is naive and already a business date, so converting
    # it would shift the very value the operator pinned.
    if BUSINESS_TIMEZONE != "UTC" and odate_dt.tzinfo is not None:
        odate_dt = odate_dt.in_timezone(BUSINESS_TIMEZONE)

    odate = odate_dt.strftime(ODATE_FORMAT)

    parts = {
        "odate": odate,
        "odate_dashed": odate_dt.strftime("%Y-%m-%d"),
        # ints for arithmetic...
        "year": odate_dt.year,
        "month": odate_dt.month,
        "day": odate_dt.day,
        # ...and zero-padded strings for building identifiers. Both, because
        # f"{year}{month}{day}" on 8 August gives "202688", not "20260808".
        "yyyy": f"{odate_dt.year:04d}",
        "mm": f"{odate_dt.month:02d}",
        "dd": f"{odate_dt.day:02d}",
        "source": source,
    }

    task_log.info(
        "[odate] logical_date=%s data_interval_end=%s -> ODATE %s (from %s, tz %s)",
        dag_run.logical_date,
        context.get("data_interval_end"),
        odate,
        source,
        BUSINESS_TIMEZONE,
    )
    task_log.info(
        "[odate] year=%s month=%s day=%s | yyyy=%s mm=%s dd=%s",
        parts["year"],
        parts["month"],
        parts["day"],
        parts["yyyy"],
        parts["mm"],
        parts["dd"],
    )

    return parts


def compare_with_logical_date(ti=None, **context):
    """Log ODATE beside the logical date, so the off-by-one is visible in one place."""
    task_log = logging.getLogger("airflow.task")

    parts = ti.xcom_pull(task_ids="get_odate_parts")
    if not parts:
        raise ValueError("get_odate_parts pushed no result")

    dag_run = context["dag_run"]
    logical = dag_run.logical_date

    # The comparison only means something on a scheduled run; a manual one has no
    # logical_date to be off by one from.
    if logical is None:
        task_log.info(
            "[odate] manual run — logical_date is None, ODATE %s came from %s",
            parts["odate"],
            parts["source"],
        )
        return {"odate": parts["odate"], "logical_ds_nodash": None, "differ": None}

    logical_nodash = logical.strftime(ODATE_FORMAT)
    differ = logical_nodash != parts["odate"]

    task_log.info(
        "[odate] ODATE %s vs logical_date %s — %s",
        parts["odate"],
        logical_nodash,
        "one interval apart, as expected for a daily job"
        if differ
        else "identical (schedule is not a daily interval, or tz is UTC)",
    )

    if differ:
        task_log.info(
            "[odate] partitioning by the wrong one shifts every downstream "
            "write by a day — ODATE_SOURCE=%r selects it",
            ODATE_SOURCE,
        )

    return {
        "odate": parts["odate"],
        "logical_ds_nodash": logical_nodash,
        "differ": differ,
    }


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-logical-date-pattern-for-odate",
    description="Convert an Airflow logical date to a Control-M ODATE and its parts",
    # Daily at 02:00 so there is a real interval: the run firing on the 21st has
    # a logical_date of the 20th, which is the point the DAG demonstrates.
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo", "scheduling", "control-m", "odate", "dates"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    parts = PythonOperator(
        task_id="get_odate_parts",
        python_callable=get_odate_parts,
        doc_md="""
Derives the Control-M **ODATE** and its year/month/day parts from the run's dates.

Resolution order: `dag_run.conf["odate"]` → `data_interval_end` (per
`ODATE_SOURCE`) → `logical_date` → `run_after`. The last step matters —
`logical_date` is `None` on every manually triggered run in Airflow 3, so
reading it directly raises `AttributeError` on exactly the run you test with.

Converts into `BUSINESS_TIMEZONE` **before** formatting, because ODATE is a local
business date while `logical_date` is always UTC — a 19:30 UTC run is already
tomorrow in Bangkok.

Returns both ints (`year`, `month`, `day`) and zero-padded strings
(`yyyy`, `mm`, `dd`): `f"{year}{month}{day}"` on 8 August gives `202688`.
""",
    )

    compare = PythonOperator(
        task_id="compare_with_logical_date",
        python_callable=compare_with_logical_date,
        doc_md="""
Logs the ODATE beside the logical date so the one-day gap is visible in a single
line.

For a daily job they *should* differ: the run firing on the 21st covers the
interval that began on the 20th, so its `logical_date` is the 20th while
Control-M calls it ODATE `20260821`.

On a manual run there is no `logical_date` to compare against, and the task says
so rather than inventing one.
""",
    )

    template = BashOperator(
        task_id="show_odate_in_template",
        # No callable needed to format a date. ds_nodash IS the YYYYMMDD of the
        # logical date; the filter form applies it to any datetime in context.
        bash_command=(
            'echo "ds                       = {{ ds }}"; '
            'echo "ds_nodash (logical)      = {{ ds_nodash }}"; '
            'echo "data_interval_start      = {{ data_interval_start }}"; '
            'echo "data_interval_end        = {{ data_interval_end }}"; '
            'echo "ODATE (interval end)     = {{ data_interval_end | ds_nodash }}"; '
            'echo "ODATE (business tz)      = '
            '{{ data_interval_end.in_timezone("' + BUSINESS_TIMEZONE + '") | ds_nodash }}"; '
            'echo "year/month/day           = {{ data_interval_end.year }} '
            '{{ \'%02d\' % data_interval_end.month }} '
            '{{ \'%02d\' % data_interval_end.day }}"; '
            'echo "via macros.ds_format     = '
            '{{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y%m%d\') }}"; '
            'echo "--- Control-M equivalents ---"; '
            # \$ so bash does not expand $YEAR to an empty variable — the
            # literal Control-M token is the whole point of the line.
            'echo "%%\\$YEAR                  = {{ ds[:4] }}"; '
            'echo "%%MONTH                  = {{ ds[5:7] }}"; '
            'echo "%%DAY                    = {{ ds[8:10] }}"; '
            'echo "%%PREV                   = '
            '{{ macros.ds_format(macros.ds_add(ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}"; '
            'echo "%%SUBSTR %%PREV 1 4      = {{ macros.ds_add(ds, -1)[:4] }}"; '
            'echo "%%SUBSTR %%PREV 5 2      = {{ macros.ds_add(ds, -1)[5:7] }}"; '
            'echo "%%SUBSTR %%PREV 7 2      = {{ macros.ds_add(ds, -1)[8:10] }}"'
        ),
        doc_md="""
The same conversion in **Jinja**, with no Python at all — printed so the two
routes can be compared in one run's logs.

`ds_nodash` is already the `YYYYMMDD` of the logical date. As a *filter* it
applies to any datetime in context, so `{{ data_interval_end | ds_nodash }}` is
the true ODATE, and `.in_timezone(...)` chains ahead of it for the business day.

The second block is the **Control-M variable mapping** — `%%$YEAR`, `%%MONTH`,
`%%DAY`, `%%PREV` and its `%%SUBSTR` parts.

Those use `ds` slicing rather than `logical_date.strftime(...)`, which is the
obvious translation and raises `UndefinedError: 'None' has no attribute
'strftime'` on a manual run, since `logical_date` is nullable in Airflow 3. `ds`
is a string Airflow always renders, so slicing it cannot fail that way.

`%%PREV` uses `macros.ds_add(ds, -1)` — calendar arithmetic on the business date.
`logical_date - timedelta(days=1)` subtracts 24 hours from a UTC instant, which
lands on the wrong day across a DST boundary in a non-UTC business timezone.

Prefer this when the date is only being passed to a command; a callable earns
its place when the date drives real logic. Note the `'%02d' %` formatting —
`{{ data_interval_end.month }}` alone is unpadded and builds `202688`.
""",
    )

    # compare reads get_odate_parts' XCom — a data dependency. The template task
    # is independent: it demonstrates the same conversion without Python, so it
    # deliberately does not consume the callable's output.
    parts >> compare
    parts >> template
