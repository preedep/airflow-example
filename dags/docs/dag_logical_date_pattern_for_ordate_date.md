# `dag_logical_date_pattern_for_ordate_date.py`

📄 **[Source: `dag_logical_date_pattern_for_ordate_date.py`](../dag_logical_date_pattern_for_ordate_date.py)**


`nix-dag-logical-date-pattern-for-odate` — converts an Airflow **logical date** into
a Control-M **ODATE**, and pulls the year, month and day out of it. Two routes: a
task callable, and pure Jinja.

Nothing is transferred. It runs daily at 02:00 so there is a real interval to reason
about, and it is safe to trigger by hand — the manual run is what exercises the main
trap.

```bash
airflow dags unpause nix-dag-logical-date-pattern-for-odate
airflow dags trigger nix-dag-logical-date-pattern-for-odate --conf '{"odate":"20260821"}'
```

---

## The off-by-one that makes this worth a DAG

ODATE and `logical_date` both mean "the date this run stands for", and for a daily
job **they disagree by one day**.

Verified on this Airflow with `schedule="0 2 * * *"`:

```
data_interval_start : 2026-08-20 02:00:00+00:00   <- logical_date
data_interval_end   : 2026-08-21 02:00:00+00:00
run_after           : 2026-08-21 02:00:00+00:00   <- when it actually fires
```

The run that **fires on the 21st** has a `logical_date` of the **20th**, because it
covers the interval that *began* on the 20th. Control-M calls that same run ODATE
`20260821`.

| | Control-M ODATE | Airflow `logical_date` |
|---|---|---|
| Means | the **scheduling day** the job belongs to | the **start of the interval** the run covers |
| Daily job firing 2026-08-21 02:00 | `20260821` | `2026-08-20 02:00` |
| Timezone | the server's local business day | always **UTC** |
| Manual run | operator supplies it | **`None`** |

So there are two defensible mappings, and picking wrong silently shifts every
downstream partition by a day:

| You want | Use |
|---|---|
| the day the data *covers* | `logical_date` — matches Airflow's own `ds`, backfills correctly |
| the day the job *runs* (true ODATE) | `data_interval_end` — equals Control-M's ODATE for a daily job |

`ODATE_SOURCE` selects it, defaulting to `data_interval_end` because this DAG is
about matching Control-M.

The rule that resolves it: **ODATE answers "which scheduling day is this?",
`logical_date` answers "which interval is this?"** They coincide only when the
interval is named after its end.

---

## Control-M variable → Jinja

The mapping most people want first:

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

### Why not `logical_date.strftime('%Y')`

That is the obvious translation, and it breaks on a manual run:

```
scheduled  -> 2026
manual     -> UndefinedError: 'None' has no attribute 'strftime'
```

`logical_date` is nullable in Airflow 3, and Jinja gives you no escape — the
expression raises before any `default` filter applies. `ds` is a **string** Airflow
always renders, so slicing it cannot fail that way, and `ds[:4]` is cheaper than
parsing a datetime to re-format it.

### Why `%%PREV` is `ds_add`, not `logical_date - timedelta(days=1)`

`macros.timedelta` does exist, so the subtraction renders fine on a scheduled run.
It just inherits the `None` problem above *and* adds one: subtracting 24 hours from
a UTC instant lands on the wrong day across a DST boundary in a non-UTC business
timezone. `%%PREV` is Control-M's *previous scheduling date*, so calendar arithmetic
on the business date is the honest equivalent.

### One caveat on the first row

`%%ODATE` maps to `data_interval_end`, **not** `ds_nodash` — that is the off-by-one
above. `ds_nodash` is the logical date, so for a daily job it is the day *before*
the ODATE Control-M would show.

The `%%$YEAR`/`%%MONTH`/`%%DAY` rows take their parts from `ds`, so they are parts
of the *logical date*. If you need the parts of the **ODATE**, take them from
`get_odate_parts`, which resolves the source once and consistently.

---

## Trap: `logical_date` is `None` on a manual run

```python
logical_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
```

A run triggered by hand has no logical date at all, so
`context["logical_date"].strftime(...)` raises `AttributeError` on exactly the run
you use to test it. `run_after` — when the run was queued — is the sane fallback,
and is what a manual Control-M order does anyway.

The DAG resolves in this order:

```
dag_run.conf["odate"]  ->  data_interval_end  ->  logical_date  ->  run_after
```

and returns which rule fired as `source`, because the difference between two of them
is a day and a log line that does not say which one was used is not much help at
03:00.

---

## Trap: ODATE is a *local* business date, `logical_date` is UTC

```
logical_date (UTC)      : 2026-08-20 19:30:00+00:00  -> ODATE 20260820
same instant in Bangkok : 2026-08-21 02:30:00+07:00  -> ODATE 20260821
```

A 19:30 UTC run is already **tomorrow** in Bangkok. Set `BUSINESS_TIMEZONE` to the
timezone Control-M schedules in and convert **before** formatting, or every evening
run lands in the wrong day's partition. `"UTC"` disables the conversion.

A conf-supplied ODATE is naive and already a business date, so it is deliberately
*not* converted — doing so would shift the very value the operator pinned.

---

## Trap: `f"{year}{month}{day}"` is not `YYYYMMDD`

`get_odate_parts` returns both ints and zero-padded strings, which looks redundant
until 8 August:

```
naive f-string would give: 202688
padded strings give      : 20260808
```

`month` and `day` are ints for arithmetic; `mm` and `dd` are the zero-padded strings
for building identifiers. Use the wrong pair and you get a six-character "date" that
sorts and joins wrong, quietly, on 15 days of the year.

| Key | Example | Note |
|---|---|---|
| `odate` | `20260821` | `YYYYMMDD`, Control-M's own format |
| `odate_dashed` | `2026-08-21` | for SQL and partition paths |
| `year` / `month` / `day` | `2026` / `8` / `21` | ints — **not** padded |
| `yyyy` / `mm` / `dd` | `2026` / `08` / `21` | strings, padded |
| `source` | `data_interval_end` | which rule produced it |

The same applies in Jinja: `{{ data_interval_end.month }}` is unpadded, so the
template task uses `{{ '%02d' % data_interval_end.month }}`.

---

## Tasks

| Task | Does |
|---|---|
| `get_odate_parts` | resolves the ODATE and returns its parts, with the source |
| `compare_with_logical_date` | logs ODATE beside the logical date so the gap is visible in one line |
| `show_odate_in_template` | the same conversion in pure Jinja, plus the Control-M mapping |

`compare` reads the callable's XCom, so that edge is a data dependency. The template
task is deliberately independent — it demonstrates the same conversion *without*
Python, so it does not consume the callable's output.

Prefer templating when the date is only being passed to a command; a callable earns
its place when the date drives real logic.

---

## Verifying it locally

`airflow dags test` runs every task for real, and is the quickest way to see the
mapping render:

```bash
airflow dags test nix-dag-logical-date-pattern-for-odate 2026-08-21
```

```
ds_nodash (logical)      = 20260821
ODATE (interval end)     = 20260821
--- Control-M equivalents ---
%%$YEAR                  = 2026
%%PREV                   = 20260820
%%SUBSTR %%PREV 5 2      = 08
```

**One caveat that will confuse you.** `dags test` synthesizes a run whose
`data_interval_start` and `data_interval_end` are the *same instant*, so ODATE and
the logical date come out equal and `compare_with_logical_date` reports
`differ: False`. That is an artifact of the test harness, not the DAG — on a real
scheduled run the interval is a full day wide and they differ by one, which is the
whole point. Unpause it and let one fire to see the gap.

The `\$` in the Bash task's `%%$YEAR` line is deliberate: without it bash expands
`$YEAR` to an empty variable and the log shows `%%  = 2026`, losing the literal
Control-M token the line exists to print.

---

## Requires

Nothing — no connections, no Variables. That makes this the easiest DAG here to run
on an unfamiliar deployment.
