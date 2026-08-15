"""nix-dag-sqs-consumer — consume messages from SQS, driven by the producer DAG."""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.standard.operators.python import PythonOperator

DAG_DOC_MD = """
### nix-dag-sqs-consumer

Consumes messages from SQS and verifies it received the batch the producer sent.

```
nix-dag-sqs-producer ──triggers──▶ wait_for_messages ──▶ process ──▶ summarise
                                   (SqsSensor)
```

#### Not meant to be triggered by hand

`nix-dag-sqs-producer` drives this DAG through `TriggerDagRunOperator` and waits
for it to finish. Triggering it directly still works, but the run conf carrying
the expected batch is then absent, so the verification step degrades to "report
whatever was on the queue".

Run conf supplied by the producer:

```json
{"batch": "demo", "expected_count": 3, "producer_run_id": "manual__..."}
```

#### `delete_message_on_reception` defaults to True

The most consequential default in `SqsSensor`, and it is easy to get backwards.

| Setting | Behaviour |
|---|---|
| `True` *(default)* | messages are deleted as soon as the sensor reads them |
| `False` | messages return to the queue after the visibility timeout |

`True` is right here — the sensor and the downstream task are one logical unit,
so a message read is a message handled. But it means **the messages are gone
before `process_messages` runs**. If that task fails, the payload exists only in
XCom; nothing is left on the queue to retry.

Setting `False` gives at-least-once delivery instead, at the cost of the same
message being re-read every run until something deletes it. That is the right
choice when the downstream work is expensive and must not be lost — with the
consequence that the consumer must delete explicitly once it has succeeded.

Neither is universally correct. The demo picks `True` and states the trade-off
rather than hiding it.

#### The sensor pushes the messages themselves

`SqsSensor` puts the received messages in XCom under `messages`, so the
downstream task reads exactly what satisfied the sensor:

```python
ti.xcom_pull(task_ids="wait_for_messages", key="messages")
```

That is the same hand-off the S3 prefix sensor has to be subclassed to provide.

#### Batching, and why `num_batches` matters

SQS returns at most 10 messages per `ReceiveMessage` call, and often fewer than
are available — a single call may return 1 of 3 messages even when all 3 are
ready, because the service samples a subset of its servers.

| Setting | Effect |
|---|---|
| `max_messages=10` | cap per call (10 is the SQS maximum) |
| `num_batches=3` | make up to 3 calls per poke |
| `wait_time_seconds=10` | long-poll, so a call waits for messages |

`num_batches > 1` with long polling is what makes a small batch arrive in one
poke rather than dribbling across several. Without it a 3-message batch commonly
needs multiple pokes, and the producer waits longer than it should.

#### Ordering is not guaranteed

A standard queue may deliver out of order and, rarely, more than once. The
verification therefore compares the **set** of sequence numbers, not their
order, and tolerates duplicates.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_sqs_test_001` | AWS credentials, conn type `aws` |

The IAM policy needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` — the latter
because `delete_message_on_reception` is on.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_sqs_test_001"

SQS_QUEUE_URL = (
    "https://sqs.ap-southeast-1.amazonaws.com/743702012710/sqs-airflow-dev001"
)

PRODUCER_DAG_ID = "nix-dag-sqs-producer"


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def process_messages(ti=None, **context):
    """Decode each message body and log it.

    The messages are already deleted from the queue by the time this runs —
    `delete_message_on_reception` is on — so this XCom is the only copy.
    """
    task_log = logging.getLogger("airflow.task")

    conf = context["dag_run"].conf or {}
    expected_batch = conf.get("batch")
    producer_run_id = conf.get("producer_run_id")

    task_log.info("=" * 68)
    task_log.info("[sqs_consumer] STEP 2/3  process")
    if producer_run_id:
        task_log.info("[sqs_consumer]   triggered by %s run %s", PRODUCER_DAG_ID, producer_run_id)
    else:
        task_log.info("[sqs_consumer]   no producer conf — triggered directly")

    # The sensor pushes what it received under "messages".
    received = ti.xcom_pull(task_ids="wait_for_messages", key="messages") or []
    if not received:
        raise ValueError("sensor succeeded but pushed no messages")

    task_log.info("[sqs_consumer]   received %d message(s) from the queue", len(received))

    decoded = []
    for raw in received:
        body = raw.get("Body", "")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            # A foreign message on a shared queue is worth surfacing rather than
            # crashing the run — it is not necessarily this pipeline's problem.
            task_log.warning("[sqs_consumer]   non-JSON body skipped: %.60s", body)
            continue

        decoded.append(payload)
        task_log.info(
            "[sqs_consumer]   seq=%s/%s batch=%s payload=%s",
            payload.get("seq"),
            payload.get("of"),
            payload.get("batch"),
            payload.get("payload"),
        )

    if expected_batch:
        # Guard against reading a previous run's leftovers: every decoded
        # message should carry the batch label the producer passed in conf.
        wrong = [p for p in decoded if p.get("batch") != expected_batch]
        if wrong:
            raise ValueError(
                f"expected batch {expected_batch!r}, but {len(wrong)} message(s) "
                f"carried a different label: {[p.get('batch') for p in wrong]}"
            )
        task_log.info("[sqs_consumer]   all message(s) match batch=%s", expected_batch)

    task_log.info("[sqs_consumer] STEP 2/3  done — %d message(s) decoded", len(decoded))
    return {"batch": expected_batch, "count": len(decoded), "messages": decoded}


def summarise(ti=None, **context):
    """Confirm the received set matches what the producer said it sent."""
    task_log = logging.getLogger("airflow.task")

    conf = context["dag_run"].conf or {}
    processed = ti.xcom_pull(task_ids="process_messages")
    if not processed:
        raise ValueError("process_messages pushed no result")

    decoded = processed["messages"]

    task_log.info("=" * 68)
    task_log.info("[sqs_consumer] STEP 3/3  verify")

    # expected_count arrives through conf as a string, since the producer
    # templates it — coerce rather than comparing str to int.
    raw_expected = conf.get("expected_count")
    expected = int(raw_expected) if raw_expected is not None else None

    # A standard queue can deliver out of order and, rarely, twice. Compare the
    # *set* of sequence numbers so neither breaks the check.
    seqs = sorted({p.get("seq") for p in decoded if p.get("seq") is not None})
    task_log.info("[sqs_consumer]   sequence numbers received: %s", seqs)

    if expected is not None:
        task_log.info("[sqs_consumer]   producer published: %d", expected)
        task_log.info("[sqs_consumer]   distinct received  : %d", len(seqs))

        missing = sorted(set(range(1, expected + 1)) - set(seqs))
        if missing:
            raise ValueError(
                f"missing sequence number(s) {missing} — expected 1..{expected}, got {seqs}"
            )
        task_log.info("[sqs_consumer]   complete — no gaps in 1..%d", expected)
    else:
        task_log.info("[sqs_consumer]   no expected_count in conf — reporting only")

    task_log.info("[sqs_consumer] CONSUMED SUCCESSFULLY")
    task_log.info("=" * 68)

    return {
        "batch": processed["batch"],
        "expected": expected,
        "received": len(seqs),
        "sequences": seqs,
        "status": "all expected messages consumed",
    }


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-sqs-consumer",
    description="Consume messages from SQS; triggered and awaited by the producer DAG",
    schedule=None,  # driven by nix-dag-sqs-producer
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "sqs", "consumer"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    wait_for_messages = SqsSensor(
        task_id="wait_for_messages",
        aws_conn_id=AWS_CONN_ID,
        sqs_queue=SQS_QUEUE_URL,
        # 10 is the SQS per-call maximum; num_batches makes several calls per
        # poke so a small batch arrives together rather than dribbling in.
        max_messages=10,
        num_batches=3,
        # Long-poll: the call waits for messages instead of returning empty.
        wait_time_seconds=10,
        # Default, stated explicitly because it is the consequential one —
        # messages are deleted as the sensor reads them. See the DAG docs.
        delete_message_on_reception=True,
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=15,
        timeout=60 * 10,  # 10 minutes — the producer is waiting on this
        doc_md="""
**Step 1 of 3.** Polls the queue and pushes what it receives to XCom under
`messages`.

`delete_message_on_reception=True` is the default and is set explicitly here
because of what it implies: the messages are **gone from the queue** before the
next task runs. If `process_messages` fails, the payload survives only in XCom.

`num_batches=3` with `wait_time_seconds=10` matters more than it looks — SQS
returns a sampled subset per call, so a 3-message batch often needs several
calls. Without it the producer waits through extra poke intervals.

`timeout` is deliberately short: the producer is blocked in
`wait_for_completion` while this polls.
""",
    )

    process = PythonOperator(
        task_id="process_messages",
        python_callable=process_messages,
        doc_md="""
**Step 2 of 3.** Decodes each JSON body and checks the batch label against the
conf the producer passed.

That check is what distinguishes "read some messages" from "read *this run's*
messages" — without it, leftovers from an earlier run would satisfy the sensor
and look like success.

A non-JSON body is logged and skipped rather than failing the run: on a shared
queue, another producer's message is not necessarily this pipeline's problem.
""",
    )

    verify = PythonOperator(
        task_id="summarise",
        python_callable=summarise,
        doc_md="""
**Step 3 of 3.** Compares the received sequence numbers against the count the
producer reported and fails on any gap.

Compares a **set**, not a list: a standard SQS queue may deliver out of order
and, rarely, deliver the same message twice. Neither should fail the run —
a genuinely missing message should.

Reaching this task successfully is what releases the producer's
`wait_for_completion`.
""",
    )

    wait_for_messages >> process >> verify
