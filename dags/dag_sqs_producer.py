"""nix-dag-sqs-producer — publish messages to SQS, then trigger the consumer and wait."""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DAG_DOC_MD = """
### nix-dag-sqs-producer

Generates messages, publishes them to SQS, then **triggers the consumer DAG and
waits for it to finish**.

```
generate ──▶ publish ──▶ trigger consumer ──(waits)──▶ report
                              │                          ▲
                              └── nix-dag-sqs-consumer ───┘
```

#### Trigger

Manual only (`schedule=None`). Optional run conf:

```json
{"message_count": 5, "batch_label": "demo"}
```

Defaults to 3 messages labelled `demo`.

#### The two DAGs are one workflow

`nix-dag-sqs-consumer` is not scheduled and is not meant to be triggered by
hand — this DAG drives it. The pairing is the point:

| Step | DAG | What it proves |
|---|---|---|
| 1 | producer | messages reach the queue |
| 2 | producer | `TriggerDagRunOperator` starts the consumer |
| 3 | consumer | the sensor sees exactly those messages |
| 4 | producer | the wait unblocks only after the consumer finishes |

Step 4 is what makes this a workflow rather than two unrelated DAGs. Without
`wait_for_completion` the producer would report success the moment it *fired*
the consumer, saying nothing about whether the messages were ever read.

#### `wait_for_completion` defaults to False

The single most important argument here, and the easiest to omit:

```python
TriggerDagRunOperator(
    trigger_dag_id="nix-dag-sqs-consumer",
    wait_for_completion=True,     # ← without this the task returns immediately
    poke_interval=10,
    allowed_states=["success"],
    failed_states=["failed"],
)
```

`allowed_states` and `failed_states` matter too. With `failed_states` unset a
failed consumer leaves the producer waiting until its own timeout rather than
failing promptly with a useful message.

#### Unpause the consumer, or this hangs

New DAGs land **paused**. A paused consumer still accepts the triggered run, but
never executes it — so the producer sits in `wait_for_completion` until its own
timeout, a confusing failure that looks like SQS being slow.

`TriggerDagRunOperator` advertises `fail_when_dag_is_paused` for exactly this,
but on Airflow 3.x it raises at DAG-parse time:

```
NotImplementedError: Setting `fail_when_dag_is_paused` not yet supported for Airflow 3.x
```

So there is no built-in guard. `execution_timeout` on the trigger task bounds
the damage — it fails in minutes rather than hanging for hours — but unpausing
the consumer is the actual fix.

#### Message shape

Each message body is JSON so the consumer can assert on it:

```json
{"batch": "demo", "seq": 1, "of": 3, "run_id": "...", "payload": "..."}
```

`batch` and `run_id` let the consumer confirm it read *this* producer's messages
rather than leftovers from an earlier run.

#### Ordering is not guaranteed

This is a **standard** queue, not FIFO, so messages can arrive out of order and
very occasionally more than once. The consumer therefore checks the *set* of
sequence numbers rather than their order. A FIFO queue (`.fifo` suffix) would
need `message_group_id` and `message_deduplication_id` on every publish.

#### Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_sqs_test_001` | AWS credentials, conn type `aws` |

Access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`.

The queue URL is passed per operation, not stored on the connection. The IAM
policy needs `sqs:SendMessage` here and `sqs:ReceiveMessage` +
`sqs:DeleteMessage` for the consumer.
"""

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_CONN_ID = "aws_sqs_test_001"

# Standard (non-FIFO) queue. Either the URL or the ARN works.
SQS_QUEUE_URL = (
    "https://sqs.ap-southeast-1.amazonaws.com/743702012710/sqs-airflow-dev001"
)

CONSUMER_DAG_ID = "nix-dag-sqs-consumer"

DEFAULT_MESSAGE_COUNT = 3
DEFAULT_BATCH_LABEL = "demo"


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #


def generate_messages(**context):
    """Build the message bodies. Kept separate from publishing so the payload is
    visible in XCom before anything is sent."""
    task_log = logging.getLogger("airflow.task")

    conf = context["dag_run"].conf or {}
    count = int(conf.get("message_count", DEFAULT_MESSAGE_COUNT))
    label = conf.get("batch_label", DEFAULT_BATCH_LABEL)
    run_id = context["dag_run"].run_id

    if count < 1:
        raise ValueError(f"message_count must be >= 1, got {count}")

    task_log.info("=" * 68)
    task_log.info("[sqs_producer] STEP 1/4  generate")
    task_log.info("[sqs_producer]   batch=%s  count=%d  run_id=%s", label, count, run_id)

    messages = [
        {
            "batch": label,
            "seq": i,
            "of": count,
            "run_id": run_id,
            "payload": f"{label}-payload-{i:03d}",
        }
        for i in range(1, count + 1)
    ]

    for m in messages:
        task_log.info(
            "[sqs_producer]   built seq=%d/%d payload=%s", m["seq"], m["of"], m["payload"]
        )

    task_log.info("[sqs_producer] STEP 1/4  done — %d message(s) ready", len(messages))
    return {"batch": label, "count": count, "run_id": run_id, "messages": messages}


def publish_messages(ti=None, **context):
    """Send every generated message to SQS.

    Uses `SqsHook` in a loop rather than `SqsPublishOperator`, which publishes a
    single message per task. A dynamic count would otherwise need dynamic task
    mapping, which is more machinery than this demo needs.
    """
    task_log = logging.getLogger("airflow.task")

    generated = ti.xcom_pull(task_ids="generate_messages")
    if not generated:
        raise ValueError("generate_messages pushed no result")

    messages = generated["messages"]
    batch = generated["batch"]

    task_log.info("=" * 68)
    task_log.info("[sqs_producer] STEP 2/4  publish")
    task_log.info("[sqs_producer]   queue=%s", SQS_QUEUE_URL.rsplit("/", 1)[-1])
    task_log.info("[sqs_producer]   sending %d message(s) for batch=%s", len(messages), batch)

    hook = SqsHook(aws_conn_id=AWS_CONN_ID)
    sent = []

    for m in messages:
        response = hook.send_message(
            queue_url=SQS_QUEUE_URL,
            message_body=json.dumps(m),
            # message_attributes keep the batch label out of the body too, so a
            # consumer can filter without parsing JSON.
            message_attributes={"batch": {"StringValue": batch, "DataType": "String"}},
        )
        message_id = response["MessageId"]
        sent.append({"seq": m["seq"], "message_id": message_id})
        task_log.info(
            "[sqs_producer]   sent seq=%d/%d id=%s", m["seq"], m["of"], message_id[:18]
        )

    task_log.info("[sqs_producer] STEP 2/4  done — %d message(s) on the queue", len(sent))
    task_log.info("[sqs_producer] STEP 3/4  handing off to %s", CONSUMER_DAG_ID)
    return {"batch": batch, "count": len(sent), "run_id": generated["run_id"], "sent": sent}


def report_outcome(ti=None, **context):
    """Close the loop: the consumer has finished by the time this runs."""
    task_log = logging.getLogger("airflow.task")

    published = ti.xcom_pull(task_ids="publish_messages")
    if not published:
        raise ValueError("publish_messages pushed no result")

    task_log.info("=" * 68)
    task_log.info("[sqs_producer] STEP 4/4  consumer finished")
    task_log.info("[sqs_producer]   batch=%s", published["batch"])
    task_log.info("[sqs_producer]   published=%d message(s)", published["count"])
    task_log.info(
        "[sqs_producer]   the trigger task waited for %s to reach success",
        CONSUMER_DAG_ID,
    )
    task_log.info("[sqs_producer] WORKFLOW COMPLETE")
    task_log.info("=" * 68)

    return {
        "batch": published["batch"],
        "published": published["count"],
        "consumer_dag_id": CONSUMER_DAG_ID,
        "status": "producer and consumer both completed",
    }


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="nix-dag-sqs-producer",
    description="Publish messages to SQS, trigger the consumer DAG, and wait for it",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "aws", "sqs", "producer", "trigger"],
    default_args={"owner": "nix", "retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=DAG_DOC_MD,
) as dag:
    generate = PythonOperator(
        task_id="generate_messages",
        python_callable=generate_messages,
        doc_md="""
**Step 1 of 4.** Builds the message bodies and pushes them to XCom.

Separate from publishing on purpose: the exact payload is visible in XCom before
anything is sent, so a failed publish can be diagnosed against what was intended.

Each body carries `batch`, `seq`, `of` and `run_id`, which is what lets the
consumer prove it read *this* run's messages rather than leftovers.
""",
    )

    publish = PythonOperator(
        task_id="publish_messages",
        python_callable=publish_messages,
        doc_md="""
**Step 2 of 4.** Sends every message with `SqsHook.send_message` and logs the
returned `MessageId` for each.

A loop rather than `SqsPublishOperator`, which sends exactly one message per
task — a run-configurable count would otherwise need dynamic task mapping.
""",
    )

    trigger_consumer = TriggerDagRunOperator(
        task_id="trigger_consumer",
        trigger_dag_id=CONSUMER_DAG_ID,
        # The consumer needs to know what to expect, so it can assert it read
        # this run's batch rather than stale messages.
        conf={
            "batch": "{{ ti.xcom_pull(task_ids='publish_messages')['batch'] }}",
            "expected_count": "{{ ti.xcom_pull(task_ids='publish_messages')['count'] }}",
            "producer_run_id": "{{ run_id }}",
        },
        # Without this the task returns as soon as the run is created, proving
        # nothing about whether the messages were consumed.
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        # Without failed_states a failed consumer leaves this waiting until
        # timeout instead of failing promptly.
        failed_states=["failed"],
        # A fixed run_id would collide on the second run.
        trigger_run_id="from-producer-{{ run_id }}",
        # No fail_when_dag_is_paused: it raises NotImplementedError on Airflow
        # 3.x. This bounds the wait instead, so a paused consumer fails the task
        # in minutes rather than hanging until the DAG-level timeout.
        execution_timeout=timedelta(minutes=12),
        doc_md="""
**Step 3 of 4.** Starts `nix-dag-sqs-consumer` and blocks until it finishes.

Four arguments carry the weight here:

- **`wait_for_completion=True`** — the default is `False`, which would make this
  task succeed the instant the consumer run is *created*.
- **`failed_states=["failed"]`** — without it, a failed consumer leaves this
  polling until timeout rather than failing with a clear cause.
- **the consumer must be unpaused** — `fail_when_dag_is_paused` exists in the
  signature but raises `NotImplementedError` on Airflow 3.x, so there is no
  guard: a paused consumer makes this task poll until `execution_timeout`.
- **`trigger_run_id`** templated on `run_id` — a fixed value collides on the
  second trigger.

`conf` passes the batch label and expected count, so the consumer can prove it
read this run's messages.
""",
    )

    report = PythonOperator(
        task_id="report",
        python_callable=report_outcome,
        doc_md="""
**Step 4 of 4.** Runs only after the consumer has reached `success`.

Its existence is the proof: reaching this task means the whole produce →
consume round trip completed, not merely that the messages were sent.
""",
    )

    generate >> publish >> trigger_consumer >> report
