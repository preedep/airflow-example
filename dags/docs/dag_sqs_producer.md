# `dag_sqs_producer.py`

📄 **[Source: `dag_sqs_producer.py`](../dag_sqs_producer.py)**

`nix-dag-sqs-producer` — generates messages, publishes them to SQS, then triggers
[`nix-dag-sqs-consumer`](dag_sqs_consumer.md) **and waits for it to finish**.

```
generate ──▶ publish ──▶ trigger consumer ──(waits)──▶ report
                              │                          ▲
                              └── nix-dag-sqs-consumer ───┘
```

```bash
airflow dags trigger nix-dag-sqs-producer \
  --conf '{"message_count":3,"batch_label":"demo"}'
```

Defaults to 3 messages labelled `demo`.

## The two DAGs are one workflow

The consumer is not scheduled and is not meant to be triggered by hand — this DAG
drives it. Each step proves something the previous one could not:

| Step | DAG | What it proves |
|---|---|---|
| 1 | producer | messages are built and visible in XCom before sending |
| 2 | producer | messages reach the queue |
| 3 | producer | `TriggerDagRunOperator` starts the consumer |
| 4 | consumer | the sensor reads exactly those messages |
| 5 | producer | the wait unblocks only once the consumer succeeds |

Step 5 is what makes this a workflow rather than two unrelated DAGs.

## Verified run

Both DAGs, one trigger:

```
[sqs_producer] STEP 1/4  generate
[sqs_producer]   batch=demo  count=3  run_id=v-sqs-001
[sqs_producer]   built seq=1/3 payload=demo-payload-001
[sqs_producer] STEP 2/4  publish
[sqs_producer]   sent seq=1/3 id=37a7970b-d487-481f
[sqs_producer] STEP 3/4  handing off to nix-dag-sqs-consumer
    [sqs_consumer] STEP 2/3  process
    [sqs_consumer]   triggered by nix-dag-sqs-producer run v-sqs-001
    [sqs_consumer] STEP 3/3  verify
    [sqs_consumer]   complete — no gaps in 1..3
    [sqs_consumer] CONSUMED SUCCESSFULLY
[sqs_producer] STEP 4/4  consumer finished
[sqs_producer] WORKFLOW COMPLETE
```

The state sequence is the proof that the wait works:

```
producer=running  consumer=none
producer=running  consumer=running   from-producer-v-sqs-001
producer=running  consumer=success   from-producer-v-sqs-001
producer=success  consumer=success   from-producer-v-sqs-001
```

## `wait_for_completion` defaults to False

The single most important argument, and the easiest to omit:

```python
TriggerDagRunOperator(
    trigger_dag_id="nix-dag-sqs-consumer",
    wait_for_completion=True,     # ← default False: returns immediately
    poke_interval=10,
    allowed_states=["success"],
    failed_states=["failed"],     # ← without this, a failure polls to timeout
    trigger_run_id="from-producer-{{ run_id }}",
)
```

Without it the task succeeds the instant the consumer run is **created**, saying
nothing about whether the messages were ever read. The DAG would look green while
the consumer failed.

`failed_states` matters nearly as much: unset, a failed consumer leaves the
producer polling until its own timeout instead of failing promptly with a cause.

`trigger_run_id` is templated on `run_id` because a fixed value collides on the
second trigger.

## `fail_when_dag_is_paused` does not work on Airflow 3.x

This looked like exactly the right guard — new DAGs land **paused**, and a paused
consumer accepts the triggered run but never executes it, so the producer waits
until timeout on a failure that looks like SQS being slow.

The argument is in the signature, but setting it fails at **parse time**:

```
NotImplementedError: Setting `fail_when_dag_is_paused` not yet supported for Airflow 3.x
```

Caught by the parse check rather than in production. There is no built-in guard,
so the DAG uses `execution_timeout` on the trigger task to bound the hang —
minutes rather than hours — and the real fix is **unpausing the consumer before
the first run**.

## Message shape

Each body is JSON so the consumer can assert on it:

```json
{"batch": "demo", "seq": 1, "of": 3, "run_id": "...", "payload": "..."}
```

`batch` and `run_id` are what let the consumer prove it read *this* run's
messages rather than leftovers from an earlier one. The batch label is also set
as a `message_attribute`, so a consumer can filter without parsing the body.

## Why a loop, not `SqsPublishOperator`

`SqsPublishOperator` publishes exactly **one** message per task. A
run-configurable count would need dynamic task mapping, which is more machinery
than the point being made — so `publish_messages` loops over `SqsHook.send_message`
and logs each returned `MessageId`.

Generation is a separate task from publishing on purpose: the exact payload is in
XCom before anything is sent, so a failed publish can be diagnosed against what
was intended.

## Ordering is not guaranteed

This is a **standard** queue, not FIFO, so messages may arrive out of order and
occasionally more than once. A FIFO queue (`.fifo` suffix) would require
`message_group_id` and `message_deduplication_id` on every publish.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_sqs_test_001` | AWS credentials, conn type `aws` |

Access key in **login**, secret in **password**, region in **extra** as
`{"region_name": "..."}` — not optional, since worker pods have no
`AWS_DEFAULT_REGION`. The queue URL is passed per operation.

IAM needs `sqs:SendMessage` here, plus `sqs:ReceiveMessage` and
`sqs:DeleteMessage` for the consumer.

---

[← back to the DAG index](../README.md)
