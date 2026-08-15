# `dag_sqs_consumer.py`

📄 **[Source: `dag_sqs_consumer.py`](../dag_sqs_consumer.py)**

`nix-dag-sqs-consumer` — consumes messages from SQS and verifies it received the
batch [`nix-dag-sqs-producer`](dag_sqs_producer.md) sent.

```
nix-dag-sqs-producer ──triggers──▶ wait_for_messages ──▶ process ──▶ summarise
                                   (SqsSensor)
```

## Not meant to be triggered by hand

The producer drives this DAG through `TriggerDagRunOperator` and waits for it.
Triggering it directly still works, but the run conf carrying the expected batch
is then absent, so verification degrades to "report whatever was on the queue".

Conf supplied by the producer:

```json
{"batch": "demo", "expected_count": 3, "producer_run_id": "v-sqs-001"}
```

Verified run:

```
[sqs_consumer] STEP 2/3  process
[sqs_consumer]   triggered by nix-dag-sqs-producer run v-sqs-001
[sqs_consumer]   received 3 message(s) from the queue
[sqs_consumer]   seq=1/3 batch=demo payload=demo-payload-001
[sqs_consumer]   all message(s) match batch=demo
[sqs_consumer] STEP 3/3  verify
[sqs_consumer]   sequence numbers received: [1, 2, 3]
[sqs_consumer]   complete — no gaps in 1..3
[sqs_consumer] CONSUMED SUCCESSFULLY
```

## `delete_message_on_reception` defaults to True

The most consequential default in `SqsSensor`, and easy to get backwards.

| Setting | Behaviour |
|---|---|
| `True` *(default)* | messages are deleted as soon as the sensor reads them |
| `False` | messages return to the queue after the visibility timeout |

`True` is right here — the sensor and its downstream task are one logical unit,
so a message read is a message handled. But it means **the messages are gone
before `process_messages` runs**. If that task fails, the payload exists only in
XCom; there is nothing left on the queue to retry.

`False` gives at-least-once delivery instead, at the cost of re-reading the same
message every run until something deletes it — which the consumer must then do
explicitly once it has succeeded.

Neither is universally correct. This demo picks `True` and states the trade-off
rather than hiding it.

## The sensor hands the messages downstream

`SqsSensor` pushes what it received into XCom under `messages`:

```python
ti.xcom_pull(task_ids="wait_for_messages", key="messages")
```

That is the same hand-off the [S3 prefix sensor](dag_s3_prefix_suffix_sensor.md)
has to be **subclassed** to provide, and the same one
[`SFTPSensor`](dag_sftp_sensor.md) provides via `python_callable`. Three
providers, three different answers to the same question — worth checking per
sensor rather than assuming.

## `num_batches` matters more than it looks

SQS returns at most 10 messages per `ReceiveMessage` call, and often **fewer than
are available** — the service samples a subset of its servers, so a call may
return 1 of 3 ready messages.

| Setting | Effect |
|---|---|
| `max_messages=10` | cap per call (10 is the SQS maximum) |
| `num_batches=3` | make up to 3 calls per poke |
| `wait_time_seconds=10` | long-poll, so a call waits rather than returning empty |

`num_batches > 1` with long polling is what makes a small batch arrive in **one
poke**. Without it a 3-message batch commonly dribbles across several pokes — and
here the producer is blocked in `wait_for_completion` the whole time, so every
extra poke is added latency on the parent DAG.

`timeout` is deliberately short (10 minutes) for the same reason.

## Verifying it read *this* run's messages

Two checks, both needed:

**Batch label** — every decoded message must carry the label from conf. Without
this, leftovers from an earlier run would satisfy the sensor and look like
success.

**Sequence completeness** — the received sequence numbers are compared against
`expected_count` and any gap fails the run.

The comparison is a **set**, not a list. A standard queue may deliver out of
order and, rarely, deliver the same message twice; neither should fail the run,
while a genuinely missing message should.

`expected_count` arrives through conf as a **string** because the producer
templates it, so it is coerced with `int()` rather than compared to an integer
directly.

## Non-JSON bodies are skipped, not fatal

A body that fails to parse is logged and skipped rather than failing the run — on
a shared queue, another producer's message is not necessarily this pipeline's
problem. A message that parses but carries the wrong batch label *is* fatal,
because that indicates the pipeline read the wrong data.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_sqs_test_001` | AWS credentials, conn type `aws` |

IAM needs `sqs:ReceiveMessage` **and** `sqs:DeleteMessage` — the latter because
`delete_message_on_reception` is on.

---

[← back to the DAG index](../README.md)
