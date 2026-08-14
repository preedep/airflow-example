# `dag_s3_prefix_suffix_sensor.py`

📄 **[Source: `dag_s3_prefix_suffix_sensor.py`](../dag_s3_prefix_suffix_sensor.py)**

`nix-dag-s3-prefix-suffix-sensor` — waits for an S3 object matching **both** a
prefix and a suffix, then reports what it found.

```bash
airflow dags trigger nix-dag-s3-prefix-suffix-sensor \
  --conf '{"prefix":"incoming/","suffix":".csv"}'
```

Defaults to prefix `probe/` and suffix `.txt`. A successful run logs:

```
[s3_sensor] prefix='probe/' suffix='.txt' — 2 matching key(s)
[s3_sensor] probe/from-cluster.txt — 28 bytes
[s3_sensor] probe/s3probe.txt — 36 bytes
```

## Pattern: subclass to fix the hand-off, not the matching

This is the S3 counterpart to #7, and the comparison is the point — **the same
pattern, subclassed for a different reason.**

| | Azure (#7) | S3 (#9) |
|---|---|---|
| Suffix matching | provider has none — the subclass **adds the capability** | `wildcard_match=True` already does `incoming/*.csv` |
| Server-side prefix | `get_blobs_list(prefix=...)` | `iter_file_metadata`, which also paginates |
| Tells you what matched | no | **no** — and that is what the subclass fixes |

`S3KeySensor` is closer to the job than `WasbPrefixSensor` was. It handles
wildcards, pushes the prefix to S3, and paginates the listing. Matching needs no
help at all.

The gap is what it hands downstream. With `wildcard_match`, `_check_key` returns
`True` on the **first** match without recording which keys matched, and
`S3KeySensor` never calls `xcom_push` anywhere. So a downstream task has to
re-list the bucket — and can get a different set than the one that fired the
sensor, because objects land and lifecycle rules delete while the DAG runs.

`S3PrefixSuffixSensor` overrides only `poke()`, to collect **every** match and
push the sorted list as `matched_keys`:

```python
class S3PrefixSuffixSensor(S3KeySensor):
    template_fields = ("aws_conn_id", "bucket_name", "prefix", "suffix",
                       "region_name", "verify")

    def poke(self, context) -> bool:
        matched = [
            item["Key"]
            for item in self.hook.iter_file_metadata(self.prefix, self.bucket_name)
            if self._matches(item["Key"])
        ]
        if not matched:
            return False
        context["ti"].xcom_push(key="matched_keys", value=sorted(matched))
        return True
```

`self.hook` is the provider's own `cached_property`, so connection handling,
`verify`, and region resolution are all inherited — only the poke logic changes.

Redeclaring `template_fields` is load-bearing: it is **not merged** with the base
class's, so dropping the inherited names would silently stop Jinja rendering on
`bucket_name` and the rest.

## A prefix is not a directory

S3 has no folders. `incoming/` is a literal string prefix, so
`incoming/2026/a.csv` matches it exactly as `incoming/a.csv` does.

That is a real difference from the Azure demo, which needs `delimiter=""` to opt
*into* recursion because `get_blobs_list` defaults to stopping at the first
level. Here recursion is the only behaviour — there is no delimiter to set.

| Setting | Effect |
|---|---|
| `prefix` | server-side filter, e.g. `incoming/2026-08-` |
| `suffix` | matched on the key, case-insensitive by default |
| `case_sensitive=True` | opt into exact-case suffix matching |

An empty `suffix` matches everything, degrading to a prefix-only watch.

## Sensor settings

`mode="reschedule"`, `poke_interval=30`, `timeout=1800`. Reschedule frees the
worker slot between pokes — under KubernetesExecutor `poke` mode would hold a pod
idle for the whole wait. A sensor with no `timeout` waits forever and blocks
`max_active_runs`.

`S3KeySensor` also supports `deferrable=True`, which is lighter still, but it
needs a triggerer process running. Reschedule works on any deployment.

## Requires

| Kind | Name | Purpose |
|---|---|---|
| Connection | `aws_s3_test_001` | AWS credentials, conn type `aws` |

Access key in **login**, secret in **password**, and the region in **extra**:

```json
{"region_name": "ap-southeast-1"}
```

**The region is not optional.** Worker pods have no `AWS_DEFAULT_REGION`, so a
connection without it fails inside the cluster even though the same credentials
work from a laptop whose CLI profile has a region configured. The bucket is not
part of the connection; it is passed per operation.

---

[← back to the DAG index](../README.md)
