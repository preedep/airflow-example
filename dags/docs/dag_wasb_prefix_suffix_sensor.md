# `dag_wasb_prefix_suffix_sensor.py`

📄 **[Source: `dag_wasb_prefix_suffix_sensor.py`](../dag_wasb_prefix_suffix_sensor.py)**


`nix-dag-wasb-prefix-suffix-sensor` — waits for an Azure blob matching **both** a
prefix and a suffix.

```bash
airflow dags trigger nix-dag-wasb-prefix-suffix-sensor \
  --conf '{"prefix":"incoming/","suffix":".csv"}'
```

**Pattern: extend a provider sensor rather than writing one from scratch.** The
provider ships `WasbBlobSensor` (exact name) and `WasbPrefixSensor` (prefix only);
neither expresses *"any `.csv` under `incoming/`"*. Subclassing `WasbPrefixSensor`
and overriding `poke()` inherits its connection handling and templated fields:

```python
class WasbPrefixSensorWithSuffix(WasbPrefixSensor):
    template_fields = ("container_name", "prefix", "suffix")

    def poke(self, context) -> bool:
        blobs = hook.get_blobs_list(container_name=..., prefix=self.prefix, delimiter="")
        matched = [b for b in blobs if b.lower().endswith(self.suffix.lower())]
        ...
```

**Push the filter as far server-side as it goes.** The prefix is passed to Azure so
it filters before returning; only the suffix is matched locally. Filtering entirely
client-side would list the whole container on every poke.

Note `get_blobs_list` defaults to `delimiter="/"`, which stops at the first level —
pass `delimiter=""` to recurse.

The sensor pushes matching names to XCom (`matched_blobs`) so the downstream task
acts on exactly what satisfied it, rather than re-listing and possibly seeing a
different set.

---

[← back to the DAG index](../README.md)
