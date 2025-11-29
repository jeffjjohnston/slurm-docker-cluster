# Loki range query helpers

This folder contains a lightweight Python helper for querying Loki with explicit
Unix nanosecond time ranges. It never relies on Loki's `now` placeholder; start
and end timestamps are calculated from the current UTC time at the moment of the
request.

## Usage

```python
from loki_utils import LokiAuth, LokiClient

# Required inputs: Loki base URL and your authentication details.
client = LokiClient(
    base_url="https://loki.example.com",
    auth=LokiAuth(bearer_token="<token>")  # or LokiAuth(basic_auth=("user", "pass"))
)

records = client.query_range(
    query='{app="api"}',
    hours_ago=6,           # look back 6 hours from now
    step_seconds=30,       # evaluation step used by Loki
    limit=100,
)

for entry in records:
    print(entry["timestamp"], entry["labels"], entry["record"])
```

Each record in `records` is a dictionary with:

- `timestamp`: the Loki-provided nanosecond timestamp string
- `labels`: the stream labels attached to the log line
- `record`: the log line payload

## Design notes

- Time ranges are always computed between "now" and a caller-supplied number of
  hours ago, expressed as Unix nanosecond epoch values.
- Critical connection details (server URL and any auth) must be provided directly
  by the caller; no environment variable lookups are used.
- Only the Python standard library is required.
