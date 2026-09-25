# API Hub contract policy

The project is intentionally strict because the upstream surfaces include undocumented browser APIs and provider-specific quirks.

Primary source: `https://hub.arbitron.dev/llms.txt`.

Rules implemented in code:

- `API_HUB_KEY` is read from the environment / `.env` only.
- It is sent only as `X-Hub-Key`.
- The key is never inserted into request URLs, raw archives, prompts, logs or reports.
- Every provider route is declared in `smartwallet.contracts.CONTRACTS` with its documentation URL.
- Query parameters not listed in the route contract raise `ValueError` before any HTTP request.
- Provider modules contain no hardcoded `/run/...` execution paths.
- `429` responses honor `Retry-After` when present.
- Hub RPC uses the documented standard JSON-RPC 2.0 `POST /rpc/{chain}` transport, one request per HTTP request.

Notably **not used**:

- undocumented Arkham `/swaps` `timeGte/timeLte` parameters observed experimentally;
- guessed Arkham counterparty signatures;
- any provider route absent from the registry;
- `trace_*` or `debug_*` RPC methods, which Hub explicitly does not expose.

Run `smartwallet contracts` to print the exact executable contract and documentation URL for every external Hub call.
