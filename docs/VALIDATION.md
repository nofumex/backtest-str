# Validation record

Validated against the API Hub documentation snapshot visible on 2026-09-25.

Primary contract source: `https://hub.arbitron.dev/llms.txt`.

At validation time it states:

- Base URL: `https://hub.arbitron.dev`;
- credential: `API_HUB_KEY` sent as `X-Hub-Key`;
- never place the key in a URL, prompt, source file, log or report;
- OpenAPI 3.1 is published at `/openapi.json`;
- registry snapshot: **22 providers / 789 executable REST routes**;
- `429` handling must honor `Retry-After`.

This project intentionally uses only the smaller **50-route** allowlist in `src/smartwallet/contracts.py`. Each entry has a direct Hub per-endpoint documentation URL and an explicit set of allowed query parameter names. `HubClient` rejects any other query parameter before HTTP.

## Static/offline validation performed

- Python bytecode compilation for `src/` and `tests/`.
- Full pytest suite: **15 tests passed** at packaging time.
- AST audit that every static `BaseProvider.call("...")` key exists in `CONTRACTS`.
- Audit that provider modules contain no direct `/run/...` URL literals.
- Mock transport test that `X-Hub-Key` is a header and never part of the URL.
- Pagination tests for documented DeBank `start_time` cursor behavior.
- Episode leakage/bounding tests and hierarchical pattern tests.
- LLM JSON/probability normalization tests.
- Market return and statistical backtest tests.

## What cannot be honestly validated inside the generated artifact

The artifact does **not** contain the user's secrets, so a real authenticated end-to-end Hub run and a real FREE_LLM request cannot be executed during packaging. Run `smartwallet doctor --live` after placing/uncovering the existing `.env`. This is deliberately different from claiming that a mocked test proves upstream availability.
