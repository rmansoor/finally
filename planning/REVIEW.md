# Plan Review

## High-priority clarifications

1. **Docker persistence wording is inconsistent.** The plan says to use a named volume (`finally-data:/app/db`) but also says the project-root `db/` directory maps to `/app/db`. Those are different approaches. Choose one and align the start scripts, Compose file, and documentation.

2. **Define atomic trade execution.** A trade must update cash, positions, trade history, and the immediate portfolio snapshot in one SQLite transaction. Otherwise a failure mid-operation can leave the portfolio inconsistent.

3. **Avoid `REAL` for money.** Store monetary values as integer cents (or decimal strings) and define rounding rules. Floating-point cash and P&L calculations will eventually create visible discrepancies.

4. **Specify the market-data contract.** Define the exact shared price object, ticker normalization rules, behavior for invalid/unknown symbols, cache initialization after adding a ticker, and stale-price handling. Currently a newly added ticker may not have a price until the provider notices it.

5. **Define "daily change %."** The UI requires it, but the cache only specifies current and previous prices. Add an opening/reference price and define simulator and Massive-provider behavior.

6. **Clarify SSE semantics.** "All tickers known to the system" is not equivalent to the user's watchlist once positions or newly requested tickers exist. State whether the stream is global or watchlist-scoped, its event format, heartbeat interval, and how the frontend treats stale connections.

## LLM and API concerns

7. **Treat LLM output as untrusted input.** Validate ticker format, finite quantities, allowed sides/actions, array-size limits, duplicate/conflicting actions, and a maximum number of actions per response before execution. Persist the actual execution results, including failures, rather than merely requested actions.

8. **Make the chat response shape explicit.** Document a response such as `{message, actions: [{requested, status, error?, trade?}], portfolio}` so the frontend and E2E tests have a stable contract.

9. **Resolve the dependency on the "cerebras-inference skill."** It is named as an implementation requirement but is not otherwise described in the project dependencies or setup. Document the exact LiteLLM configuration, model/provider fallback behavior, timeouts, and the behavior when `OPENROUTER_API_KEY` is absent.

10. **Add rate-limit and failure behavior for Massive.** Specify the endpoint(s), batching, provider response mapping, retry/backoff, and whether a provider error retains the last valid cached price.

## Delivery and testing gaps

11. **Add an SPA static-file fallback.** FastAPI needs an explicit API-route precedence and fallback to `index.html` for client-side navigation, if the frontend has any routes beyond `/`.

12. **Make initialization startup-based.** "On startup (or first request)" is ambiguous. Startup initialization makes health checks and first requests deterministic; if lazy initialization is retained, it needs concurrency protection.

13. **Strengthen E2E isolation.** The test environment needs a fresh database volume per run. Restarting the `app` service from within a Playwright scenario may also disrupt the test network/orchestrator; document the exact test-control mechanism.

14. **Define snapshot behavior.** Record an initial snapshot at seed/init time, prevent duplicate immediate snapshots around a trade, and document ordering/timezone/precision. Otherwise a newly opened P&L chart may be empty.

15. **Add operational endpoints/logging.** `/api/health` should state whether it checks database readiness and market-stream status. Structured logs for market-provider failures, rejected trades, and LLM parse failures would make demo failures diagnosable.

## Minor consistency notes

- "Single Docker command" conflicts slightly with optional Compose and build/start-script behavior; provide one canonical quick-start command.
- The plan says no CORS is needed, which is true for the deployed single origin, but local frontend development likely needs a proxy or CORS configuration.
- Define whether selling the last shares deletes the position row and whether removing a watched ticker with an open position is allowed.
- Validate ticker symbols case-insensitively at the API boundary and return canonical uppercase values.
