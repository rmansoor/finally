# FinAlly — AI Trading Workstation

Bloomberg terminal energy, AI copilot brains. FinAlly streams live market data, lets you trade a simulated portfolio, and puts an LLM in the loop that can analyze your positions and pull the trigger on trades — all from natural language.

The twist: it's built entirely by coding agents, as the capstone project for an agentic AI coding course. Nothing here was hand-typed. The full vision lives in [`planning/PLAN.md`](planning/PLAN.md).

## Where things stand

The **market data engine is live and battle-tested** — real-time GBM price simulation with correlated moves and the occasional dramatic shock, or a live feed via the Massive (Polygon.io) API, all pushed over SSE, backed by 73 passing tests. Details in [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md).

The rest of the terminal — trading, portfolio, the AI copilot, the frontend, the one-command Docker launch — is next up.

**Shipped** (`backend/app/market/`):
- GBM price simulator with correlated ticker movement + shock events, or real data via `MASSIVE_API_KEY`
- Thread-safe live price cache
- SSE endpoint (`GET /api/stream/prices`)
- 73 tests, 84% coverage

**Coming up:** frontend, portfolio/trade/watchlist/chat API routes, SQLite schema, LLM integration, Docker packaging, start/stop scripts.

## Running the Backend

```bash
cd backend
uv sync --extra dev

uv run --extra dev pytest -v          # run tests
uv run market_data_demo.py            # live terminal dashboard of simulated prices
```

See [`backend/CLAUDE.md`](backend/CLAUDE.md) for the market data module's API.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENROUTER_API_KEY` | Not yet used | Will be required once LLM chat is implemented |
| `MASSIVE_API_KEY` | No | Massive (Polygon.io) key for real market data; omit to use the simulator |
| `LLM_MOCK` | Not yet used | Will enable deterministic mock LLM responses for testing |

## Project Structure

```
finally/
├── backend/     # FastAPI uv project — market data subsystem built, rest pending
└── planning/    # Project plan, docs, and agent contracts
```

## License

See [LICENSE](LICENSE).
