# Massive API Reference

Research notes on the Massive (formerly Polygon.io) REST API, focused on what FinAlly needs: current prices and end-of-day prices for a set of watched tickers, polled on a timer (see `PLAN.md` §6 — REST polling, not WebSocket).

## Background

Polygon.io rebranded to **Massive** on October 30, 2025. Existing API keys, accounts, and integrations continue to work unmodified. The Python package was renamed from `polygon-api-client` to `massive`.

- Docs: https://massive.com/docs
- Python client: https://github.com/massive-com/client-python
- Base URL: `api.massive.com` (the legacy `api.polygon.io` host is still supported)

## Installation & Authentication

```bash
pip install -U massive
# or
uv add massive
```

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from the environment automatically
client = RESTClient()

# Or pass the key explicitly
client = RESTClient(api_key="your_key_here")
```

`MASSIVE_API_KEY` is the environment variable the client looks for — this lines up directly with the env var name FinAlly already uses in `PLAN.md` §5, so no adapter/renaming is needed between the two.

## Rate Limits

| Tier | Limit |
|------|-------|
| Free | 5 requests/minute |
| Paid (all tiers) | No hard cap; be a reasonable citizen (stay well under 100 req/s) |

This is the binding constraint on polling cadence: on the free tier, polling all watched tickers in **one** snapshot call per tick (see below) means one request per poll, so a 15-second interval (4 req/min) stays safely under the 5 req/min ceiling. Per-ticker polling would blow through the limit immediately with more than 5 tickers.

## Endpoints for FinAlly

### 1. Multi-ticker snapshot (primary endpoint — current prices)

Returns current-session pricing for a set of tickers in a **single call**, which is exactly the shape needed for polling a watchlist.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

**Python**:
```python
tickers = ["AAPL", "GOOGL", "MSFT", "META"]
snapshot = client.get_snapshot_all("stocks", tickers)

for item in snapshot:
    print(item.ticker, item.day.close, item.prev_day.close, item.todays_change_percent)
```

**Response shape**:
```json
{
  "status": "OK",
  "count": 1,
  "tickers": [
    {
      "ticker": "AAPL",
      "day":     { "o": 190.1, "h": 191.4, "l": 189.8, "c": 190.9, "v": 41213000 },
      "prevDay": { "o": 188.0, "h": 190.2, "l": 187.5, "c": 189.6, "v": 39880000 },
      "todaysChange": 1.30,
      "todaysChangePerc": 0.686,
      "updated": 1731260894630916600
    }
  ]
}
```

- `day` — today's session OHLCV so far (this is the live/current price via `day.c`)
- `prevDay` — full previous session OHLCV (this doubles as the EOD close for the prior day)
- `todaysChangePerc` — pre-computed daily change %, saves us deriving it from `day`/`prevDay` ourselves
- `updated` — nanosecond epoch timestamp of the last update
- Passing an empty `tickers` param returns the **entire** market (10,000+ tickers) — always pass an explicit ticker list scoped to the watchlist

### 2. Previous close / end-of-day bar

For an explicit EOD lookup on a single ticker (as opposed to the `prevDay` field embedded in the snapshot above):

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

**Python**:
```python
prev = client.get_previous_close_agg("AAPL")
# prev.open, prev.high, prev.low, prev.close, prev.volume, prev.vwap, prev.timestamp
```

**Response shape**:
```json
{
  "ticker": "AAPL",
  "status": "OK",
  "adjusted": true,
  "resultsCount": 1,
  "results": [
    { "o": 188.0, "h": 190.2, "l": 187.5, "c": 189.6, "v": 39880000, "vw": 189.02, "t": 1731196800000 }
  ]
}
```

For FinAlly's purposes, the snapshot endpoint's `prevDay` field already supplies this data for every watched ticker in the same call — this endpoint is only useful if EOD data for a ticker is needed independent of a live snapshot poll.

### 3. Historical / intraday aggregate bars (not needed for the live poll loop, noted for completeness)

**Python**:
```python
for bar in client.list_aggs(ticker="AAPL", multiplier=1, timespan="minute",
                             from_="2026-07-01", to="2026-07-28", limit=50000):
    print(bar.open, bar.high, bar.low, bar.close, bar.volume, bar.timestamp)
```

Useful if a longer historical chart backfill is ever wanted beyond what SSE has accumulated client-side since page load — out of scope for the current plan, which sources chart history purely from the SSE stream (`PLAN.md` §2).

## Design Implications for FinAlly

1. **One poll = one call.** Use `get_snapshot_all("stocks", tickers)` with the full watchlist on every poll tick, not one call per ticker. This is what makes the free tier's 5 req/min limit workable at all.
2. **`MASSIVE_API_KEY` needs no translation** — same name end-to-end from `.env` → `RESTClient()` → this reference.
3. **Daily change % comes pre-computed** (`todaysChangePerc`) — no need to compute it manually from cached prices, unlike the simulator, which has to compute it itself (see `MARKET_SIMULATOR.md`).
4. **Polling interval must track plan tier**: free tier → 15s poll (per `PLAN.md` §6); paid tiers can poll faster (2–15s) since the rate limit is no longer binding.
5. **Ticker not found**: the snapshot response's `results[].error` (unified `/v3/snapshot`) or a missing entry in `tickers[]` (legacy `/v2/snapshot/.../tickers`) signals an invalid/delisted symbol — the unified interface (see `MARKET_INTERFACE.md`) needs to define what happens to that ticker's cache entry when this occurs.

## Sources

- [Polygon.io is Now Massive](https://massive.com/blog/polygon-is-now-massive)
- [massive-com/client-python (GitHub)](https://github.com/massive-com/client-python)
- [Massive + Python: Unlocking Real-Time and Historical Stock Market Data](https://massive.com/blog/polygon-io-with-python-for-stock-market-data)
- [Full Market Snapshot | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Unified Snapshot | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/snapshots/unified-snapshot)
- [`stocks-snapshots_all.py` example](https://github.com/massive-com/client-python/blob/master/examples/rest/stocks-snapshots_all.py)
- [What is the request limit for Massive's RESTful APIs?](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
- [Getting Started | massive-com/client-python (DeepWiki)](https://deepwiki.com/massive-com/client-python/2-getting-started)
- [Previous Day Bar endpoint](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar) *(fetched via cache; endpoint path confirmed against client method `get_previous_close_agg`)*
