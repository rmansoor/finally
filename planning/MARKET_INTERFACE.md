# Market Data Interface Design

Design for the unified Python market data layer described in `PLAN.md` §6: one abstract interface, two implementations (simulator and Massive), selected by environment variable, with all downstream code (SSE streaming, price cache, frontend) agnostic to which one is running underneath.

## Goals

- Downstream code (SSE endpoint, portfolio valuation, trade execution) reads prices from **one place** and never knows or cares whether a human-written simulator or a live REST poller put them there.
- Switching between simulator and Massive is a **pure environment-variable decision**, made once at process startup — no runtime branching scattered through the codebase.
- Both implementations run as the same kind of thing: a background task that owns its own update loop and writes into a shared cache.

## Component Overview

```
                    ┌─────────────────────────┐
                    │   create_market_data_    │
                    │   source(cache) factory   │
                    └────────────┬─────────────┘
                                 │  reads MASSIVE_API_KEY
                 ┌───────────────┴────────────────┐
                 │                                 │
     MASSIVE_API_KEY unset                MASSIVE_API_KEY set
                 │                                 │
                 ▼                                 ▼
      SimulatorDataSource                 MassiveDataSource
      (implements MarketDataSource)      (implements MarketDataSource)
                 │                                 │
                 └────────────────┬────────────────┘
                                  ▼
                            PriceCache
                    (thread-safe, in-memory, single writer per ticker)
                                  │
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
        SSE stream          Portfolio valuation   Trade execution
     (/api/stream/prices)   (unrealized P&L)      (fill price)
```

Only one `MarketDataSource` implementation is ever active in a process — the factory picks exactly one at startup. There's no need for a runtime switch or hybrid mode; that decision belongs to `.env`, per `PLAN.md` §5.

## `PriceUpdate` — the shared value type

Both implementations produce the same immutable record; nothing downstream needs to know which one produced it.

```python
from dataclasses import dataclass
from datetime import datetime, timezone

@dataclass(frozen=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: datetime

    @property
    def change(self) -> float:
        return self.price - self.previous_price

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return (self.change / self.previous_price) * 100

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "timestamp": self.timestamp.isoformat(),
        }
```

`previous_price` is deliberately carried on every update rather than computed by the consumer diffing two cache reads — it keeps `PriceCache` a plain store instead of something that has to reason about "the value before this one."

## `MarketDataSource` — the abstract interface

```python
from abc import ABC, abstractmethod
from collections.abc import Iterable

class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: Iterable[str]) -> None:
        """Begin producing price updates for the given tickers, writing into the shared cache."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background update loop and release any resources (HTTP sessions, tasks)."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a new ticker without restarting the whole source."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker (e.g., removed from the watchlist)."""

    @abstractmethod
    def get_tickers(self) -> frozenset[str]:
        """Currently tracked tickers."""
```

Both `SimulatorDataSource` and `MassiveDataSource` satisfy this. `start`/`stop` bracket a single asyncio background task per source (a `while True` GBM tick loop for the simulator; a `while True` poll-and-sleep loop for Massive) — the task is the thing that calls into `PriceCache.update(...)` on every tick.

`add_ticker` / `remove_ticker` exist because the watchlist is mutable at runtime (`PLAN.md` §8, watchlist endpoints) — the data source has to be able to pick up a newly-added ticker without a full restart, and the two implementations handle this very differently:

- **Simulator**: seed a starting price/volatility for the new ticker (see `MARKET_SIMULATOR.md`) and include it in the next GBM tick.
- **Massive**: add it to the ticker list included in the next snapshot poll call.

## `PriceCache` — the shared sink

```python
import threading

class PriceCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._prices: dict[str, PriceUpdate] = {}
        self._version = 0

    def update(self, ticker: str, price: float, timestamp: datetime | None = None) -> PriceUpdate:
        with self._lock:
            previous = self._prices.get(ticker)
            previous_price = previous.price if previous else price
            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                timestamp=timestamp or datetime.now(timezone.utc),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)
            self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version
```

`PriceCache` is a plain thread-safe dict wrapper with a monotonic `version` counter — it doesn't know anything about GBM or REST polling. This is what makes "downstream code is agnostic to the source" literally true: SSE, portfolio valuation, and trade execution only ever call `cache.get(ticker)` / `cache.get_all()`.

`version` exists purely so the SSE endpoint (below) can cheaply detect "did anything change since I last looked" without diffing dictionaries.

## Factory — the one place the env var is read

```python
import os

def create_market_data_source(cache: PriceCache) -> MarketDataSource:
    if os.environ.get("MASSIVE_API_KEY"):
        return MassiveDataSource(cache)
    return SimulatorDataSource(cache)
```

This function is the **only** place `MASSIVE_API_KEY` is checked to decide behavior. Everything else — route handlers, SSE streaming, tests — depends on `MarketDataSource`, never on which concrete class is behind it. Called once at app startup:

```python
price_cache = PriceCache()
market_data_source = create_market_data_source(price_cache)

@app.on_event("startup")
async def startup() -> None:
    watchlist_tickers = get_watchlist_tickers()  # from DB, or seed defaults on first run
    await market_data_source.start(watchlist_tickers)

@app.on_event("shutdown")
async def shutdown() -> None:
    await market_data_source.stop()
```

## SSE streaming off the cache

```python
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import asyncio, json

def create_stream_router(cache: PriceCache) -> APIRouter:
    router = APIRouter()

    @router.get("/api/stream/prices")
    async def stream_prices():
        async def event_generator():
            last_version = -1
            while True:
                if cache.version != last_version:
                    last_version = cache.version
                    for update in cache.get_all().values():
                        yield f"data: {json.dumps(update.to_dict())}\n\n"
                await asyncio.sleep(0.5)  # matches ~500ms cadence in PLAN.md §6
        return StreamingResponse(event_generator(), media_type="text/event-stream")

    return router
```

The SSE loop polls `cache.version` rather than being pushed to directly by the data source — this decouples "how fast prices change" (GBM tick rate, or Massive poll interval) from "how fast the client is told," and means multiple SSE connections can share one cache without the data source needing to know how many clients are subscribed.

## Where the two implementations differ

| | `SimulatorDataSource` | `MassiveDataSource` |
|---|---|---|
| Update trigger | Internal timer loop (~500ms tick) | Internal timer loop (poll interval driven by tier, see `MASSIVE_API.md`) |
| Update mechanism | Computes next price via GBM (see `MARKET_SIMULATOR.md`) | Fetches `get_snapshot_all("stocks", tickers)`, writes each `day.c` into the cache |
| External dependency | None | Massive REST API, network failures possible |
| Failure mode | N/A (pure computation) | On a failed poll, **keep the last cached price** rather than writing nothing or erroring the cache — a transient network blip shouldn't flash the UI to a null state |
| `add_ticker` | Seed price + volatility params for the new ticker | Ticker included in next poll's `tickers` list — no price until that poll returns |

This table is also the concrete spec for what `MassiveDataSource` needs to implement, so nothing in `MARKET_DATA.md` §6's "unknown symbol" / "stale price" concerns are left implicit: an unrecognized ticker (per `MASSIVE_API.md`'s `results[].error` note) should simply not be written to the cache, leaving the endpoint free to omit it or report it as unavailable, rather than crashing the poll loop for every other ticker in the same batch.

## Why this shape

- **Strategy pattern, not a flag threaded through every function.** The alternative — `if MASSIVE_API_KEY: fetch_from_massive() else: simulate()` sprinkled through the codebase — would make every consumer of price data aware of both code paths. Here, exactly one function (`create_market_data_source`) knows both classes exist.
- **Cache as the only integration point.** Because both sources only ever call `cache.update(...)`, adding a third source later (e.g., a different provider) requires implementing `MarketDataSource` and nothing else — SSE, portfolio, and trades don't change.
- **Version counter over pub/sub.** A push-based fan-out (e.g., each source notifying subscribers directly) would couple the data source to "how many people are listening." Polling `cache.version` from the SSE loop keeps that concern entirely on the consumer side, and trivially supports multiple concurrent SSE connections from a future multi-tab or multi-user scenario without any change to the data source.
