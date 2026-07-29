# Market Data Backend — Design Document

Consolidated, implementation-accurate design for the market data subsystem required by `PLAN.md` §6: a single abstract interface (`MarketDataSource`) with two implementations — a GBM simulator (default) and a Massive (Polygon.io) REST poller — writing into a shared, thread-safe `PriceCache` that everything downstream (SSE stream, portfolio valuation, trade execution) reads from.

**Status**: This subsystem is built, tested (73 tests, 84% coverage), and reviewed — see `planning/MARKET_DATA_SUMMARY.md`. This document is the detailed reference: it explains *why* each piece is shaped the way it is and shows the actual code, so the agents building the rest of the backend (portfolio, watchlist, chat, `main.py`) know exactly what they're integrating with. Where the original design (`MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`) differs from what actually got built, this document describes the **as-built** behavior and calls out the deltas.

All code lives in `backend/app/market/` (8 modules) with tests in `backend/tests/market/`.

---

## 1. Goals & Architecture

- Downstream code reads prices from **one place** (`PriceCache`) and never knows or cares whether a GBM simulator or a live REST poller put them there.
- Choosing simulator vs. Massive is a **pure environment-variable decision** (`MASSIVE_API_KEY`), made once at process startup by a single factory function — no `if MASSIVE_API_KEY` branches anywhere else in the codebase.
- Both implementations are the same *kind* of thing: an `asyncio` background task that owns its own update loop and calls `cache.update(...)`.

```
                    create_market_data_source(cache)
                                  │
                     reads MASSIVE_API_KEY env var
                 ┌────────────────┴─────────────────┐
                 │                                   │
        unset / empty                        set & non-empty
                 │                                   │
                 ▼                                   ▼
      SimulatorDataSource                    MassiveDataSource
    (GBM, ~500ms tick loop)              (REST poll loop, ~15s)
                 │                                   │
                 └─────────────────┬─────────────────┘
                                   ▼
                             PriceCache
                (thread-safe, in-memory, single active writer)
                                   │
                 ┌─────────────────┼─────────────────┐
                 ▼                 ▼                 ▼
        GET /api/stream/prices  Portfolio valuation  Trade execution
              (SSE)              (unrealized P&L)     (fill price)
```

Only one `MarketDataSource` is ever active per process. There's no runtime switch or hybrid mode — that decision is made once at startup and lives in `.env` (`PLAN.md` §5).

### Module layout

```
backend/app/market/
├── __init__.py         # Public API surface (see §7 below)
├── models.py            # PriceUpdate
├── interface.py           # MarketDataSource ABC
├── cache.py                # PriceCache
├── seed_prices.py           # SEED_PRICES, TICKER_PARAMS, correlation groups
├── simulator.py               # GBMSimulator, SimulatorDataSource
├── massive_client.py            # MassiveDataSource
└── stream.py                     # create_stream_router (SSE endpoint)

backend/tests/market/
├── test_models.py       # 11 tests — PriceUpdate properties, serialization
├── test_cache.py         # 13 tests — thread safety, version counter, CRUD
├── test_simulator.py      # 17 tests — GBM math, correlation, shocks
├── test_simulator_source.py # 10 tests — SimulatorDataSource lifecycle (integration)
├── test_factory.py          # 7 tests — env var selection logic
└── test_massive.py           # 13 tests — polling, error handling (client mocked)
```

---

## 2. `PriceUpdate` — the shared value type

Both data sources produce the exact same immutable record; nothing downstream needs to know which one produced it. This is `backend/app/market/models.py` in full:

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

Design notes:

- **`frozen=True, slots=True`** — immutable and memory-tight; a `PriceUpdate` is created on every tick for every ticker (10 tickers × 2/sec = 20 allocations/sec at baseline), so `slots` avoids per-instance `__dict__` overhead.
- **`previous_price` travels with the record** rather than being derived by the consumer diffing two reads — this is what keeps `PriceCache` a plain store instead of something that has to reason about "the value before this one."
- **`timestamp` is a Unix float, not a `datetime`** — cheaper to construct on the hot path and trivially JSON-serializable without an `isoformat()` call.
- **`change`/`change_percent` are rounded to 4dp** — enough precision for sub-cent GBM moves to still show a nonzero change on fast ticks, without leaking float noise into the JSON payload.
- **`direction` and `change_percent` answer `PLAN.md` §10's "daily change %" and flash-animation requirements directly off this one object** — the frontend doesn't need to compute anything itself; it colors the flash from `direction` and displays `change_percent` as-is. Note this is *tick-over-tick* change, not change-since-market-open; see §9 for the gap that leaves.

---

## 3. `MarketDataSource` — the abstract interface

`backend/app/market/interface.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

`add_ticker`/`remove_ticker` exist because the watchlist is mutable at runtime (`PLAN.md` §8) — the active data source must pick up a newly-watched ticker without a full restart. The two implementations handle this very differently (simulator: synchronous, seeds a price immediately; Massive: asynchronous, no price until the next poll — see §5 and §6 for exactly what each does).

---

## 4. `PriceCache` — the shared sink

`backend/app/market/cache.py`:

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
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

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Design notes:

- **`threading.Lock`, not an `asyncio.Lock`** — deliberate. The Massive source calls `RESTClient` (synchronous) inside `asyncio.to_thread(...)` (see §6), so writes can arrive from a worker thread, not just the event loop. A plain `threading.Lock` is correct in both contexts; an `asyncio.Lock` would not be safe to acquire from a thread-pool thread.
- **Prices are rounded to 2dp on write**, not on read — every consumer (SSE, portfolio, trade fill) gets the same canonical cent-precision value with no repeated rounding logic scattered around the codebase.
- **First update for a ticker sets `previous_price == price`** → `direction` comes out `"flat"` and `change`/`change_percent` are `0`. This is what lets `SimulatorDataSource.start()` seed the cache before the first GBM tick (§5) without a fake "up" or "down" flash on the client's first paint.
- **`version` is a plain `int`, read without the lock** in the property getter. This is intentionally cheap: `int` reads are atomic under the GIL, and the SSE loop (§7) polls this every 500ms just to decide "did anything change" — it doesn't need read-after-write ordering guarantees, only eventual visibility, which the GIL already provides. Using the lock here would serialize every SSE connection's poll against every cache write for no correctness benefit.
- **`remove()` doesn't bump `version`** in the current implementation — a client already mid-stream will keep seeing the removed ticker in `get_all()` output until the *next* price update elsewhere bumps the version and the whole dict is re-sent. In practice this self-corrects within one tick (simulator) or one poll (Massive) because some other ticker updates and triggers a resend of `get_all()`, which by then no longer contains the removed ticker. If a strict "remove reflected within one event" guarantee is ever needed, bump `_version` in `remove()` too — flagged here as a known minor gap, not fixed, because it's cosmetic (worst case: a removed ticker lingers one extra ~500ms tick in the SSE payload) and not worth the extra lock scope.

---

## 5. Simulator — `SimulatorDataSource`

The default (no `MASSIVE_API_KEY`) implementation. No external dependencies; pure computation.

### 5.1 Why GBM

Geometric Brownian Motion is the standard model for a price that (a) can never go negative and (b) has *returns* — not absolute changes — that are roughly normally distributed. Discrete-time update for one ticker over a tick of length `dt`:

```
S(t+dt) = S(t) * exp[(μ - σ²/2) * dt + σ * √dt * Z]
```

- `S(t)` — current price
- `μ` (mu) — annualized drift (expected trend, e.g. `0.05` = 5%/year)
- `σ` (sigma) — annualized volatility (e.g. `0.50` for TSLA-grade swings, `0.17` for a calm payments stock)
- `Z` — a standard normal draw, **correlated across tickers** (§5.2), not independent per ticker
- `dt` — tick length expressed as a fraction of a trading year, so `μ`/`σ` stay in familiar annualized units regardless of tick rate

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8, for a 500ms tick
```

This is why changing the tick interval later doesn't require re-tuning every ticker's `(μ, σ)` — they're defined in "per year" terms, and only `dt` changes.

### 5.2 Correlated moves via Cholesky decomposition

`PLAN.md` §6 asks for correlated moves ("tech stocks move together"), which independent per-ticker `Z` draws wouldn't produce. Tickers are grouped into sectors; a correlation matrix is built once per ticker-set change and Cholesky-factored so a single vector of independent normal draws becomes a correlated vector in one matrix multiply per tick.

`backend/app/market/seed_prices.py` (correlation section):

```python
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR = 0.3     # Between sectors / unknown tickers
TSLA_CORR = 0.3            # TSLA does its own thing
```

Note the as-built grouping differs slightly from the original design draft: NFLX is folded into `tech` (not a separate `media` group) and TSLA is deliberately *excluded* from any group's high-correlation treatment — even though it appears in `tech`'s superset conceptually, `_pairwise_correlation` special-cases it to always return `TSLA_CORR` (0.3) against every other ticker, so it visibly "does its own thing" on screen rather than tracking the NASDAQ megacaps.

Matrix build + Cholesky, from `GBMSimulator._rebuild_cholesky` / `_pairwise_correlation`:

```python
def _rebuild_cholesky(self) -> None:
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None
        return

    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = rho
            corr[j, i] = rho

    self._cholesky = np.linalg.cholesky(corr)

@staticmethod
def _pairwise_correlation(t1: str, t2: str) -> float:
    tech = CORRELATION_GROUPS["tech"]
    finance = CORRELATION_GROUPS["finance"]

    if t1 == "TSLA" or t2 == "TSLA":
        return TSLA_CORR
    if t1 in tech and t2 in tech:
        return INTRA_TECH_CORR
    if t1 in finance and t2 in finance:
        return INTRA_FINANCE_CORR
    return CROSS_GROUP_CORR
```

The Cholesky factor is `O(n²)` to build and only gets rebuilt when the *set* of tracked tickers changes (`add_ticker`/`remove_ticker`), not on every tick — at demo scale (`n < 50`) this is trivially fast even done synchronously inline.

Applying it every tick (`GBMSimulator.step`):

```python
def step(self) -> dict[str, float]:
    n = len(self._tickers)
    if n == 0:
        return {}

    z_independent = np.random.standard_normal(n)
    z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

    result: dict[str, float] = {}
    for i, ticker in enumerate(self._tickers):
        params = self._params[ticker]
        mu, sigma = params["mu"], params["sigma"]

        drift = (mu - 0.5 * sigma**2) * self._dt
        diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
        self._prices[ticker] *= math.exp(drift + diffusion)

        # Random shock event — see §5.3
        if random.random() < self._event_prob:
            shock_magnitude = random.uniform(0.02, 0.05)
            shock_sign = random.choice([-1, 1])
            self._prices[ticker] *= 1 + shock_magnitude * shock_sign

        result[ticker] = round(self._prices[ticker], 2)

    return result
```

### 5.3 Shock events

`PLAN.md` §6 calls for "occasional random events — sudden 2-5% moves... for drama," layered on top of the GBM step, independent per ticker (a shock on AAPL doesn't imply one on GOOGL — it's drawn per-ticker inside the same loop, after the correlated GBM term is already applied).

```python
event_probability: float = 0.001   # ~0.1% chance per ticker per tick
```

At a ~500ms tick (2 ticks/sec), `0.001` per tick per ticker works out to roughly one shock every ~500 seconds (~8 minutes) per ticker on average — with 10 tickers watched simultaneously, that's a shock *somewhere* in the watchlist roughly every ~50 seconds, frequent enough to notice during a demo without feeling chaotic. This is the one constant to tune if shocks feel too frequent/rare.

### 5.4 Seed prices and per-ticker parameters

`backend/app/market/seed_prices.py` (full):

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},    # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},      # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

For a ticker with no seed entry (a user adds an arbitrary symbol via the watchlist or AI chat), `GBMSimulator._add_ticker_internal` falls back to a *random* starting price in `[50, 300]` plus `DEFAULT_PARAMS`, rather than rejecting the add:

```python
def _add_ticker_internal(self, ticker: str) -> None:
    if ticker in self._prices:
        return
    self._tickers.append(ticker)
    self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
    self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))
```

The simulator's job is to always produce *some* plausible price, never to validate that a ticker is real — symbol validation, if wanted, belongs at the watchlist API layer, not here.

### 5.5 `SimulatorDataSource` — the `MarketDataSource` implementation

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Key properties:

- **`start()` seeds the cache before the first tick** — a client connecting to SSE in the gap between process startup and the first `step()` still sees a full watchlist of prices, not blanks.
- **`_run_loop` never dies from a single bad step** — a caught exception is logged and the loop sleeps and retries on the next interval, rather than silently killing the background task (which would freeze all prices with no visible error beyond the log).
- **`add_ticker` is synchronous in effect** despite the `async def` — it seeds a price into the cache immediately, unlike Massive (§6) where the new ticker has no price until the next poll completes.

---

## 6. Massive (Polygon.io) — `MassiveDataSource`

Optional, used when `MASSIVE_API_KEY` is set. REST polling, not WebSocket (`PLAN.md` §6 — simpler, works on all pricing tiers).

### 6.1 Client & rate limits

```bash
uv add massive
```

```python
from massive import RESTClient
client = RESTClient(api_key="...")
```

| Tier | Limit | Poll interval used |
|------|-------|---------------------|
| Free | 5 req/min | 15s default (4 req/min, safe margin) |
| Paid | No hard cap | Can lower `poll_interval` (2–5s) since the free-tier ceiling no longer applies |

**One poll = one call.** All watched tickers are fetched in a single `get_snapshot_all(...)` request, never one request per ticker — this is what keeps the free tier's 5 req/min limit workable at all with a 10-ticker default watchlist.

### 6.2 Implementation

`backend/app/market/massive_client.py` (full):

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache."""

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # Immediate first poll — cache has data right away
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            # No price until the next poll completes — see §6.3

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous — run in a thread to avoid blocking the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms → s
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — retry on next interval. Common: 401 bad key, 429 rate limit, network errors.

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### 6.3 Behavioral notes / deltas from the original research doc

- **Price source is `last_trade.price`, not `day.c`.** The original research in `MASSIVE_API.md` proposed using the snapshot's `day.c` (today's session close-so-far) as the live price. The as-built client instead reads `snap.last_trade.price` — the most recent executed trade price — which is a tighter "live" signal for a ticker that's actively trading and matches what a trading-terminal UI implies by "current price." `day.c` remains available on the same snapshot object if EOD-anchored change % is wanted later (see the open item in §9).
- **Timestamps convert from milliseconds to seconds** (`/ 1000.0`) to match `PriceUpdate.timestamp`'s Unix-seconds convention — Massive's `last_trade.timestamp` is epoch milliseconds.
- **Failure mode: keep the last cached price.** `_poll_once` catches all exceptions around the fetch+parse and logs rather than raising. A failed poll (network blip, 429, 401) leaves the cache exactly as it was — the UI keeps showing the last known price rather than flashing to a null/error state. The `_poll_loop` continues on its normal interval regardless of whether the previous poll succeeded.
- **Per-snapshot parse failures don't abort the batch.** The `try/except (AttributeError, TypeError)` inside the loop means one malformed/missing snapshot (e.g., a delisted or invalid ticker) is logged and skipped without discarding the other tickers' data from the same poll — directly resolving the "unknown symbol" concern raised in `MASSIVE_API.md` §Design Implications and `REVIEW.md` item 4.
- **`add_ticker` has no immediate cache effect**, unlike the simulator. A newly-watched ticker has *no* price in the cache until the next scheduled poll (up to `poll_interval` seconds later — worst case 15s on the free tier). Downstream code (watchlist API, SSE) must tolerate `cache.get(ticker) is None` for a brand-new ticker under the Massive source; the simulator never has this gap. This asymmetry is the concrete spec other backend modules should code against — don't assume every watchlist ticker has a cache entry the instant it's added.

---

## 7. Factory — the one place the env var is read

`backend/app/market/factory.py` (full):

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty → MassiveDataSource; otherwise SimulatorDataSource.

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

`.strip()` matters: an `.env` file with `MASSIVE_API_KEY=` (present but empty) or stray whitespace must fall through to the simulator, not attempt to construct a `RESTClient` with a blank key.

### Public API surface

`backend/app/market/__init__.py` re-exports exactly what downstream code should import:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
```

Nothing else in the package is meant to be imported directly by other backend modules — `GBMSimulator`, `SimulatorDataSource`, `MassiveDataSource` are implementation details reached only through the factory.

---

## 8. SSE Streaming — `create_stream_router`

`backend/app/market/stream.py` (full):

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )
    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"  # Tell EventSource to retry after 1s on drop

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

Design notes:

- **Payload shape is a dict keyed by ticker**, `{"AAPL": {...}, "GOOGL": {...}, ...}`, not an array — one SSE `data:` event per version bump, containing the *full* current cache snapshot (`get_all()`), not a diff. This is simple and correct at watchlist scale (≤ a few dozen tickers): the frontend just replaces its whole price map on every event rather than merging patches.
- **Change detection is version-polling, not push.** The SSE loop wakes every 500ms and compares `price_cache.version` against what it last saw; it does not get notified directly by the data source. This decouples "how fast the underlying source ticks" (simulator: 500ms: Massive: up to 15s) from "how fast the client is told," and lets any number of concurrent SSE connections share one `PriceCache` without the data source needing to know how many listeners exist.
- **`retry: 1000` is the reconnection contract with `PLAN.md` §12's SSE-resilience test** — `EventSource`'s built-in auto-reconnect will retry after 1 second on a dropped connection (e.g. the E2E test's `docker compose restart app`), and once the server comes back the very next tick reseeds the client with a full snapshot.
- **Disconnect detection via `request.is_disconnected()`** ends the generator cleanly, which is what lets a `docker compose restart` (killing the whole process) or a client closing the tab stop the loop without leaking the task — the check happens every iteration, so at most one `interval` (500ms) of latency before cleanup.
- **If the cache is empty (`prices` falsy), nothing is yielded** that tick — avoids sending `data: {}\n\n` before `SimulatorDataSource.start()` or `MassiveDataSource.start()`'s first poll has seeded anything. In practice this window is sub-second since both sources seed synchronously in `start()` (§5.5, §6.2) before the FastAPI app finishes startup (see §9's wiring).

---

## 9. Wiring into the FastAPI app (forward-looking — `main.py` doesn't exist yet)

The market module is complete but the rest of the backend (`main.py`, portfolio, watchlist, chat, db) is still to be built per `PLAN.md`. This is the intended startup/shutdown wiring for whoever builds `main.py`, using only the public API from §7:

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()
market_data_source = create_market_data_source(price_cache)

@asynccontextmanager
async def lifespan(app: FastAPI):
    watchlist_tickers = get_watchlist_tickers()  # from DB; seed defaults on first run (PLAN.md §7)
    await market_data_source.start(watchlist_tickers)
    yield
    await market_data_source.stop()

app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

Integration points for other modules, all reading `price_cache` — never the concrete `SimulatorDataSource`/`MassiveDataSource` classes — and all calling `market_data_source.add_ticker`/`remove_ticker` on watchlist mutation, per the `MarketDataSource` contract in §3:

| Module | Reads | Writes (triggers) |
|---|---|---|
| `POST /api/watchlist` | — | `await market_data_source.add_ticker(ticker)` |
| `DELETE /api/watchlist/{ticker}` | — | `await market_data_source.remove_ticker(ticker)` |
| `GET /api/watchlist` | `price_cache.get(ticker)` per watched ticker | — |
| `POST /api/portfolio/trade` | `price_cache.get_price(ticker)` for the fill price | — |
| Portfolio valuation (`GET /api/portfolio`) | `price_cache.get_price(ticker)` per position | — |
| `GET /api/stream/prices` | `price_cache.version` / `get_all()` | — (handled entirely inside `app/market`) |

**Open item, not yet resolved by this module** (carried over from `REVIEW.md` items 4–5 and 10, deliberately left to the API layer): if `price_cache.get_price(ticker)` returns `None` — e.g. a ticker just added under the Massive source, before its first poll (§6.3) — the watchlist/portfolio/trade endpoints need an explicit decision about what to return or whether to reject the trade. This module guarantees it never crashes or returns a stale/wrong value for an unseeded ticker; it simply has nothing yet. The API layer should treat `None` as "price not yet available" (e.g. `503`-style "pending" for that row, or block the trade with `"error": "price unavailable"`) rather than assuming a price always exists once a ticker is on the watchlist.

Also open: `change_percent` on `PriceUpdate` is *tick-over-tick*, not change-since-market-open. `PLAN.md` §10's watchlist "daily change %" column will read as noisy/near-zero on each individual SSE event rather than a meaningful daily figure. Two options for whoever builds the watchlist UI/API: (a) accept tick-over-tick as "current momentum" and rename the UI label accordingly, or (b) track each ticker's session-open price separately (simulator: capture at `start()`/midnight reset; Massive: already available as `prevDay.c` on the same snapshot object, unused today per §6.3) and compute daily change from that instead of consuming `PriceUpdate.change_percent` for that column. This module doesn't currently do either — flagged here rather than silently left for someone to discover.

---

## 10. Testing strategy (as implemented)

| Module | Tests | What's verified |
|---|---|---|
| `test_models.py` | 11 | `PriceUpdate` property math (`change`, `change_percent`, `direction`), `to_dict()` shape, zero-previous-price edge case |
| `test_cache.py` | 13 | Thread-safety under concurrent `update()`, version increments on write, `get`/`get_all`/`get_price`/`remove` correctness, first-update-is-flat behavior |
| `test_simulator.py` | 17 | GBM drift convergence (`σ=0` → deterministic `S(0)·exp(μT)`), correlation strength between same-sector vs. cross-sector tickers via repeated ticks, shock magnitude range with `event_probability=1.0` forced, `add_ticker`/`remove_ticker` keep `_tickers`/`_prices`/`_params`/Cholesky dimensions consistent |
| `test_simulator_source.py` | 10 | `SimulatorDataSource` lifecycle: `start()` seeds cache before first tick, `_run_loop` survives a raised exception and keeps ticking, `stop()` cancels cleanly and is idempotent |
| `test_factory.py` | 7 | Empty/missing/whitespace `MASSIVE_API_KEY` → simulator; set key → Massive; correct constructor args passed through |
| `test_massive.py` | 13 | Poll loop calls `get_snapshot_all` with the right args, `last_trade.price`/`timestamp` parsed correctly, a poll exception leaves the cache untouched and doesn't kill the loop, a malformed single snapshot is skipped without dropping the rest of the batch |

Coverage: 84% overall; `massive_client.py` sits lower (56%) by design — the actual network call (`RESTClient.get_snapshot_all`) is mocked rather than hit in tests, so the real HTTP path is untested by this suite (acceptable for a capstone project; would need a recorded-fixture or contract test against the real API for production hardening).

Interactive verification: `backend/market_data_demo.py` (Rich terminal dashboard) exercises the simulator live for manual sanity-checking — 10 tickers, sparklines, color-coded direction, event log — without needing the rest of the backend built.

---

## 11. Design decisions — summary rationale

- **Strategy pattern over a flag threaded through every function.** The alternative (`if MASSIVE_API_KEY: fetch() else: simulate()` sprinkled through route handlers) would make every consumer aware of both code paths. Here, exactly one function (`create_market_data_source`) knows both concrete classes exist; everything else — routes, SSE, tests — depends only on `MarketDataSource`.
- **Cache as the only integration point.** Because both sources only ever call `cache.update(...)`, adding a third provider later means implementing `MarketDataSource` and nothing else changes downstream.
- **Version counter over pub/sub fan-out.** A push model (data source notifying subscribers directly) would couple the source to "how many people are listening." Polling `cache.version` keeps that concern on the consumer side and trivially supports multiple concurrent SSE connections without the source knowing.
- **SSE over WebSockets** (`PLAN.md` §6) — one-way push is all that's needed; simpler, no bidirectional complexity, universal `EventSource` browser support, and pairs naturally with the version-polling cache read pattern above.
- **`threading.Lock`, not `asyncio.Lock`, on `PriceCache`** — required because `MassiveDataSource` writes from a thread-pool thread via `asyncio.to_thread`, not just the event loop (§4, §6.2).
- **Keep-last-price on provider failure** — a transient Massive network blip or rate-limit response must not flash the UI to null/zero; the cache simply isn't written that cycle (§6.3).
