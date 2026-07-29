# Market Simulator Design

Design for `SimulatorDataSource`, the default (no `MASSIVE_API_KEY`) market data implementation required by `PLAN.md` §6: GBM-based price generation, correlated moves across related tickers, occasional shock events, ~500ms update cadence, no external dependencies.

## Why Geometric Brownian Motion

GBM is the standard model for simulating a price that (a) can never go negative and (b) has returns, not absolute changes, that are roughly normally distributed — both true of real stock prices and both needed for a simulator that "looks right" to anyone glancing at the screen. The discrete-time update for one ticker over a tick of length `dt`:

```
S(t + dt) = S(t) * exp[(μ - σ²/2) * dt + σ * √dt * Z]
```

- `S(t)` — current price
- `μ` (mu) — drift: the ticker's expected trend, annualized (e.g., `0.08` for a stock that drifts up ~8%/year on average)
- `σ` (sigma) — volatility: annualized standard deviation of returns (e.g., `0.35` for a volatile tech stock, `0.15` for a stable blue chip)
- `Z` — a draw from a standard normal distribution, **correlated across tickers** (see below) rather than independent per ticker
- `dt` — the tick length expressed in years, so that `μ`/`σ` stay in familiar "annualized" units regardless of how fast the simulator actually ticks

At a ~500ms tick and treating a trading year as ~252 days × 6.5 hours:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # ≈ 5,896,800
dt = TICK_INTERVAL_SECONDS / TRADING_SECONDS_PER_YEAR  # 0.5 / 5,896,800 ≈ 8.48e-8
```

This keeps `μ`/`σ` as numbers a developer can reason about ("AAPL drifts 8%/year, 35% annualized vol") without needing to know the tick rate, and means changing the tick interval later doesn't require re-tuning every ticker's parameters.

## Correlated moves via Cholesky decomposition

`PLAN.md` §6 asks for correlated moves ("tech stocks move together"), which independent per-ticker `Z` draws wouldn't produce. Instead, tickers are grouped into sectors, a sector-level correlation matrix defines how tightly each pair of sectors moves together, and each tick draws one correlated vector of `Z` values for all tickers at once.

```python
SECTOR_GROUPS = {
    "tech":    ["AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA"],
    "finance": ["JPM", "V"],
    "auto":    ["TSLA"],
    "media":   ["NFLX"],
}

# Correlation between sectors; same-sector pairs use the diagonal-adjacent value,
# cross-sector pairs fall back to a low baseline correlation.
SECTOR_CORRELATION = {
    ("tech", "tech"): 0.6,
    ("finance", "finance"): 0.5,
    ("tech", "finance"): 0.3,
    # any unlisted pair defaults to 0.2 (broad market co-movement)
}
```

Each tick:

1. Build the `n × n` ticker-level correlation matrix `R` from `SECTOR_CORRELATION`, with `1.0` on the diagonal.
2. Compute the Cholesky factor `L` such that `L @ L.T == R` (`numpy.linalg.cholesky`).
3. Draw `n` independent standard normals `z ~ N(0, I)`.
4. Correlated draws: `Z = L @ z` — each ticker gets a `Z` component that's a weighted mix of the independent draws, weighted by that ticker's correlation to every other ticker.
5. Feed each ticker's `Z[i]` into the GBM formula above.

```python
import numpy as np

class CorrelationModel:
    def __init__(self, tickers: list[str], sector_of: dict[str, str]):
        self.tickers = tickers
        n = len(tickers)
        R = np.eye(n)
        for i, a in enumerate(tickers):
            for j, b in enumerate(tickers):
                if i == j:
                    continue
                sa, sb = sector_of[a], sector_of[b]
                R[i, j] = SECTOR_CORRELATION.get((sa, sb)) or SECTOR_CORRELATION.get((sb, sa)) or 0.2
        self._L = np.linalg.cholesky(R)

    def draw(self, rng: np.random.Generator) -> np.ndarray:
        z = rng.standard_normal(len(self.tickers))
        return self._L @ z
```

The Cholesky factor only needs to be recomputed when the *set* of tracked tickers changes (a ticker added/removed from the watchlist), not on every tick — it's cached on the `SimulatorDataSource` instance and rebuilt on `add_ticker`/`remove_ticker`.

## Shock events

`PLAN.md` §6 calls for "occasional random events — sudden 2-5% moves... for drama." This is layered on top of the GBM tick, independent per ticker (a shock on AAPL shouldn't imply one on GOOGL):

```python
SHOCK_PROBABILITY_PER_TICK = 0.001  # ~0.1% chance per ticker per tick
SHOCK_MAGNITUDE_RANGE = (0.02, 0.05)  # 2-5% absolute move

def maybe_apply_shock(price: float, rng: np.random.Generator) -> float:
    if rng.random() >= SHOCK_PROBABILITY_PER_TICK:
        return price
    magnitude = rng.uniform(*SHOCK_MAGNITUDE_RANGE)
    direction = rng.choice([-1, 1])
    return price * (1 + direction * magnitude)
```

At ~500ms ticks, `0.001` probability per tick works out to roughly one shock every ~15–20 minutes per ticker on average — frequent enough to be noticed during a demo session, rare enough not to make the simulator feel chaotic. This constant is the one knob to turn if shocks feel too frequent/rare in practice.

## Seed prices and per-ticker parameters

Every ticker needs a starting price plus its own `(μ, σ, sector)` triple. `PLAN.md` §7 fixes the default watchlist to: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX.

```python
@dataclass(frozen=True)
class TickerParams:
    seed_price: float
    mu: float     # annualized drift
    sigma: float  # annualized volatility
    sector: str

SEED_PARAMS: dict[str, TickerParams] = {
    "AAPL":  TickerParams(190.00, mu=0.10, sigma=0.28, sector="tech"),
    "GOOGL": TickerParams(175.00, mu=0.09, sigma=0.30, sector="tech"),
    "MSFT":  TickerParams(420.00, mu=0.10, sigma=0.26, sector="tech"),
    "AMZN":  TickerParams(185.00, mu=0.11, sigma=0.32, sector="tech"),
    "TSLA":  TickerParams(250.00, mu=0.05, sigma=0.55, sector="auto"),
    "NVDA":  TickerParams(130.00, mu=0.18, sigma=0.50, sector="tech"),
    "META":  TickerParams(500.00, mu=0.09, sigma=0.34, sector="tech"),
    "JPM":   TickerParams(200.00, mu=0.07, sigma=0.22, sector="finance"),
    "V":     TickerParams(280.00, mu=0.08, sigma=0.20, sector="finance"),
    "NFLX":  TickerParams(650.00, mu=0.08, sigma=0.38, sector="media"),
}
```

Prices are illustrative starting points, not sourced from a live quote — they only need to look plausible at a glance for the demo. Volatility differences are deliberate: TSLA/NVDA get high `σ` for visibly bigger, more exciting swings; JPM/V get low `σ` to visually anchor the watchlist with some calmer tickers.

For a ticker added at runtime with no entry in `SEED_PARAMS` (a user adds an arbitrary symbol via the watchlist or AI chat), fall back to a neutral default (`mu=0.08, sigma=0.30, sector="tech"`) rather than rejecting the add — the simulator's job is to always produce *some* plausible price, not to validate that a ticker is real (real-symbol validation, if wanted, belongs at the watchlist API layer, not here).

## Code structure

```
backend/app/market/
├── models.py         # PriceUpdate (shared with MassiveDataSource, see MARKET_INTERFACE.md)
├── cache.py           # PriceCache (shared)
├── interface.py        # MarketDataSource ABC (shared)
├── seed_prices.py       # SEED_PARAMS, SECTOR_GROUPS, SECTOR_CORRELATION
├── simulator.py          # GBMSimulator, CorrelationModel, SimulatorDataSource
└── factory.py             # create_market_data_source()
```

```python
# simulator.py

class GBMSimulator:
    def __init__(self, tickers: list[str], params: dict[str, TickerParams]):
        self.tickers = tickers
        self.params = params
        self.prices = {t: params[t].seed_price for t in tickers}
        self.correlation = CorrelationModel(tickers, {t: params[t].sector for t in tickers})
        self.rng = np.random.default_rng()

    def tick(self, dt: float) -> dict[str, float]:
        z = self.correlation.draw(self.rng)
        next_prices = {}
        for i, ticker in enumerate(self.tickers):
            p = self.params[ticker]
            s = self.prices[ticker]
            drift_term = (p.mu - p.sigma ** 2 / 2) * dt
            shock_term = p.sigma * math.sqrt(dt) * z[i]
            new_price = s * math.exp(drift_term + shock_term)
            new_price = maybe_apply_shock(new_price, self.rng)
            next_prices[ticker] = new_price
        self.prices = next_prices
        return next_prices


class SimulatorDataSource(MarketDataSource):
    def __init__(self, cache: PriceCache):
        self._cache = cache
        self._simulator: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: Iterable[str]) -> None:
        params = {t: SEED_PARAMS.get(t, DEFAULT_PARAMS) for t in tickers}
        self._simulator = GBMSimulator(list(tickers), params)
        for ticker, price in self._simulator.prices.items():
            self._cache.update(ticker, price)  # seed the cache before the first tick
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            prices = self._simulator.tick(dt=TICK_DT)
            for ticker, price in prices.items():
                self._cache.update(ticker, price)
            await asyncio.sleep(TICK_INTERVAL_SECONDS)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def add_ticker(self, ticker: str) -> None:
        params = SEED_PARAMS.get(ticker, DEFAULT_PARAMS)
        self._simulator.tickers.append(ticker)
        self._simulator.params[ticker] = params
        self._simulator.prices[ticker] = params.seed_price
        self._simulator.correlation = CorrelationModel(  # must rebuild — ticker set changed
            self._simulator.tickers,
            {t: self._simulator.params[t].sector for t in self._simulator.tickers},
        )
        self._cache.update(ticker, params.seed_price)

    async def remove_ticker(self, ticker: str) -> None:
        self._simulator.tickers.remove(ticker)
        del self._simulator.params[ticker]
        del self._simulator.prices[ticker]
        self._simulator.correlation = CorrelationModel(
            self._simulator.tickers,
            {t: self._simulator.params[t].sector for t in self._simulator.tickers},
        )
        self._cache.remove(ticker)

    def get_tickers(self) -> frozenset[str]:
        return frozenset(self._simulator.tickers)
```

`start()` seeds the cache immediately (before the first tick) so a client connecting to the SSE stream in the gap between process startup and the first tick still sees a full watchlist of prices, not blanks.

## Testing implications

- **GBM math**: with `σ=0` and a large number of ticks, price should converge to `S(0) * exp(μ * T)` — a deterministic check that the drift term is applied correctly.
- **Correlation**: with two tickers forced into the same sector (correlation ≈ 1.0), their tick-over-tick returns should be highly correlated across many ticks (Pearson correlation coefficient close to the configured value); with two tickers in unrelated sectors, correlation should be low.
- **Shocks**: with `SHOCK_PROBABILITY_PER_TICK` temporarily set to `1.0` in a test, a single tick should reliably produce a 2–5% move — confirms the magnitude range without needing thousands of ticks to hit the probability.
- **`add_ticker`/`remove_ticker`**: correlation matrix dimensions and the cache's ticker set should always match `simulator.tickers` after either operation.
