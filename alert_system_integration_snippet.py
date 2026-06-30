
import logging
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.models import Bar
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest

 
ALPACA_API_KEY     = os.getenv("ALPACA_API_KEY",     "")
ALPACA_SECRET_KEY  = os.getenv("ALPACA_SECRET_KEY",  "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

REQUIRED_ENV_VARS = [
    "ALPACA_API_KEY",
    "ALPACA_SECRET_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
]

missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]

if missing:
    raise RuntimeError(
        "Missing required environment variables: "
        + ", ".join(missing)
        + "\nSet them locally, in Docker, or in GitHub Actions secrets."
    )

STREAM_ALL = True


STAGE1_PRICE_MULT  = 2.0   
STAGE1_VOLUME_MULT = 3.0  

# ── Stage 2 threshold ──────────────────────────────────────────
ZSCORE_THRESHOLD   = 10.0  


BASELINE_CANDLES   = 234


COOLDOWN_MINUTES   = 60


CONFIRM_COUNT      = 2    
CONFIRM_WINDOW_MIN = 30    
CONFIRM_DIRECTION  = True  


CANDLE_INTERVAL    = 5    


BOOTSTRAP_BATCH    = 200   
BOOTSTRAP_PAUSE    = 0.4   


WARMUP_MIN_CANDLES = 30    


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


@dataclass
class Candle:
    """One completed N-minute candle."""
    symbol:    str
    timestamp: datetime
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def pct_change(self) -> float:
        return (self.close - self.open) / self.open * 100 if self.open else 0.0


@dataclass
class Baseline:
    """
    Cached rolling statistics for one symbol.
    """
    mean_range:  float = 0.0
    m2_range:    float = 0.0   
    mean_volume: float = 0.0
    m2_volume:   float = 0.0
    n:           int   = 0

    @property
    def std_range(self) -> float:
        return math.sqrt(self.m2_range / (self.n - 1)) if self.n > 1 else 0.0

    @property
    def std_volume(self) -> float:
        return math.sqrt(self.m2_volume / (self.n - 1)) if self.n > 1 else 0.0


@dataclass
class PendingAnomaly:
    """One anomalous candle waiting for confirmation."""
    timestamp: datetime
    z_range:   float
    z_volume:  float
    direction: int      



def fetch_all_symbols() -> list[str]:
    """
    Pulls every active, tradable US equity from Alpaca's assets API.
    Returns ~8,000–10,000 symbols. Falls back to WATCHLIST on failure.
    """
    log.info("Fetching full symbol universe from Alpaca assets API…")
    try:
        client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
        assets = client.get_all_assets(GetAssetsRequest(
            asset_class=AssetClass.US_EQUITY,
            status=AssetStatus.ACTIVE,
        ))
        symbols = [a.symbol for a in assets if a.tradable]
        log.info(f"Symbol universe: {len(symbols):,} tradable US equities")
        return symbols
    except Exception as exc:
        log.warning(f"Assets API failed ({exc}) — falling back to WATCHLIST")
        return WATCHLIST


class CooldownTracker:
    """Blocks re-alerts for the same symbol within COOLDOWN_MINUTES."""

    def __init__(self, minutes: int = COOLDOWN_MINUTES):
        self._last: dict[str, float] = {}
        self._window = minutes * 60

    def is_cooling(self, symbol: str) -> bool:
        return time.monotonic() - self._last.get(symbol, 0) < self._window

    def mark(self, symbol: str) -> None:
        self._last[symbol] = time.monotonic()



class ConfirmationTracker:
    """
    Requires CONFIRM_COUNT anomalous candles 
    """

    def __init__(self) -> None:
        self._pending: dict[str, list[PendingAnomaly]] = defaultdict(list)

    def observe(self, symbol: str, candle: Candle,
                z_range: float, z_volume: float) -> bool:
        direction = 1 if candle.close >= candle.open else -1
        cutoff    = candle.timestamp - timedelta(minutes=CONFIRM_WINDOW_MIN)
        pending   = self._pending[symbol]

        # Drop stale entries outside the time window
        pending[:] = [p for p in pending if p.timestamp >= cutoff]

        # Drop entries moving the wrong way (if direction filter is on)
        if CONFIRM_DIRECTION:
            pending[:] = [p for p in pending if p.direction == direction]

        pending.append(PendingAnomaly(candle.timestamp, z_range, z_volume, direction))

        if len(pending) >= CONFIRM_COUNT:
            self._pending[symbol] = []
            return True
        return False

    def pending_count(self, symbol: str) -> int:
        return len(self._pending.get(symbol, []))

    def reset(self, symbol: str) -> None:
        self._pending[symbol] = []


class StateStore:
    """
    Rolling window
    """

    def __init__(self, window: int = BASELINE_CANDLES):
        self._window  = window
        self._candles: dict[str, deque[Candle]] = defaultdict(
            lambda: deque(maxlen=window)
        )
        self._stats: dict[str, Baseline] = {}

    def push(self, candle: Candle) -> None:
        buf     = self._candles[candle.symbol]
        evicted = buf[0] if len(buf) == buf.maxlen else None
        buf.append(candle)
        self._update(candle.symbol, candle, evicted)

    def stats(self, symbol: str) -> Optional[Baseline]:
        return self._stats.get(symbol)

    def ready(self, symbol: str) -> bool:
        return len(self._candles[symbol]) >= WARMUP_MIN_CANDLES

    def _update(self, symbol: str, added: Candle,
                evicted: Optional[Candle]) -> None:
        """
        Sliding Welford update.
        """
        buf  = self._candles[symbol]
        n    = len(buf)
        prev = self._stats.get(symbol) or Baseline()

        if n < 2:
            # Not enough data yet — do a full pass (only happens in warmup)
            ranges   = [c.range  for c in buf]
            volumes  = [c.volume for c in buf]
            mu_r = sum(ranges)   / n
            mu_v = sum(volumes)  / n
            m2_r = sum((v - mu_r) ** 2 for v in ranges)
            m2_v = sum((v - mu_v) ** 2 for v in volumes)
            self._stats[symbol] = Baseline(mu_r, m2_r, mu_v, m2_v, n)
            return

        mu_r, m2_r = self._slide(
            prev.mean_range,  prev.m2_range,
            added.range,      evicted.range   if evicted else None, n
        )
        mu_v, m2_v = self._slide(
            prev.mean_volume, prev.m2_volume,
            added.volume,     evicted.volume  if evicted else None, n
        )
        self._stats[symbol] = Baseline(mu_r, m2_r, mu_v, m2_v, n)

    @staticmethod
    def _slide(old_mean: float, old_m2: float,
               add_val: float, evict_val: Optional[float],
               n: int) -> tuple[float, float]:
        """
        One-step sliding-window Welford for a single metric.
        """
        # Add new value (online Welford step)
        new_mean = old_mean + (add_val - old_mean) / n
        new_m2   = old_m2   + (add_val - old_mean) * (add_val - new_mean)

        # Remove evicted value (reverse Welford step)
        if evict_val is not None:
            prev_n    = n          # window size before eviction counted
            prev_mean = new_mean + (evict_val - new_mean) / (prev_n - 1)
            new_m2    = max(new_m2 - (evict_val - prev_mean) * (evict_val - new_mean), 0.0)
            new_mean  = new_mean + (new_mean - prev_mean) / max(prev_n - 2, 1)

        return new_mean, new_m2


class CandleBuilder:
    """
    Aggregates 1-min bars from the WebSocket 
    """

    def __init__(self, interval_min: int = CANDLE_INTERVAL):
        self._n       = interval_min
        self._buckets: dict[str, dict] = {}

    def update(self, bar: Bar) -> Optional[Candle]:
        sym    = bar.symbol
        bucket = self._floor(bar.timestamp)
        curr   = self._buckets.get(sym)

        if curr is None:
            self._buckets[sym] = self._new_bucket(bar, bucket)
            return None

        if bucket == curr["bucket"]:
            curr["high"]   = max(curr["high"],  bar.high)
            curr["low"]    = min(curr["low"],   bar.low)
            curr["close"]  = bar.close
            curr["volume"] += bar.volume
            return None

        # New interval — emit completed candle, start fresh bucket
        completed = Candle(
            symbol=sym,
            timestamp=curr["bucket"],
            open=curr["open"],  high=curr["high"],
            low=curr["low"],    close=curr["close"],
            volume=curr["volume"],
        )
        self._buckets[sym] = self._new_bucket(bar, bucket)
        return completed

    def _floor(self, ts: datetime) -> datetime:
        return ts.replace(minute=(ts.minute // self._n) * self._n,
                          second=0, microsecond=0)

    @staticmethod
    def _new_bucket(bar: Bar, bucket: datetime) -> dict:
        return dict(bucket=bucket, open=bar.open, high=bar.high,
                    low=bar.low, close=bar.close, volume=bar.volume)



class Stage1Filter:
    """
    Relative threshold gate.
    """

    @staticmethod
    def check(candle: Candle, baseline: Baseline) -> bool:
        is_upward    = candle.close > candle.open          # skip bearish candles
        if not is_upward:
            return False
        price_spike  = (baseline.mean_range  > 0 and
                        candle.range  > STAGE1_PRICE_MULT  * baseline.mean_range)
        volume_spike = (baseline.mean_volume > 0 and
                        candle.volume > STAGE1_VOLUME_MULT * baseline.mean_volume)
        return price_spike or volume_spike



class Stage2Filter:
    """
    Computes Z-scores for candle range and volume vs. rolling baseline.
    """

    @staticmethod
    def check(candle: Candle, baseline: Baseline) -> tuple[bool, float, float]:
        def z(val: float, mu: float, sigma: float) -> float:
            return (val - mu) / sigma if sigma > 0 else 0.0

        z_r = z(candle.range,  baseline.mean_range,  baseline.std_range)
        z_v = z(candle.volume, baseline.mean_volume, baseline.std_volume)
        return (z_r > ZSCORE_THRESHOLD or z_v > ZSCORE_THRESHOLD), z_r, z_v


class AlertSender:
    """Deliversnomaly alert"""

    def __init__(self, token: str, chat_id: str):
        self._url     = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id

    def send(self, candle: Candle, baseline: Baseline,
             z_range: float, z_volume: float) -> None:
        arrow = "▲" if candle.close >= candle.open else "▼"
        msg = (
            f"🚨 *CONFIRMED ANOMALY: {candle.symbol}*\n\n"
            f"{arrow} Move:       `{candle.pct_change:.2f}%`   (Z = {z_range:.1f})\n"
            f"📊 Volume:      `{int(candle.volume):,}`   (Z = {z_volume:.1f})\n"
            f"💰 Close:       `${candle.close:.2f}`\n"
            f"🕐 Candle:      `{candle.timestamp.strftime('%H:%M')} UTC`\n"
            f"✅ Confirmed:   `{CONFIRM_COUNT} anomalies in ≤{CONFIRM_WINDOW_MIN} min`\n\n"
            f"_Baseline: {baseline.n} candles | "
            f"avg range {baseline.mean_range:.4f} | "
            f"avg vol {int(baseline.mean_volume):,}_"
        )
        try:
            resp = requests.post(
                self._url,
                json={"chat_id": self._chat_id, "text": msg, "parse_mode": "Markdown"},
                timeout=5,
            )
            resp.raise_for_status()
            log.info(f"✅  Alert sent → {candle.symbol}")
        except Exception as exc:
            log.error(f"Alert delivery failed for {candle.symbol}: {exc}")


class AlertSystem:
    """Wires all components"""

    def __init__(self) -> None:
        self.store    = StateStore()
        self.builder  = CandleBuilder()
        self.stage1   = Stage1Filter()
        self.stage2   = Stage2Filter()
        self.confirm  = ConfirmationTracker()
        self.cooldown = CooldownTracker()
        self.sender   = AlertSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    # ── bootstrap ────────────────────────────────────────────────
    def bootstrap(self, symbols: list[str]) -> None:
        """
        Seed the rolling window .
        """
        fetch_days = 5   
        total_syms = len(symbols)
        chunks     = [symbols[i:i + BOOTSTRAP_BATCH]
                      for i in range(0, total_syms, BOOTSTRAP_BATCH)]

        log.info(
            f"Bootstrapping {total_syms:,} symbols in {len(chunks)} batches "
            f"of {BOOTSTRAP_BATCH} — fetching last {fetch_days} calendar days "
            f"of {CANDLE_INTERVAL}-min bars…"
        )

        client      = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        total_count = 0
        start_time  = datetime.now(timezone.utc) - timedelta(days=fetch_days)

        for i, chunk in enumerate(chunks, 1):
            try:
                result = client.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=chunk,
                    timeframe=TimeFrame(CANDLE_INTERVAL, TimeFrameUnit.Minute),
                    start=start_time,
                ))
                batch_count = 0
                for sym, bar_list in result.data.items():
                    for bar in bar_list:
                        self.store.push(Candle(
                            symbol=sym,
                            timestamp=bar.timestamp,
                            open=float(bar.open),   high=float(bar.high),
                            low=float(bar.low),     close=float(bar.close),
                            volume=float(bar.volume),
                        ))
                        batch_count += 1
                total_count += batch_count
                log.info(
                    f"  Batch {i}/{len(chunks)} — "
                    f"{len(chunk)} symbols, {batch_count:,} candles loaded"
                )
            except Exception as exc:
                log.warning(f"  Batch {i}/{len(chunks)} failed: {exc} — skipping")

            if i < len(chunks):
                time.sleep(BOOTSTRAP_PAUSE)

        ready = sum(1 for s in symbols if self.store.ready(s))
        log.info(
            f"Bootstrap complete — {total_count:,} candles across {total_syms:,} symbols. "
            f"{ready:,} symbols ready for detection immediately. "
            f"Remainder warm up from live stream (~{WARMUP_MIN_CANDLES} candles needed)."
        )

    
    async def process_bar(self, bar: Bar) -> None:
        """Full pipeline for every incoming 1-min bar."""

        
        candle = self.builder.update(bar)
        if candle is None:
            return   

        sym = candle.symbol
        log.debug(
            f"{sym:6}  O={candle.open:.2f} H={candle.high:.2f} "
            f"L={candle.low:.2f} C={candle.close:.2f} V={int(candle.volume):,}"
        )

        
        if not self.store.ready(sym):
            self.store.push(candle)
            return

        
        if self.cooldown.is_cooling(sym):
            self.store.push(candle)
            return

        baseline = self.store.stats(sym)

        
        if not self.stage1.check(candle, baseline):
            self.store.push(candle)
            return

        log.info(
            f"Stage 1 ✓  {sym}  "
            f"range={candle.range:.4f} (avg {baseline.mean_range:.4f})  |  "
            f"vol={int(candle.volume):,} (avg {int(baseline.mean_volume):,})"
        )

        
        is_anomaly, z_r, z_v = self.stage2.check(candle, baseline)

        if not is_anomaly:
            log.info(
                f"Stage 2 ✗  {sym}  "
                f"z_range={z_r:.2f}  z_vol={z_v:.2f}  (below {ZSCORE_THRESHOLD}σ)"
            )
            self.store.push(candle)
            return

        
        direction_str = "▲ bullish" if candle.close >= candle.open else "▼ bearish"
        pending_now   = self.confirm.pending_count(sym) + 1
        confirmed     = self.confirm.observe(sym, candle, z_r, z_v)

        if not confirmed:
            log.info(
                f"Stage 2 ✓  {sym}  z_range={z_r:.2f}  z_vol={z_v:.2f}  "
                f"{direction_str}  → pending {pending_now}/{CONFIRM_COUNT} "
                f"(need {CONFIRM_COUNT - pending_now} more within {CONFIRM_WINDOW_MIN} min)"
            )
            self.store.push(candle)
            return

        
        log.info(
            f"CONFIRMED  {sym}  z_range={z_r:.2f}  z_vol={z_v:.2f}  "
            f"{direction_str}  {CONFIRM_COUNT}/{CONFIRM_COUNT}  → ALERT"
        )
        self.sender.send(candle, baseline, z_r, z_v)
        self.cooldown.mark(sym)
        self.store.push(candle)




def is_market_hours() -> tuple[bool, str]:
    now     = datetime.now(timezone.utc)
    weekday = now.weekday()
    hhmm    = now.hour * 100 + now.minute
    if weekday >= 5:
        return False, "Weekend — market closed"
    if 1430 <= hhmm < 2100:
        return True,  "Regular market hours (09:30–16:00 ET)"
    if 1000 <= hhmm < 1430:
        return False, "Pre-market (sparse activity)"
    if hhmm >= 2100 or hhmm < 100:
        return False, "After-hours (sparse activity)"
    return False, "Market closed (overnight)"


def _heartbeat_thread(interval: int = 300) -> None:
    while True:
        time.sleep(interval)
        ok, status = is_market_hours()
        log.info(f"Heartbeat — {'🟢' if ok else '🔴'} {status} | stream alive")


from saxo_trader import SaxoApiError, SaxoTrader


self.trader = SaxoTrader()


try:
    trade_result = self.trader.handle_alert(candle.symbol)
    log.info("Saxo result for %s: %s - %s", candle.symbol, trade_result.status, trade_result.message)
    if trade_result.status in {"ORDER_SENT", "DRY_RUN_PRECHECK_OK", "SKIPPED_SINGLE_SHARE_TOO_EXPENSIVE", "DISABLED"}:
     
        pass
except SaxoApiError as exc:
    log.error("Saxo trading failed for %s: %s", candle.symbol, exc)
except Exception as exc:
    log.exception("Unexpected Saxo trading error for %s: %s", candle.symbol, exc)

def main() -> None:
    import threading

    system = AlertSystem()

   
    if STREAM_ALL:
        symbols = fetch_all_symbols()
    else:
        symbols = WATCHLIST
        log.info(f"Watchlist mode — {len(symbols)} symbols")

   
    system.bootstrap(symbols)

    
    threading.Thread(target=_heartbeat_thread, args=(300,), daemon=True).start()

   
    log.info("Connecting WebSocket stream…")
    log.info("Data feed: IEX (free). Change to DataFeed.SIP for full tape (paid).")

    stream = StockDataStream(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
        feed=DataFeed.IEX,
    )

    async def on_bar(bar: Bar) -> None:
        await system.process_bar(bar)

    if STREAM_ALL:
        
        stream.subscribe_bars(on_bar, "*")
        log.info("Wildcard subscription active — streaming entire US equity market")
    else:
        stream.subscribe_bars(on_bar, *symbols)
        log.info(f"Subscribed to {len(symbols)} symbols")

    is_open, status = is_market_hours()
    log.info(f"Market status: {'🟢' if is_open else '🔴'} {status}")
    log.info("Heartbeat every 5 min. No output = market quiet, not frozen.")
    log.info("Regular hours: 14:30–21:00 UTC | Pre-market: 10:00–14:30 UTC")

    stream.run()   


if __name__ == "__main__":
    main()
 


