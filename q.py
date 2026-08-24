import asyncio
import json
import logging
import math
import os
import threading
import time
import xml.etree.ElementTree as E
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import gspread
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
from google.oauth2.service_account import Credentials


def a(n, d=None):
    v = os.getenv(n)
    return d if v is None or not v.strip() else v.strip()


def b(n, d=False):
    return a(n, str(d)).lower() in {"1", "true", "yes", "y", "on"}


def c(n, d=0):
    return int(float(a(n, d)))


def d(n, x=0.0):
    return float(a(n, x))


def e(n):
    v = a(n, "")
    if not v:
        raise RuntimeError(n)
    return v


K = e("ALPACA_API_KEY")
S = e("ALPACA_SECRET_KEY")
T = e("TELEGRAM_BOT_TOKEN")
C = e("TELEGRAM_CHAT_ID")
G = e("GOOGLE_SERVICE_ACCOUNT_JSON")
H = e("GOOGLE_SHEET_ID")

F = b("STREAM_ALL", True)
W = [x.strip().upper() for x in a("WATCHLIST", "").split(",") if x.strip()]
I = c("CANDLE_INTERVAL", 5)
J = d("STAGE1_PRICE_MULT", 2)
L = d("STAGE1_VOLUME_MULT", 3)
M = d("ZSCORE_THRESHOLD", 10)
N = c("BASELINE_CANDLES", 234)
O = c("WARMUP_MIN_CANDLES", 30)
P = c("BOOTSTRAP_BATCH", 200)
Q = d("BOOTSTRAP_PAUSE", 0.4)
R = c("QUARANTINE_MINUTES", 2)
U = d("QUARANTINE_RATE_TOLERANCE", 0.25)
Y1 = d("MIN_TWO_MIN_RETURN", 0.005)
Y2 = d("MAX_QUARANTINE_PULLBACK", 0.0025)
Y3 = c("SELL_CONFIRM_MINUTES", 2)
Y4 = d("MAX_SELL_PULLBACK", 0.003)
V = c("HOLD_MINUTES", 5)
X = d("BUDGET_DKK", 500)
Y = d("PENNY_STOCK_USD", 1)
Z = c("COOLDOWN_MINUTES", 60)
AA = a("USD_DKK_RATE", "")
AB = a("GOOGLE_WORKSHEET", "Events")
AC = a("GOOGLE_TRADES_WORKSHEET", "Trades")
AD = a("GOOGLE_SUMMARY_WORKSHEET", "Summary")
AJ = a("GOOGLE_DEBUG_WORKSHEET", "Debug")
AE = a("GOOGLE_EVENT_COLUMNS", "timestamp,symbol,event,price_usd,z_range,z_volume,quarantine_rate_per_min,reason,status")
AF = a("GOOGLE_TRADE_COLUMNS", "symbol,alert_time,alert_price,buy_time,buy_price,shares,invested_dkk,sell_time,sell_price,pnl_dkk,pnl_pct,status,ignore_reason,latest_price_at_close,close_price_time")
AG = a("GOOGLE_SUMMARY_COLUMNS", "metric,value")
AK = a("GOOGLE_DEBUG_COLUMNS", "timestamp,level,symbol,stage,message,price,volume,z_range,z_volume,status")
AQ = a("GOOGLE_DEBUG_MODE", "signals").lower()
AL = c("GOOGLE_FLUSH_SECONDS", 60)
AM = c("GOOGLE_RETRY_SECONDS", 60)
AN = c("GOOGLE_MAX_ROWS_PER_FLUSH", 500)
AO = ZoneInfo("America/New_York")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


@dataclass
class A1:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def range(self):
        return self.high - self.low


@dataclass
class A2:
    mean_range: float = 0.0
    m2_range: float = 0.0
    mean_volume: float = 0.0
    m2_volume: float = 0.0
    n: int = 0

    @property
    def std_range(self):
        return math.sqrt(self.m2_range / (self.n - 1)) if self.n > 1 else 0.0

    @property
    def std_volume(self):
        return math.sqrt(self.m2_volume / (self.n - 1)) if self.n > 1 else 0.0


@dataclass
class A3:
    symbol: str
    alert_time: datetime
    alert_price: float
    z_range: float
    z_volume: float
    prices: list[float] = field(default_factory=list)
    times: list[datetime] = field(default_factory=list)
    status: str = "QUARANTINED"
    buy_time: Optional[datetime] = None
    buy_price: Optional[float] = None
    shares: int = 0
    invested_dkk: float = 0.0
    sell_time: Optional[datetime] = None
    sell_price: Optional[float] = None
    pnl_dkk: Optional[float] = None
    latest_price: Optional[float] = None
    latest_time: Optional[datetime] = None
    ignore_reason: Optional[str] = None
    sell_checks: int = 0
    growths: list[float] = field(default_factory=list)
    alert_volume: float = 0.0


class B1:
    def __init__(self):
        self.a = {}
        self.b = Z * 60

    def q(self, s):
        return time.monotonic() - self.a.get(s, 0) < self.b

    def r(self, s):
        self.a[s] = time.monotonic()


class B2:
    def __init__(self):
        self.a = defaultdict(lambda: deque(maxlen=N))

    def q(self, x):
        self.a[x.symbol].append(x)

    def s(self, s):
        z = self.a[s]
        if not z:
            return None
        rr = [x.range for x in z]
        vv = [x.volume for x in z]
        mr = sum(rr) / len(rr)
        mv = sum(vv) / len(vv)
        return A2(mr, sum((x - mr) ** 2 for x in rr), mv, sum((x - mv) ** 2 for x in vv), len(z))

    def t(self, s):
        return len(self.a[s]) >= O


class B3:
    def __init__(self):
        self.a = {}

    def q(self, z):
        s = z.symbol
        w = self.a.get(s)
        f = z.timestamp.astimezone(timezone.utc).replace(minute=(z.timestamp.minute // I) * I, second=0, microsecond=0)
        if w is None:
            self.a[s] = [f, float(z.open), float(z.high), float(z.low), float(z.close), float(z.volume)]
            return None
        if f == w[0]:
            w[2] = max(w[2], float(z.high))
            w[3] = min(w[3], float(z.low))
            w[4] = float(z.close)
            w[5] += float(z.volume)
            return None
        x = A1(s, w[0], w[1], w[2], w[3], w[4], w[5])
        self.a[s] = [f, float(z.open), float(z.high), float(z.low), float(z.close), float(z.volume)]
        return x


class B4:
    def __init__(self):
        self.a = f"https://api.telegram.org/bot{T}/sendMessage"

    def q(self, x):
        try:
            r = requests.post(self.a, json={"chat_id": C, "text": x}, timeout=8)
            r.raise_for_status()
        except Exception as z:
            log.error("Telegram error: %s", z)


def q1():
    if AA:
        return float(AA)
    u = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
    try:
        r = requests.get(u, timeout=10)
        r.raise_for_status()
        root = E.fromstring(r.text)
        x = {}
        for z in root.iter():
            k = z.attrib.get("currency")
            v = z.attrib.get("rate")
            if k and v:
                x[k] = float(v)
        return x["DKK"] / x["USD"]
    except Exception:
        return float(a("USD_DKK_FALLBACK", "6.5"))


def q2(x):
    return x.isoformat() if x else None


class C1:
    def __init__(self):
        self.b = q1()
        self.a = []
        self.z = deque()
        self.y = deque()
        self.x = deque()
        self.l = threading.Lock()
        self.m = threading.Event()
        self.n = time.monotonic()
        self.r = 0
        self.closed = False
        credentials = Credentials.from_service_account_info(json.loads(G), scopes=["https://www.googleapis.com/auth/spreadsheets"])
        self.c = gspread.authorize(credentials).open_by_key(H)
        self.d = self.g(AB)
        self.e = self.g(AC)
        self.f = self.g(AD)
        self.p = self.g(AJ)
        self.h(self.d, AE.split(","))
        self.h(self.e, AF.split(","))
        self.h(self.f, AG.split(","))
        self.h(self.p, AK.split(","))
        self.q("INFO", "", "STARTUP", "Google Sheets connection established")
        self.j = threading.Thread(target=self.o, daemon=True)
        self.j.start()

    def g(self, name):
        try:
            return self.c.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.c.add_worksheet(title=name, rows=1000, cols=20)

    def h(self, w, headers):
        try:
            if not w.get_all_values():
                w.append_row(headers, value_input_option="USER_ENTERED")
        except Exception as z:
            log.error("Worksheet setup error: %s", z)

    def q(self, level, symbol, stage, message, price=None, volume=None, z_range=None, z_volume=None, status=None):
        ts = datetime.now(timezone.utc).isoformat()
        row = [ts, level, symbol, stage, message, price, volume, z_range, z_volume, status]
        if level == "ERROR":
            log.error("[%s] %s %s", stage, symbol or "-", message)
        elif level == "WARNING":
            log.warning("[%s] %s %s", stage, symbol or "-", message)
        else:
            log.info("[%s] %s %s", stage, symbol or "-", message)
        if AQ == "all" or stage in {"STARTUP", "CONFIG", "BOOTSTRAP", "ANOMALY", "QUARANTINE", "BUY", "SELL", "IGNORE", "CLOSE", "WEBSOCKET", "BAR_HANDLER", "BACKGROUND", "HEARTBEAT"} or level == "ERROR":
            with self.l:
                self.y.append(row)

    def i(self, x):
        with self.l:
            self.z.append(x)

    def k(self, x):
        with self.l:
            self.x.append(x)

    def o(self):
        backoff = 0
        while not self.m.is_set():
            self.m.wait(AL if backoff == 0 else backoff)
            if self.m.is_set():
                break
            try:
                self.flush()
                backoff = 0
                self.n = time.monotonic()
            except Exception as z:
                backoff = min(max(AM, AL), max(AL, 2 ** min(self.r, 6) * AL))
                self.r += 1
                log.error("Google batch flush error: %s", z)

    def flush(self):
        with self.l:
            debug_rows = [self.y.popleft() for _ in range(min(AN, len(self.y)))]
            event_rows = [self.z.popleft() for _ in range(min(AN, len(self.z)))]
            trade_rows = [self.x.popleft() for _ in range(min(AN, len(self.x)))]
        try:
            if debug_rows:
                self.p.append_rows(debug_rows, value_input_option="USER_ENTERED")
            if event_rows:
                self.d.append_rows(event_rows, value_input_option="USER_ENTERED")
            if trade_rows:
                self.e.append_rows(trade_rows, value_input_option="USER_ENTERED")
            self.r = 0
        except Exception:
            with self.l:
                for row in reversed(debug_rows):
                    self.y.appendleft(row)
                for row in reversed(event_rows):
                    self.z.appendleft(row)
                for row in reversed(trade_rows):
                    self.x.appendleft(row)
            raise

    def close(self, qs, latest):
        self.m.set()
        self.closed = True
        self.closed = True
        try:
            self.flush()
        except Exception as z:
            log.error("Final Google flush error: %s", z)
        rows = []
        for y in qs.values():
            rows.append([
                y.symbol, q2(y.alert_time), y.alert_price, q2(y.buy_time), y.buy_price,
                y.shares, y.invested_dkk, q2(y.sell_time), y.sell_price, y.pnl_dkk,
                (y.pnl_dkk / y.invested_dkk * 100) if y.invested_dkk else None,
                y.status, y.ignore_reason, latest.get(y.symbol, (None, None))[0],
                q2(latest.get(y.symbol, (None, None))[1])
            ])
        try:
            self.e.clear()
            self.e.append_row(AF.split(","), value_input_option="USER_ENTERED")
            if rows:
                self.e.append_rows(rows, value_input_option="USER_ENTERED")
            metrics = [
                ["budget_dkk", X], ["usd_dkk", self.b], ["penny_floor_usd", Y],
                ["quarantine_minutes", R], ["rate_tolerance", U], ["hold_minutes", V],
                ["events", len(self.a)], ["tracked_results", len(qs)],
                ["bought", sum(1 for y in qs.values() if y.shares)],
                ["closed", sum(1 for y in qs.values() if y.sell_price is not None)],
                ["realized_pnl_dkk", sum(float(y.pnl_dkk or 0) for y in qs.values())]
            ]
            self.f.clear()
            self.f.append_row(AG.split(","), value_input_option="USER_ENTERED")
            self.f.append_rows(metrics, value_input_option="USER_ENTERED")
        except Exception as z:
            log.error("Final sheet update error: %s", z)


class D1:
    def __init__(self):
        self.a = B2()
        self.b = B3()
        self.c = B1()
        self.d = B4()
        self.e = C1()
        self.f = {}
        self.g = {}
        self.h = self.e.b
        self.i = asyncio.Lock()
        self.j = 0
        self.k = 0
        self.l = 0
        self.m = 0
        self.n = 0
        self.o = 0
        self.p = 0
        self.history = {}
        self.qw = 0
        self.qc = 0
        self.qb = 0
        self.qn = 0
        self.q1 = 0
        self.q2 = 0
        self.qr = 0
        self.hb = time.monotonic()
        self.e.q("INFO", "", "CONFIG", f"stream_all={F}; watchlist={len(W)}; candle={I}; stage1_price={J}; stage1_volume={L}; z={M}; baseline={N}; warmup={O}; quarantine={R}; tolerance={U}; hold={V}; budget={X}; penny_floor={Y}")

    def q(self, ss):
        self.e.q("INFO", "", "BOOTSTRAP", f"Starting bootstrap for {len(ss)} symbols")
        cl = StockHistoricalDataClient(K, S)
        st = datetime.now(timezone.utc) - timedelta(days=5)
        for i in range(0, len(ss), P):
            zz = ss[i:i + P]
            try:
                rr = cl.get_stock_bars(StockBarsRequest(symbol_or_symbols=zz, timeframe=TimeFrame(I, TimeFrameUnit.Minute), start=st))
                count = 0
                for sy, bb in rr.data.items():
                    for v in bb:
                        self.a.q(A1(sy, v.timestamp, float(v.open), float(v.high), float(v.low), float(v.close), float(v.volume)))
                        count += 1
                self.e.q("INFO", "", "BOOTSTRAP", f"Loaded {count} historical candles for batch {i // P + 1}")
            except Exception as z:
                self.e.q("ERROR", "", "BOOTSTRAP", str(z))
            time.sleep(Q)
        self.e.q("INFO", "", "BOOTSTRAP", "Bootstrap completed")

    async def r(self, z):
        async with self.i:
            self.j += 1
            s = z.symbol
            p = float(z.close)
            t = z.timestamp.astimezone(timezone.utc)
            self.f[s] = (p, t)
            self.s(s, p, t)
            y = self.b.q(z)
            if y is None:
                self._heartbeat()
                return
            self.k += 1
            v = self.a.s(s)
            if not self.a.t(s):
                self.qw += 1
                if AQ == "all":
                    self.e.q("INFO", s, "FILTER", "Warmup incomplete", price=p, volume=y.volume)
                self.a.q(y)
                return
            if self.c.q(s):
                self.qc += 1
                if AQ == "all":
                    self.e.q("INFO", s, "FILTER", "Cooldown active", price=p, volume=y.volume)
                self.a.q(y)
                return
            if s in self.g:
                self.qn += 1
                if AQ == "all":
                    self.e.q("INFO", s, "FILTER", "Already tracked", price=p, volume=y.volume)
                self.a.q(y)
                return
            if v is None:
                self.qb += 1
                self.e.q("WARNING", s, "FILTER", "No baseline", price=p, volume=y.volume)
                self.a.q(y)
                return
            r1, r2 = self.u(y, v)
            self.e.q("INFO", s, "CANDLE", f"O={y.open:.4f} H={y.high:.4f} L={y.low:.4f} C={y.close:.4f}", price=p, volume=y.volume, z_range=r1, z_volume=r2)
            if y.close <= y.open:
                self.q1 += 1
                if AQ == "all":
                    self.e.q("INFO", s, "FILTER", "Rejected: not bullish", price=p, volume=y.volume)
                self.a.q(y)
                return
            if not self.v1(y, v):
                self.q1 += 1
                if AQ == "all":
                    self.e.q("INFO", s, "FILTER", f"Stage1 reject: range={y.range:.6f}/{v.mean_range:.6f}, volume={y.volume:.0f}/{v.mean_volume:.0f}", price=p, volume=y.volume, z_range=r1, z_volume=r2)
                self.a.q(y)
                return
            self.o += 1
            if not (r1 > M or r2 > M):
                self.q2 += 1
                if AQ == "all":
                    self.e.q("INFO", s, "FILTER", f"Stage2 reject: z_range={r1:.2f}, z_volume={r2:.2f}, threshold={M:.2f}", price=p, volume=y.volume, z_range=r1, z_volume=r2)
                self.a.q(y)
                return
            self.a.q(y)
            self.g[s] = A3(s, t, p, r1, r2, [p], [t], alert_volume=y.volume)
            self.history[s] = self.g[s]
            self.c.r(s)
            self.l += 1
            self.e.i([t.isoformat(), s, "Q", p, r1, r2, None, "ANOMALY", "QUARANTINED"])
            self.e.q("WARNING", s, "QUARANTINE", f"Started after anomaly z_range={r1:.2f}, z_volume={r2:.2f}", price=p, volume=y.volume, z_range=r1, z_volume=r2, status="QUARANTINED")

    def s(self, s, p, t):
        q = self.g.get(s)

        if q is None:
            return

        q.latest_price = p
        q.latest_time = t

        if q.status == "BOUGHT":
            if not q.times or t > q.times[-1]:
                if q.prices and q.prices[-1] > 0 and p > 0:
                    q.growths.append(math.log(p / q.prices[-1]))
                q.prices.append(p)
                q.times.append(t)

            if q.buy_time and t > q.buy_time and q.growths:
                g = q.growths[-1]
                if g <= -Y4:
                    q.sell_checks += 1
                else:
                    q.sell_checks = 0

                self.e.q(
                    "INFO",
                    s,
                    "HOLD",
                    f"minute_growth={g:.6f}, weak_streak={q.sell_checks}/{Y3}",
                    price=p,
                    status="BOUGHT",
                )

                if q.sell_checks >= Y3:
                    self.e.q(
                        "WARNING",
                        s,
                        "SELL",
                        f"Confirmed breakdown after {q.sell_checks} consecutive weak minutes",
                        price=p,
                        status="SOLD",
                    )
                    self.x(q, p, t)
            return

        if q.status != "QUARANTINED":
            return

        if t <= q.alert_time:
            return

        if not q.times or t > q.times[-1]:
            q.prices.append(p)
            q.times.append(t)
            minute_no = len(q.prices) - 1

            self.e.q(
                "INFO",
                s,
                "QUARANTINE",
                f"Observation {minute_no}/{R}: price={p:.4f}",
                price=p,
                status="QUARANTINED",
            )

        if len(q.prices) < 3:
            return

        p0, p1, p2 = q.prices[0], q.prices[1], q.prices[2]

        if min(p0, p1, p2) <= 0:
            self.w(q, "INVALID_PRICE")
            return

        r1 = math.log(p1 / p0)
        r2 = math.log(p2 / p1)
        total = math.log(p2 / p0)
        pullback = max(0.0, -r2)

        self.e.q(
            "INFO",
            s,
            "QUARANTINE",
            f"r1={r1:.6f}, r2={r2:.6f}, total={total:.6f}, pullback={pullback:.6f}",
            price=p,
            status="QUARANTINED",
        )

        if total < Y1:
            self.e.q(
                "WARNING",
                s,
                "QUARANTINE",
                f"Failed: two-minute return {total:.6f} < minimum {Y1:.6f}",
                price=p,
                status="IGNORED",
            )
            self.w(q, "TWO_MIN_RETURN_TOO_LOW")
            return

        if pullback > Y2:
            self.e.q(
                "WARNING",
                s,
                "QUARANTINE",
                f"Failed: minute-2 pullback {pullback:.6f} > maximum {Y2:.6f}",
                price=p,
                status="IGNORED",
            )
            self.w(q, "SECOND_MINUTE_PULLBACK_TOO_LARGE")
            return

        self.e.q(
            "WARNING",
            s,
            "QUARANTINE",
            f"Passed: total={total:.6f}, pullback={pullback:.6f}",
            price=p,
            status="PASSED",
        )

        self.t(q, p, t)

    def t(self, q, p, t):
        if p < Y:
            self.w(q, "PENNY_STOCK")
            return
        n = math.floor((X / self.h) / p)
        if n < 1:
            self.w(q, "SINGLE_SHARE_TOO_EXPENSIVE")
            return
        cost = n * p * self.h
        q.status = "BOUGHT"
        q.buy_time = t
        q.buy_price = p
        q.shares = n
        q.invested_dkk = cost
        self.m += 1
        self.e.i([t.isoformat(), q.symbol, "B", p, q.z_range, q.z_volume, None, "QUARANTINE_PASS", "BOUGHT"])
        self.e.k([q.symbol, q2(q.alert_time), q.alert_price, q2(q.buy_time), q.buy_price, q.shares, q.invested_dkk, None, None, None, None, "BOUGHT", None, p, q2(t)])
        self.d.q(f"BUY\n{q.symbol}\nshares: {n}\nprice: ${p:.4f}\ncost: DKK {cost:.2f}\nvolume: {q.alert_volume:.0f}")
        self.e.q("WARNING", q.symbol, "BUY", f"Bought {n} shares @ ${p:.4f}, invested DKK {cost:.2f}, volume={q.alert_volume:.0f}", price=p, volume=q.alert_volume, z_range=q.z_range, z_volume=q.z_volume, status="BOUGHT")

    def x(self, q, p, t):
        q.status = "SOLD"
        q.sell_time = t
        q.sell_price = p
        q.latest_price = p
        q.latest_time = t
        q.pnl_dkk = q.shares * p * self.h - q.invested_dkk
        pct = q.pnl_dkk / q.invested_dkk * 100 if q.invested_dkk else 0.0
        self.n += 1
        self.e.i([t.isoformat(), q.symbol, "S", p, q.z_range, q.z_volume, None, "HOLD_COMPLETE", "SOLD"])
        self.e.k([q.symbol, q2(q.alert_time), q.alert_price, q2(q.buy_time), q.buy_price, q.shares, q.invested_dkk, q2(q.sell_time), q.sell_price, q.pnl_dkk, pct, "SOLD", None, p, q2(t)])
        self.d.q(f"SELL\n{q.symbol}\nprice: ${p:.4f}\nP/L: DKK {q.pnl_dkk:+.2f} ({pct:+.2f}%)")
        self.e.q("WARNING", q.symbol, "SELL", f"Sold @ ${p:.4f}, P/L DKK {q.pnl_dkk:+.2f} ({pct:+.2f}%)", price=p, status="SOLD")
        self.g.pop(q.symbol, None)

    def w(self, q, r):
        q.status = "IGNORED"
        q.ignore_reason = r
        self.p += 1
        self.e.i([(q.latest_time or q.alert_time).isoformat(), q.symbol, "I", q.latest_price or q.alert_price, q.z_range, q.z_volume, None, r, "IGNORED"])
        self.e.q("INFO", q.symbol, "IGNORE", f"Ignored: {r}", price=q.latest_price or q.alert_price, z_range=q.z_range, z_volume=q.z_volume, status="IGNORED")
        self.g.pop(q.symbol, None)

    def u(self, q, b):
        zr = (q.range - b.mean_range) / b.std_range if b.std_range > 0 else 0.0
        zv = (q.volume - b.mean_volume) / b.std_volume if b.std_volume > 0 else 0.0
        return zr, zv

    def v1(self, q, b):
        return (b.mean_range > 0 and q.range > J * b.mean_range) or (b.mean_volume > 0 and q.volume > L * b.mean_volume)

    def y(self):
        ss = sorted({x[1] for x in self.e.z if len(x) > 1 and x[1]})
        if not ss:
            return
        hh = {"APCA-API-KEY-ID": K, "APCA-API-SECRET-KEY": S}
        for i in range(0, len(ss), 100):
            bb = ss[i:i + 100]
            try:
                r = requests.get("https://data.alpaca.markets/v2/stocks/bars/latest", params={"symbols": ",".join(bb), "feed": "iex"}, headers=hh, timeout=15)
                r.raise_for_status()
                for sy, z in r.json().get("bars", {}).items():
                    ts = datetime.fromisoformat(z["t"].replace("Z", "+00:00"))
                    self.f[sy] = (float(z["c"]), ts)
            except Exception as z:
                self.e.q("ERROR", "", "CLOSE", f"Latest price request failed: {z}")

    def z(self):
        try:
            ss = sorted({q.symbol for q in self.g.values()})
            if ss:
                hh = {"APCA-API-KEY-ID": K, "APCA-API-SECRET-KEY": S}
                for i in range(0, len(ss), 100):
                    bb = ss[i:i + 100]
                    r = requests.get("https://data.alpaca.markets/v2/stocks/bars/latest", params={"symbols": ",".join(bb), "feed": "iex"}, headers=hh, timeout=15)
                    r.raise_for_status()
                    for sy, z in r.json().get("bars", {}).items():
                        ts = datetime.fromisoformat(z["t"].replace("Z", "+00:00"))
                        self.f[sy] = (float(z["c"]), ts)
            self.e.close(dict(self.history), dict(self.f))
            self.e.q("INFO", "", "CLOSE", f"Close update: bars={self.j}, candles={self.k}, stage1={self.o}, quarantines={self.l}, buys={self.m}, sells={self.n}, ignored={self.p}")
        except Exception as z:
            self.e.q("ERROR", "", "CLOSE", str(z))

    def _heartbeat(self):
        if time.monotonic() - self.hb >= 60:
            self.hb = time.monotonic()
            self.e.q("INFO", "", "HEARTBEAT", f"bars={self.j}, candles={self.k}, active={len(self.g)}, stage1_pass={self.o}, quarantines={self.l}, buys={self.m}, sells={self.n}, ignored={self.p}, warmup_reject={self.qw}, cooldown_reject={self.qc}, tracked_reject={self.qn}, baseline_missing={self.qb}, bullish_or_stage1_reject={self.q1}, stage2_reject={self.q2}")


def f1():
    try:
        x = TradingClient(K, S, paper=True).get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE))
        return sorted({z.symbol for z in x if z.tradable})
    except Exception:
        return W


def f2():
    n = datetime.now(AO)
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return 570 <= m < 960


def f3():
    s = D1()
    ss = f1() if F else W
    s.e.q("INFO", "", "STARTUP", f"Monitoring {len(ss)} symbols")
    s.q(ss)
    z = StockDataStream(K, S, feed=DataFeed.IEX)

    async def h(bar: Bar):
        try:
            if f2():
                await s.r(bar)
            else:
                s._heartbeat()
        except Exception as ex:
            s.e.q("ERROR", getattr(bar, "symbol", ""), "BAR_HANDLER", str(ex))
            log.exception("Bar handler error")

    if F:
        z.subscribe_bars(h, "*")
    else:
        z.subscribe_bars(h, *ss)

    def k():
        seen = set()
        while True:
            try:
                n = datetime.now(AO)
                if n.weekday() < 5 and n.hour == 16 and n.minute >= 5 and n.date() not in seen:
                    s.z()
                    seen.add(n.date())
            except Exception as ex:
                s.e.q("ERROR", "", "BACKGROUND", str(ex))
            time.sleep(30)

    threading.Thread(target=k, daemon=True).start()
    s.e.q("INFO", "", "STARTUP", "Starting Alpaca websocket")
    try:
        z.run()
    finally:
        s.z()


if __name__ == "__main__":
    f3()
