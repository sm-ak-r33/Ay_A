import asyncio
import json
import logging
import math
import os
import threading
import time
import xml.etree.ElementTree as E
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

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
V = c("HOLD_MINUTES", 5)
X = d("BUDGET_DKK", 500)
Y = d("PENNY_STOCK_USD", 1)
Z = c("COOLDOWN_MINUTES", 60)
AA = a("USD_DKK_RATE", "")
AB = a("GOOGLE_WORKSHEET", "Events")
AC = a("GOOGLE_TRADES_WORKSHEET", "Trades")
AD = a("GOOGLE_SUMMARY_WORKSHEET", "Summary")
AE = a("GOOGLE_EVENT_COLUMNS", "timestamp,symbol,event,price_usd,z_range,z_volume,quarantine_rate_per_min,reason,status")
AF = a("GOOGLE_TRADE_COLUMNS", "symbol,alert_time,alert_price,buy_time,buy_price,shares,invested_dkk,sell_time,sell_price,pnl_dkk,pnl_pct,status,ignore_reason,latest_price_at_close,close_price_time")
AG = a("GOOGLE_SUMMARY_COLUMNS", "metric,value")

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
    alert_rate: float
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
        self.b = {}
    def q(self, x):
        z = self.a[x.symbol]
        z.append(x)
        self.b[x.symbol] = self.r(z)
    def r(self, z):
        if not z:
            return None
        rr = [x.range for x in z]
        vv = [x.volume for x in z]
        mr = sum(rr) / len(rr)
        mv = sum(vv) / len(vv)
        return A2(mr, sum((x - mr) ** 2 for x in rr), mv, sum((x - mv) ** 2 for x in vv), len(z))
    def s(self, s):
        return self.b.get(s)
    def t(self, s):
        return len(self.a[s]) >= O


class B3:
    def __init__(self):
        self.a = {}
    def q(self, z):
        s = z.symbol
        w = self.a.get(s)
        f = z.timestamp.replace(minute=(z.timestamp.minute // I) * I, second=0, microsecond=0)
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
            log.error("%s", z)


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


def q3(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(type(o).__name__)


class C1:
    def __init__(self):
        self.a = []
        self.b = q1()
        self.c = gspread.authorize(Credentials.from_service_account_info(json.loads(G), scopes=["https://www.googleapis.com/auth/spreadsheets"])).open_by_key(H)
        self.d = self.c.worksheet(AB)
        self.e = self.c.worksheet(AC)
        self.f = self.c.worksheet(AD)
        self._x(self.d, AE.split(","))
        self._x(self.e, AF.split(","))
        self._x(self.f, AG.split(","))
    def _x(self, w, h):
        if not w.get_all_values():
            w.append_row(h, value_input_option="USER_ENTERED")
    def q(self, x):
        self.a.append(x)
        self.d.append_row([x.get("timestamp"), x.get("symbol"), x.get("event"), x.get("price_usd"), x.get("z_range"), x.get("z_volume"), x.get("quarantine_rate_per_min"), x.get("reason"), x.get("status")], value_input_option="USER_ENTERED")
    def r(self, qs, p):
        z = []
        for v in qs.values():
            y = v
            z.append([
                y.symbol, q2(y.alert_time), y.alert_price, q2(y.buy_time), y.buy_price, y.shares, y.invested_dkk,
                q2(y.sell_time), y.sell_price, y.pnl_dkk, (y.pnl_dkk / y.invested_dkk * 100) if y.invested_dkk else None,
                y.status, y.ignore_reason, p.get(y.symbol, (None, None))[0], q2(p.get(y.symbol, (None, None))[1])
            ])
        self.e.clear()
        self.e.append_row(AF.split(","), value_input_option="USER_ENTERED")
        if z:
            self.e.append_rows(z, value_input_option="USER_ENTERED")
        m = [
            ["budget_dkk", X], ["usd_dkk", self.b], ["penny_floor_usd", Y], ["quarantine_minutes", R],
            ["rate_tolerance", U], ["hold_minutes", V], ["events", len(self.a)], ["bought", sum(1 for y in qs.values() if y.shares)],
            ["closed", sum(1 for y in qs.values() if y.sell_price is not None)], ["realized_pnl_dkk", sum(float(y.pnl_dkk or 0) for y in qs.values())]
        ]
        self.f.clear()
        self.f.append_row(AG.split(","), value_input_option="USER_ENTERED")
        self.f.append_rows(m, value_input_option="USER_ENTERED")


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
    def q(self, ss):
        cl = StockHistoricalDataClient(K, S)
        st = datetime.now(timezone.utc) - timedelta(days=5)
        for i in range(0, len(ss), P):
            zz = ss[i:i+P]
            try:
                rr = cl.get_stock_bars(StockBarsRequest(symbol_or_symbols=zz, timeframe=TimeFrame(I, TimeFrameUnit.Minute), start=st))
                for sy, bb in rr.data.items():
                    for v in bb:
                        self.a.q(A1(sy, v.timestamp, float(v.open), float(v.high), float(v.low), float(v.close), float(v.volume)))
            except Exception as z:
                log.warning("%s", z)
            time.sleep(Q)
    async def r(self, z):
        async with self.i:
            s = z.symbol
            p = float(z.close)
            t = z.timestamp
            self.f[s] = (p, t)
            self.s(s, p, t)
            y = self.b.q(z)
            if y is None:
                return
            if not self.a.t(s) or self.c.q(s) or s in self.g:
                self.a.q(y)
                return
            v = self.a.s(s)
            if v is None or not self.k(y, v):
                self.a.q(y)
                return
            a1, a2 = self.l(y, v)
            if not a1:
                self.a.q(y)
                return
            r = math.log(y.close / y.open) / I if y.open > 0 and y.close > 0 else 0.0
            self.g[s] = A3(s, t, p, r, a2, a1, [p], [t])
            self.c.r(s)
            self.e.q({"timestamp": t.isoformat(), "symbol": s, "event": "Q", "price_usd": p, "z_range": a2, "z_volume": a1, "quarantine_rate_per_min": r, "reason": "V" if a1 > a2 else "R", "status": "QUARANTINED"})
            self.a.q(y)
    def s(self, s, p, t):
        q = self.g.get(s)
        if q is None:
            return
        q.latest_price = p
        q.latest_time = t
        if q.status == "BOUGHT" and q.buy_time and t >= q.buy_time + timedelta(minutes=V):
            self.x(q, p, t)
            return
        if q.status != "QUARANTINED" or t <= q.alert_time:
            return
        if not q.times or t > q.times[-1]:
            q.prices.append(p)
            q.times.append(t)
        if len(q.prices) < R + 1:
            return
        rr = []
        for i in range(1, R + 1):
            u, v = q.prices[i-1], q.prices[i]
            if u <= 0 or v <= 0:
                self.w(q, "P")
                return
            rr.append(math.log(v / u))
        if q.alert_rate <= 0 or not all(v > 0 and abs(v - q.alert_rate) <= abs(q.alert_rate) * U for v in rr):
            self.w(q, "M")
            return
        self.t(q, p, t)
    def t(self, q, p, t):
        if p < Y:
            self.w(q, "X")
            return
        n = math.floor((X / self.h) / p)
        if n < 1:
            self.w(q, "B")
            return
        cost = n * p * self.h
        q.status = "BOUGHT"
        q.buy_time = t
        q.buy_price = p
        q.shares = n
        q.invested_dkk = cost
        self.e.q({"timestamp": t.isoformat(), "symbol": q.symbol, "event": "B", "price_usd": p, "reason": "C", "status": "BOUGHT"})
        self.d.q(f"BUY\n{q.symbol}\nshares: {n}\nprice: ${p:.4f}\ncost: DKK {cost:.2f}\nvolume: {q.z_volume:.0f}")
    def x(self, q, p, t):
        q.status = "SOLD"
        q.sell_time = t
        q.sell_price = p
        q.pnl_dkk = q.shares * p * self.h - q.invested_dkk
        q.latest_price = p
        q.latest_time = t
        self.e.q({"timestamp": t.isoformat(), "symbol": q.symbol, "event": "S", "price_usd": p, "reason": "H", "status": "SOLD"})
        pct = q.pnl_dkk / q.invested_dkk * 100 if q.invested_dkk else 0
        self.d.q(f"SELL\n{q.symbol}\nprice: ${p:.4f}\nP/L: DKK {q.pnl_dkk:+.2f} ({pct:+.2f}%)")
        self.g.pop(q.symbol, None)
    def w(self, q, r):
        q.status = "IGNORED"
        q.ignore_reason = r
        self.e.q({"timestamp": (q.latest_time or q.alert_time).isoformat(), "symbol": q.symbol, "event": "I", "price_usd": q.latest_price or q.alert_price, "z_range": q.z_range, "z_volume": q.z_volume, "quarantine_rate_per_min": q.alert_rate, "reason": r, "status": "IGNORED"})
        self.g.pop(q.symbol, None)
    def k(self, q, b):
        if q.close <= q.open:
            return False
        return (b.mean_range > 0 and q.range > J * b.mean_range) or (b.mean_volume > 0 and q.volume > L * b.mean_volume)
    def l(self, q, b):
        zr = (q.range - b.mean_range) / b.std_range if b.std_range > 0 else 0
        zv = (q.volume - b.mean_volume) / b.std_volume if b.std_volume > 0 else 0
        return zr > M or zv > M, zr, zv
    def y(self):
        ss = sorted({x.get("symbol") for x in self.e.a if x.get("symbol")})
        if not ss:
            return
        hh = {"APCA-API-KEY-ID": K, "APCA-API-SECRET-KEY": S}
        for i in range(0, len(ss), 100):
            bb = ss[i:i+100]
            try:
                r = requests.get("https://data.alpaca.markets/v2/stocks/bars/latest", params={"symbols": ",".join(bb), "feed": "iex"}, headers=hh, timeout=15)
                r.raise_for_status()
                for sy, z in r.json().get("bars", {}).items():
                    ts = datetime.fromisoformat(z["t"].replace("Z", "+00:00"))
                    self.f[sy] = (float(z["c"]), ts)
            except Exception as z:
                log.warning("%s", z)
    def z(self):
        self.y()
        self.e.r({x.symbol: x for x in self.g.values()}, self.f)


def f1():
    try:
        x = TradingClient(K, S, paper=True).get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE))
        return sorted({z.symbol for z in x if z.tradable})
    except Exception:
        return W


def f2():
    n = datetime.now(timezone.utc)
    if n.weekday() >= 5:
        return False
    h = n.hour * 100 + n.minute
    return (1330 <= h < 2000) or (1430 <= h < 2100)


def f3():
    s = D1()
    ss = f1() if F else W
    s.q(ss)
    z = StockDataStream(K, S, feed=DataFeed.IEX)
    async def h(bar: Bar):
        if f2():
            await s.r(bar)
    if F:
        z.subscribe_bars(h, "*")
    else:
        z.subscribe_bars(h, *ss)
    def k():
        tz = __import__("zoneinfo").ZoneInfo("America/New_York")
        seen = set()
        while True:
            n = datetime.now(tz)
            if n.weekday() < 5 and n >= n.replace(hour=16, minute=5, second=0, microsecond=0) and n.date() not in seen:
                s.z()
                seen.add(n.date())
            time.sleep(30)
    threading.Thread(target=k, daemon=True).start()
    try:
        z.run()
    finally:
        s.z()


if __name__ == "__main__":
    f3()
