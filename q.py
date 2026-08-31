import asyncio
import json
import logging
import math
import os
import threading
import time
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
OR = a("OPENROUTER_API_KEY", "")

F = b("STREAM_ALL", True)
W = [x.strip().upper() for x in a("WATCHLIST", "").split(",") if x.strip()]

I = c("CANDLE_INTERVAL", 5)
J = d("STAGE1_PRICE_MULT", 2)
L = d("STAGE1_VOLUME_MULT", 3)
M = d("ZSCORE_THRESHOLD", 3)
N = c("BASELINE_CANDLES", 234)
O = c("WARMUP_MIN_CANDLES", 30)
P = c("BOOTSTRAP_BATCH", 200)
Q = d("BOOTSTRAP_PAUSE", 0.4)

R = c("QUARANTINE_MINUTES", 2)
U = d("QUARANTINE_RATE_TOLERANCE", 0.25)
Y1 = d("MIN_TWO_MIN_RETURN", 0.005)
Y2 = d("MAX_QUARANTINE_PULLBACK", 0.0025)

X = d("BUDGET_DKK", 500)
Y = d("PENNY_STOCK_USD", 1)
Z = c("COOLDOWN_MINUTES", 60)

TR0 = d("TRAIL_0_10_PCT", 4)
TR1 = d("TRAIL_10_25_PCT", 7)
TR2 = d("TRAIL_25_50_PCT", 10)
TR3 = d("TRAIL_50_PLUS_PCT", 12)

AA = a("USD_DKK_RATE", "")
AB = a("GOOGLE_WORKSHEET", "Events")
AC = a("GOOGLE_TRADES_WORKSHEET", "Trades")
AD = a("GOOGLE_SUMMARY_WORKSHEET", "Summary")
AJ = a("GOOGLE_DEBUG_WORKSHEET", "Debug")
AK = a("GOOGLE_QUARANTINE_WORKSHEET", "Quarantine")
AL = a("GOOGLE_AI_WORKSHEET", "AI")

AQ = a("GOOGLE_DEBUG_MODE", "signals").lower()
FL = c("GOOGLE_FLUSH_SECONDS", 60)
RT = c("GOOGLE_RETRY_SECONDS", 60)
MX = c("GOOGLE_MAX_ROWS_PER_FLUSH", 500)

GE = b("GROK_ENABLED", True)
GM = a("GROK_MODEL", "x-ai/grok-4.20")
GR = c("GROK_MAX_RESULTS", 2)
GC = c("GROK_CACHE_HOURS", 24)
GT = c("GROK_TIMEOUT_SECONDS", 25)
GW = c("GROK_NEWS_WEEKDAY_HOURS", 24)
GEW = c("GROK_NEWS_WEEKEND_HOURS", 72)

DB = b("DAILY_BRIEF_ENABLED", True)
DH = c("BRIEF_HOUR_ET", 8)
DM = c("BRIEF_MINUTE_ET", 30)
HB = b("HOURLY_HEARTBEAT_ENABLED", True)
HM = c("HEARTBEAT_MINUTE_ET", 0)

RB = c("RANK_BOOTSTRAP_DAYS", 35)
RR = c("RANK_BOOTSTRAP_BATCH", 200)
RP = d("RANK_BOOTSTRAP_PAUSE", 0.4)
TOPN = c("HEARTBEAT_TOP_N", 5)

NY = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
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
    mean_range: float
    m2_range: float
    mean_volume: float
    m2_volume: float
    n: int

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
    alert_volume: float
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
    peak_price: Optional[float] = None
    peak_gain_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    news_decision: str = ""
    news_hint: str = ""
    news_window_hours: int = 0
    news_checked_at: Optional[datetime] = None
    news_sources: list[str] = field(default_factory=list)
    trade_id: str = ""


class B1:
    def __init__(self):
        self.a = {}
        self.b = Z * 60

    def q(self, s):
        t = self.a.get(s)
        if t is None:
            return False
        return time.monotonic() - t < self.b

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
        return A2(
            mr,
            sum((x - mr) ** 2 for x in rr),
            mv,
            sum((x - mv) ** 2 for x in vv),
            len(z),
        )

    def t(self, s):
        return len(self.a[s]) >= O


class B3:
    def __init__(self):
        self.a = {}

    def q(self, z):
        s = z.symbol
        w = self.a.get(s)
        f = z.timestamp.astimezone(timezone.utc).replace(
            minute=(z.timestamp.minute // I) * I,
            second=0,
            microsecond=0,
        )
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


class B5:
    def __init__(self):
        self.a = OR
        self.b = GM
        self.c = {}
        self.d = {}
        self.e = threading.Lock()

    def _q(self, messages, tools, response_format=None, max_tokens=None):
        if not self.a or not GE:
            return None
        body = {
            "model": self.b,
            "messages": messages,
            "temperature": 0,
            "reasoning": {"enabled": False},
        }
        if tools:
            body["tools"] = tools
        if response_format:
            body["response_format"] = response_format
        if max_tokens is not None:
            body["max_tokens"] = max_tokens
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.a}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/sm-ak-r33/Ay_A",
                "X-Title": "Ay_A",
            },
            json=body,
            timeout=GT,
        )
        r.raise_for_status()
        return r.json()

    def q(self, q):
        if not self.a or not GE:
            return "NO", "", [], 0, None

        now = datetime.now(timezone.utc)
        key = f"{q.symbol}:{now.date().isoformat()}"

        with self.e:
            if key in self.c:
                z = self.c[key]
                if now - z[4] < timedelta(hours=GC):
                    return z

        dt = q.alert_time.astimezone(NY)
        hours = GEW if dt.weekday() == 0 else GW
        start = q.alert_time - timedelta(hours=hours)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a strict market catalyst gate. Search current web news. "
                    "Return only YES when a credible, material, recent company-specific "
                    "catalyst plausibly explains the abnormal move. Otherwise return only NO. "
                    "Do not use general market movement, old articles, unrelated mentions, "
                    "or unsupported speculation."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Ticker: {q.symbol}\n"
                    f"Alert UTC: {q.alert_time.astimezone(timezone.utc).isoformat()}\n"
                    f"Price: {q.alert_price:.6f}\n"
                    f"5-minute z-range: {q.z_range:.3f}\n"
                    f"5-minute z-volume: {q.z_volume:.3f}\n"
                    f"Volume: {q.alert_volume:.0f}\n"
                    f"Search window start: {start.isoformat()}\n"
                    f"Search window end: {q.alert_time.isoformat()}\n"
                    "Return exactly YES or NO."
                ),
            },
        ]

        data = self._q(
            messages,
            [
                {
                    "type": "openrouter:web_search",
                    "parameters": {
                        "max_results": max(1, min(GR, 3)),
                        "max_total_results": max(1, min(GR, 3)),
                        "search_context_size": "low",
                    },
                }
            ],
            max_tokens=2,
        )

        raw = ""
        sources = []

        if data:
            raw = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
                .upper()
            )

            anns = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("annotations", [])
                or []
            )

            for x in anns:
                u = x.get("url_citation", {}).get("url")
                if u and u not in sources:
                    sources.append(u)

        decision = "YES" if raw == "YES" else "NO"
        checked = datetime.now(timezone.utc)
        hint = ""

        if decision == "YES":
            titles = []
            if data:
                anns = (
                    data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("annotations", [])
                    or []
                )
                for x in anns[:GR]:
                    uc = x.get("url_citation", {})
                    title = uc.get("title")
                    content = uc.get("content", "")
                    if title:
                        titles.append(f"{title}: {content[:500]}")

            if titles:
                hdata = self._q(
                    [
                        {
                            "role": "system",
                            "content": (
                                "Create a factual Telegram hint of exactly 5 or 6 words "
                                "from the supplied news evidence. No ticker, no punctuation."
                            ),
                        },
                        {
                            "role": "user",
                            "content": "\n".join(titles),
                        },
                    ],
                    [],
                    max_tokens=10,
                )
                if hdata:
                    hint = (
                        hdata.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                        .strip()
                    )

        result = (decision, hint, sources, hours, checked)

        with self.e:
            self.c[key] = result

        return result

    def brief(self, when):
        if not self.a or not GE or not DB:
            return ""

        day = when.astimezone(NY).strftime("%Y-%m-%d")
        key = f"BRIEF:{day}"

        with self.e:
            if key in self.d:
                return self.d[key]

        prompt = (
            f"Today is {day} in New York.\n"
            "Create a concise pre-market day-trader briefing for U.S. equities.\n"
            "Use current web information only.\n\n"
            "1) Earnings: important Nasdaq and NYSE listed companies reporting today.\n"
            "2) Premarket: fetch https://marketchameleon.com/Reports/PremarketTrading and identify "
            f"the top {TOPN} notable premarket gainers and top {TOPN} notable premarket losers. "
            "Give each a 5 or 6 word reason.\n"
            "3) Biotech: identify companies with Phase 2 or Phase 3 data events today, "
            "or FDA/PDUFA decisions today.\n"
            "4) Energy: identify relevant EIA crude inventory, OPEC+, geopolitics, "
            "and weather catalysts for energy stocks today.\n\n"
            "Use compact Telegram formatting. Do not invent dates, events, or reasons. "
            "Prioritize actionable names and omit weak/unverified items."
        )

        data = self._q(
            [
                {
                    "role": "system",
                    "content": "You are a concise pre-market research analyst. Verify facts with current web sources.",
                },
                {"role": "user", "content": prompt},
            ],
            [
                {
                    "type": "openrouter:web_search",
                    "parameters": {
                        "max_results": max(3, min(GR + 1, 5)),
                        "max_total_results": 18,
                        "search_context_size": "low",
                    },
                },
                {
                    "type": "openrouter:web_fetch",
                    "parameters": {
                        "max_content_tokens": 5000,
                    },
                },
            ],
            max_tokens=1200,
        )

        text = ""
        if data:
            text = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )

        with self.e:
            self.d[key] = text

        return text

    def hb(self, rows):
        if not self.a or not GE:
            return ""

        if not rows:
            return "No ranked movers available."

        payload = "\n".join(
            f"{i+1}. {x[0]} {x[1]:+.2f}% {x[2]}"
            for i, x in enumerate(rows)
        )

        data = self._q(
            [
                {
                    "role": "system",
                    "content": (
                        "For each listed stock, give exactly a factual 4 or 5 word reason "
                        "for today's move. Use current web search. Output one line per stock "
                        "in the form SYMBOL|REASON. Do not add other text."
                    ),
                },
                {"role": "user", "content": payload},
            ],
            [
                {
                    "type": "openrouter:web_search",
                    "parameters": {
                        "max_results": 1,
                        "max_total_results": max(5, min(len(rows), 15)),
                        "search_context_size": "low",
                    },
                }
            ],
            max_tokens=max(20, len(rows) * 8),
        )

        if not data:
            return ""

        return (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )


class C1:
    def __init__(self):
        self.b = q1()
        self.a = []
        self.z = deque()
        self.y = deque()
        self.x = deque()
        self.q = deque()
        self.aiq = deque()
        self.l = threading.Lock()
        self.m = threading.Event()
        self.r = 0

        credentials = Credentials.from_service_account_info(
            json.loads(G),
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )

        self.c = gspread.authorize(credentials).open_by_key(H)

        self.d = self.g(AB)
        self.e = self.g(AC)
        self.f = self.g(AD)
        self.p = self.g(AJ)
        self.qw = self.g(AK)
        self.ai = self.g(AL)

        self.h(self.d, [
            "timestamp","symbol","event","price_usd","z_range","z_volume",
            "alert_volume","rate_per_min","reason","status"
        ])
        self.h(self.e, [
            "trade_id","symbol","alert_time","alert_price","buy_time","buy_price",
            "shares","invested_dkk","sell_time","sell_price","pnl_dkk","pnl_pct",
            "status","exit_reason","peak_price","peak_gain_pct","max_drawdown_pct",
            "latest_price_at_close","close_price_time","news_decision","news_hint",
            "news_window_hours","news_checked_at","news_sources"
        ])
        self.h(self.f, ["section","metric","value"])
        self.h(self.p, [
            "timestamp","level","symbol","stage","message","price","volume",
            "z_range","z_volume","status"
        ])
        self.h(self.qw, [
            "trade_id","symbol","alert_time","alert_price","minute_1_time",
            "minute_1_price","rate_1","minute_2_time","minute_2_price","rate_2",
            "two_min_return","minute_2_pullback","decision","reason"
        ])
        self.h(self.ai, [
            "timestamp","type","symbol","decision","hint","window_hours",
            "checked_at","sources"
        ])

        self.qx("INFO", "", "STARTUP", "Google Sheets connection established")

        threading.Thread(target=self.o, daemon=True).start()

    def g(self, name):
        try:
            return self.c.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.c.add_worksheet(title=name, rows=1000, cols=30)

    def h(self, w, headers):
        if not w.get_all_values():
            w.append_row(headers, value_input_option="USER_ENTERED")

    def qx(self, level, symbol, stage, message, price=None, volume=None, z_range=None, z_volume=None, status=None):
        row = [
            datetime.now(timezone.utc).isoformat(),
            level,
            symbol,
            stage,
            message,
            price,
            volume,
            z_range,
            z_volume,
            status,
        ]

        if level == "ERROR":
            log.error("[%s] %s %s", stage, symbol or "-", message)
        elif level == "WARNING":
            log.warning("[%s] %s %s", stage, symbol or "-", message)
        else:
            log.info("[%s] %s %s", stage, symbol or "-", message)

        if AQ == "all" or stage in {
            "STARTUP","CONFIG","BOOTSTRAP","ANOMALY","QUARANTINE","BUY",
            "SELL","IGNORE","CLOSE","WEBSOCKET","BAR_HANDLER","BACKGROUND",
            "HEARTBEAT","NEWS","BRIEF"
        } or level == "ERROR":
            with self.l:
                self.y.append(row)

    def ev(self, row):
        with self.l:
            self.z.append(row)

    def tr(self, row):
        with self.l:
            self.x.append(row)

    def qu(self, row):
        with self.l:
            self.q.append(row)

    def airow(self, row):
        with self.l:
            self.aiq = getattr(self, "aiq", deque())
            self.aiq.append(row)

    def o(self):
        while not self.m.is_set():
            self.m.wait(FL)
            if self.m.is_set():
                break
            try:
                self.f1()
            except Exception as z:
                log.error("Google flush error: %s", z)

    def f1(self):
        with self.l:
            a1 = [self.y.popleft() for _ in range(min(MX, len(self.y)))]
            a2 = [self.z.popleft() for _ in range(min(MX, len(self.z)))]
            a3 = [self.x.popleft() for _ in range(min(MX, len(self.x)))]
            a4 = [self.q.popleft() for _ in range(min(MX, len(self.q)))]
            a5 = [self.aiq.popleft() for _ in range(min(MX, len(self.aiq)))]

        try:
            if a1:
                self.p.append_rows(a1, value_input_option="USER_ENTERED")
            if a2:
                self.d.append_rows(a2, value_input_option="USER_ENTERED")
            if a3:
                self.e.append_rows(a3, value_input_option="USER_ENTERED")
            if a4:
                self.qw.append_rows(a4, value_input_option="USER_ENTERED")
            if a5:
                self.ai.append_rows(a5, value_input_option="USER_ENTERED")
        except Exception:
            with self.l:
                for r in reversed(a1): self.y.appendleft(r)
                for r in reversed(a2): self.z.appendleft(r)
                for r in reversed(a3): self.x.appendleft(r)
                for r in reversed(a4): self.q.appendleft(r)
                for r in reversed(a5): self.aiq.appendleft(r)
            raise

    def close(self, qs, latest, counts):
        self.m.set()
        try:
            self.f1()
        except Exception as z:
            log.error("Final Google flush error: %s", z)

        try:
            self.e.clear()
            self.e.append_row([
                "trade_id","symbol","alert_time","alert_price","buy_time","buy_price",
                "shares","invested_dkk","sell_time","sell_price","pnl_dkk","pnl_pct",
                "status","exit_reason","peak_price","peak_gain_pct","max_drawdown_pct",
                "latest_price_at_close","close_price_time","news_decision","news_hint",
                "news_window_hours","news_checked_at","news_sources"
            ], value_input_option="USER_ENTERED")

            rows = []
            for y in qs.values():
                pct = y.pnl_dkk / y.invested_dkk * 100 if y.invested_dkk else None
                rows.append([
                    y.trade_id,
                    y.symbol,
                    q2(y.alert_time),
                    y.alert_price,
                    q2(y.buy_time),
                    y.buy_price,
                    y.shares,
                    y.invested_dkk,
                    q2(y.sell_time),
                    y.sell_price,
                    y.pnl_dkk,
                    pct,
                    y.status,
                    y.ignore_reason,
                    y.peak_price,
                    y.peak_gain_pct,
                    y.max_drawdown_pct,
                    latest.get(y.symbol, (None,None))[0],
                    q2(latest.get(y.symbol, (None,None))[1]),
                    y.news_decision,
                    y.news_hint,
                    y.news_window_hours,
                    q2(y.news_checked_at),
                    "|".join(y.news_sources),
                ])

            if rows:
                self.e.append_rows(rows, value_input_option="USER_ENTERED")

            self.f.clear()
            self.f.append_row(["section","metric","value"], value_input_option="USER_ENTERED")

            metrics = [
                ["Signals","candles",counts["candles"]],
                ["Signals","stage1_pass",counts["stage1"]],
                ["Signals","stage2_pass",counts["stage2"]],
                ["Signals","quarantines",counts["quarantine"]],
                ["Signals","quarantine_pass",counts["qpass"]],
                ["Signals","quarantine_fail",counts["qfail"]],
                ["Grok","news_yes",counts["news_yes"]],
                ["Grok","news_no",counts["news_no"]],
                ["Trades","buys",counts["buys"]],
                ["Trades","sells",counts["sells"]],
                ["Trades","wins",counts["wins"]],
                ["Trades","losses",counts["losses"]],
                ["Trades","flat",counts["flat"]],
                ["Performance","realized_pnl_dkk",counts["pnl"]],
                ["Performance","win_rate_pct",counts["win_rate"]],
                ["Strategy","budget_dkk",X],
                ["Strategy","zscore_threshold",M],
                ["Strategy","min_two_min_return",Y1],
                ["Strategy","max_quarantine_pullback",Y2],
                ["Strategy","trail_0_10_pct",TR0],
                ["Strategy","trail_10_25_pct",TR1],
                ["Strategy","trail_25_50_pct",TR2],
                ["Strategy","trail_50_plus_pct",TR3],
            ]

            self.f.append_rows(metrics, value_input_option="USER_ENTERED")
        except Exception as z:
            log.error("Final Google close error: %s", z)


def q1():
    x = a("USD_DKK_RATE", "")
    if x:
        return float(x)
    try:
        r = requests.get(
            "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            timeout=10,
        )
        r.raise_for_status()
        import xml.etree.ElementTree as E
        root = E.fromstring(r.text)
        d1 = {}
        for z in root.iter():
            k = z.attrib.get("currency")
            v = z.attrib.get("rate")
            if k and v:
                d1[k] = float(v)
        return d1["DKK"] / d1["USD"]
    except Exception:
        return float(a("USD_DKK_FALLBACK", "6.5"))


def q2(x):
    return x.isoformat() if x else None


class D1:
    def __init__(self):
        self.a = B2()
        self.b = B3()
        self.c = B1()
        self.d = B4()
        self.e = C1()
        self.ai = B5()
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
        self.history = {}
        self.qw = 0
        self.qc = 0
        self.qb = 0
        self.qn = 0
        self.q1 = 0
        self.q2 = 0
        self.qr = 0
        self.qpass = 0
        self.qfail = 0
        self.gy = 0
        self.gn = 0
        self.winners = 0
        self.losers = 0
        self.flat = 0
        self.premarket_sent = set()
        self.heartbeat_sent = set()
        self.daily = {}
        self.rank_dates = []
        self.e.qx("INFO", "", "CONFIG", f"stream_all={F}; watchlist={len(W)}; candle={I}; stage1_price={J}; stage1_volume={L}; z={M}; baseline={N}; warmup={O}; quarantine={R}; tolerance={U}; min_two_min_return={Y1}; max_quarantine_pullback={Y2}; budget={X}; penny_floor={Y}; trails={TR0}/{TR1}/{TR2}/{TR3}; grok={GE}; model={GM}; brief={DB}; heartbeat={HB}")

    def q(self, ss):
        self.e.qx("INFO", "", "BOOTSTRAP", f"Starting bootstrap for {len(ss)} symbols")
        cl = StockHistoricalDataClient(K, S)
        st = datetime.now(timezone.utc) - timedelta(days=5)
        for i in range(0, len(ss), P):
            zz = ss[i:i + P]
            try:
                rr = cl.get_stock_bars(
                    StockBarsRequest(
                        symbol_or_symbols=zz,
                        timeframe=TimeFrame(I, TimeFrameUnit.Minute),
                        start=st,
                    )
                )
                count = 0
                for sy, bb in rr.data.items():
                    for v in bb:
                        self.a.q(A1(sy, v.timestamp, float(v.open), float(v.high), float(v.low), float(v.close), float(v.volume)))
                        count += 1
                self.e.qx("INFO", "", "BOOTSTRAP", f"Loaded {count} historical candles for batch {i // P + 1}")
            except Exception as z:
                self.e.qx("ERROR", "", "BOOTSTRAP", str(z))
            time.sleep(Q)

        self.e.qx("INFO", "", "BOOTSTRAP", "Bootstrap completed")

    def qb(self, ss):
        self.e.qx("INFO", "", "RANK_BOOTSTRAP", f"Starting daily rank bootstrap for {len(ss)} symbols")
        cl = StockHistoricalDataClient(K, S)
        st = datetime.now(timezone.utc) - timedelta(days=RB)
        for i in range(0, len(ss), RR):
            zz = ss[i:i + RR]
            try:
                rr = cl.get_stock_bars(
                    StockBarsRequest(
                        symbol_or_symbols=zz,
                        timeframe=TimeFrame(1, TimeFrameUnit.Day),
                        start=st,
                    )
                )
                for sy, bb in rr.data.items():
                    self.daily.setdefault(sy, {})
                    for v in bb:
                        day = v.timestamp.astimezone(timezone.utc).date()
                        self.daily[sy][day] = float(v.close)
            except Exception as z:
                self.e.qx("ERROR", "", "RANK_BOOTSTRAP", str(z))
            time.sleep(RP)

        self.e.qx("INFO", "", "RANK_BOOTSTRAP", "Daily rank bootstrap completed")

    def _trail(self, gain):
        if gain < 10:
            return TR0
        if gain < 25:
            return TR1
        if gain < 50:
            return TR2
        return TR3

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
                return

            self.k += 1
            v = self.a.s(s)

            if not self.a.t(s):
                self.qw += 1
                self.a.q(y)
                return

            if self.c.q(s):
                self.qc += 1
                self.a.q(y)
                return

            if s in self.g:
                self.qn += 1
                self.a.q(y)
                return

            if v is None:
                self.qb += 1
                self.a.q(y)
                return

            r1, r2 = self.u(y, v)

            if y.close <= y.open:
                self.q1 += 1
                self.a.q(y)
                return

            if not self.v1(y, v):
                self.q1 += 1
                self.a.q(y)
                return

            self.o += 1

            if not (r1 > M or r2 > M):
                self.q2 += 1
                self.a.q(y)
                return

            self.qr += 1
            self.a.q(y)

            q = A3(
                s,
                t,
                p,
                r1,
                r2,
                y.volume,
                [p],
                [t],
            )
            q.trade_id = f"{s}-{int(t.timestamp())}"
            self.g[s] = q
            self.history[s] = q
            self.c.r(s)
            self.l += 1

            self.e.ev([
                t.isoformat(), s, "Q", p, r1, r2, y.volume, None,
                "ANOMALY", "QUARANTINED"
            ])

            self.e.qx(
                "WARNING",
                s,
                "QUARANTINE",
                f"Started after anomaly z_range={r1:.2f}, z_volume={r2:.2f}",
                price=p,
                volume=y.volume,
                z_range=r1,
                z_volume=r2,
                status="QUARANTINED",
            )

    def s(self, s, p, t):
        q = self.g.get(s)
        if q is None:
            return

        q.latest_price = p
        q.latest_time = t

        if q.status == "BOUGHT":
            if q.buy_price and p > 0 and q.peak_price:
                if p > q.peak_price:
                    q.peak_price = p
                    q.peak_gain_pct = (p / q.buy_price - 1) * 100

                dd = (p / q.peak_price - 1) * 100
                q.max_drawdown_pct = min(q.max_drawdown_pct, dd)
                threshold = self._trail(q.peak_gain_pct)

                if dd <= -threshold:
                    self.x(q, p, t, "ADAPTIVE_TRAILING_STOP")
            return

        if q.status != "QUARANTINED" or t <= q.alert_time:
            return

        if not q.times or t > q.times[-1]:
            if q.prices[-1] > 0 and p > 0:
                q.prices.append(p)
                q.times.append(t)

        if len(q.prices) < 3:
            return

        a0, a1, a2 = q.prices[0], q.prices[1], q.prices[2]
        if min(a0, a1, a2) <= 0:
            self.w(q, "INVALID_PRICE")
            return

        r1 = math.log(a1 / a0)
        r2 = math.log(a2 / a1)
        total = math.log(a2 / a0)
        pb = -r2 if r2 < 0 else 0.0

        self.e.qu([
            q.trade_id, q.symbol, q2(q.alert_time), q.alert_price,
            q2(q.times[1]), q.prices[1], r1,
            q2(q.times[2]), q.prices[2], r2,
            total, pb, "PENDING", ""
        ])

        if total < Y1:
            self.qfail += 1
            self.w(q, "TWO_MIN_RETURN_TOO_LOW")
            return

        if pb > Y2:
            self.qfail += 1
            self.w(q, "SECOND_MINUTE_PULLBACK_TOO_LARGE")
            return

        self.qpass += 1
        self.e.qu([
            q.trade_id, q.symbol, q2(q.alert_time), q.alert_price,
            q2(q.times[1]), q.prices[1], r1,
            q2(q.times[2]), q.prices[2], r2,
            total, pb, "PASS", ""
        ])

        self._news(q, p, t)

    def _news(self, q, p, t):
        if q.status != "QUARANTINED":
            return

        dec, hint, sources, hours, checked = self.ai.q(q)
        q.news_decision = dec
        q.news_hint = hint
        q.news_sources = sources
        q.news_window_hours = hours
        q.news_checked_at = checked

        self.e.airow([
            checked.isoformat(),
            "SIGNAL",
            q.symbol,
            dec,
            hint,
            hours,
            checked.isoformat(),
            "|".join(sources)
        ])

        self.e.qx(
            "WARNING" if dec == "YES" else "INFO",
            q.symbol,
            "NEWS",
            f"Decision={dec}",
            price=p,
            volume=q.alert_volume,
            z_range=q.z_range,
            z_volume=q.z_volume,
            status=dec,
        )

        if dec != "YES":
            self.gn += 1
            self.w(q, "NEWS_NO")
            return

        self.gy += 1
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
        q.peak_price = p
        q.peak_gain_pct = 0.0
        q.max_drawdown_pct = 0.0

        self.m += 1

        self.e.ev([
            t.isoformat(), q.symbol, "B", p, q.z_range, q.z_volume,
            q.alert_volume, None, "NEWS_YES", "BOUGHT"
        ])

        self.e.tr([
            q.trade_id, q.symbol, q2(q.alert_time), q.alert_price,
            q2(q.buy_time), q.buy_price, q.shares, q.invested_dkk,
            None, None, None, None, "BOUGHT", None, q.peak_price,
            q.peak_gain_pct, q.max_drawdown_pct, p, q2(t),
            q.news_decision, q.news_hint, q.news_window_hours,
            q2(q.news_checked_at), "|".join(q.news_sources)
        ])

        hint = q.news_hint or "Recent catalyst supports move"

        self.d.q(
            f"BUY\n{q.symbol}\n{hint}\nshares: {n}\nprice: ${p:.4f}\n"
            f"cost: DKK {cost:.2f}\nvolume: {q.alert_volume:.0f}"
        )

        self.e.qx(
            "WARNING",
            q.symbol,
            "BUY",
            f"Bought {n} shares @ ${p:.4f}; news=YES; hint={hint}",
            price=p,
            volume=q.alert_volume,
            z_range=q.z_range,
            z_volume=q.z_volume,
            status="BOUGHT",
        )

    def x(self, q, p, t, reason):
        q.status = "SOLD"
        q.sell_time = t
        q.sell_price = p
        q.latest_price = p
        q.latest_time = t
        q.ignore_reason = reason
        q.pnl_dkk = q.shares * p * self.h - q.invested_dkk

        pct = q.pnl_dkk / q.invested_dkk * 100 if q.invested_dkk else 0.0

        self.n += 1
        if q.pnl_dkk > 0:
            self.winners += 1
        elif q.pnl_dkk < 0:
            self.losers += 1
        else:
            self.flat += 1

        self.e.ev([
            t.isoformat(), q.symbol, "S", p, q.z_range, q.z_volume,
            q.alert_volume, None, reason, "SOLD"
        ])

        self.e.tr([
            q.trade_id, q.symbol, q2(q.alert_time), q.alert_price,
            q2(q.buy_time), q.buy_price, q.shares, q.invested_dkk,
            q2(q.sell_time), q.sell_price, q.pnl_dkk, pct, "SOLD",
            reason, q.peak_price, q.peak_gain_pct, q.max_drawdown_pct,
            p, q2(t), q.news_decision, q.news_hint,
            q.news_window_hours, q2(q.news_checked_at),
            "|".join(q.news_sources)
        ])

        self.d.q(
            f"SELL\n{q.symbol}\nprice: ${p:.4f}\nP/L: DKK {q.pnl_dkk:+.2f} ({pct:+.2f}%)"
        )

        self.e.qx(
            "WARNING",
            q.symbol,
            "SELL",
            f"Sold @ ${p:.4f}; P/L DKK {q.pnl_dkk:+.2f} ({pct:+.2f}%); reason={reason}",
            price=p,
            status="SOLD",
        )

        self.g.pop(q.symbol, None)

    def w(self, q, r):
        q.status = "IGNORED"
        q.ignore_reason = r

        self.e.ev([
            (q.latest_time or q.alert_time).isoformat(),
            q.symbol,
            "I",
            q.latest_price or q.alert_price,
            q.z_range,
            q.z_volume,
            q.alert_volume,
            None,
            r,
            "IGNORED",
        ])

        self.e.qx(
            "INFO",
            q.symbol,
            "IGNORE",
            f"Ignored: {r}",
            price=q.latest_price or q.alert_price,
            volume=q.alert_volume,
            z_range=q.z_range,
            z_volume=q.z_volume,
            status="IGNORED",
        )

        self.g.pop(q.symbol, None)

    def u(self, q, b):
        zr = (q.range - b.mean_range) / b.std_range if b.std_range > 0 else 0.0
        zv = (q.volume - b.mean_volume) / b.std_volume if b.std_volume > 0 else 0.0
        return zr, zv

    def v1(self, q, b):
        return (
            b.mean_range > 0 and q.range > J * b.mean_range
        ) or (
            b.mean_volume > 0 and q.volume > L * b.mean_volume
        )

    def ranks(self):
        rows = []
        now = datetime.now(NY)
        today = now.date()
        keys = sorted(self.f.keys())

        for s in keys:
            p, ts = self.f[s]
            hist = self.daily.get(s, {})
            if not hist:
                continue

            days = sorted(hist.keys())
            prev = [x for x in days if x < today]
            if not prev:
                continue

            d1 = hist[prev[-1]]
            widx = prev[-5] if len(prev) >= 5 else prev[0]
            midx = prev[-21] if len(prev) >= 21 else prev[0]

            daily = (p / d1 - 1) * 100 if d1 > 0 else None
            weekly = (p / hist[widx] - 1) * 100 if hist.get(widx, 0) > 0 else None
            monthly = (p / hist[midx] - 1) * 100 if hist.get(midx, 0) > 0 else None

            rows.append((s, daily, weekly, monthly))

        return rows

    def heartbeat(self):
        now = datetime.now(NY)
        if not HB:
            return
        if now.weekday() >= 5:
            return
        if now.hour < 10 or now.hour > 15 or now.minute != HM:
            return

        key = now.strftime("%Y-%m-%d-%H")
        if key in self.heartbeat_sent:
            return

        rows = self.ranks()
        if not rows:
            return

        a1 = sorted([x for x in rows if x[1] is not None], key=lambda x: x[1], reverse=True)[:TOPN]
        a2 = sorted([x for x in rows if x[2] is not None], key=lambda x: x[2], reverse=True)[:TOPN]
        a3 = sorted([x for x in rows if x[3] is not None], key=lambda x: x[3], reverse=True)[:TOPN]

        flat = []
        for label, block, idx in [("D", a1, 1), ("W", a2, 2), ("M", a3, 3)]:
            for x in block:
                flat.append((x[0], x[idx], label))

        if not flat:
            return

        hints = self.ai.hb(flat)
        hm = {}
        for line in hints.splitlines():
            if "|" in line:
                s, h = line.split("|", 1)
                hm[s.strip().upper()] = h.strip()

        def fmt(title, block, idx):
            out = [title]
            for n, x in enumerate(block, 1):
                out.append(f"{n}. {x[0]} {x[idx]:+.2f}% — {hm.get(x[0], 'No catalyst confirmed')}")
            return "\n".join(out)

        text = (
            "📊 MARKET HEARTBEAT\n\n"
            + fmt("DAILY", a1, 1)
            + "\n\n"
            + fmt("WEEKLY", a2, 2)
            + "\n\n"
            + fmt("MONTHLY", a3, 3)
        )

        self.d.q(text)

        self.e.qx("INFO", "", "HEARTBEAT", f"Sent hourly heartbeat for {key}")
        self.heartbeat_sent.add(key)

    def brief(self):
        now = datetime.now(NY)
        if not DB:
            return
        if now.weekday() >= 5:
            return
        if now.hour != DH or now.minute != DM:
            return

        key = now.date().isoformat()
        if key in self.premarket_sent:
            return

        text = self.ai.brief(datetime.now(timezone.utc))
        if not text:
            self.e.qx("WARNING", "", "BRIEF", "No briefing returned")
            return

        self.d.q(f"🌅 PRE-MARKET BRIEF\n\n{text}")
        self.e.qx("WARNING", "", "BRIEF", "Pre-market briefing sent")
        self.premarket_sent.add(key)

    def tick(self):
        self.brief()
        self.heartbeat()

    def scheduler(self):
        seen_day = None
        seen_hours = set()

        while True:
            try:
                now = datetime.now(NY)

                if now.weekday() < 5:
                    if now.hour == DH and now.minute == DM and seen_day != now.date():
                        self.brief()
                        seen_day = now.date()

                    if now.minute == HM and now.hour >= 10 and now.hour <= 15:
                        key = now.strftime("%Y-%m-%d-%H")
                        if key not in seen_hours:
                            self.heartbeat()
                            seen_hours.add(key)

                    cutoff = now - timedelta(days=3)
                    seen_hours = {x for x in seen_hours if x[:10] >= cutoff.strftime("%Y-%m-%d")}
                time.sleep(10)
            except Exception as ex:
                self.e.qx("ERROR", "", "SCHEDULER", str(ex))
                time.sleep(10)

    def close(self):
        latest = dict(self.f)
        qs = dict(self.history)

        realized = sum(float(x.pnl_dkk or 0) for x in qs.values())
        completed = sum(1 for x in qs.values() if x.sell_price is not None)
        wr = self.winners / completed * 100 if completed else 0.0

        counts = {
            "candles": self.k,
            "stage1": self.o,
            "stage2": self.qr,
            "quarantine": self.l,
            "qpass": self.qpass,
            "qfail": self.qfail,
            "news_yes": self.gy,
            "news_no": self.gn,
            "buys": self.m,
            "sells": self.n,
            "wins": self.winners,
            "losses": self.losers,
            "flat": self.flat,
            "pnl": realized,
            "win_rate": wr,
        }

        try:
            self.e.close(qs, latest, counts)
        except Exception as z:
            log.error("Close error: %s", z)


def f1():
    try:
        x = TradingClient(K, S, paper=True).get_all_assets(
            GetAssetsRequest(
                asset_class=AssetClass.US_EQUITY,
                status=AssetStatus.ACTIVE,
            )
        )
        return sorted({z.symbol for z in x if z.tradable})
    except Exception as z:
        log.warning("Asset universe failed: %s", z)
        return W


def f2():
    n = datetime.now(NY)
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return 570 <= m < 960


def f3():
    s = D1()
    ss = f1() if F else W

    s.e.qx("INFO", "", "STARTUP", "VERSION=4.0-GROK-BRIEF-HEARTBEAT")
    s.e.qx("INFO", "", "STARTUP", f"Monitoring {len(ss)} symbols")

    s.q(ss)
    s.qb(ss)

    z = StockDataStream(K, S, feed=DataFeed.IEX)

    async def h(bar: Bar):
        try:
            if f2():
                await s.r(bar)
        except Exception as ex:
            s.e.qx(
                "ERROR",
                getattr(bar, "symbol", ""),
                "BAR_HANDLER",
                str(ex),
            )
            log.exception("Bar handler error")

    if F:
        z.subscribe_bars(h, "*")
    else:
        z.subscribe_bars(h, *ss)

    s.e.qx("INFO", "", "STARTUP", "Starting background scheduler")
    threading.Thread(target=s.scheduler, daemon=True).start()

    s.e.qx("INFO", "", "STARTUP", "Starting Alpaca websocket")

    try:
        z.run()
    except Exception as ex:
        s.e.qx("ERROR", "", "WEBSOCKET", str(ex))
        raise
    finally:
        s.close()


if __name__ == "__main__":
    f3()
