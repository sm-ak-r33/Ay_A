import argparse
import json
import logging
import math
import os
import queue
import re
import ipaddress
import socket
from urllib.parse import urljoin, urlsplit, urlunsplit
import threading
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = timezone.utc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
L = logging.getLogger("ay")

SHEETS = {
    1: "S1_News_Longer",
    2: "S2_Quick_Shorter",
    3: "S3_Fall_Buyer",
    4: "S4_Premarket_Buyer",
    5: "S5_Earning_Staker",
    6: "S6_ORB_Retest",
    7: "S7_Repeat_Gainers",
}
NAMES = {
    1: "News-Longer",
    2: "Quick-Shorter",
    3: "Fall-Buyer",
    4: "Premarket-Buyer",
    5: "Earning-Staker",
    6: "ORB-Retest",
    7: "Repeat-Gainers",
}

# Every strategy sheet keeps Ticker in column D and entry_time_et in column F.
# That lets the same Day_end_price formula work across all strategy sheets.
CLOSED_NEWS_HEADERS = [
    "recorded_at_et", "strategy", "trade_id", "Ticker", "side",
    "entry_time_et", "entry_price", "exit_time_et", "exit_price",
    "quantity", "invested_dkk", "pnl_dkk", "pnl_pct", "signal_time_et",
    "signal_price", "news_decision", "news_hint", "exit_reason", "meta",
    "Day_end_price",
]
S2_HEADERS = [
    "recorded_at_et", "strategy", "trade_id", "Ticker", "side",
    "entry_time_et", "entry_price", "quantity", "invested_dkk",
    "first_alert_time_et", "first_alert_price", "second_alert_time_et",
    "second_alert_price", "minutes_between_alerts", "news_decision",
    "news_hint", "reason", "meta", "Day_end_price",
]
S4_HEADERS = [
    "recorded_at_et", "strategy", "trade_id", "Ticker", "side",
    "entry_time_et", "entry_price", "quantity", "invested_dkk",
    "premarket_rank", "premarket_change_pct", "premarket_price",
    "previous_close", "source", "reason", "meta", "Day_end_price",
]
S5_HEADERS = [
    "recorded_at_et", "strategy", "trade_id", "Ticker", "side",
    "entry_time_et", "entry_price", "quantity", "invested_dkk",
    "event_date", "event_type", "source_url", "reason", "meta",
    "Day_end_price",
]
S6_HEADERS = [
    "recorded_at_et", "strategy", "trade_id", "Ticker", "side",
    "entry_time_et", "entry_price", "exit_time_et", "exit_price",
    "quantity", "invested_dkk", "pnl_dkk", "pnl_pct", "stop_price",
    "target_price", "opening_range_high", "opening_range_low",
    "second_15m_high", "box_height", "stop_distance", "exit_reason",
    "meta", "Day_end_price",
]
S7_HEADERS = [
    "recorded_at_et", "strategy", "trade_id", "Ticker", "side",
    "entry_time_et", "entry_price", "quantity", "invested_dkk",
    "rank_at_entry", "appearances_60m", "reason", "meta", "Day_end_price",
]
STRATEGY_HEADERS = {
    1: CLOSED_NEWS_HEADERS,
    2: S2_HEADERS,
    3: CLOSED_NEWS_HEADERS,
    4: S4_HEADERS,
    5: S5_HEADERS,
    6: S6_HEADERS,
    7: S7_HEADERS,
}
SYSTEM_HEADERS = [
    "timestamp_et", "level", "stage", "Ticker", "message", "meta", "Day_end_price"
]
AI_HEADERS = [
    "timestamp_et", "kind", "Ticker", "decision", "text", "input_tokens",
    "output_tokens", "total_tokens", "cost", "error", "sources", "Day_end_price"
]
RANK_HEADERS = [
    "timestamp_et", "period", "rank", "Ticker", "change_pct", "price",
    "baseline", "reason", "Day_end_price"
]

NO_EXIT_STRATEGIES = {2, 4, 5, 7}
CLOSED_STRATEGIES = {1, 3, 6}

# No K08 change is required. These are deliberately code-level rules.
S4_SCAN_MINUTE_ET = 9 * 60 + 10
S5_SCAN_MINUTE_ET = 9 * 60 + 10
MARKET_OPEN_MINUTE_ET = 9 * 60 + 30
S6_FIRST_END = 9 * 60 + 45
S6_SECOND_END = 10 * 60
S6_STOP_BOX_FRACTION = 0.25
S6_MIN_FIRST_BARS = 10
S6_MIN_SECOND_BARS = 10
S6_MIN_RANGE_PCT = 0.10
MAX_AI_CALLS_HARD_CAP = 300
AI_WORKERS = 4
OPENROUTER_RPM = 18
MAX_STALE_5M_LAG_MINUTES = 2.25
AI_SIGNAL_MAX_TOKENS = 512

# Yahoo Finance is the primary news source for S1/S2/S3 and ranking reasons.
# These are code-level defaults so no K08/secret changes are required.
YAHOO_NEWS_COUNT = 10
YAHOO_CACHE_SECONDS = 90
YAHOO_RPM = 24
YAHOO_TIMEOUT_SECONDS = 8.0
AI_HEADLINE_MAX_TOKENS = 220

# Article extraction is only used after the AI headline classifier fails.
ARTICLE_CONNECT_TIMEOUT = 4.0
ARTICLE_READ_TIMEOUT = 6.0
ARTICLE_DOWNLOAD_SECONDS = 15.0  # checked between streamed chunks/redirects
ARTICLE_MAX_BYTES = 5 * 1024 * 1024  # oversized pages fail; never score truncated HTML
ARTICLE_MAX_REDIRECTS = 5
ARTICLE_CACHE_SECONDS = 300
ARTICLE_FAILURE_CACHE_SECONDS = 30
ARTICLE_CACHE_ENTRIES = 128
ARTICLE_MIN_WORDS = 60  # reject empty pages, short snippets, and many consent screens
ARTICLE_MAX_ITEMS = 6  # same six news items supplied to the AI
ARTICLE_SENTIMENT_CHUNK_WORDS = 150

# Local deterministic fallback for extracted article-body classification.
# VADER is used because it is fast, offline, and deterministic; we then add
# finance-specific catalyst phrase weights so obvious positive/negative corporate
# news is handled better than generic sentiment alone. FinBERT is deliberately
# not used. Scoring is offline; downloading article HTML requires internet access.
FALLBACK_POS_THRESHOLD = 2.0
FALLBACK_NEG_THRESHOLD = -2.0
FALLBACK_VADER_WEIGHT = 1.25

FIN_POSITIVE_PHRASES = {
    "fda approval": 4.5,
    "fda approves": 4.5,
    "receives fda approval": 4.5,
    "granted accelerated approval": 4.0,
    "granted breakthrough therapy": 3.5,
    "priority review": 2.5,
    "meets primary endpoint": 4.0,
    "met primary endpoint": 4.0,
    "positive phase 2": 3.5,
    "positive phase 3": 4.0,
    "positive topline": 3.5,
    "trial success": 3.5,
    "beats estimates": 3.0,
    "beats expectations": 3.0,
    "beats consensus": 3.0,
    "earnings beat": 3.0,
    "revenue beat": 2.5,
    "raises guidance": 3.0,
    "raised guidance": 3.0,
    "increases guidance": 3.0,
    "increased guidance": 3.0,
    "record revenue": 2.5,
    "record sales": 2.5,
    "record profit": 3.0,
    "turns profitable": 2.5,
    "contract awarded": 2.5,
    "wins contract": 2.5,
    "awarded contract": 2.5,
    "strategic partnership": 2.0,
    "major partnership": 2.0,
    "acquisition offer": 3.0,
    "to be acquired": 3.0,
    "buyout offer": 3.0,
    "share repurchase": 2.5,
    "stock buyback": 2.5,
    "increases dividend": 2.0,
    "raises dividend": 2.0,
    "analyst upgrade": 2.0,
    "upgraded to buy": 2.5,
    "upgraded to outperform": 2.5,
    "patent granted": 2.0,
    "receives patent": 2.0,
    "regulatory approval": 3.0,
    "approval granted": 3.0,
}

FIN_NEGATIVE_PHRASES = {
    "fda rejects": -4.5,
    "fda rejected": -4.5,
    "complete response letter": -4.5,
    "clinical hold": -4.0,
    "fails primary endpoint": -4.0,
    "failed primary endpoint": -4.0,
    "misses primary endpoint": -4.0,
    "trial failure": -4.0,
    "misses estimates": -3.0,
    "misses expectations": -3.0,
    "misses consensus": -3.0,
    "earnings miss": -3.0,
    "revenue miss": -2.5,
    "lowers guidance": -3.0,
    "lowered guidance": -3.0,
    "cuts guidance": -3.0,
    "cut guidance": -3.0,
    "withdraws guidance": -3.0,
    "registered direct offering": -3.5,
    "public offering": -3.0,
    "secondary offering": -2.5,
    "at-the-market offering": -2.5,
    "dilutive offering": -4.0,
    "share dilution": -4.0,
    "files for bankruptcy": -5.0,
    "bankruptcy filing": -5.0,
    "defaults on": -4.0,
    "debt default": -4.0,
    "delisting notice": -3.5,
    "to be delisted": -4.0,
    "sec investigation": -3.0,
    "doj investigation": -3.0,
    "fraud investigation": -4.0,
    "product recall": -3.0,
    "recalls product": -3.0,
    "data breach": -2.5,
    "ceo resigns": -2.0,
    "chief executive resigns": -2.0,
    "analyst downgrade": -2.0,
    "downgraded to sell": -2.5,
    "downgraded to underperform": -2.5,
    "terminates trial": -3.5,
    "halts trial": -3.5,
    "suspends trial": -3.0,
}


def _need(k: str) -> str:
    v = os.getenv(k, "").strip()
    if not v:
        raise RuntimeError(f"missing environment variable {k}")
    return v


def _et(x: Optional[datetime]) -> str:
    if not x:
        return ""
    if x.tzinfo is None:
        x = x.replace(tzinfo=UTC)
    return x.astimezone(NY).isoformat()


def _j(x: Any) -> str:
    try:
        return json.dumps(x, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return str(x)


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    return str(x).lower() in {"1", "true", "yes", "on", "y"}


def _float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _formula_day_end() -> str:
    # D = Ticker, F = entry_time_et on every strategy sheet.
    # During the session GOOGLEFINANCE(...,"close",today) generally has no EOD row,
    # so the formula falls back to the delayed current price. After EOD data is
    # published, the historical close becomes the stable Day_end_price.
    return (
        '=IFERROR(INDEX(GOOGLEFINANCE(INDIRECT("D"&ROW()),"close",'
        'DATEVALUE(LEFT(INDIRECT("F"&ROW()),10)),'
        'DATEVALUE(LEFT(INDIRECT("F"&ROW()),10))+1),2,2),'
        'IFERROR(GOOGLEFINANCE(INDIRECT("D"&ROW()),"price"),""))'
    )


class Cfg:
    def __init__(self, raw: dict[str, Any]):
        self.r = raw
        required = [
            "sa", "wl", "sp", "sv", "zs", "bc", "wm", "bb", "bp", "mr",
            "mp", "bd", "pf", "cd", "tr", "fx", "fl", "mx", "gsr", "ge",
            "gm", "gr", "gc", "gt", "gw", "gwe", "br", "bh", "bm", "rk",
            "rn", "rr", "rb", "rbb", "rbp", "s", "n", "nr", "nb", "ne",
            "s2w", "s3r", "s4m", "s5m", "s6t", "s6b", "s6p", "s6r", "hi",
            "rows",
        ]
        miss = [k for k in required if k not in raw]
        if miss:
            raise RuntimeError("K08 missing config keys: " + ",".join(miss))
        if len(raw["s"]) != 7 or len(raw["n"]) != 7:
            raise RuntimeError("K08 keys 's' and 'n' must each contain 7 values")
        if len(raw["tr"]) != 4:
            raise RuntimeError("K08 key 'tr' must contain 4 trailing-stop values")
        if int(raw["hi"]) != 15:
            raise RuntimeError("This build expects hi=15 for 15-minute ranking cycles")

    def __getitem__(self, k):
        return self.r[k]

    def get(self, k, default=None):
        return self.r.get(k, default)

    def enabled(self, i: int) -> bool:
        return _bool(self.r["s"][i - 1])

    def notify(self, i: int) -> bool:
        return _bool(self.r["n"][i - 1])


@dataclass
class M1:
    s: str
    t: datetime
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class M5:
    s: str
    start: datetime
    end: datetime
    o: float
    h: float
    l: float
    c: float
    v: float

    @property
    def rng(self):
        return self.h - self.l


class Agg5:
    def __init__(self):
        self.x: dict[str, list[Any]] = {}

    def push(self, b: M1) -> Optional[M5]:
        t = b.t.astimezone(UTC)
        st = t.replace(minute=(t.minute // 5) * 5, second=0, microsecond=0)
        w = self.x.get(b.s)
        if w is None:
            self.x[b.s] = [st, b.o, b.h, b.l, b.c, b.v]
            return None
        if w[0] == st:
            w[2] = max(w[2], b.h)
            w[3] = min(w[3], b.l)
            w[4] = b.c
            w[5] += b.v
            return None
        z = M5(b.s, w[0], w[0] + timedelta(minutes=5), w[1], w[2], w[3], w[4], w[5])
        self.x[b.s] = [st, b.o, b.h, b.l, b.c, b.v]
        return z


class Roll:
    def __init__(self, n: int):
        self.n = n
        self.q: dict[str, deque[tuple[float, float]]] = defaultdict(lambda: deque(maxlen=n))
        self.sr = defaultdict(float)
        self.sr2 = defaultdict(float)
        self.sv = defaultdict(float)
        self.sv2 = defaultdict(float)

    def add(self, s: str, r: float, v: float):
        q = self.q[s]
        if len(q) == self.n:
            a, b = q[0]
            self.sr[s] -= a
            self.sr2[s] -= a * a
            self.sv[s] -= b
            self.sv2[s] -= b * b
        q.append((r, v))
        self.sr[s] += r
        self.sr2[s] += r * r
        self.sv[s] += v
        self.sv2[s] += v * v

    def stats(self, s: str):
        q = self.q[s]
        n = len(q)
        if n < 2:
            return None
        mr = self.sr[s] / n
        mv = self.sv[s] / n
        vr = max(0.0, (self.sr2[s] - n * mr * mr) / (n - 1))
        vv = max(0.0, (self.sv2[s] - n * mv * mv) / (n - 1))
        return mr, math.sqrt(vr), mv, math.sqrt(vv), n


@dataclass
class Sig:
    id: str
    s: str
    direction: str
    t: datetime
    p: float
    zr: float
    zv: float
    vol: float
    px: list[float] = field(default_factory=list)
    mt: list[datetime] = field(default_factory=list)
    status: str = "WATCH"
    cont: float = 0.0
    confirm_p: float = 0.0
    news: str = ""
    hint: str = ""
    sources: list[str] = field(default_factory=list)


@dataclass
class AIJob:
    kind: str
    sig: Sig
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class Pos:
    id: str
    st: int
    s: str
    side: str
    t: datetime
    p: float
    q: int
    dkk: float
    stop: float = 0.0
    target: float = 0.0
    peak: float = 0.0
    trough: float = 0.0
    sig: Optional[Sig] = None
    meta: dict[str, Any] = field(default_factory=dict)


class TG:
    def __init__(self, tok: str, chat: str):
        import requests
        self.rq = requests.Session()
        self.u = f"https://api.telegram.org/bot{tok}/sendMessage"
        self.c = chat

    def send(self, text: str):
        try:
            r = self.rq.post(self.u, json={"chat_id": self.c, "text": text}, timeout=10)
            r.raise_for_status()
        except Exception as ex:
            L.error("Telegram: %s", ex)


class GS:
    def __init__(self, sa: str, sid: str, cfg: Cfg):
        import gspread
        from google.oauth2.service_account import Credentials

        cr = Credentials.from_service_account_info(
            json.loads(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        self.gspread = gspread
        self.book = gspread.authorize(cr).open_by_key(sid)
        self.cfg = cfg
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.q: dict[str, deque[list[Any]]] = defaultdict(deque)
        self.ws = {}
        self.headers = {SHEETS[i]: STRATEGY_HEADERS[i] for i in range(1, 8)}
        self.headers.update({
            "System_Log": SYSTEM_HEADERS,
            "AI_Log": AI_HEADERS,
            "Rankings": RANK_HEADERS,
        })
        for name, headers in self.headers.items():
            self.ws[name] = self._sheet(name, headers)
        threading.Thread(target=self._loop, daemon=True).start()

    def _archive_name(self, name: str) -> str:
        stamp = datetime.now(NY).strftime("%Y%m%d_%H%M%S")
        return (f"{name}_archive_{stamp}")[:95]

    def _sheet(self, name, headers):
        try:
            w = self.book.worksheet(name)
        except self.gspread.WorksheetNotFound:
            w = self.book.add_worksheet(title=name, rows=int(self.cfg["rows"]), cols=len(headers))
            w.append_row(headers, value_input_option="USER_ENTERED")
            return w

        if _bool(self.cfg["gsr"]):
            w.clear()
            w.resize(rows=int(self.cfg["rows"]), cols=len(headers))
            w.update([headers], "A1", value_input_option="USER_ENTERED")
            return w

        vals = w.row_values(1)
        if vals != headers:
            # Schema migration without deleting the prior research run.
            old_name = self._archive_name(name)
            try:
                w.update_title(old_name)
                L.warning("Archived incompatible sheet %s as %s", name, old_name)
            except Exception as ex:
                raise RuntimeError(
                    f"Sheet {name} has incompatible headers and could not be archived: {ex}"
                ) from ex
            w = self.book.add_worksheet(title=name, rows=int(self.cfg["rows"]), cols=len(headers))
            w.append_row(headers, value_input_option="USER_ENTERED")
            return w

        try:
            w.resize(cols=len(headers))
        except Exception:
            pass
        return w

    def put(self, name: str, row: list[Any], day_end_formula: bool = False):
        h = self.headers[name]
        row = list(row[:len(h)]) + [""] * max(0, len(h) - len(row))
        if day_end_formula and "Day_end_price" in h:
            row[h.index("Day_end_price")] = _formula_day_end()
        with self.lock:
            self.q[name].append(row)

    def _loop(self):
        while not self.stop.wait(float(self.cfg["fl"])):
            self.flush()

    def flush(self):
        packs = {}
        mx = int(self.cfg["mx"])
        with self.lock:
            for k, v in self.q.items():
                if v:
                    packs[k] = [v.popleft() for _ in range(min(mx, len(v)))]
        for name, rows in packs.items():
            try:
                self.ws[name].append_rows(rows, value_input_option="USER_ENTERED")
            except Exception as ex:
                L.error("Google flush %s: %s", name, ex)
                with self.lock:
                    for r in reversed(rows):
                        self.q[name].appendleft(r)

    def close(self):
        self.stop.set()
        self.flush()


class AI:
    def __init__(self, key: str, cfg: Cfg, gs: GS):
        import requests
        self.rq = requests.Session()
        self._requests = requests
        self._thread_local = threading.local()
        self.k = key
        self.cfg = cfg
        self.gs = gs
        self.cache: dict[str, tuple[datetime, Any]] = {}
        self.lock = threading.Lock()
        self.off_until = datetime.min.replace(tzinfo=UTC)
        self.auth_bad = False
        self.call_day = datetime.now(NY).date()
        configured = int(cfg.get("ec", MAX_AI_CALLS_HARD_CAP) or MAX_AI_CALLS_HARD_CAP)
        self.daily_limit = max(1, min(configured, MAX_AI_CALLS_HARD_CAP))
        # calls counts logical AI decisions, not HTTP retry attempts.
        self.calls = 0
        # Global limiter shared by all AI workers. openrouter/free is rate-limited,
        # so keep a little headroom instead of hammering the endpoint.
        self.rate_lock = threading.Lock()
        self.request_times = deque()

        # Yahoo news is intentionally separate from OpenRouter. S1/S2/S3 first
        # retrieve ticker-specific Yahoo Finance headlines and only use the LLM
        # for cheap text classification when Yahoo actually has recent news.
        self.yahoo_cache: dict[str, tuple[datetime, list[dict[str, Any]]]] = {}
        self.yahoo_rate_lock = threading.Lock()
        self.yahoo_request_times = deque()

        self.article_cache = OrderedDict()
        self.article_cache_lock = threading.Lock()

        # Offline sentiment fallback. vaderSentiment ships its lexicon inside the
        # package, so there is no model download and no runtime network dependency.
        # If the local model cannot load, the article fallback reports ERROR instead
        # of silently treating missing sentiment as neutral evidence.
        self.sentiment = None
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            self.sentiment = SentimentIntensityAnalyzer()
            self.sentiment.lexicon.update({
                "approved": 2.6, "approval": 2.1, "breakthrough": 2.6,
                "profitable": 2.3, "profitability": 2.1, "buyback": 2.7,
                "repurchase": 2.2, "outperform": 2.1, "upgrade": 1.9,
                "upgraded": 2.0, "beats": 2.2, "beat": 1.9,
                "awarded": 1.9, "partnership": 1.6, "patent": 1.3,
                "rejected": -2.9, "downgrade": -2.1, "downgraded": -2.2,
                "dilution": -3.2, "dilutive": -3.2, "bankruptcy": -4.0,
                "default": -3.0, "delisting": -3.0, "recall": -2.5,
                "fraud": -3.8, "lawsuit": -1.8, "investigation": -2.0,
            })
        except Exception:
            self.sentiment = None

    def _log(self, kind, symbol, decision, text, data=None, err=""):
        u = (data or {}).get("usage", {}) or {}
        src = []
        try:
            anns = (data or {}).get("choices", [{}])[0].get("message", {}).get("annotations", []) or []
            for a in anns:
                x = a.get("url_citation", {}) or {}
                if x.get("url") and x["url"] not in src:
                    src.append(x["url"])
        except Exception:
            pass
        self.gs.put("AI_Log", [
            _et(datetime.now(UTC)), kind, symbol, decision, text,
            u.get("prompt_tokens", u.get("input_tokens", "")),
            u.get("completion_tokens", u.get("output_tokens", "")),
            u.get("total_tokens", ""), u.get("cost", ""), err, "|".join(src), "",
        ])
        return src

    def _session(self):
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = self._requests.Session()
            self._thread_local.session = sess
        return sess

    def _claim_logical_call(self) -> tuple[bool, str]:
        with self.lock:
            td = datetime.now(NY).date()
            if td != self.call_day:
                self.call_day = td
                self.calls = 0
            if self.calls >= self.daily_limit:
                return False, f"AI_DAILY_LIMIT_{self.daily_limit}"
            self.calls += 1
            return True, ""

    def _wait_rate_slot(self):
        while True:
            wait = 0.0
            now = time.monotonic()
            with self.rate_lock:
                while self.request_times and now - self.request_times[0] >= 60.0:
                    self.request_times.popleft()
                if len(self.request_times) < OPENROUTER_RPM:
                    self.request_times.append(now)
                    return
                wait = max(0.05, 60.0 - (now - self.request_times[0]) + 0.05)
            time.sleep(wait)

    @staticmethod
    def _message_text(msg: dict[str, Any]) -> str:
        content = msg.get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for x in content:
                if isinstance(x, dict):
                    t = x.get("text")
                    if isinstance(t, str):
                        parts.append(t)
            return "\n".join(parts).strip()
        return ""

    def preflight(self) -> tuple[bool, str]:
        if not _bool(self.cfg["ge"]):
            return False, "AI_DISABLED"
        if not self.k:
            self.auth_bad = True
            self._log("PREFLIGHT", "", "ERROR", "", None, "K07_EMPTY")
            return False, "K07_EMPTY"
        hdr = {"Authorization": f"Bearer {self.k}"}
        try:
            r = self.rq.get("https://openrouter.ai/api/v1/key", headers=hdr, timeout=float(self.cfg["gt"]))
            if r.status_code == 200:
                self.auth_bad = False
                self._log("PREFLIGHT", "", "OK", "OpenRouter key accepted", None, "")
                return True, ""
            err = f"HTTP {r.status_code}: {r.text[:240]}"
            self.auth_bad = r.status_code in {401, 403}
            self._log("PREFLIGHT", "", "ERROR", "", None, err)
            return False, err
        except Exception as ex:
            err = f"{type(ex).__name__}: {ex}"
            self._log("PREFLIGHT", "", "ERROR", "", None, err)
            return False, err

    def _call(self, kind, symbol, messages, max_tokens, web=True, response_format=None):
        """Call OpenRouter with resilient web grounding.

        OpenRouter's legacy ``plugins:[{"id":"web"}]`` path is deprecated.
        Use the server-side ``openrouter:web_search`` tool instead and keep the
        search engine fixed to Exa so the behavior is consistent even when
        ``openrouter/free`` randomly selects different free models.
        """
        if not _bool(self.cfg["ge"]) or not self.k:
            return None, "AI_DISABLED", []
        if self.auth_bad:
            return None, "AI_AUTH_FAILED", []
        if datetime.now(UTC) < self.off_until:
            return None, "AI_COOLDOWN", []

        claimed, claim_err = self._claim_logical_call()
        if not claimed:
            return None, claim_err, []

        body = {
            "model": self.cfg["gm"],
            "messages": messages,
            "temperature": 0,
            "max_tokens": int(max_tokens),
        }

        if response_format:
            body["response_format"] = response_format
            body["provider"] = {"require_parameters": True}

        if web:
            mr = max(1, min(int(self.cfg["gr"]), 3))
            body["tools"] = [{
                "type": "openrouter:web_search",
                "parameters": {
                    "engine": "exa",
                    "max_results": mr,
                    "max_total_results": mr,
                    "max_characters": 2200,
                },
            }]

        hdr = {
            "Authorization": f"Bearer {self.k}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/",
            "X-Title": "research-runner",
        }

        last = ""
        for i in range(2):
            try:
                self._wait_rate_slot()
                r = self._session().post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=hdr,
                    json=body,
                    timeout=float(self.cfg["gt"]),
                )

                if r.status_code in {401, 403}:
                    self.auth_bad = True
                    last = f"HTTP {r.status_code}: {r.text[:500]}"
                    self._log(kind, symbol, "ERROR", "", None, last)
                    return None, last, []

                if r.status_code == 429:
                    last = f"HTTP 429: 
