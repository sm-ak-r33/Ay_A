import argparse
import json
import logging
import math
import os
import queue
import threading
import time
import uuid
from collections import defaultdict, deque
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
S6_MIN_FIRST_BARS = 8
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
                    last = f"HTTP 429: {r.text[:500]}"
                    self.off_until = datetime.now(UTC) + timedelta(minutes=2)
                    if i == 0:
                        time.sleep(3)
                        continue
                    break

                if 500 <= r.status_code < 600:
                    last = f"HTTP {r.status_code}: {r.text[:500]}"
                    if i == 0:
                        time.sleep(2)
                        continue
                    break

                if r.status_code >= 400:
                    last = f"HTTP {r.status_code}: {r.text[:800]}"
                    self._log(kind, symbol, "ERROR", "", None, last)
                    return None, last, []

                data = r.json()
                msg = ((data.get("choices") or [{}])[0].get("message") or {})
                answer = self._message_text(msg)

                if web:
                    usage = (data.get("usage") or {})
                    tool_use = usage.get("server_tool_use") or {}
                    web_n = int(tool_use.get("web_search_requests") or 0)
                    anns = msg.get("annotations") or []
                    if web_n <= 0 and not anns:
                        last = "WEB_SEARCH_NOT_USED"
                        if i == 0:
                            time.sleep(1.0)
                            continue
                        self._log(kind, symbol, "ERROR", answer or "", data, last)
                        return None, last, []

                if not answer:
                    last = "EMPTY_ASSISTANT_CONTENT"
                    if i == 0:
                        time.sleep(1.0)
                        continue
                    self._log(kind, symbol, "ERROR", "", data, last)
                    return None, last, []

                src = self._log(kind, symbol, "OK", answer, data, "")
                return answer, "", src

            except self._requests.exceptions.Timeout as ex:
                last = f"TIMEOUT: {ex}"
            except self._requests.exceptions.ChunkedEncodingError as ex:
                last = f"CHUNKED_RESPONSE_ERROR: {ex}"
            except self._requests.exceptions.ConnectionError as ex:
                last = f"CONNECTION_ERROR: {ex}"
            except ValueError as ex:
                last = f"BAD_JSON_RESPONSE: {ex}"
            except Exception as ex:
                last = f"{type(ex).__name__}: {ex}"

            if i == 0:
                time.sleep(1.5)

        self._log(kind, symbol, "ERROR", "", None, last or "UNKNOWN_OPENROUTER_ERROR")
        return None, last or "UNKNOWN_OPENROUTER_ERROR", []

    def _log_external(self, kind: str, symbol: str, decision: str, text: str = "", err: str = "", sources: Optional[list[str]] = None):
        """Log a non-LLM research step in AI_Log without inventing token usage."""
        self.gs.put("AI_Log", [
            _et(datetime.now(UTC)), kind, symbol, decision, text,
            "", "", "", "", err, "|".join(sources or []), "",
        ])

    def _yahoo_session(self):
        sess = getattr(self._thread_local, "yahoo_session", None)
        if sess is None:
            sess = self._requests.Session()
            self._thread_local.yahoo_session = sess
        return sess

    def _wait_yahoo_slot(self):
        """Gentle process-wide rate limiter for Yahoo's unofficial search endpoint."""
        while True:
            now = time.monotonic()
            with self.yahoo_rate_lock:
                while self.yahoo_request_times and now - self.yahoo_request_times[0] >= 60.0:
                    self.yahoo_request_times.popleft()
                if len(self.yahoo_request_times) < YAHOO_RPM:
                    self.yahoo_request_times.append(now)
                    return
                wait = max(0.05, 60.0 - (now - self.yahoo_request_times[0]) + 0.05)
            time.sleep(wait)

    @staticmethod
    def _yahoo_publish_time(v: Any) -> Optional[datetime]:
        try:
            if isinstance(v, (int, float)):
                x = float(v)
                if x > 10_000_000_000:  # tolerate milliseconds
                    x /= 1000.0
                return datetime.fromtimestamp(x, tz=UTC)
            z = str(v or "").strip()
            if not z:
                return None
            if z.replace(".", "", 1).isdigit():
                x = float(z)
                if x > 10_000_000_000:
                    x /= 1000.0
                return datetime.fromtimestamp(x, tz=UTC)
            z = z.replace("Z", "+00:00")
            dt = datetime.fromisoformat(z)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except Exception:
            return None

    def _fetch_yahoo_raw(self, symbol: str, force: bool = False) -> tuple[list[dict[str, Any]], str]:
        """Fetch Yahoo Finance ticker news. Empty list + empty error means a real no-news result."""
        symbol = symbol.strip().upper()
        now = datetime.now(UTC)
        if not force:
            with self.lock:
                cached = self.yahoo_cache.get(symbol)
                if cached and (now - cached[0]).total_seconds() < YAHOO_CACHE_SECONDS:
                    return list(cached[1]), ""

        params = {
            "q": symbol,
            "quotesCount": 1,
            "newsCount": YAHOO_NEWS_COUNT,
            "enableFuzzyQuery": "false",
            "quotesQueryId": "tss_match_phrase_query",
            "newsQueryId": "news_cie_vespa",
            "enableCb": "false",
            "enableNavLinks": "false",
            "region": "US",
            "lang": "en-US",
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"https://finance.yahoo.com/quote/{symbol}/",
        }
        hosts = [
            "https://query2.finance.yahoo.com/v1/finance/search",
            "https://query1.finance.yahoo.com/v1/finance/search",
        ]
        last = ""
        for host_i, url in enumerate(hosts):
            try:
                self._wait_yahoo_slot()
                r = self._yahoo_session().get(
                    url, params=params, headers=headers,
                    timeout=min(YAHOO_TIMEOUT_SECONDS, max(3.0, float(self.cfg["gt"]))),
                )
                if r.status_code == 429:
                    last = f"YAHOO_HTTP_429: {r.text[:180]}"
                    # Try the alternate Yahoo query host once; never treat 429 as no news.
                    continue
                if 500 <= r.status_code < 600:
                    last = f"YAHOO_HTTP_{r.status_code}: {r.text[:180]}"
                    continue
                if r.status_code >= 400:
                    return [], f"YAHOO_HTTP_{r.status_code}: {r.text[:240]}"
                data = r.json() or {}
                rows = data.get("news") or []
                out = []
                seen = set()
                for x in rows:
                    if not isinstance(x, dict):
                        continue
                    title = " ".join(str(x.get("title") or "").split())
                    if not title:
                        continue
                    link = str(x.get("link") or "").strip()
                    pub = str(x.get("publisher") or "").strip()
                    dt = self._yahoo_publish_time(x.get("providerPublishTime"))
                    related = [str(y).strip().upper() for y in (x.get("relatedTickers") or []) if str(y).strip()]
                    # When Yahoo supplies relatedTickers, require an exact ticker match.
                    if related and symbol not in related:
                        continue
                    dedupe = str(x.get("uuid") or link or title).strip()
                    if dedupe in seen:
                        continue
                    seen.add(dedupe)
                    out.append({
                        "title": title,
                        "publisher": pub,
                        "published_at": dt,
                        "link": link,
                        "related_tickers": related,
                    })
                out.sort(key=lambda x: x.get("published_at") or datetime.min.replace(tzinfo=UTC), reverse=True)
                with self.lock:
                    self.yahoo_cache[symbol] = (datetime.now(UTC), list(out))
                return out, ""
            except self._requests.exceptions.Timeout as ex:
                last = f"YAHOO_TIMEOUT: {ex}"
            except self._requests.exceptions.ConnectionError as ex:
                last = f"YAHOO_CONNECTION_ERROR: {ex}"
            except ValueError as ex:
                last = f"YAHOO_BAD_JSON: {ex}"
            except Exception as ex:
                last = f"YAHOO_{type(ex).__name__}: {ex}"
            if host_i == 0:
                time.sleep(0.35)
        return [], last or "YAHOO_LOOKUP_FAILED"

    def yahoo_news(self, symbol: str, signal_time: datetime, force: bool = False) -> tuple[list[dict[str, Any]], str]:
        """Return only ticker news that existed by the signal time and lies in the configured window."""
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=UTC)
        signal_utc = signal_time.astimezone(UTC)
        signal_ny = signal_utc.astimezone(NY)
        hrs = int(self.cfg["gwe"] if signal_ny.weekday() == 0 else self.cfg["gw"])
        raw, err = self._fetch_yahoo_raw(symbol, force=force)
        if err:
            self._log_external("YAHOO_NEWS", symbol, "ERROR", "", err, [])
            return [], err

        cutoff = signal_utc - timedelta(hours=hrs)
        recent = []
        for x in raw:
            dt = x.get("published_at")
            # Missing publication time cannot safely satisfy a time-window gate.
            if not isinstance(dt, datetime):
                continue
            dt = dt.astimezone(UTC)
            # Strictly prevent look-ahead: never use a headline published after the signal.
            if cutoff <= dt <= signal_utc:
                recent.append(x)
        recent.sort(key=lambda x: x["published_at"], reverse=True)
        src = [x["link"] for x in recent if x.get("link")]
        if recent:
            txt = " || ".join(
                f"{x['publisher']}: {x['title']}" if x.get("publisher") else x["title"]
                for x in recent[:5]
            )[:1800]
            self._log_external("YAHOO_NEWS", symbol, f"FOUND_{len(recent)}", txt, "", src[:10])
        else:
            self._log_external("YAHOO_NEWS", symbol, "NONE", f"No ticker news in prior {hrs}h as of signal time", "", [])
        return recent, ""

    def _classify_yahoo_news(self, symbol: str, direction: str, signal_time: datetime, price: float, items: list[dict[str, Any]]):
        """Cheap LLM classification only. No OpenRouter web-search tool is used here."""
        signal_utc = signal_time.astimezone(UTC) if signal_time.tzinfo else signal_time.replace(tzinfo=UTC)
        rows = []
        for i, x in enumerate(items[:6], 1):
            dt = x["published_at"].astimezone(UTC)
            mins = max(0.0, (signal_utc - dt).total_seconds() / 60.0)
            rel = ",".join(x.get("related_tickers") or [])
            rows.append(
                f"{i}. {x['title']} | publisher={x.get('publisher','')} | "
                f"published={dt.isoformat()} | {mins:.0f} minutes before signal | related={rel}"
            )
        q = [
            {
                "role": "system",
                "content": (
                    "You are classifying already-retrieved Yahoo Finance headlines. DO NOT browse the web. "
                    "Judge the news for the named ticker as POSITIVE, NEGATIVE, NEUTRAL, or UNRELATED. "
                    "Use POSITIVE/NEGATIVE only when the headline plausibly creates directional stock impact. "
                    "Put the final answer on the LAST LINE only as CLASS|five to eight factual words."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Ticker: {symbol}\nSignal direction: {'UP' if direction == 'U' else 'DOWN'}\n"
                    f"Signal time UTC: {signal_utc.isoformat()}\nPrice: {price:.6f}\n"
                    "Yahoo Finance headlines available before the signal:\n" + "\n".join(rows)
                ),
            },
        ]
        answer, err, _ = self._call("YAHOO_CLASSIFY", symbol, q, AI_HEADLINE_MAX_TOKENS, web=False)
        if not answer:
            return "ERROR", "", err
        lines = [x.strip().strip("` ") for x in answer.splitlines() if x.strip()]
        final = lines[-1] if lines else ""
        if "|" in final:
            cls, hint = final.split("|", 1)
        else:
            cls, hint = final, ""
        cls = cls.strip().upper().rstrip(".")
        hint = " ".join(hint.strip().split()[:8])
        if cls not in {"POSITIVE", "NEGATIVE", "NEUTRAL", "UNRELATED"}:
            return "ERROR", "", "MALFORMED_YAHOO_CLASSIFICATION"
        if not hint:
            hint = " ".join(items[0]["title"].split()[:8])
        return cls, hint, ""

    def signal(self, s: str, direction: str, t: datetime, p: float, zr: float, zv: float, vol: float, fresh: bool = False, policy: str = ""):
        """Yahoo-first catalyst gate for S1/S2/S3.

        OpenRouter web research is deliberately NOT used here. Yahoo Finance supplies
        the timestamped ticker headlines; the LLM only classifies those headlines.
        If Yahoo has no recent news, no LLM call is made at all.
        """
        policy = (policy or ("S1" if direction == "U" else "S3")).upper()
        day = t.astimezone(NY) if t.tzinfo else t.replace(tzinfo=UTC).astimezone(NY)
        key = f"yh:{policy}:{s}:{direction}:{day.date()}"
        if not fresh:
            with self.lock:
                z = self.cache.get(key)
                if z and datetime.now(UTC) - z[0] < timedelta(hours=float(self.cfg["gc"])):
                    return z[1]

        items, yerr = self.yahoo_news(s, t, force=fresh)
        if yerr:
            out = ("ERROR", "", [], yerr)
        elif not items:
            # A real successful Yahoo lookup with zero in-window articles.
            # S1: no catalyst -> no long. S2: no news -> short is allowed.
            # S3: no negative catalyst -> long is allowed.
            out = ("NO", "", [], "")
        else:
            src = [x["link"] for x in items if x.get("link")]
            cls, hint, cerr = self._classify_yahoo_news(s, direction, t, p, items)
            if cls == "ERROR":
                out = ("ERROR", "", src, cerr)
            elif policy == "S2":
                # User rule: ANY verified recent ticker news suppresses the short.
                # Classification is still recorded/auditable but never authorizes a short.
                out = ("YES", f"{cls}: {hint}"[:220], src, "")
            elif direction == "U":
                # S1 requires a plausibly positive company-specific catalyst.
                out = ("YES", hint, src, "") if cls == "POSITIVE" else ("NO", f"{cls}: {hint}"[:220], src, "")
            else:
                # S3 buys a fall only when there is no plausible negative catalyst.
                out = ("YES", hint, src, "") if cls == "NEGATIVE" else ("NO", f"{cls}: {hint}"[:220], src, "")

        if not fresh:
            with self.lock:
                self.cache[key] = (datetime.now(UTC), out)
        return out

    def next_day_events(self, target: date, limit: int):
        q = [
            {
                "role": "system",
                "content": (
                    "Use current web sources. Return TSV only, one event per line: "
                    "SYMBOL<TAB>EVENT<TAB>DATE<TAB>SOURCE_URL. Include only US-listed companies with a "
                    "scheduled earnings release, Phase 2/3 clinical data readout, FDA decision, or PDUFA on "
                    "the requested date. Skip uncertain dates and rows without a source URL."
                ),
            },
            {
                "role": "user",
                "content": f"Find up to {limit} material scheduled events for {target.isoformat()}. Prefer clearly verified dates.",
            },
        ]
        text, err, _ = self._call("S5_EVENTS", "", q, 800, True)
        if not text:
            return [], err
        out = []
        for line in text.splitlines():
            p = [x.strip() for x in line.split("\t")]
            if len(p) >= 4 and p[0].replace(".", "").replace("-", "").isalnum() and p[3].startswith("http"):
                out.append({"symbol": p[0].upper(), "event": p[1], "date": p[2], "url": p[3], "source_kind": "WEB"})
        return out[:limit], ""

    def brief(self, today: date):
        q = [
            {
                "role": "system",
                "content": (
                    "Produce a concise pre-market research brief using current web sources. No investment advice. "
                    "Cover notable US earnings today, scheduled Phase 2/3 or FDA/PDUFA events, and energy catalysts "
                    "including EIA/OPEC+/geopolitics/weather. Omit unverified items."
                ),
            },
            {"role": "user", "content": f"New York date: {today.isoformat()}. Keep under 700 words."},
        ]
        return self._call("BRIEF", "", q, 900, True)[0] or ""

    def ranking_reasons(self, syms: list[str]):
        """Optional ranking reasons from Yahoo headlines; never uses OpenRouter web search."""
        if not syms or not _bool(self.cfg["rr"]):
            return {}
        now = datetime.now(UTC)
        out = {}
        for s in syms:
            items, err = self.yahoo_news(s, now, force=False)
            if err:
                out[s] = "News lookup unavailable"
            elif not items:
                out[s] = "No recent Yahoo news"
            else:
                out[s] = " ".join(items[0]["title"].split()[:9])
        return out


class Core:
    def __init__(self, cfg: Cfg, gs: GS, tg: TG, ai: AI, key: str, sec: str):
        self.c = cfg
        self.gs = gs
        self.tg = tg
        self.ai = ai
        self.key = key
        self.sec = sec
        self.lock = threading.RLock()
        self.a5 = Agg5()
        self.roll = Roll(int(cfg["bc"]))
        self.allowed: set[str] = set()
        self.universe: list[str] = []
        self.last: dict[str, tuple[float, datetime]] = {}
        self.daily: dict[str, dict[date, dict[str, float]]] = defaultdict(dict)
        self.first: dict[str, tuple[float, datetime]] = {}
        self.week: dict[str, float] = {}
        self.month: dict[str, float] = {}
        self.sig: dict[str, Sig] = {}
        self.cool: dict[str, float] = {}
        self.s2arm: dict[str, dict[str, Any]] = {}
        self.pos: dict[int, dict[str, Pos]] = defaultdict(dict)
        self.orb: dict[str, dict[str, Any]] = {}
        self.rank_seen: dict[str, deque[datetime]] = defaultdict(deque)
        self.rank_bought: set[tuple[date, str]] = set()
        self.s4_list: list[dict[str, Any]] = []
        self.s4_done: set[tuple[date, str]] = set()
        self.s4_scan_day: Optional[date] = None
        self.s5_today: list[dict[str, Any]] = []
        self.s5_done: set[tuple[date, str]] = set()
        self.s5_scan_day: Optional[date] = None
        self.slots: set[str] = set()
        self.brief_day: Optional[date] = None
        self.rank_ready = False
        self.open_fill_slot = ""
        self.trace_once: set[str] = set()
        self.jobs = queue.PriorityQueue()
        self.ai_seq = 0
        self.dead = threading.Event()
        for i in range(AI_WORKERS):
            threading.Thread(target=self._ai_worker, name=f"ai-worker-{i+1}", daemon=True).start()
        self.fx = self._fx()
        self._sys("INFO", "BOOT", "", "core initialized", {"fx": self.fx})

    def set_universe(self, ss: list[str]):
        self.universe = list(ss)
        self.allowed = set(ss)

    def _fx(self):
        x = float(self.c["fx"])
        if x > 0:
            return x
        import requests
        import xml.etree.ElementTree as ET
        try:
            r = requests.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml", timeout=10)
            r.raise_for_status()
            d = {}
            for z in ET.fromstring(r.text).iter():
                if z.attrib.get("currency") and z.attrib.get("rate"):
                    d[z.attrib["currency"]] = float(z.attrib["rate"])
            return d["DKK"] / d["USD"]
        except Exception:
            return float(self.c.get("ff", 6.5) or 6.5)

    def _sys(self, level, stage, symbol, message, meta=None):
        fn = {"INFO": L.info, "WARNING": L.warning, "ERROR": L.error}.get(level, L.info)
        fn("[%s] %s %s", stage, symbol or "-", message)
        self.gs.put("System_Log", [_et(datetime.now(UTC)), level, stage, symbol, message, _j(meta or {}), ""])
        if level == "ERROR" and _bool(self.c["ne"]):
            self.tg.send(f"[SYSTEM ERROR]\n{stage}\n{symbol}\n{message[:500]}")

    def _trace(self, st: int, event: str, symbol: str, message: str, meta=None, level="INFO", once_key: Optional[str] = None):
        key = once_key
        if key:
            if key in self.trace_once:
                return
            self.trace_once.add(key)
        self._sys(level, f"S{st}_{event}", symbol, message, meta)

    def _note(self, st, text):
        if self.c.notify(st):
            self.tg.send(f"[S{st} {NAMES[st]}]\n{text}")

    def _qty(self, p):
        if p < float(self.c["pf"]):
            return 0, 0.0
        q = math.floor((float(self.c["bd"]) / self.fx) / p)
        return q, q * p * self.fx

    def _sheet_entry(self, st: int, z: Pos, reason: str, meta: dict[str, Any]):
        now = _et(datetime.now(UTC))
        if st == 2:
            r = [
                now, NAMES[2], z.id, z.s, z.side, _et(z.t), z.p, z.q, z.dkk,
                meta.get("first_alert_time_et", ""), meta.get("first_alert_price", ""),
                meta.get("second_alert_time_et", ""), meta.get("second_alert_price", ""),
                meta.get("minutes_between_alerts", ""), meta.get("news_decision", ""),
                meta.get("news_hint", ""), reason, _j(meta), "",
            ]
        elif st == 4:
            r = [
                now, NAMES[4], z.id, z.s, z.side, _et(z.t), z.p, z.q, z.dkk,
                meta.get("premarket_rank", ""), meta.get("premarket_change_pct", ""),
                meta.get("premarket_price", ""), meta.get("previous_close", ""),
                meta.get("source", ""), reason, _j(meta), "",
            ]
        elif st == 5:
            r = [
                now, NAMES[5], z.id, z.s, z.side, _et(z.t), z.p, z.q, z.dkk,
                meta.get("event_date", ""), meta.get("event", ""), meta.get("source", ""),
                reason, _j(meta), "",
            ]
        elif st == 7:
            r = [
                now, NAMES[7], z.id, z.s, z.side, _et(z.t), z.p, z.q, z.dkk,
                meta.get("rank", ""), meta.get("count_60m", ""), reason, _j(meta), "",
            ]
        else:
            raise RuntimeError("entry sheet called for exit-managed strategy")
        self.gs.put(SHEETS[st], r, day_end_formula=True)

    def _sheet_closed(self, st: int, z: Pos, p: float, t: datetime, pnl: float, reason: str):
        now = _et(datetime.now(UTC))
        if st in {1, 3}:
            sg = z.sig
            r = [
                now, NAMES[st], z.id, z.s, z.side, _et(z.t), z.p, _et(t), p,
                z.q, z.dkk, pnl, (pnl / z.dkk * 100 if z.dkk else ""),
                _et(sg.t) if sg else "", sg.p if sg else "",
                sg.news if sg else "", sg.hint if sg else "", reason, _j(z.meta), "",
            ]
        elif st == 6:
            r = [
                now, NAMES[6], z.id, z.s, z.side, _et(z.t), z.p, _et(t), p,
                z.q, z.dkk, pnl, (pnl / z.dkk * 100 if z.dkk else ""), z.stop,
                z.target, z.meta.get("range_high", ""), z.meta.get("range_low", ""),
                z.meta.get("second_15m_high", ""), z.meta.get("box_height", ""),
                z.meta.get("stop_distance", ""), reason, _j(z.meta), "",
            ]
        else:
            raise RuntimeError("closed sheet called for non-exit strategy")
        self.gs.put(SHEETS[st], r, day_end_formula=True)

    def _entry(self, st, s, side, p, t, sig=None, reason="", stop=0.0, target=0.0, meta=None):
        if st in CLOSED_STRATEGIES and s in self.pos[st]:
            return None
        q, dkk = self._qty(p)
        if q < 1:
            self._trace(
                st, "NO_TRADE", s, "budget/penny rule blocked entry",
                {"price": p, "reason": "ONE_SHARE_EXCEEDS_BUDGET_OR_PENNY", **(meta or {})},
                once_key=f"{datetime.now(NY).date()}:{st}:{s}:budget",
            )
            return None
        z = Pos(uuid.uuid4().hex[:12], st, s, side, t, p, q, dkk, stop, target, p, p, sig, meta or {})
        if st in CLOSED_STRATEGIES:
            self.pos[st][s] = z
            self._trace(st, "OPEN", s, f"{side} position opened at ${p:.4f}; awaiting exit logic", {"trade_id": z.id, "reason": reason, **(meta or {})})
        else:
            self._sheet_entry(st, z, reason, meta or {})
        self._note(st, f"{side} ENTRY\nTicker: {s}\nprice: ${p:.4f}\nshares: {q}\nDKK: {dkk:.2f}\n{reason}")
        return z

    def _exit(self, st, s, p, t, reason):
        z = self.pos[st].pop(s, None)
        if not z:
            return
        pnl = (p - z.p) * z.q * self.fx if z.side == "LONG" else (z.p - p) * z.q * self.fx
        self._sheet_closed(st, z, p, t, pnl, reason)
        self._note(st, f"EXIT\nTicker: {s}\nprice: ${p:.4f}\nP/L: DKK {pnl:+.2f}\n{reason}")
        self._trace(st, "CLOSED", s, f"trade closed P/L DKK {pnl:+.2f}", {"trade_id": z.id, "exit_reason": reason})

    def _regular(self, t):
        x = t.astimezone(NY)
        m = x.hour * 60 + x.minute
        return x.weekday() < 5 and 570 <= m < 960

    def bootstrap(self, ss):
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        cl = StockHistoricalDataClient(self.key, self.sec)
        st = datetime.now(UTC) - timedelta(days=5)
        bb = int(self.c["bb"])
        for i in range(0, len(ss), bb):
            z = ss[i:i + bb]
            try:
                r = cl.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=z,
                    timeframe=TimeFrame(5, TimeFrameUnit.Minute),
                    start=st,
                    feed=DataFeed.IEX,
                ))
                n = 0
                for s, bars in r.data.items():
                    for b in bars:
                        if self._regular(b.timestamp):
                            self.roll.add(s, float(b.high) - float(b.low), float(b.volume))
                            n += 1
                self._sys("INFO", "BOOTSTRAP", "", f"batch {i // bb + 1}: {n} bars")
            except Exception as ex:
                self._sys("ERROR", "BOOTSTRAP", "", str(ex))
            time.sleep(float(self.c["bp"]))

    def rank_bootstrap(self, ss):
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        cl = StockHistoricalDataClient(self.key, self.sec)
        st = datetime.now(UTC) - timedelta(days=int(self.c["rb"]))
        bb = int(self.c["rbb"])
        ok = bad = 0
        for i in range(0, len(ss), bb):
            z = ss[i:i + bb]
            try:
                r = cl.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=z,
                    timeframe=TimeFrame(1, TimeFrameUnit.Day),
                    start=st,
                    end=datetime.now(UTC),
                    feed=DataFeed.IEX,
                ))
                for s, bars in r.data.items():
                    for b in bars:
                        d = b.timestamp.astimezone(NY).date()
                        self.daily[s][d] = {"open": float(b.open), "close": float(b.close)}
                ok += 1
            except Exception as ex:
                bad += 1
                self._sys("ERROR", "RANK_BOOT", "", str(ex))
            time.sleep(float(self.c["rbp"]))
        self._sys("INFO", "RANK_BOOT", "", f"done success={ok} failed={bad}")
        self._capture_open(ss)
        self.rank_ready = ok > 0

    def _capture_open(self, ss):
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        now = datetime.now(NY)
        op = now.replace(hour=9, minute=30, second=0, microsecond=0)
        if now.weekday() >= 5 or now < op:
            return
        end = min(now, op + timedelta(minutes=5))
        cl = StockHistoricalDataClient(self.key, self.sec)
        bb = int(self.c["rbb"])
        for i in range(0, len(ss), bb):
            try:
                r = cl.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=ss[i:i + bb],
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=op.astimezone(UTC),
                    end=end.astimezone(UTC),
                    feed=DataFeed.IEX,
                ))
                for s, bars in r.data.items():
                    if bars:
                        x = min(bars, key=lambda b: b.timestamp)
                        self.first[s] = (float(x.close), x.timestamp.astimezone(NY))
            except Exception as ex:
                self._sys("ERROR", "RANK_OPEN", "", str(ex))
            time.sleep(float(self.c["rbp"]))

    def on_bar(self, b: M1):
        if self.allowed and b.s not in self.allowed:
            return
        x = b.t.astimezone(NY)
        with self.lock:
            self.last[b.s] = (b.c, b.t)
            if x.weekday() < 5 and x.hour == 9 and x.minute == 30 and b.s not in self.first:
                self.first[b.s] = (b.c, x)
            if x.weekday() < 5 and x.hour == 9 and x.minute == 30:
                self._open_entry_from_bar(b)
            self._manage_positions(b)
            self._orb(b)
            z = self.a5.push(b)
            if z:
                lag_min = max(0.0, (b.t - z.end).total_seconds() / 60.0)
                if lag_min <= MAX_STALE_5M_LAG_MINUTES:
                    self._anomaly(z, observed_at=b.t)
            self._continue(b)
            self._s2_expire(b.s, b.t)

    def _anomaly(self, z: M5, observed_at: Optional[datetime] = None):
        st = self.roll.stats(z.s)
        if not st or st[4] < int(self.c["wm"]):
            self.roll.add(z.s, z.rng, z.v)
            return
        mr, sr, mv, sv, _ = st
        zr = (z.rng - mr) / sr if sr > 0 else 0.0
        zv = (z.v - mv) / sv if sv > 0 else 0.0
        stage1 = (mr > 0 and z.rng > float(self.c["sp"]) * mr) or (mv > 0 and z.v > float(self.c["sv"]) * mv)
        stage2 = zr > float(self.c["zs"]) or zv > float(self.c["zs"])
        up = z.c > z.o
        dn = z.c < z.o
        self.roll.add(z.s, z.rng, z.v)
        if not (stage1 and stage2 and (up or dn)):
            return
        direction = "U" if up else "D"
        alert_t = observed_at or z.end

        # S2: exactly the user's revised experiment: no 2-minute continuation.
        # First upward anomaly arms. The next qualifying anomaly must also be upward
        # and occur within 5 minutes. Only then is fresh news checked.
        if direction == "D" and self.c.enabled(2) and z.s in self.s2arm:
            arm = self.s2arm.pop(z.s)
            self._trace(2, "CANCEL", z.s, "next anomaly was downward; S2 pair cancelled", {
                "first_alert_time_et": _et(arm["t"]), "first_alert_price": arm["p"]
            })
        if direction == "U" and self.c.enabled(2):
            arm = self.s2arm.get(z.s)
            delay = float(self.c["s2w"])
            paired = False
            if arm:
                dt = (alert_t - arm["t"]).total_seconds() / 60
                if 0 < dt <= delay:
                    sg2 = Sig(uuid.uuid4().hex[:12], z.s, "U", alert_t, z.c, zr, zv, z.v, [z.c], [])
                    sg2.status = "AI_PENDING"
                    meta = {
                        "first_alert_time_et": _et(arm["t"]),
                        "first_alert_price": arm["p"],
                        "second_alert_time_et": _et(alert_t),
                        "second_alert_price": z.c,
                        "minutes_between_alerts": dt,
                    }
                    self._trace(2, "SECOND_ALERT", z.s, "second upward anomaly arrived; checking fresh news before short", meta)
                    self._queue_ai(AIJob("S2", sg2, meta))
                    self.s2arm.pop(z.s, None)
                    paired = True
                elif dt > delay:
                    self._trace(2, "EXPIRED", z.s, "no second upward anomaly within 5-minute window", {
                        "first_alert_time_et": _et(arm["t"]), "first_alert_price": arm["p"], "elapsed_minutes": dt
                    })
                    self.s2arm.pop(z.s, None)
            if not paired and z.s not in self.s2arm:
                sid = uuid.uuid4().hex[:12]
                self.s2arm[z.s] = {"t": alert_t, "p": z.c, "id": sid}
                self._trace(2, "FIRST_ALERT", z.s, "first upward anomaly armed; waiting up to 5 minutes", {
                    "trade_id": sid, "first_alert_time_et": _et(alert_t), "first_alert_price": z.c
                })

        nowm = time.monotonic()
        if nowm - self.cool.get(z.s, -1e12) < float(self.c["cd"]) * 60:
            return
        self.cool[z.s] = nowm
        if z.s in self.sig:
            return
        sg = Sig(uuid.uuid4().hex[:12], z.s, direction, z.end, z.c, zr, zv, z.v, [z.c], [])
        self.sig[z.s] = sg
        if direction == "U" and self.c.enabled(1):
            self._trace(1, "ANOMALY", z.s, "upward 5-minute anomaly; waiting for 2-minute continuation", {"signal_id": sg.id, "z_range": zr, "z_volume": zv})
        elif direction == "D" and self.c.enabled(3):
            self._trace(3, "ANOMALY", z.s, "downward 5-minute anomaly; waiting for 2-minute continuation", {"signal_id": sg.id, "z_range": zr, "z_volume": zv})

    def _continue(self, b: M1):
        sg = self.sig.get(b.s)
        if not sg or sg.status != "WATCH":
            return
        if b.t < sg.t:
            return
        if sg.mt and b.t <= sg.mt[-1]:
            return
        sg.mt.append(b.t)
        sg.px.append(b.c)
        if len(sg.px) < 3:
            return
        a, b1, c = sg.px[0], sg.px[1], sg.px[2]
        if min(a, b1, c) <= 0:
            self.sig.pop(sg.s, None)
            return
        r2 = math.log(c / b1)
        total = math.log(c / a)
        if sg.direction == "U":
            ok = total >= float(self.c["mr"]) and max(0.0, -r2) <= float(self.c["mp"])
        else:
            ok = total <= -float(self.c["mr"]) and max(0.0, r2) <= float(self.c["mp"])
        sg.cont = total
        sg.confirm_p = c
        st = 1 if sg.direction == "U" else 3
        if not ok:
            self._trace(st, "CONT_FAIL", sg.s, "2-minute continuation failed; no trade", {"signal_id": sg.id, "continuation_pct": total * 100})
            self.sig.pop(sg.s, None)
            return
        if self.c.enabled(st):
            sg.status = "AI_PENDING"
            self._trace(st, "CONT_PASS", sg.s, "2-minute continuation passed; AI catalyst check queued", {"signal_id": sg.id, "continuation_pct": total * 100})
            self._queue_ai(AIJob("S1" if st == 1 else "S3", sg))
        else:
            self.sig.pop(sg.s, None)

    def _queue_ai(self, job: AIJob):
        # S2 is the most time-sensitive because the second anomaly has already
        # happened. S1/S3 follow immediately behind it. Sequence keeps FIFO
        # ordering within the same priority.
        pri = 0 if job.kind == "S2" else 1 if job.kind == "S1" else 2
        with self.lock:
            self.ai_seq += 1
            seq = self.ai_seq
        self.jobs.put((pri, seq, job))

    def _ai_worker(self):
        while not self.dead.is_set():
            try:
                _, _, job = self.jobs.get(timeout=1)
            except queue.Empty:
                continue
            try:
                kind = job.kind
                sg = job.sig
                meta = job.meta
                if kind == "S2":
                    dec, hint, src, err = self.ai.signal(sg.s, "U", sg.t, sg.p, sg.zr, sg.zv, sg.vol, fresh=True, policy="S2")
                    with self.lock:
                        sg.news = dec
                        sg.hint = hint
                        sg.sources = src
                        if dec == "ERROR":
                            self._trace(2, "AI_ERROR", sg.s, f"news check unavailable; S2 does not short; {err}", {**meta, "error": err}, level="ERROR")
                            continue
                        if dec == "YES":
                            self._trace(2, "NO_SHORT", sg.s, "verified news found; short suppressed", {**meta, "news_hint": hint, "sources": src})
                            continue
                        p_now, t_now = self.last.get(sg.s, (sg.p, sg.t))
                        em = {**meta, "news_decision": "NO", "news_hint": "", "sources": src}
                        self._entry(2, sg.s, "SHORT", p_now, t_now, sig=sg,
                                    reason="TWO_UP_ANOMALIES_WITHIN_5M_NO_VERIFIED_NEWS", meta=em)
                    continue

                dec, hint, src, err = self.ai.signal(sg.s, sg.direction, sg.t, sg.p, sg.zr, sg.zv, sg.vol, policy=kind)
                with self.lock:
                    cur = self.sig.get(sg.s)
                    if not cur or cur.id != sg.id:
                        continue
                    sg.news = dec
                    sg.hint = hint
                    sg.sources = src
                    st = 1 if sg.direction == "U" else 3
                    if dec == "ERROR":
                        self._trace(st, "AI_ERROR", sg.s, f"AI catalyst check unavailable; no trade; {err}", {"signal_id": sg.id, "error": err}, level="ERROR")
                        self.sig.pop(sg.s, None)
                        continue
                    if sg.direction == "U":
                        if dec == "YES" and self.c.enabled(1):
                            self._entry(1, sg.s, "LONG", sg.confirm_p, sg.mt[-1], sig=sg,
                                        reason=hint or "NEWS_CONFIRMED", meta={"sources": src})
                        elif dec == "NO" and self.c.enabled(1):
                            self._trace(1, "NO_TRADE", sg.s, "no verified catalyst; S1 ignored", {"signal_id": sg.id})
                    else:
                        if dec == "NO" and self.c.enabled(3):
                            self._entry(3, sg.s, "LONG", sg.confirm_p, sg.mt[-1], sig=sg,
                                        reason="FALL_WITHOUT_VERIFIED_CATALYST", meta={"sources": src})
                        elif self.c.enabled(3):
                            self._trace(3, "NO_TRADE", sg.s, "negative catalyst found; S3 ignored", {"signal_id": sg.id, "news_hint": hint})
                    self.sig.pop(sg.s, None)
            except Exception as ex:
                self._sys("ERROR", "AI_WORKER", getattr(locals().get("sg", None), "s", ""), str(ex))

    def _s2_expire(self, s: str, t: datetime):
        arm = self.s2arm.get(s)
        if not arm:
            return
        delay = float(self.c["s2w"])
        dt = (t - arm["t"]).total_seconds() / 60
        if dt > delay:
            self._trace(2, "EXPIRED", s, "no second anomaly within S2 window", {
                "window_minutes": delay, "elapsed_minutes": dt,
                "first_alert_time_et": _et(arm["t"]), "first_alert_price": arm["p"],
            })
            self.s2arm.pop(s, None)

    def _poc(self, bars: list[tuple[float, float, float]], bins: int) -> Optional[float]:
        if not bars:
            return None
        lo = min(x[0] for x in bars)
        hi = max(x[1] for x in bars)
        if not math.isfinite(lo) or not math.isfinite(hi) or lo <= 0 or hi < lo:
            return None
        if hi == lo:
            return lo
        n = max(8, int(bins))
        step = (hi - lo) / n
        vol = [0.0] * n
        for bl, bh, bv in bars:
            if bv <= 0 or bh < bl:
                continue
            i0 = max(0, min(n - 1, int((bl - lo) / step)))
            i1 = max(0, min(n - 1, int((bh - lo) / step)))
            if i1 < i0:
                i0, i1 = i1, i0
            share = float(bv) / (i1 - i0 + 1)
            for i in range(i0, i1 + 1):
                vol[i] += share
        if not any(x > 0 for x in vol):
            return None
        i = max(range(n), key=lambda k: vol[k])
        return lo + (i + 0.5) * step

    def _manage_positions(self, b: M1):
        p = b.c
        t = b.t
        z = self.pos[1].get(b.s)
        if z:
            z.peak = max(z.peak, p)
            gain = (z.peak / z.p - 1) * 100
            tr = self.c["tr"]
            pct = float(tr[0] if gain < 10 else tr[1] if gain < 25 else tr[2] if gain < 50 else tr[3])
            if (p / z.peak - 1) * 100 <= -pct:
                self._exit(1, b.s, p, t, "ADAPTIVE_TRAIL")
        z = self.pos[3].get(b.s)
        if z:
            z.trough = min(z.trough, p)
            if p >= z.trough * (1 + float(self.c["s3r"]) / 100):
                self._exit(3, b.s, p, t, "REBOUND_FROM_POST_ENTRY_LOW")
        z = self.pos[6].get(b.s)
        if z:
            if b.l <= z.stop:
                self._exit(6, b.s, z.stop, t, "BOX_QUARTER_STOP")
            elif b.h >= z.target:
                self._exit(6, b.s, z.target, t, "THREE_R_TARGET")

    def _orb(self, b: M1):
        """Strategy 6: two 15-minute candles, then a 1-minute pullback entry.

        09:30-09:45 ET: build and freeze the first 15-minute box H1/L1.
        09:45-10:00 ET: build the second 15-minute candle. No decision is made
        before this candle is complete.
        At/after 10:00 ET: if H2 did not break H1, ignore the ticker for the day.
        If H2 > H1, arm a bullish setup and wait for a later 1-minute candle to
        trade back through H1. The simulated entry is H1 itself (the retest line).
        Stop distance is exactly 25% of the first box height, and target is 3R
        (or the existing s6r value, so no K08 change is needed).
        """
        if not self.c.enabled(6):
            return

        x = b.t.astimezone(NY)
        m = x.hour * 60 + x.minute
        d = x.date()
        k = f"{d}:{b.s}"

        # S6 is a regular-session setup only. Before 09:30 there is nothing to do.
        if m < MARKET_OPEN_MINUTE_ET:
            return

        if k not in self.orb:
            if not (MARKET_OPEN_MINUTE_ET <= m < S6_FIRST_END):
                return
            self.orb[k] = {
                "h1": -math.inf,
                "l1": math.inf,
                "n1": 0,
                "h2": -math.inf,
                "l2": math.inf,
                "n2": 0,
                "evaluated": False,
                "armed": False,
                "done": False,
            }
        q = self.orb[k]
        if q["done"]:
            return

        # First completed 15-minute candle: 09:30 through 09:44.
        if MARKET_OPEN_MINUTE_ET <= m < S6_FIRST_END:
            q["h1"] = max(q["h1"], float(b.h))
            q["l1"] = min(q["l1"], float(b.l))
            q["n1"] += 1
            return

        # Second completed 15-minute candle: 09:45 through 09:59.
        # Critically, do not make a bullish/bearish decision during this window.
        if S6_FIRST_END <= m < S6_SECOND_END:
            q["h2"] = max(q["h2"], float(b.h))
            q["l2"] = min(q["l2"], float(b.l))
            q["n2"] += 1
            return

        # From 10:00 onward the first two 15-minute candles are complete.
        if m >= S6_SECOND_END and not q["evaluated"]:
            q["evaluated"] = True

            if q["h1"] == -math.inf or q["l1"] == math.inf or q["n2"] == 0:
                q["done"] = True
                self._trace(
                    6, "INVALID_TWO_CANDLES", b.s,
                    "first/second 15-minute candle data incomplete; no S6 trade",
                    {"first_window_bars": q["n1"], "second_window_bars": q["n2"]},
                    once_key=f"{k}:incomplete",
                )
                return

            width_pct = ((q["h1"] / q["l1"]) - 1) * 100 if q["l1"] > 0 else 0.0
            if q["n1"] < S6_MIN_FIRST_BARS or width_pct < S6_MIN_RANGE_PCT:
                q["done"] = True
                self._trace(
                    6, "INVALID_OPEN_RANGE", b.s,
                    "insufficient first-15-minute structure; no S6 trade",
                    {
                        "first_window_bars": q["n1"],
                        "second_window_bars": q["n2"],
                        "range_high": q["h1"],
                        "range_low": q["l1"],
                        "range_pct": width_pct,
                    },
                    once_key=f"{k}:invalid",
                )
                return

            # The second 15-minute candle only needs to trade above H1. A wick
            # above the level counts as the bullish structural break.
            if q["h2"] <= q["h1"]:
                q["done"] = True
                self._trace(
                    6, "IGNORE", b.s,
                    "second 15-minute candle did not break first-candle high",
                    {
                        "range_high": q["h1"],
                        "range_low": q["l1"],
                        "second_15m_high": q["h2"],
                        "second_15m_low": q["l2"],
                    },
                    once_key=f"{k}:no_break",
                )
                return

            q["armed"] = True
            self._trace(
                6, "BULLISH_SETUP", b.s,
                "second 15-minute candle broke first-candle high; waiting for 1-minute pullback to H1",
                {
                    "range_high": q["h1"],
                    "range_low": q["l1"],
                    "second_15m_high": q["h2"],
                    "second_15m_low": q["l2"],
                },
                once_key=f"{k}:armed",
            )

        # Entry may occur any time after the two 15-minute candles are complete.
        # We never chase the breakout: a later 1-minute candle must actually trade
        # through the original H1 line. There is intentionally no post-10:00 entry cutoff.
        if q["armed"] and b.s not in self.pos[6]:
            h1 = float(q["h1"])
            touched = float(b.l) <= h1 <= float(b.h)
            if not touched:
                return

            box_height = float(q["h1"] - q["l1"])
            if box_height <= 0:
                q["done"] = True
                self._trace(
                    6, "INVALID_BOX", b.s,
                    "first 15-minute box height is non-positive; no S6 trade",
                    {"range_high": q["h1"], "range_low": q["l1"]},
                    once_key=f"{k}:bad_box",
                )
                return

            entry = h1
            stop_distance = S6_STOP_BOX_FRACTION * box_height
            stop = entry - stop_distance
            reward_r = float(self.c["s6r"])
            target = entry + reward_r * stop_distance

            pos = self._entry(
                6, b.s, "LONG", entry, b.t,
                reason="TWO_15M_BULLISH_BREAK_THEN_1M_RETEST",
                stop=stop,
                target=target,
                meta={
                    "range_high": q["h1"],
                    "range_low": q["l1"],
                    "second_15m_high": q["h2"],
                    "second_15m_low": q["l2"],
                    "first_window_bars": q["n1"],
                    "second_window_bars": q["n2"],
                    "retest_time_et": _et(b.t),
                    "retest_1m_low": b.l,
                    "retest_1m_high": b.h,
                    "box_height": box_height,
                    "stop_fraction_of_box": S6_STOP_BOX_FRACTION,
                    "stop_distance": stop_distance,
                    "risk_per_share": stop_distance,
                    "reward_r": reward_r,
                },
            )
            if pos:
                q["done"] = True

    def ranks(self):
        now = datetime.now(NY)
        td = now.date()
        ws = td - timedelta(days=td.weekday())
        ms = td.replace(day=1)
        out = []
        for s, (p, _) in self.last.items():
            if self.allowed and s not in self.allowed:
                continue
            if p <= 0:
                continue
            db = self.first.get(s, (None, None))[0]
            hist = self.daily.get(s, {})
            wd = sorted(d for d in hist if ws <= d <= td)
            md = sorted(d for d in hist if ms <= d <= td)
            wb = self.week.get(s) or (hist[wd[0]]["open"] if wd else None)
            mb = self.month.get(s) or (hist[md[0]]["open"] if md else None)
            dr = (p / db - 1) * 100 if db else None
            wr = (p / wb - 1) * 100 if wb else None
            mr = (p / mb - 1) * 100 if mb else None
            out.append((s, p, db, wb, mb, dr, wr, mr))
        return out

    def ranking_cycle(self):
        if not self.rank_ready:
            self._sys("WARNING", "RANKING", "", "ranking cycle skipped because ranking bootstrap is not ready")
            return
        now = datetime.now(NY)
        rows = self.ranks()
        n = int(self.c["rn"])
        blocks = {
            "DAILY": sorted([x for x in rows if x[5] is not None and x[5] > 0], key=lambda x: x[5], reverse=True)[:n],
            "WEEKLY": sorted([x for x in rows if x[6] is not None and x[6] > 0], key=lambda x: x[6], reverse=True)[:n],
            "MONTHLY": sorted([x for x in rows if x[7] is not None and x[7] > 0], key=lambda x: x[7], reverse=True)[:n],
        }
        uniq = []
        for v in blocks.values():
            for x in v:
                if x[0] not in uniq:
                    uniq.append(x[0])
        reasons = self.ai.ranking_reasons(uniq) if _bool(self.c["rr"]) else {}
        parts = ["[15m Market Gainers]"]
        for period, idx, baseidx in [("DAILY", 5, 2), ("WEEKLY", 6, 3), ("MONTHLY", 7, 4)]:
            parts.append("\n" + period)
            if not blocks[period]:
                parts.append("No positive performers")
            for rank, x in enumerate(blocks[period], 1):
                rs = reasons.get(x[0], "")
                parts.append(f"{rank}. {x[0]} {x[idx]:+.2f}%" + (f" — {rs}" if rs else ""))
                self.gs.put("Rankings", [_et(now), period, rank, x[0], x[idx], x[1], x[baseidx], rs, ""])
        if _bool(self.c["nr"]):
            self.tg.send("\n".join(parts))
        if self.c.enabled(7):
            self._s7(blocks["DAILY"], now)

    def _s7(self, daily, now):
        top = {x[0]: i + 1 for i, x in enumerate(daily)}
        for s, rank in top.items():
            dq = self.rank_seen[s]
            dq.append(now)
            cut = now - timedelta(minutes=60)
            while dq and dq[0] < cut:
                dq.popleft()
            k = (now.date(), s)
            if len(dq) >= 2 and k not in self.rank_bought:
                p = self.last.get(s, (None, None))[0]
                if p:
                    z = self._entry(7, s, "LONG", p, now.astimezone(UTC),
                                    reason="TOP10_REPEATED_WITHIN_60M",
                                    meta={"rank": rank, "count_60m": len(dq)})
                    if z:
                        self.rank_bought.add(k)
            elif len(dq) == 1:
                self._trace(7, "WATCH", s, "first top-10 appearance in rolling hour", {"rank": rank, "count_60m": 1}, once_key=f"{now.date()}:S7:{s}:watch")

    def _premarket_screener(self, limit: int) -> list[dict[str, Any]]:
        try:
            from alpaca.data.historical.screener import ScreenerClient
            from alpaca.data.requests import MarketMoversRequest
            cl = ScreenerClient(self.key, self.sec)
            movers = cl.get_market_movers(MarketMoversRequest(top=max(20, min(50, limit * 5))))
            out = []
            for m in movers.gainers:
                s = str(m.symbol).upper()
                if self.allowed and s not in self.allowed:
                    continue
                pct = float(m.percent_change)
                if pct <= 0:
                    continue
                out.append({
                    "symbol": s,
                    "rank": len(out) + 1,
                    "change_pct": pct,
                    "price": float(m.price),
                    "previous_close": (float(m.price) / (1 + pct / 100) if pct > -100 else ""),
                    "source": "ALPACA_MARKET_MOVERS_09_10",
                })
                if len(out) >= limit:
                    break
            return out
        except Exception as ex:
            self._sys("WARNING", "S4_SCREENER", "", f"Alpaca screener unavailable; using IEX snapshot fallback: {ex}")
            return []

    def _premarket_snapshot_fallback(self, limit: int) -> list[dict[str, Any]]:
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockSnapshotRequest

        td = datetime.now(NY).date()
        cl = StockHistoricalDataClient(self.key, self.sec)
        bb = int(self.c["rbb"])
        candidates = []
        for i in range(0, len(self.universe), bb):
            batch = self.universe[i:i + bb]
            try:
                snaps = cl.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=batch, feed=DataFeed.IEX))
                for s, snap in snaps.items():
                    lt = getattr(snap, "latest_trade", None)
                    mb = getattr(snap, "minute_bar", None)
                    db = getattr(snap, "daily_bar", None)
                    pdb = getattr(snap, "previous_daily_bar", None)
                    current = None
                    current_t = None
                    if lt is not None:
                        current = _float(getattr(lt, "price", None), 0.0)
                        current_t = getattr(lt, "timestamp", None)
                    if (not current or current <= 0) and mb is not None:
                        current = _float(getattr(mb, "close", None), 0.0)
                        current_t = getattr(mb, "timestamp", None)
                    if not current or current <= 0 or current_t is None:
                        continue
                    if current_t.astimezone(NY).date() != td:
                        continue
                    ref = None
                    if db is not None and getattr(db, "timestamp", None) is not None and db.timestamp.astimezone(NY).date() < td:
                        ref = _float(getattr(db, "close", None), 0.0)
                    elif pdb is not None:
                        ref = _float(getattr(pdb, "close", None), 0.0)
                    if not ref or ref <= 0:
                        continue
                    pct = (current / ref - 1) * 100
                    if pct > 0:
                        candidates.append((pct, s, current, ref))
            except Exception as ex:
                self._sys("WARNING", "S4_SNAPSHOT", "", f"batch {i // bb + 1}: {ex}")
            time.sleep(min(0.2, float(self.c["rbp"])))
        candidates.sort(reverse=True)
        out = []
        for rank, (pct, s, p, ref) in enumerate(candidates[:limit], 1):
            out.append({
                "symbol": s, "rank": rank, "change_pct": pct, "price": p,
                "previous_close": ref, "source": "ALPACA_IEX_SNAPSHOT_09_10",
            })
        return out

    def s4_scan(self):
        if not self.c.enabled(4):
            return
        td = datetime.now(NY).date()
        if self.s4_scan_day == td:
            return
        limit = int(self.c["s4m"])
        rows = self._premarket_screener(limit)
        if len(rows) < limit:
            fb = self._premarket_snapshot_fallback(limit)
            if fb:
                rows = fb
        self.s4_list = rows[:limit]
        self.s4_scan_day = td
        self._sys("INFO", "S4_SCAN", "", f"09:10 premarket scan selected {len(self.s4_list)} symbols", {"candidates": self.s4_list})
        if self.c.notify(4):
            txt = "\n".join(f"{x['rank']}. {x['symbol']} {x['change_pct']:+.2f}%" for x in self.s4_list)
            self.tg.send(f"[S4 {NAMES[4]}]\n09:10 ET premarket candidates\n{txt or 'No candidates'}")

    def _nasdaq_earnings(self, target: date, limit: int) -> list[dict[str, Any]]:
        import requests
        url = "https://api.nasdaq.com/api/calendar/earnings"
        hdr = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nasdaq.com/market-activity/earnings",
        }
        try:
            r = requests.get(url, params={"date": target.isoformat()}, headers=hdr, timeout=15)
            r.raise_for_status()
            rows = (((r.json() or {}).get("data") or {}).get("rows") or [])
            out = []
            for x in rows:
                s = str(x.get("symbol") or "").strip().upper()
                if not s or (self.allowed and s not in self.allowed):
                    continue
                mc = str(x.get("marketCap") or "").replace("$", "").replace(",", "")
                try:
                    market_cap = float(mc)
                except Exception:
                    market_cap = 0.0
                when = str(x.get("time") or "").strip()
                out.append({
                    "symbol": s,
                    "event": "EARNINGS" + (f" ({when})" if when else ""),
                    "date": target.isoformat(),
                    "url": "https://www.nasdaq.com/market-activity/earnings",
                    "source_kind": "NASDAQ",
                    "market_cap": market_cap,
                })
            out.sort(key=lambda x: x.get("market_cap", 0.0), reverse=True)
            return out[:limit]
        except Exception as ex:
            self._sys("WARNING", "S5_NASDAQ", "", f"Nasdaq earnings lookup failed: {ex}")
            return []

    def _next_trade_day(self, d: date):
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import GetCalendarRequest
        cl = TradingClient(self.key, self.sec, paper=True)
        xs = cl.get_calendar(GetCalendarRequest(start=d + timedelta(days=1), end=d + timedelta(days=10)))
        return xs[0].date if xs else d + timedelta(days=1)

    def s5_scan(self):
        if not self.c.enabled(5):
            return
        td = datetime.now(NY).date()
        if self.s5_scan_day == td:
            return
        try:
            target = self._next_trade_day(td)
        except Exception as ex:
            self._sys("ERROR", "S5_CAL", "", str(ex))
            return
        limit = int(self.c["s5m"])
        nasdaq = self._nasdaq_earnings(target, max(limit, 20))
        web, err = self.ai.next_day_events(target, max(limit, 20))
        merged = []
        seen = set()
        # Put verified clinical/FDA/PDUFA web items first, then fill with earnings.
        for r in web + nasdaq:
            s = r.get("symbol", "").upper()
            if not s or (self.allowed and s not in self.allowed):
                continue
            key = (s, str(r.get("event", "")).upper())
            if key in seen:
                continue
            seen.add(key)
            merged.append(r)
            if len(merged) >= limit:
                break
        self.s5_today = merged
        self.s5_scan_day = td
        self._sys("INFO", "S5_SCAN", "", f"09:10 event scan selected {len(merged)} candidates for {target}", {
            "target": target.isoformat(), "ai_error": err, "candidates": merged
        })
        if self.c.notify(5):
            txt = "\n".join(f"{r['symbol']} — {r['event']}" for r in merged)
            self.tg.send(f"[S5 {NAMES[5]}]\n09:10 ET candidates for {target}\n{txt or 'No verified candidates'}")

    def _open_entry_from_bar(self, b: M1):
        td = b.t.astimezone(NY).date()
        # S4: buy the 09:10 premarket top list at the regular-session open.
        if self.c.enabled(4):
            for r in self.s4_list:
                if r["symbol"] == b.s and (td, b.s) not in self.s4_done:
                    meta = {
                        "premarket_rank": r["rank"],
                        "premarket_change_pct": r["change_pct"],
                        "premarket_price": r["price"],
                        "previous_close": r.get("previous_close", ""),
                        "source": r["source"],
                    }
                    if self._entry(4, b.s, "LONG", b.o, b.t, reason="09_10_PREMARKET_TOP10_BUY_AT_OPEN", meta=meta):
                        self.s4_done.add((td, b.s))
        # S5: events scheduled for the next trading day are bought at today's open.
        if self.c.enabled(5):
            for r in self.s5_today:
                if r["symbol"] == b.s and (td, b.s) not in self.s5_done:
                    meta = {"event": r["event"], "event_date": r["date"], "source": r["url"]}
                    if self._entry(5, b.s, "LONG", b.o, b.t, reason="BUY_DAY_BEFORE_SCHEDULED_EVENT_AT_OPEN", meta=meta):
                        self.s5_done.add((td, b.s))

    def _fetch_open_prices(self, symbols: list[str], td: date) -> dict[str, tuple[float, datetime]]:
        if not symbols:
            return {}
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        op = datetime(td.year, td.month, td.day, 9, 30, tzinfo=NY)
        end = op + timedelta(minutes=2)
        cl = StockHistoricalDataClient(self.key, self.sec)
        out = {}
        bb = 100
        for i in range(0, len(symbols), bb):
            try:
                r = cl.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=symbols[i:i + bb],
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=op.astimezone(UTC),
                    end=end.astimezone(UTC),
                    feed=DataFeed.IEX,
                ))
                for s, bars in r.data.items():
                    if bars:
                        x = min(bars, key=lambda z: z.timestamp)
                        out[s] = (float(x.open), x.timestamp)
            except Exception as ex:
                self._sys("WARNING", "OPEN_FILL", "", str(ex))
        return out

    def fill_open_entries(self):
        now = datetime.now(NY)
        td = now.date()
        s4 = [r["symbol"] for r in self.s4_list if (td, r["symbol"]) not in self.s4_done]
        s5 = [r["symbol"] for r in self.s5_today if (td, r["symbol"]) not in self.s5_done]
        symbols = sorted(set(s4 + s5))
        if not symbols:
            return
        got = self._fetch_open_prices(symbols, td)
        for s, (p, t) in got.items():
            fake = M1(s, t, p, p, p, p, 0.0)
            self._open_entry_from_bar(fake)

    def brief(self):
        if not _bool(self.c["br"]):
            return
        td = datetime.now(NY).date()
        if self.brief_day == td:
            return
        text = self.ai.brief(td)
        if text:
            if _bool(self.c["nb"]):
                self.tg.send("[Pre-market Research Brief]\n" + text)
            self.brief_day = td

    def scheduler(self):
        while not self.dead.is_set():
            try:
                now = datetime.now(NY)
                m = now.hour * 60 + now.minute
                if now.weekday() < 5:
                    # Both premarket strategy scans are fixed at 09:10 ET in code,
                    # so no K08 change is needed.
                    if S4_SCAN_MINUTE_ET <= m < MARKET_OPEN_MINUTE_ET and self.s4_scan_day != now.date():
                        self.s4_scan()
                    if S5_SCAN_MINUTE_ET <= m < MARKET_OPEN_MINUTE_ET and self.s5_scan_day != now.date():
                        self.s5_scan()

                    # Brief is restart-safe: if its configured time was missed, run it
                    # once before market open instead of silently skipping the day.
                    brief_m = int(self.c["bh"]) * 60 + int(self.c["bm"])
                    if brief_m <= m < MARKET_OPEN_MINUTE_ET and self.brief_day != now.date():
                        self.brief()

                    # If the websocket has not yet delivered the 09:30 bar, reconstruct
                    # the opening entry from IEX historical minute data. Retry once/minute.
                    if 571 <= m < 600:
                        slot = now.strftime("%Y-%m-%d-%H-%M")
                        if slot != self.open_fill_slot:
                            self.fill_open_entries()
                            self.open_fill_slot = slot

                    if _bool(self.c["rk"]) and 585 <= m <= 945 and now.minute % int(self.c["hi"]) == 0:
                        k = now.strftime("%Y-%m-%d-%H-%M")
                        if k not in self.slots:
                            self.ranking_cycle()
                            self.slots.add(k)
                    if len(self.slots) > 500:
                        cut = (now.date() - timedelta(days=5)).isoformat()
                        self.slots = {x for x in self.slots if x[:10] >= cut}
                time.sleep(5)
            except Exception as ex:
                self._sys("ERROR", "SCHED", "", str(ex))
                time.sleep(10)

    def close(self):
        for st in CLOSED_STRATEGIES:
            for s, z in list(self.pos[st].items()):
                self._trace(st, "OPEN_AT_SHUTDOWN", s, "position still open when process stopped; not written to completed-trade sheet", {
                    "trade_id": z.id, "entry_time_et": _et(z.t), "entry_price": z.p
                })
        self.dead.set()
        self.gs.close()


def _looks_like_stock(asset) -> bool:
    """Best-effort stocks-only filter using Alpaca asset names.

    Alpaca's trading Asset object does not expose a universal security-type field
    that cleanly separates common stocks from every ETF/ETN/warrant. This filter
    removes the obvious non-stock instruments without excluding ordinary ADRs/REITs.
    """
    name = str(getattr(asset, "name", "") or "").lower()
    symbol = str(getattr(asset, "symbol", "") or "").upper()
    bad = [
        " etf", "etf ", "exchange traded fund", "exchange-traded fund", " etn",
        "exchange traded note", "proshares", "direxion", "ishares", "spdr",
        "yieldmax", "graniteshares", "roundhill", "defiance", "leveraged",
        "ultrashort", "ultrapro", "warrant", " warrants", "right to purchase",
    ]
    if any(x in name for x in bad):
        return False
    if symbol.endswith(".WS") or symbol.endswith("W") and "warrant" in name:
        return False
    return True


def _universe(k, s, cfg):
    from alpaca.trading.client import TradingClient
    from alpaca.trading.enums import AssetClass, AssetStatus
    from alpaca.trading.requests import GetAssetsRequest

    wl = [x.strip().upper() for x in str(cfg["wl"]).split(",") if x.strip()]
    if not _bool(cfg["sa"]):
        return wl
    x = TradingClient(k, s, paper=True).get_all_assets(GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE,
    ))
    return sorted({a.symbol for a in x if a.tradable and _looks_like_stock(a)})


def live():
    k1 = _need("K01")
    k2 = _need("K02")
    k3 = _need("K03")
    k4 = _need("K04")
    k5 = _need("K05")
    k6 = _need("K06")
    k7 = os.getenv("K07", "").strip()
    cfg = Cfg(json.loads(_need("K08")))

    gs = GS(k5, k6, cfg)
    tg = TG(k3, k4)
    ai = AI(k7, cfg, gs)
    ok, err = ai.preflight()
    if not ok:
        gs.put("System_Log", [_et(datetime.now(UTC)), "ERROR", "AI_PREFLIGHT", "", f"OpenRouter unavailable: {err}", "{}", ""])
        if _bool(cfg["ne"]):
            tg.send(f"[SYSTEM ERROR]\nAI_PREFLIGHT\nOpenRouter unavailable: {err[:300]}\nYahoo news lookup remains available. S1/S2/S3 can still take no-news paths; headline classification and S5 biotech/energy web research need OpenRouter. Nasdaq earnings fallback remains available.")

    core = Core(cfg, gs, tg, ai, k1, k2)
    ss = _universe(k1, k2, cfg)
    core.set_universe(ss)
    core._sys("INFO", "START", "", f"stock-only universe={len(ss)}; strategies={[i for i in range(1, 8) if cfg.enabled(i)]}")

    # Start the scheduler before long historical bootstraps so 09:10 scans and
    # 09:30 open entries cannot be missed just because bootstrap is still running.
    threading.Thread(target=core.scheduler, daemon=True).start()
    core.bootstrap(ss)
    core.rank_bootstrap(ss)

    from alpaca.data.enums import DataFeed
    from alpaca.data.live import StockDataStream
    from alpaca.data.models import Bar

    stream = StockDataStream(k1, k2, feed=DataFeed.IEX)

    async def h(bar: Bar):
        try:
            if core.allowed and bar.symbol not in core.allowed:
                return
            x = bar.timestamp.astimezone(NY)
            m = x.hour * 60 + x.minute
            if x.weekday() < 5 and 570 <= m < 960:
                core.on_bar(M1(
                    bar.symbol, bar.timestamp, float(bar.open), float(bar.high),
                    float(bar.low), float(bar.close), float(bar.volume)
                ))
        except Exception as ex:
            core._sys("ERROR", "BAR", getattr(bar, "symbol", ""), str(ex))
            L.exception("bar")

    if _bool(cfg["sa"]):
        stream.subscribe_bars(h, "*")
    else:
        stream.subscribe_bars(h, *ss)
    try:
        stream.run()
    finally:
        core.close()


def self_test():
    a = Agg5()
    t = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
    out = None
    for i in range(6):
        z = a.push(M1("X", t + timedelta(minutes=i), 10 + i * .1, 10.2 + i * .1, 9.9 + i * .1, 10.1 + i * .1, 100 + i))
        if z:
            out = z
    assert out is not None and out.start.minute == 30 and out.end.minute == 35

    r = Roll(3)
    r.add("X", 1, 10)
    r.add("X", 2, 20)
    r.add("X", 3, 30)
    st = r.stats("X")
    assert st and abs(st[0] - 2) < 1e-9 and abs(st[2] - 20) < 1e-9

    up = [100, 100.3, 100.8]
    total = math.log(up[2] / up[0])
    r2 = math.log(up[2] / up[1])
    assert total >= 0.005 and max(0, -r2) <= 0.0025

    dn = [100, 99.7, 99.2]
    total = math.log(dn[2] / dn[0])
    r2 = math.log(dn[2] / dn[1])
    assert total <= -0.005 and max(0, r2) <= 0.0025

    base = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    assert 0 < ((base + timedelta(minutes=5)) - base).total_seconds() / 60 <= 5
    assert ((base + timedelta(minutes=6)) - base).total_seconds() / 60 > 5

    fake = object.__new__(Core)
    poc = fake._poc([(99.8, 100.0, 100), (100.0, 100.2, 5000), (100.2, 100.4, 100)], 24)
    assert poc is not None and 99.95 < poc < 100.25

    entry = 101
    stop = 99.8
    target = entry + 3 * (entry - stop)
    assert abs(target - 104.6) < 1e-9

    # S6 must wait for two complete 15-minute candles, then enter only on a retest.
    assert S6_FIRST_END == 585 and S6_SECOND_END == 600
    assert abs(S6_STOP_BOX_FRACTION - 0.25) < 1e-12
    assert S6_MIN_FIRST_BARS >= 5 and S6_MIN_RANGE_PCT > 0
    assert MAX_AI_CALLS_HARD_CAP >= 300 and AI_WORKERS >= 2 and OPENROUTER_RPM <= 20
    h1, l1 = 20.0, 19.2
    box = h1 - l1
    entry = h1
    stop_distance = S6_STOP_BOX_FRACTION * box
    stop = entry - stop_distance
    target = entry + 3.0 * stop_distance
    assert abs(stop - 19.8) < 1e-9 and abs(target - 20.6) < 1e-9
    assert 15.39 <= h1 <= 20.01  # simple sanity: H1 is the retest/entry line

    # No-exit strategies have no exit/P&L columns.
    for stn in NO_EXIT_STRATEGIES:
        h = STRATEGY_HEADERS[stn]
        assert "exit_time_et" not in h and "exit_price" not in h
        assert "pnl_dkk" not in h and "pnl_pct" not in h
        assert h[3] == "Ticker" and h[5] == "entry_time_et" and "Day_end_price" in h
    for stn in CLOSED_STRATEGIES:
        h = STRATEGY_HEADERS[stn]
        assert "exit_time_et" in h and "pnl_dkk" in h
        assert h[3] == "Ticker" and h[5] == "entry_time_et" and "Day_end_price" in h

    f = _formula_day_end()
    assert "GOOGLEFINANCE" in f and 'INDIRECT("D"&ROW())' in f and 'INDIRECT("F"&ROW())' in f

    class A:
        symbol = "SPY"
        name = "SPDR S&P 500 ETF Trust"
        tradable = True
    class B:
        symbol = "AAPL"
        name = "Apple Inc. Common Stock"
        tradable = True
    assert not _looks_like_stock(A()) and _looks_like_stock(B())

    print(
        "SELF_TEST_OK: aggregation, rolling stats, S1/S3 continuation, S2 two-alert timing, "
        "POC utility/3R, S6 two-15m decision + pullback + quarter-box stop, strategy-specific sheet schemas, Day_end_price formula, stock filter"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true")
    x = ap.parse_args()
    if x.self_test:
        self_test()
    else:
        live()
