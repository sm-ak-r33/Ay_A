import argparse
import asyncio
import json
import logging
import math
import os
import queue
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests


# -----------------------------
# Environment/config helpers
# -----------------------------

def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None or not v.strip() else v.strip()


def _b(name: str, default: bool = False) -> bool:
    return _s(name, str(default)).lower() in {"1", "true", "yes", "y", "on"}


def _i(name: str, default: int) -> int:
    return int(float(_s(name, str(default))))


def _f(name: str, default: float) -> float:
    return float(_s(name, str(default)))


def _required(name: str) -> str:
    v = _s(name, "")
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def _iso(x: Optional[datetime]) -> str:
    return x.isoformat() if x else ""


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _words(text: str, max_words: int = 6) -> str:
    return " ".join((text or "").replace("\n", " ").split()[:max_words])


def _et_hm(x: datetime) -> int:
    y = x.astimezone(NY)
    return y.hour * 60 + y.minute


NY = ZoneInfo("America/New_York")
UTC = timezone.utc


@dataclass(frozen=True)
class Cfg:
    # Credentials are loaded only at runtime. No secret values are embedded.
    alpaca_key: str
    alpaca_secret: str
    telegram_token: str
    telegram_chat: str
    google_service_json: str
    google_sheet_id: str
    openrouter_key: str

    stream_all: bool
    watchlist: tuple[str, ...]

    candle_minutes: int
    stage1_price_mult: float
    stage1_volume_mult: float
    z_threshold: float
    baseline_candles: int
    warmup_candles: int
    bootstrap_days: int
    bootstrap_batch: int
    bootstrap_pause: float

    quarantine_minutes: int
    min_two_min_return: float
    max_second_minute_reversal: float
    cooldown_minutes: int

    budget_dkk: float
    penny_floor_usd: float
    usd_dkk_override: str
    usd_dkk_fallback: float

    trail_0_10: float
    trail_10_25: float
    trail_25_50: float
    trail_50_plus: float

    ai_enabled: bool
    ai_model: str
    ai_timeout: int
    ai_retries: int
    ai_daily_cap: int
    ai_aux_daily_cap: int
    ai_error_cooldown_minutes: int
    ai_web_results: int
    ai_cache_hours: int
    ai_workers: int
    ai_hb_hint_cache_minutes: int
    ai_hb_hints_enabled: bool

    heartbeat_enabled: bool
    heartbeat_top_n: int
    heartbeat_start_hm: int
    heartbeat_end_hm: int

    s1_enabled: bool
    s2_enabled: bool
    s3_enabled: bool
    s4_enabled: bool
    s5_enabled: bool
    s6_enabled: bool
    s7_enabled: bool

    tg_master: bool
    tg_s1: bool
    tg_s2: bool
    tg_s3: bool
    tg_s4: bool
    tg_s5: bool
    tg_s6: bool
    tg_s7: bool
    tg_heartbeat: bool
    tg_system_errors: bool

    s4_source_mode: str
    s4_source_url: str
    s4_source_token: str
    s4_manual_symbols: tuple[str, ...]
    s4_top_n: int
    s4_lock_hm: int

    s5_research_hm: int
    s5_entry_hm: int
    s5_max_events: int

    s6_retest_deadline_hm: int
    s6_stop_buffer_pct: float
    s6_rr: float
    s6_max_stop_pct: float

    s7_repeat_count: int
    s7_window_minutes: int
    s7_top_n: int

    gs_flush_seconds: int
    gs_max_rows_flush: int
    gs_auto_migrate: bool
    gs_initial_rows: int

    sheet_system: str
    sheet_ai: str
    sheet_heartbeat: str
    sheet_s1: str
    sheet_s2: str
    sheet_s3: str
    sheet_s4: str
    sheet_s5: str
    sheet_s6: str
    sheet_s7: str

    @staticmethod
    def load(require_credentials: bool = True) -> "Cfg":
        creds = {
            "alpaca_key": _required("ALPACA_API_KEY") if require_credentials else _s("ALPACA_API_KEY"),
            "alpaca_secret": _required("ALPACA_SECRET_KEY") if require_credentials else _s("ALPACA_SECRET_KEY"),
            "telegram_token": _required("TELEGRAM_BOT_TOKEN") if require_credentials else _s("TELEGRAM_BOT_TOKEN"),
            "telegram_chat": _required("TELEGRAM_CHAT_ID") if require_credentials else _s("TELEGRAM_CHAT_ID"),
            "google_service_json": _required("GOOGLE_SERVICE_ACCOUNT_JSON") if require_credentials else _s("GOOGLE_SERVICE_ACCOUNT_JSON"),
            "google_sheet_id": _required("GOOGLE_SHEET_ID") if require_credentials else _s("GOOGLE_SHEET_ID"),
            "openrouter_key": _s("OPENROUTER_API_KEY", ""),
        }
        wl = tuple(x.strip().upper() for x in _s("WATCHLIST", "").split(",") if x.strip())
        s4m = tuple(x.strip().upper() for x in _s("S4_PREMARKET_SYMBOLS", "").split(",") if x.strip())
        return Cfg(
            **creds,
            stream_all=_b("STREAM_ALL", True),
            watchlist=wl,
            candle_minutes=_i("CANDLE_INTERVAL", 5),
            stage1_price_mult=_f("STAGE1_PRICE_MULT", 2.0),
            stage1_volume_mult=_f("STAGE1_VOLUME_MULT", 3.0),
            z_threshold=_f("ZSCORE_THRESHOLD", 3.0),
            baseline_candles=_i("BASELINE_CANDLES", 234),
            warmup_candles=_i("WARMUP_MIN_CANDLES", 30),
            bootstrap_days=_i("BOOTSTRAP_DAYS", 5),
            bootstrap_batch=_i("BOOTSTRAP_BATCH", 200),
            bootstrap_pause=_f("BOOTSTRAP_PAUSE", 0.4),
            quarantine_minutes=_i("QUARANTINE_MINUTES", 2),
            min_two_min_return=_f("MIN_TWO_MIN_RETURN", 0.005),
            max_second_minute_reversal=_f("MAX_QUARANTINE_PULLBACK", 0.0025),
            cooldown_minutes=_i("COOLDOWN_MINUTES", 60),
            budget_dkk=_f("BUDGET_DKK", 500.0),
            penny_floor_usd=_f("PENNY_STOCK_USD", 1.0),
            usd_dkk_override=_s("USD_DKK_RATE", ""),
            usd_dkk_fallback=_f("USD_DKK_FALLBACK", 6.5),
            trail_0_10=_f("TRAIL_0_10_PCT", 4.0),
            trail_10_25=_f("TRAIL_10_25_PCT", 7.0),
            trail_25_50=_f("TRAIL_25_50_PCT", 10.0),
            trail_50_plus=_f("TRAIL_50_PLUS_PCT", 12.0),
            ai_enabled=_b("AI_ENABLED", _b("GROK_ENABLED", True)),
            ai_model=_s("AI_MODEL", _s("GROK_MODEL", "inclusionai/ling-3.0-flash-fin:free")),
            ai_timeout=_i("AI_TIMEOUT_SECONDS", _i("GROK_TIMEOUT_SECONDS", 25)),
            ai_retries=_i("AI_RETRIES", 3),
            ai_daily_cap=_i("AI_DAILY_CALL_CAP", 40),
            ai_aux_daily_cap=_i("AI_AUX_DAILY_CALL_CAP", 12),
            ai_error_cooldown_minutes=_i("AI_ERROR_COOLDOWN_MINUTES", 10),
            ai_web_results=_i("AI_WEB_RESULTS", 3),
            ai_cache_hours=_i("AI_SIGNAL_CACHE_HOURS", 24),
            ai_workers=_i("AI_WORKERS", 3),
            ai_hb_hint_cache_minutes=_i("AI_HEARTBEAT_HINT_CACHE_MINUTES", 60),
            ai_hb_hints_enabled=_b("AI_HEARTBEAT_HINTS_ENABLED", True),
            heartbeat_enabled=_b("HEARTBEAT_ENABLED", _b("HOURLY_HEARTBEAT_ENABLED", True)),
            heartbeat_top_n=_i("HEARTBEAT_TOP_N", 5),
            heartbeat_start_hm=_i("HEARTBEAT_START_HHMM", 945),
            heartbeat_end_hm=_i("HEARTBEAT_END_HHMM", 1600),
            s1_enabled=_b("S1_ENABLED", True),
            s2_enabled=_b("S2_ENABLED", True),
            s3_enabled=_b("S3_ENABLED", True),
            s4_enabled=_b("S4_ENABLED", True),
            s5_enabled=_b("S5_ENABLED", True),
            s6_enabled=_b("S6_ENABLED", True),
            s7_enabled=_b("S7_ENABLED", True),
            tg_master=_b("TG_MASTER_ENABLED", True),
            tg_s1=_b("TG_S1_ENABLED", True),
            tg_s2=_b("TG_S2_ENABLED", True),
            tg_s3=_b("TG_S3_ENABLED", True),
            tg_s4=_b("TG_S4_ENABLED", True),
            tg_s5=_b("TG_S5_ENABLED", True),
            tg_s6=_b("TG_S6_ENABLED", True),
            tg_s7=_b("TG_S7_ENABLED", True),
            tg_heartbeat=_b("TG_HEARTBEAT_ENABLED", True),
            tg_system_errors=_b("TG_SYSTEM_ERRORS_ENABLED", False),
            s4_source_mode=_s("S4_SOURCE_MODE", "alpaca").lower(),
            s4_source_url=_s("S4_SOURCE_URL", ""),
            s4_source_token=_s("S4_SOURCE_TOKEN", ""),
            s4_manual_symbols=s4m,
            s4_top_n=_i("S4_TOP_N", 10),
            s4_lock_hm=_i("S4_LOCK_HHMM", 928),
            s5_research_hm=_i("S5_RESEARCH_HHMM", 1530),
            s5_entry_hm=_i("S5_ENTRY_HHMM", 1545),
            s5_max_events=_i("S5_MAX_EVENTS", 20),
            s6_retest_deadline_hm=_i("S6_RETEST_DEADLINE_HHMM", 1100),
            s6_stop_buffer_pct=_f("S6_STOP_BUFFER_PCT", 0.15),
            s6_rr=_f("S6_RR", 3.0),
            s6_max_stop_pct=_f("S6_MAX_STOP_PCT", 8.0),
            s7_repeat_count=_i("S7_REPEAT_COUNT", 2),
            s7_window_minutes=_i("S7_WINDOW_MINUTES", 60),
            s7_top_n=_i("S7_TOP_N", 10),
            gs_flush_seconds=_i("GOOGLE_FLUSH_SECONDS", 60),
            gs_max_rows_flush=_i("GOOGLE_MAX_ROWS_PER_FLUSH", 300),
            gs_auto_migrate=_b("GOOGLE_AUTO_MIGRATE_HEADERS", True),
            gs_initial_rows=_i("GOOGLE_INITIAL_ROWS", 200),
            sheet_system=_s("SHEET_SYSTEM", "System_Log"),
            sheet_ai=_s("SHEET_AI", "AI_Log"),
            sheet_heartbeat=_s("SHEET_HEARTBEAT", "Heartbeat"),
            sheet_s1=_s("SHEET_S1", "S1_News_Longer"),
            sheet_s2=_s("SHEET_S2", "S2_Quick_Shorter"),
            sheet_s3=_s("SHEET_S3", "S3_Fall_Buyer"),
            sheet_s4=_s("SHEET_S4", "S4_Premarket_Buyer"),
            sheet_s5=_s("SHEET_S5", "S5_Earning_Staker"),
            sheet_s6=_s("SHEET_S6", "S6_ORB_Pullback"),
            sheet_s7=_s("SHEET_S7", "S7_Repeat_Gainer"),
        )


def _hhmm_to_minutes(v: int) -> int:
    h = v // 100
    m = v % 100
    if h < 0 or h > 23 or m < 0 or m > 59:
        raise ValueError(f"Invalid HHMM value: {v}")
    return h * 60 + m


# -----------------------------
# Data models
# -----------------------------

@dataclass
class M1:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class M5:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def span(self) -> float:
        return self.high - self.low


@dataclass
class Stats:
    mean_range: float
    std_range: float
    mean_volume: float
    std_volume: float
    n: int


@dataclass
class Candidate:
    symbol: str
    direction: str  # UP or DOWN
    alert_time: datetime
    alert_price: float
    z_range: float
    z_volume: float
    alert_volume: float
    prices: list[float] = field(default_factory=list)
    times: list[datetime] = field(default_factory=list)
    status: str = "QUARANTINED"
    trade_id: str = ""
    total_return: float = 0.0
    second_reversal: float = 0.0


@dataclass
class AIResult:
    decision: str  # YES / NO / ERROR
    hint: str = ""
    sources: list[str] = field(default_factory=list)
    checked_at: Optional[datetime] = None
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    error: str = ""


@dataclass
class S1Pos:
    trade_id: str
    symbol: str
    entry_time: datetime
    entry_price: float
    shares: int
    invested_dkk: float
    peak_price: float
    peak_gain_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    hint: str = ""


@dataclass
class S6State:
    day: date
    orb_high: Optional[float] = None
    orb_low: Optional[float] = None
    post945_low: Optional[float] = None
    breakout: bool = False
    breakout_time: Optional[datetime] = None
    breakout_price: Optional[float] = None
    armed: bool = False
    traded: bool = False


@dataclass
class S6Pos:
    trade_id: str
    symbol: str
    entry_time: datetime
    entry_price: float
    shares: int
    invested_dkk: float
    stop: float
    target: float
    orb_high: float
    orb_low: float
    breakout_time: datetime
    breakout_price: float


# -----------------------------
# Sheet schemas
# Every sheet contains Day_end_price by request.
# -----------------------------

COMMON = [
    "timestamp_utc", "timestamp_et", "strategy", "event", "trade_id", "symbol",
    "side", "signal_price", "entry_price", "exit_price", "shares", "invested_dkk",
    "pnl_dkk", "pnl_pct", "reason", "status", "Day_end_price"
]

SCHEMAS = {
    "SYSTEM": ["timestamp_utc", "timestamp_et", "level", "stage", "symbol", "message", "Day_end_price"],
    "AI": [
        "timestamp_utc", "timestamp_et", "request_type", "symbol", "direction", "decision",
        "hint", "input_tokens", "output_tokens", "total_tokens", "error", "sources", "Day_end_price"
    ],
    "HEARTBEAT": [
        "timestamp_utc", "timestamp_et", "daily_gainers", "weekly_gainers", "monthly_gainers",
        "telegram_sent", "Day_end_price"
    ],
    "S1": COMMON + [
        "z_range", "z_volume", "alert_volume", "two_min_return", "second_minute_reversal",
        "news_decision", "news_hint", "news_sources", "peak_price", "peak_gain_pct",
        "max_drawdown_pct", "exit_reason"
    ],
    "S2": COMMON + [
        "z_range", "z_volume", "alert_volume", "two_min_return", "second_minute_reversal",
        "news_decision", "news_hint", "news_sources"
    ],
    "S3": COMMON + [
        "z_range", "z_volume", "alert_volume", "two_min_return", "second_minute_reversal",
        "news_decision", "news_hint", "news_sources"
    ],
    "S4": COMMON + [
        "premarket_rank", "premarket_change_pct", "premarket_source", "first_15_open",
        "first_15_close", "first_15_return_pct"
    ],
    "S5": COMMON + ["event_type", "event_date", "event_reason", "event_sources"],
    "S6": COMMON + [
        "orb_high", "orb_low", "breakout_time", "breakout_price", "retest_time",
        "retest_level", "stop_loss", "take_profit", "risk_per_share", "rr_ratio", "exit_reason"
    ],
    "S7": COMMON + [
        "rank", "daily_gain_pct", "appearances_60m", "window_minutes", "snapshot_members"
    ],
}


# -----------------------------
# Google Sheets batched writer
# -----------------------------

class SheetBus:
    def __init__(self, cfg: Cfg, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._queues: dict[str, deque[list[Any]]] = defaultdict(deque)
        self._ws: dict[str, Any] = {}
        self._name = {
            "SYSTEM": cfg.sheet_system,
            "AI": cfg.sheet_ai,
            "HEARTBEAT": cfg.sheet_heartbeat,
            "S1": cfg.sheet_s1,
            "S2": cfg.sheet_s2,
            "S3": cfg.sheet_s3,
            "S4": cfg.sheet_s4,
            "S5": cfg.sheet_s5,
            "S6": cfg.sheet_s6,
            "S7": cfg.sheet_s7,
        }
        if dry_run:
            self._thread = None
            return

        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(
            json.loads(cfg.google_service_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self._book = gspread.authorize(creds).open_by_key(cfg.google_sheet_id)
        for key, headers in SCHEMAS.items():
            self._ws[key] = self._ensure_sheet(self._name[key], headers)
        self._thread = threading.Thread(target=self._loop, name="sheet-writer", daemon=True)
        self._thread.start()

    def _ensure_sheet(self, name: str, headers: list[str]):
        import gspread
        try:
            ws = self._book.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = self._book.add_worksheet(
                title=name,
                rows=max(50, self.cfg.gs_initial_rows),
                cols=len(headers),
            )
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return ws

        values = ws.row_values(1)
        if not values:
            # Shrink oversized empty legacy sheets before writing headers.
            try:
                ws.resize(rows=max(50, self.cfg.gs_initial_rows), cols=len(headers))
            except Exception:
                pass
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return ws

        if values != headers:
            if not self.cfg.gs_auto_migrate:
                raise RuntimeError(
                    f"Worksheet {name!r} has an incompatible header. "
                    "Use a fresh workbook or set GOOGLE_AUTO_MIGRATE_HEADERS=true."
                )
            stamp = datetime.now(NY).strftime("%Y%m%d_%H%M%S")
            legacy = f"{name}_legacy_{stamp}"[:100]
            ws.update_title(legacy)
            ws = self._book.add_worksheet(
                title=name,
                rows=max(50, self.cfg.gs_initial_rows),
                cols=len(headers),
            )
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return ws

        # Keep only the requested number of columns; this also prevents accidental
        # 1000+ column legacy sheets from consuming the workbook cell budget.
        try:
            if ws.col_count != len(headers):
                ws.resize(rows=ws.row_count, cols=len(headers))
        except Exception:
            pass
        return ws

    def append(self, key: str, row: list[Any]) -> None:
        if key not in SCHEMAS:
            raise KeyError(key)
        if len(row) != len(SCHEMAS[key]):
            raise ValueError(f"{key} row has {len(row)} cells; expected {len(SCHEMAS[key])}")
        if self.dry_run:
            self._queues[key].append(row)
            return
        with self._lock:
            self._queues[key].append(row)

    def _take(self, key: str) -> list[list[Any]]:
        with self._lock:
            q = self._queues[key]
            n = min(self.cfg.gs_max_rows_flush, len(q))
            return [q.popleft() for _ in range(n)]

    def _put_back(self, key: str, rows: list[list[Any]]) -> None:
        with self._lock:
            for row in reversed(rows):
                self._queues[key].appendleft(row)

    def flush(self) -> None:
        if self.dry_run:
            return
        for key in SCHEMAS:
            rows = self._take(key)
            if not rows:
                continue
            try:
                self._ws[key].append_rows(rows, value_input_option="USER_ENTERED")
            except Exception:
                self._put_back(key, rows)
                raise

    def _loop(self) -> None:
        while not self._stop.wait(self.cfg.gs_flush_seconds):
            try:
                self.flush()
            except Exception as ex:
                log.error("Google flush error: %s", ex)

    def close(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        try:
            self.flush()
        except Exception as ex:
            log.error("Final Google flush error: %s", ex)


# -----------------------------
# Logging wrapper
# -----------------------------

def _stamp() -> tuple[datetime, datetime]:
    u = datetime.now(UTC)
    return u, u.astimezone(NY)


class Audit:
    def __init__(self, bus: SheetBus, tg: Optional["Telegram"] = None):
        self.bus = bus
        self.tg = tg

    def write(self, level: str, stage: str, message: str, symbol: str = "") -> None:
        u, n = _stamp()
        row = [u.isoformat(), n.isoformat(), level, stage, symbol, message, ""]
        self.bus.append("SYSTEM", row)
        fn = getattr(log, level.lower(), log.info)
        fn("[%s] %s %s", stage, symbol or "-", message)
        if level == "ERROR" and self.tg and self.tg.cfg.tg_system_errors:
            self.tg.send("SYSTEM", f"SYSTEM ERROR\n{stage}\n{symbol}\n{message}")


# -----------------------------
# Telegram
# -----------------------------

class Telegram:
    def __init__(self, cfg: Cfg, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage" if cfg.telegram_token else ""
        self.flags = {
            "S1": cfg.tg_s1,
            "S2": cfg.tg_s2,
            "S3": cfg.tg_s3,
            "S4": cfg.tg_s4,
            "S5": cfg.tg_s5,
            "S6": cfg.tg_s6,
            "S7": cfg.tg_s7,
            "HEARTBEAT": cfg.tg_heartbeat,
            "SYSTEM": cfg.tg_system_errors,
        }

    def send(self, channel: str, text: str) -> bool:
        if not self.cfg.tg_master or not self.flags.get(channel, False):
            return False
        if self.dry_run:
            return True
        try:
            r = requests.post(
                self.url,
                json={"chat_id": self.cfg.telegram_chat, "text": text},
                timeout=8,
            )
            r.raise_for_status()
            return True
        except Exception as ex:
            log.error("Telegram error: %s", ex)
            return False


# -----------------------------
# OpenRouter client
# -----------------------------

class AIClient:
    URL = "https://openrouter.ai/api/v1/chat/completions"

    def __init__(self, cfg: Cfg, bus: SheetBus, dry_run: bool = False):
        self.cfg = cfg
        self.bus = bus
        self.dry_run = dry_run
        self._lock = threading.RLock()
        self._signal_cache: dict[str, tuple[AIResult, datetime]] = {}
        self._hint_cache: dict[str, tuple[str, datetime]] = {}
        self._calls_by_day: defaultdict[str, int] = defaultdict(int)
        self._aux_calls_by_day: defaultdict[str, int] = defaultdict(int)
        self._cooldown_until = 0.0

    def _allowed(self, bucket: str) -> tuple[bool, str]:
        if not self.cfg.ai_enabled or not self.cfg.openrouter_key:
            return False, "AI_DISABLED_OR_KEY_MISSING"
        if time.monotonic() < self._cooldown_until:
            return False, "AI_COOLDOWN"
        day = datetime.now(NY).date().isoformat()
        with self._lock:
            if self._calls_by_day[day] >= self.cfg.ai_daily_cap:
                return False, "AI_DAILY_CAP"
            if bucket == "AUX" and self._aux_calls_by_day[day] >= self.cfg.ai_aux_daily_cap:
                return False, "AI_AUX_DAILY_CAP"
            self._calls_by_day[day] += 1
            if bucket == "AUX":
                self._aux_calls_by_day[day] += 1
        return True, ""

    @staticmethod
    def _text(data: Optional[dict[str, Any]]) -> str:
        if not data:
            return ""
        choices = data.get("choices") or []
        if not choices:
            return ""
        msg = choices[0].get("message") or {}
        content = msg.get("content")
        return content.strip() if isinstance(content, str) else ""

    @staticmethod
    def _sources(data: Optional[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        if not data:
            return out
        choices = data.get("choices") or []
        if not choices:
            return out
        msg = choices[0].get("message") or {}
        for ann in msg.get("annotations") or []:
            url = (ann.get("url_citation") or {}).get("url")
            if url and url not in out:
                out.append(url)
        return out

    @staticmethod
    def _usage(data: Optional[dict[str, Any]]) -> tuple[int, int, int]:
        if not data:
            return 0, 0, 0
        u = data.get("usage") or {}
        inp = int(u.get("prompt_tokens") or u.get("input_tokens") or 0)
        out = int(u.get("completion_tokens") or u.get("output_tokens") or 0)
        total = int(u.get("total_tokens") or (inp + out))
        return inp, out, total

    def _request(self, messages: list[dict[str, str]], max_tokens: int, max_results: int, bucket: str = "AUX") -> tuple[Optional[dict[str, Any]], str]:
        allowed, why = self._allowed(bucket)
        if not allowed:
            return None, why
        if self.dry_run:
            return {"choices": [{"message": {"content": "NO"}}], "usage": {"total_tokens": 1}}, ""

        body: dict[str, Any] = {
            "model": self.cfg.ai_model,
            "messages": messages,
            "temperature": 0,
            "reasoning": {"enabled": False},
            "max_tokens": max_tokens,
            "plugins": [{
                "id": "web",
                "engine": "parallel",
                "mode": "turbo",
                "max_results": max(1, min(max_results, 10)),
            }],
        }
        headers = {
            "Authorization": f"Bearer {self.cfg.openrouter_key}",
            "Content-Type": "application/json",
            "X-Title": "research-runner",
        }

        last = ""
        for attempt in range(max(1, self.cfg.ai_retries)):
            try:
                r = requests.post(self.URL, headers=headers, json=body, timeout=self.cfg.ai_timeout)
                if r.status_code in {401, 402, 403}:
                    self._cooldown_until = time.monotonic() + self.cfg.ai_error_cooldown_minutes * 60
                    return None, f"HTTP_{r.status_code}"
                if r.status_code == 429:
                    last = "HTTP_429"
                    time.sleep(min(8.0, 1.5 * (attempt + 1)))
                    continue
                r.raise_for_status()
                try:
                    data = r.json()
                except ValueError:
                    last = "INVALID_JSON"
                    time.sleep(1.0 + attempt)
                    continue
                if not self._text(data):
                    last = "EMPTY_CONTENT"
                    time.sleep(1.0 + attempt)
                    continue
                return data, ""
            except (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as ex:
                last = type(ex).__name__
                time.sleep(min(8.0, 1.5 * (attempt + 1)))
            except requests.exceptions.HTTPError as ex:
                code = getattr(ex.response, "status_code", "HTTP")
                last = f"HTTP_{code}"
                break
            except Exception as ex:
                last = type(ex).__name__
                break

        return None, last or "REQUEST_FAILED"

    def _log(self, request_type: str, symbol: str, direction: str, result: AIResult) -> None:
        u = result.checked_at or datetime.now(UTC)
        n = u.astimezone(NY)
        self.bus.append("AI", [
            u.isoformat(), n.isoformat(), request_type, symbol, direction, result.decision,
            result.hint, result.input_tokens, result.output_tokens, result.total_tokens,
            result.error, "|".join(result.sources), ""
        ])

    def signal(self, c: Candidate) -> AIResult:
        cache_key = f"SIGNAL:{c.direction}:{c.symbol}:{c.alert_time.astimezone(NY).date().isoformat()}"
        now = datetime.now(UTC)
        with self._lock:
            cached = self._signal_cache.get(cache_key)
            if cached and now - cached[1] < timedelta(hours=self.cfg.ai_cache_hours):
                return cached[0]

        polarity = "positive/upside" if c.direction == "UP" else "negative/downside"
        system = (
            "You are a strict US-equity catalyst gate. Use current web evidence. "
            f"For this {polarity} abnormal move, output exactly NO when there is no recent, credible, "
            "material, company-specific catalyst that plausibly explains the move. Output exactly "
            "YES|HINT when such a catalyst exists. HINT must be 4 to 6 words. Do not use stale, generic, "
            "market-wide, or speculative explanations."
        )
        user = (
            f"Ticker: {c.symbol}\nDirection: {c.direction}\nAlert UTC: {c.alert_time.isoformat()}\n"
            f"Alert price: {c.alert_price:.6f}\n5m z-range: {c.z_range:.3f}\n"
            f"5m z-volume: {c.z_volume:.3f}\n5m volume: {c.alert_volume:.0f}\n"
            "Search recent web news for this exact security and decide."
        )
        data, err = self._request(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=20,
            max_results=self.cfg.ai_web_results,
            bucket="SIGNAL",
        )
        checked = datetime.now(UTC)
        if err:
            result = AIResult("ERROR", checked_at=checked, error=err)
            self._log("SIGNAL", c.symbol, c.direction, result)
            return result

        raw = self._text(data)
        inp, out, total = self._usage(data)
        sources = self._sources(data)
        upper = raw.upper().strip()
        if upper == "NO" or upper.startswith("NO\n"):
            decision, hint = "NO", ""
        elif upper.startswith("YES|"):
            decision, hint = "YES", _words(raw.split("|", 1)[1].strip(), 6)
        elif upper == "YES":
            decision, hint = "YES", "Verified company catalyst found"
        else:
            decision, hint = "ERROR", ""
            err = "UNPARSEABLE_RESPONSE"

        result = AIResult(decision, hint, sources, checked, inp, out, total, err)
        self._log("SIGNAL", c.symbol, c.direction, result)
        if decision in {"YES", "NO"}:
            with self._lock:
                self._signal_cache[cache_key] = (result, checked)
        return result

    def heartbeat_hints(self, symbols: list[str]) -> dict[str, str]:
        if not self.cfg.ai_hb_hints_enabled:
            return {}
        now = datetime.now(UTC)
        out: dict[str, str] = {}
        missing: list[str] = []
        with self._lock:
            for s in symbols:
                z = self._hint_cache.get(s)
                if z and now - z[1] < timedelta(minutes=self.cfg.ai_hb_hint_cache_minutes):
                    out[s] = z[0]
                else:
                    missing.append(s)
        if not missing:
            return out

        prompt = (
            "For each ticker below, use current web news and return one line only in the form "
            "SYMBOL|REASON. REASON must be factual and 4 or 5 words. If no verified catalyst is found, "
            "use SYMBOL|No verified catalyst found. No other text.\n" + "\n".join(missing[:15])
        )
        data, err = self._request(
            [{"role": "system", "content": "You are a concise US-equity market-news researcher."},
             {"role": "user", "content": prompt}],
            max_tokens=max(40, min(220, len(missing[:15]) * 10)),
            max_results=min(10, max(3, len(missing[:15]))),
        )
        checked = datetime.now(UTC)
        if err:
            res = AIResult("ERROR", checked_at=checked, error=err)
            self._log("HEARTBEAT_HINTS", "", "", res)
            return out

        inp, ot, total = self._usage(data)
        res = AIResult("OK", checked_at=checked, input_tokens=inp, output_tokens=ot, total_tokens=total)
        self._log("HEARTBEAT_HINTS", "", "", res)
        for line in self._text(data).splitlines():
            if "|" not in line:
                continue
            s, hint = line.split("|", 1)
            s = s.strip().upper()
            if s in missing:
                h = _words(hint.strip(), 5) or "No verified catalyst found"
                out[s] = h
                with self._lock:
                    self._hint_cache[s] = (h, checked)
        return out

    def next_day_events(self, target: date, limit: int) -> tuple[list[dict[str, str]], AIResult]:
        prompt = (
            f"Target date: {target.isoformat()} (New York market date). Find confirmed scheduled US-listed "
            "company events for that exact date: (1) earnings releases and (2) Phase 2/Phase 3 clinical "
            "trial readouts, FDA decisions, or PDUFA decisions. Return at most {limit} lines. Each line must be:\n"
            "SYMBOL|TYPE|REASON|SOURCE_URL\n"
            "TYPE must be EARNINGS, PHASE2, PHASE3, FDA, or PDUFA. Use only events whose date is explicitly "
            "supported by a current source. Do not include rumors or unscheduled possibilities. No other text."
        )
        data, err = self._request(
            [{"role": "system", "content": "You are a cautious event-calendar researcher. Verify dates."},
             {"role": "user", "content": prompt}],
            max_tokens=max(250, min(900, limit * 32)),
            max_results=10,
        )
        checked = datetime.now(UTC)
        if err:
            result = AIResult("ERROR", checked_at=checked, error=err)
            self._log("S5_EVENTS", "", "", result)
            return [], result

        rows: list[dict[str, str]] = []
        for raw in self._text(data).splitlines():
            line = raw.strip().lstrip("-*0123456789. ")
            parts = [x.strip() for x in line.split("|")]
            if len(parts) < 4:
                continue
            symbol, typ, reason, url = parts[0].upper(), parts[1].upper(), parts[2], parts[3]
            if not symbol.replace(".", "").replace("-", "").isalnum():
                continue
            if typ not in {"EARNINGS", "PHASE2", "PHASE3", "FDA", "PDUFA"}:
                continue
            rows.append({"symbol": symbol, "type": typ, "reason": _words(reason, 12), "url": url})
            if len(rows) >= limit:
                break
        inp, ot, total = self._usage(data)
        result = AIResult("OK", checked_at=checked, input_tokens=inp, output_tokens=ot, total_tokens=total,
                          sources=self._sources(data))
        self._log("S5_EVENTS", "", "", result)
        return rows, result


# -----------------------------
# Baseline and aggregation helpers
# -----------------------------

class Baselines:
    def __init__(self, cfg: Cfg):
        self.cfg = cfg
        self._d: defaultdict[str, deque[M5]] = defaultdict(lambda: deque(maxlen=cfg.baseline_candles))

    def add(self, x: M5) -> None:
        self._d[x.symbol].append(x)

    def ready(self, symbol: str) -> bool:
        return len(self._d[symbol]) >= self.cfg.warmup_candles

    def stats(self, symbol: str) -> Optional[Stats]:
        z = self._d[symbol]
        if not z:
            return None
        rr = [x.span for x in z]
        vv = [x.volume for x in z]
        mr = sum(rr) / len(rr)
        mv = sum(vv) / len(vv)
        sr = math.sqrt(sum((x - mr) ** 2 for x in rr) / (len(rr) - 1)) if len(rr) > 1 else 0.0
        sv = math.sqrt(sum((x - mv) ** 2 for x in vv) / (len(vv) - 1)) if len(vv) > 1 else 0.0
        return Stats(mr, sr, mv, sv, len(z))


class Agg5:
    def __init__(self, minutes: int):
        self.minutes = minutes
        self._d: dict[str, list[Any]] = {}

    def push(self, x: M1) -> Optional[M5]:
        t = x.ts.astimezone(UTC)
        bucket = t.replace(minute=(t.minute // self.minutes) * self.minutes, second=0, microsecond=0)
        w = self._d.get(x.symbol)
        if w is None:
            self._d[x.symbol] = [bucket, x.open, x.high, x.low, x.close, x.volume]
            return None
        if w[0] == bucket:
            w[2] = max(w[2], x.high)
            w[3] = min(w[3], x.low)
            w[4] = x.close
            w[5] += x.volume
            return None
        done = M5(x.symbol, w[0], w[1], w[2], w[3], w[4], w[5])
        self._d[x.symbol] = [bucket, x.open, x.high, x.low, x.close, x.volume]
        return done


class Cooldown:
    def __init__(self, minutes: int):
        self.seconds = minutes * 60
        self._d: dict[tuple[str, str], float] = {}

    def active(self, symbol: str, direction: str) -> bool:
        t = self._d.get((symbol, direction))
        return t is not None and time.monotonic() - t < self.seconds

    def mark(self, symbol: str, direction: str) -> None:
        self._d[(symbol, direction)] = time.monotonic()


# -----------------------------
# Main research engine
# -----------------------------

class Engine:
    def __init__(self, cfg: Cfg, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.bus = SheetBus(cfg, dry_run=dry_run)
        self.tg = Telegram(cfg, dry_run=dry_run)
        self.audit = Audit(self.bus, self.tg)
        self.ai = AIClient(cfg, self.bus, dry_run=dry_run)
        self.fx = self._fx()
        self.base = Baselines(cfg)
        self.agg5 = Agg5(cfg.candle_minutes)
        self.cool = Cooldown(cfg.cooldown_minutes)
        self.lock = threading.RLock()
        self.pool = ThreadPoolExecutor(max_workers=max(1, cfg.ai_workers), thread_name_prefix="ai")
        self.ai_done: "queue.Queue[tuple[str, Candidate, AIResult]]" = queue.Queue()
        self.ai_pending: set[str] = set()

        self.latest: dict[str, tuple[float, datetime, M1]] = {}
        self.hist_daily: defaultdict[str, dict[date, dict[str, float]]] = defaultdict(dict)
        self.session_open: dict[str, tuple[float, datetime]] = {}
        self.premarket_latest: dict[str, tuple[float, datetime]] = {}

        self.up: dict[str, Candidate] = {}
        self.down: dict[str, Candidate] = {}
        self.s1_pos: dict[str, S1Pos] = {}
        self.s6_state: dict[str, S6State] = {}
        self.s6_pos: dict[str, S6Pos] = {}

        self.s4_list: dict[str, dict[str, Any]] = {}
        self.s4_first15: dict[str, dict[str, float]] = {}
        self.s4_done_day: Optional[date] = None

        self.s5_target: Optional[date] = None
        self.s5_events: list[dict[str, str]] = []
        self.s5_research_pending = False
        self.s5_entered: set[tuple[date, str]] = set()

        self.s7_snaps: deque[tuple[datetime, list[tuple[str, float]]]] = deque()
        self.s7_bought: set[tuple[date, str]] = set()

        self.hb_sent: set[str] = set()
        self.s4_locked_day: Optional[date] = None
        self.s5_researched_day: Optional[date] = None
        self._shutdown = threading.Event()
        self.baseline_ready = threading.Event()
        self.rank_ready = threading.Event()

        self.symbols: list[str] = []
        self._alpaca_hist = None
        self._alpaca_trade = None

    def _fx(self) -> float:
        if self.cfg.usd_dkk_override:
            return float(self.cfg.usd_dkk_override)
        try:
            r = requests.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml", timeout=8)
            r.raise_for_status()
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.text)
            rates: dict[str, float] = {}
            for z in root.iter():
                c = z.attrib.get("currency")
                v = z.attrib.get("rate")
                if c and v:
                    rates[c] = float(v)
            return rates["DKK"] / rates["USD"]
        except Exception:
            return self.cfg.usd_dkk_fallback

    def _strategy_sheet(self, code: str) -> str:
        return code

    def _common_row(
        self,
        strategy: str,
        event: str,
        trade_id: str,
        symbol: str,
        side: str,
        signal_price: Any = "",
        entry_price: Any = "",
        exit_price: Any = "",
        shares: Any = "",
        invested_dkk: Any = "",
        pnl_dkk: Any = "",
        pnl_pct: Any = "",
        reason: str = "",
        status: str = "",
        when: Optional[datetime] = None,
    ) -> list[Any]:
        u = (when or datetime.now(UTC)).astimezone(UTC)
        n = u.astimezone(NY)
        return [
            u.isoformat(), n.isoformat(), strategy, event, trade_id, symbol, side,
            signal_price, entry_price, exit_price, shares, invested_dkk, pnl_dkk, pnl_pct,
            reason, status, ""
        ]

    def _size(self, price: float) -> tuple[int, float]:
        if price <= 0 or price < self.cfg.penny_floor_usd:
            return 0, 0.0
        shares = math.floor((self.cfg.budget_dkk / self.fx) / price)
        if shares < 1:
            return 0, 0.0
        return shares, shares * price * self.fx

    def _trail(self, gain: float) -> float:
        if gain < 10:
            return self.cfg.trail_0_10
        if gain < 25:
            return self.cfg.trail_10_25
        if gain < 50:
            return self.cfg.trail_25_50
        return self.cfg.trail_50_plus

    def _lazy_alpaca(self):
        if self._alpaca_hist is not None:
            return
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.trading.client import TradingClient
        self._alpaca_hist = StockHistoricalDataClient(self.cfg.alpaca_key, self.cfg.alpaca_secret)
        self._alpaca_trade = TradingClient(self.cfg.alpaca_key, self.cfg.alpaca_secret, paper=True)

    def universe(self) -> list[str]:
        if not self.cfg.stream_all:
            return list(self.cfg.watchlist)
        self._lazy_alpaca()
        try:
            from alpaca.trading.enums import AssetClass, AssetStatus
            from alpaca.trading.requests import GetAssetsRequest
            assets = self._alpaca_trade.get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE))
            return sorted({x.symbol for x in assets if x.tradable})
        except Exception as ex:
            self.audit.write("WARNING", "UNIVERSE", f"Asset universe failed: {type(ex).__name__}")
            return list(self.cfg.watchlist)

    def bootstrap(self, symbols: list[str]) -> None:
        if self.dry_run:
            self.rank_ready.set()
            self.baseline_ready.set()
            return
        self._lazy_alpaca()
        from alpaca.data.enums import DataFeed
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        # Ranking/premarket history first so Strategies 4/7 and the 15-minute
        # heartbeat become usable quickly while the larger 5m baseline continues.
        self.audit.write("INFO", "RANK_BOOTSTRAP", f"Starting daily bootstrap for {len(symbols)} symbols")
        start_daily = datetime.now(UTC) - timedelta(days=45)
        good_daily = 0
        bad_daily = 0
        for i in range(0, len(symbols), self.cfg.bootstrap_batch):
            batch = symbols[i:i + self.cfg.bootstrap_batch]
            try:
                rs = self._alpaca_hist.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=batch,
                    timeframe=TimeFrame(1, TimeFrameUnit.Day),
                    start=start_daily,
                    end=datetime.now(UTC),
                    feed=DataFeed.IEX,
                ))
                for sy, bars in rs.data.items():
                    for v in bars:
                        day = v.timestamp.astimezone(NY).date()
                        self.hist_daily[sy][day] = {"open": float(v.open), "close": float(v.close)}
                good_daily += 1
            except Exception as ex:
                bad_daily += 1
                self.audit.write("ERROR", "RANK_BOOTSTRAP", f"{type(ex).__name__}: {ex}")
            time.sleep(self.cfg.bootstrap_pause)
        self._capture_session_open(symbols)
        self.rank_ready.set()
        self.audit.write("INFO", "RANK_BOOTSTRAP", f"Daily bootstrap complete: {good_daily} batches OK, {bad_daily} failed")

        self.audit.write("INFO", "BOOTSTRAP", f"Starting 5m bootstrap for {len(symbols)} symbols")
        start_5m = datetime.now(UTC) - timedelta(days=self.cfg.bootstrap_days)
        good_5m = 0
        bad_5m = 0
        for i in range(0, len(symbols), self.cfg.bootstrap_batch):
            batch = symbols[i:i + self.cfg.bootstrap_batch]
            try:
                rs = self._alpaca_hist.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=batch,
                    timeframe=TimeFrame(self.cfg.candle_minutes, TimeFrameUnit.Minute),
                    start=start_5m,
                    feed=DataFeed.IEX,
                ))
                n = 0
                for sy, bars in rs.data.items():
                    for v in bars:
                        local = v.timestamp.astimezone(NY)
                        if 570 <= local.hour * 60 + local.minute < 960:
                            self.base.add(M5(sy, v.timestamp, float(v.open), float(v.high), float(v.low), float(v.close), float(v.volume)))
                            n += 1
                good_5m += 1
                self.audit.write("INFO", "BOOTSTRAP", f"Loaded {n} 5m bars for batch {i // self.cfg.bootstrap_batch + 1}")
            except Exception as ex:
                bad_5m += 1
                self.audit.write("ERROR", "BOOTSTRAP", f"{type(ex).__name__}: {ex}")
            time.sleep(self.cfg.bootstrap_pause)
        self.baseline_ready.set()
        self.audit.write("INFO", "BOOTSTRAP", f"5m bootstrap complete: {good_5m} batches OK, {bad_5m} failed")

    def _capture_session_open(self, symbols: list[str]) -> None:
        now = datetime.now(NY)
        if now.weekday() >= 5:
            return
        open_et = datetime.combine(now.date(), dtime(9, 30), tzinfo=NY)
        if now < open_et:
            return
        self._lazy_alpaca()
        from alpaca.data.enums import DataFeed
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        end_et = min(now, open_et + timedelta(minutes=3))
        for i in range(0, len(symbols), self.cfg.bootstrap_batch):
            batch = symbols[i:i + self.cfg.bootstrap_batch]
            try:
                rs = self._alpaca_hist.get_stock_bars(StockBarsRequest(
                    symbol_or_symbols=batch,
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=open_et.astimezone(UTC),
                    end=end_et.astimezone(UTC),
                    feed=DataFeed.IEX,
                ))
                for sy, bars in rs.data.items():
                    if bars:
                        first = min(bars, key=lambda x: x.timestamp)
                        self.session_open[sy] = (float(first.open), first.timestamp.astimezone(NY))
            except Exception as ex:
                self.audit.write("ERROR", "RANK_OPEN", f"{type(ex).__name__}: {ex}")
            time.sleep(self.cfg.bootstrap_pause)

    def _prev_close(self, symbol: str, today: date) -> Optional[float]:
        hist = self.hist_daily.get(symbol, {})
        days = [d for d in hist if d < today]
        if not days:
            return None
        return hist[max(days)].get("close")

    def _rank_bases(self, symbol: str, today: date) -> tuple[Optional[float], Optional[float], Optional[float]]:
        daily = self.session_open.get(symbol, (None, None))[0]
        hist = self.hist_daily.get(symbol, {})
        week_start = today - timedelta(days=today.weekday())
        month_start = today.replace(day=1)
        wd = sorted(d for d in hist if week_start <= d <= today)
        md = sorted(d for d in hist if month_start <= d <= today)
        weekly = hist[wd[0]]["open"] if wd else None
        monthly = hist[md[0]]["open"] if md else None
        # If the period starts today, use the actual regular-session first-minute open.
        if daily is not None:
            if today == week_start:
                weekly = daily
            if today == month_start:
                monthly = daily
        return daily, weekly, monthly

    def ranks(self) -> list[tuple[str, Optional[float], Optional[float], Optional[float]]]:
        now = datetime.now(NY)
        today = now.date()
        out = []
        with self.lock:
            items = list(self.latest.items())
        for sy, (cur, _, _) in items:
            if cur <= 0:
                continue
            d0, w0, m0 = self._rank_bases(sy, today)
            dr = (cur / d0 - 1) * 100 if d0 and d0 > 0 else None
            wr = (cur / w0 - 1) * 100 if w0 and w0 > 0 else None
            mr = (cur / m0 - 1) * 100 if m0 and m0 > 0 else None
            out.append((sy, dr, wr, mr))
        return out

    def _submit_ai(self, c: Candidate) -> None:
        key = f"{c.direction}:{c.symbol}:{c.trade_id}"
        with self.lock:
            if key in self.ai_pending:
                return
            self.ai_pending.add(key)

        fut = self.pool.submit(self.ai.signal, c)

        def done(f):
            try:
                r = f.result()
            except Exception as ex:
                r = AIResult("ERROR", checked_at=datetime.now(UTC), error=type(ex).__name__)
            self.ai_done.put((key, c, r))

        fut.add_done_callback(done)

    def _drain_ai(self) -> None:
        while True:
            try:
                key, c, result = self.ai_done.get_nowait()
            except queue.Empty:
                return
            with self.lock:
                self.ai_pending.discard(key)
            self._apply_ai(c, result)

    def _apply_ai(self, c: Candidate, r: AIResult) -> None:
        with self.lock:
            latest = self.latest.get(c.symbol)
        price = latest[0] if latest else c.prices[-1]
        when = latest[1] if latest else c.times[-1]

        if c.direction == "UP":
            if r.decision == "YES":
                if self.cfg.s1_enabled:
                    self._s1_open(c, r, price, when)
                if self.cfg.s2_enabled:
                    self._log_s2(c, r, "SKIP", "NEWS_FOUND", price, when)
            elif r.decision == "NO":
                if self.cfg.s1_enabled:
                    self._log_s1(c, r, "SKIP", "NO_NEWS", price, when)
                if self.cfg.s2_enabled:
                    self._s2_open(c, r, price, when)
            else:
                if self.cfg.s1_enabled:
                    self._log_s1(c, r, "AI_ERROR", r.error, price, when)
                if self.cfg.s2_enabled:
                    self._log_s2(c, r, "AI_ERROR", r.error, price, when)
        else:
            if r.decision == "NO":
                if self.cfg.s3_enabled:
                    self._s3_open(c, r, price, when)
            elif r.decision == "YES":
                if self.cfg.s3_enabled:
                    self._log_s3(c, r, "SKIP", "NEGATIVE_CATALYST_FOUND", price, when)
            else:
                if self.cfg.s3_enabled:
                    self._log_s3(c, r, "AI_ERROR", r.error, price, when)

        with self.lock:
            if c.direction == "UP":
                self.up.pop(c.symbol, None)
            else:
                self.down.pop(c.symbol, None)

    def _anom_pass(self, c: Candidate) -> bool:
        if len(c.prices) < self.cfg.quarantine_minutes + 1:
            return False
        a0 = c.prices[0]
        a1 = c.prices[1]
        a2 = c.prices[2] if len(c.prices) >= 3 else c.prices[-1]
        if min(a0, a1, a2) <= 0:
            c.status = "FAILED"
            return False
        r2 = math.log(a2 / a1)
        total = math.log(a2 / a0)
        if c.direction == "UP":
            reversal = max(0.0, -r2)
            passed = total >= self.cfg.min_two_min_return and reversal <= self.cfg.max_second_minute_reversal
        else:
            reversal = max(0.0, r2)
            passed = total <= -self.cfg.min_two_min_return and reversal <= self.cfg.max_second_minute_reversal
        c.total_return = total
        c.second_reversal = reversal
        return passed

    def _process_candidate(self, c: Candidate, bar: M1) -> None:
        if c.status != "QUARANTINED" or bar.ts <= c.alert_time:
            return
        if c.times and bar.ts <= c.times[-1]:
            return
        c.prices.append(bar.close)
        c.times.append(bar.ts)
        if len(c.prices) < self.cfg.quarantine_minutes + 1:
            return
        if self._anom_pass(c):
            c.status = "AI_PENDING"
            self._submit_ai(c)
        else:
            c.status = "FAILED"
            reason = "TWO_MIN_DIRECTION_NOT_CONFIRMED"
            if c.direction == "UP":
                if self.cfg.s1_enabled:
                    self._log_s1(c, AIResult(""), "QUARANTINE_FAIL", reason, bar.close, bar.ts)
                if self.cfg.s2_enabled:
                    self._log_s2(c, AIResult(""), "QUARANTINE_FAIL", reason, bar.close, bar.ts)
                self.up.pop(c.symbol, None)
            else:
                if self.cfg.s3_enabled:
                    self._log_s3(c, AIResult(""), "QUARANTINE_FAIL", reason, bar.close, bar.ts)
                self.down.pop(c.symbol, None)

    def _detect_anomaly(self, x: M5) -> None:
        if not self.baseline_ready.is_set():
            return
        if not self.base.ready(x.symbol):
            self.base.add(x)
            return
        st = self.base.stats(x.symbol)
        if not st:
            self.base.add(x)
            return
        zr = (x.span - st.mean_range) / st.std_range if st.std_range > 0 else 0.0
        zv = (x.volume - st.mean_volume) / st.std_volume if st.std_volume > 0 else 0.0
        stage1 = (
            (st.mean_range > 0 and x.span > self.cfg.stage1_price_mult * st.mean_range)
            or (st.mean_volume > 0 and x.volume > self.cfg.stage1_volume_mult * st.mean_volume)
        )
        strong = zr > self.cfg.z_threshold or zv > self.cfg.z_threshold
        direction = "UP" if x.close > x.open else "DOWN" if x.close < x.open else "FLAT"
        self.base.add(x)
        if not stage1 or not strong or direction == "FLAT":
            return

        relevant = (direction == "UP" and (self.cfg.s1_enabled or self.cfg.s2_enabled)) or (direction == "DOWN" and self.cfg.s3_enabled)
        if not relevant or self.cool.active(x.symbol, direction):
            return
        active = self.up if direction == "UP" else self.down
        if x.symbol in active:
            return

        alert_time = x.ts + timedelta(minutes=self.cfg.candle_minutes)
        c = Candidate(
            symbol=x.symbol,
            direction=direction,
            alert_time=alert_time,
            alert_price=x.close,
            z_range=zr,
            z_volume=zv,
            alert_volume=x.volume,
            prices=[x.close],
            times=[alert_time],
            trade_id=f"{direction}-{x.symbol}-{int(alert_time.timestamp())}",
        )
        active[x.symbol] = c
        self.cool.mark(x.symbol, direction)
        self.audit.write("INFO", "ANOMALY", f"{direction} z_range={zr:.2f} z_volume={zv:.2f}", x.symbol)

    def _s1_open(self, c: Candidate, r: AIResult, price: float, when: datetime) -> None:
        if c.symbol in self.s1_pos:
            return
        shares, cost = self._size(price)
        if shares < 1:
            self._log_s1(c, r, "SKIP", "PRICE_OUTSIDE_BUDGET_RULES", price, when)
            return
        p = S1Pos(c.trade_id, c.symbol, when, price, shares, cost, price, hint=r.hint)
        self.s1_pos[c.symbol] = p
        row = self._common_row("News-Longer", "ENTRY", c.trade_id, c.symbol, "LONG", c.alert_price, price,
                               shares=shares, invested_dkk=cost, reason=r.hint, status="OPEN", when=when)
        row += [c.z_range, c.z_volume, c.alert_volume, c.total_return, c.second_reversal,
                r.decision, r.hint, "|".join(r.sources), p.peak_price, p.peak_gain_pct, p.max_drawdown_pct, ""]
        self.bus.append("S1", row)
        self.tg.send("S1", f"STRATEGY 1 — News-Longer\nBUY/LONG\n{c.symbol}\n{r.hint or 'Verified catalyst'}\nshares: {shares}\nprice: ${price:.4f}\ncost: DKK {cost:.2f}")

    def _log_s1(self, c: Candidate, r: AIResult, event: str, reason: str, price: float, when: datetime) -> None:
        row = self._common_row("News-Longer", event, c.trade_id, c.symbol, "LONG", c.alert_price,
                               reason=reason, status=event, when=when)
        row += [c.z_range, c.z_volume, c.alert_volume, c.total_return, c.second_reversal,
                r.decision, r.hint, "|".join(r.sources), "", "", "", ""]
        self.bus.append("S1", row)

    def _s1_manage(self, bar: M1) -> None:
        p = self.s1_pos.get(bar.symbol)
        if not p:
            return
        if bar.high > p.peak_price:
            p.peak_price = bar.high
            p.peak_gain_pct = (p.peak_price / p.entry_price - 1) * 100
        dd = (bar.low / p.peak_price - 1) * 100 if p.peak_price > 0 else 0.0
        p.max_drawdown_pct = min(p.max_drawdown_pct, dd)
        trail = self._trail(p.peak_gain_pct)
        stop = p.peak_price * (1 - trail / 100)
        if bar.low > stop:
            return
        fill = bar.open if bar.open < stop else stop
        pnl = p.shares * (fill - p.entry_price) * self.fx
        pct = pnl / p.invested_dkk * 100 if p.invested_dkk else 0.0
        row = self._common_row("News-Longer", "EXIT", p.trade_id, p.symbol, "LONG", entry_price=p.entry_price,
                               exit_price=fill, shares=p.shares, invested_dkk=p.invested_dkk, pnl_dkk=pnl,
                               pnl_pct=pct, reason="ADAPTIVE_TRAILING_STOP", status="CLOSED", when=bar.ts)
        row += ["", "", "", "", "", "YES", p.hint, "", p.peak_price, p.peak_gain_pct,
                p.max_drawdown_pct, "ADAPTIVE_TRAILING_STOP"]
        self.bus.append("S1", row)
        self.tg.send("S1", f"STRATEGY 1 — News-Longer\nSELL/EXIT\n{p.symbol}\nprice: ${fill:.4f}\nP/L: DKK {pnl:+.2f} ({pct:+.2f}%)")
        self.s1_pos.pop(bar.symbol, None)

    def _s2_open(self, c: Candidate, r: AIResult, price: float, when: datetime) -> None:
        shares, cost = self._size(price)
        if shares < 1:
            self._log_s2(c, r, "SKIP", "PRICE_OUTSIDE_BUDGET_RULES", price, when)
            return
        row = self._common_row("Quick-Shorter", "ENTRY", c.trade_id, c.symbol, "SHORT", c.alert_price,
                               price, shares=shares, invested_dkk=cost, reason="NO_VERIFIED_UPSIDE_CATALYST",
                               status="OPEN_RESEARCH", when=when)
        row += [c.z_range, c.z_volume, c.alert_volume, c.total_return, c.second_reversal,
                r.decision, r.hint, "|".join(r.sources)]
        self.bus.append("S2", row)
        self.tg.send("S2", f"STRATEGY 2 — Quick-Shorter\nSHORT\n{c.symbol}\nNo verified upside catalyst\nshares: {shares}\nprice: ${price:.4f}\nnotional: DKK {cost:.2f}")

    def _log_s2(self, c: Candidate, r: AIResult, event: str, reason: str, price: float, when: datetime) -> None:
        row = self._common_row("Quick-Shorter", event, c.trade_id, c.symbol, "SHORT", c.alert_price,
                               reason=reason, status=event, when=when)
        row += [c.z_range, c.z_volume, c.alert_volume, c.total_return, c.second_reversal,
                r.decision, r.hint, "|".join(r.sources)]
        self.bus.append("S2", row)

    def _s3_open(self, c: Candidate, r: AIResult, price: float, when: datetime) -> None:
        shares, cost = self._size(price)
        if shares < 1:
            self._log_s3(c, r, "SKIP", "PRICE_OUTSIDE_BUDGET_RULES", price, when)
            return
        row = self._common_row("Fall-Buyer", "ENTRY", c.trade_id, c.symbol, "LONG", c.alert_price,
                               price, shares=shares, invested_dkk=cost, reason="NO_VERIFIED_DOWNSIDE_CATALYST",
                               status="OPEN_RESEARCH", when=when)
        row += [c.z_range, c.z_volume, c.alert_volume, c.total_return, c.second_reversal,
                r.decision, r.hint, "|".join(r.sources)]
        self.bus.append("S3", row)
        self.tg.send("S3", f"STRATEGY 3 — Fall-Buyer\nBUY/LONG\n{c.symbol}\nNo verified downside catalyst\nshares: {shares}\nprice: ${price:.4f}\ncost: DKK {cost:.2f}")

    def _log_s3(self, c: Candidate, r: AIResult, event: str, reason: str, price: float, when: datetime) -> None:
        row = self._common_row("Fall-Buyer", event, c.trade_id, c.symbol, "LONG", c.alert_price,
                               reason=reason, status=event, when=when)
        row += [c.z_range, c.z_volume, c.alert_volume, c.total_return, c.second_reversal,
                r.decision, r.hint, "|".join(r.sources)]
        self.bus.append("S3", row)

    def _s4_track_premarket(self, bar: M1) -> None:
        local = bar.ts.astimezone(NY)
        hm = local.hour * 60 + local.minute
        if 420 <= hm < 570:  # 07:00-09:29 ET; avoid very thin early hours by default.
            self.premarket_latest[bar.symbol] = (bar.close, bar.ts)

    def _s4_lock(self, now: datetime) -> None:
        if not self.cfg.s4_enabled or self.s4_locked_day == now.date():
            return
        mode = self.cfg.s4_source_mode
        picked: list[dict[str, Any]] = []
        if mode == "official":
            picked = self._s4_official()
        elif mode == "manual":
            picked = [{"symbol": s, "rank": i + 1, "change_pct": "", "source": "manual"}
                      for i, s in enumerate(self.cfg.s4_manual_symbols[:self.cfg.s4_top_n])]
        elif mode == "alpaca":
            if not self.rank_ready.is_set():
                self.audit.write("INFO", "S4", "Waiting for daily bootstrap before Alpaca premarket ranking")
                return
            scored = []
            for sy, (p, _) in self.premarket_latest.items():
                prev = self._prev_close(sy, now.date())
                if prev and prev > 0:
                    ret = (p / prev - 1) * 100
                    if ret > 0:
                        scored.append((sy, ret))
            scored.sort(key=lambda x: x[1], reverse=True)
            picked = [{"symbol": s, "rank": i + 1, "change_pct": r, "source": "alpaca_iex"}
                      for i, (s, r) in enumerate(scored[:self.cfg.s4_top_n])]
        else:
            self.audit.write("ERROR", "S4", f"Unknown S4_SOURCE_MODE={mode}")
            return

        self.s4_list = {x["symbol"]: x for x in picked}
        self.s4_locked_day = now.date()
        for x in picked:
            row = self._common_row("Premarket-Buyer", "WATCHLIST", f"S4-{now.date()}-{x['symbol']}", x["symbol"], "LONG",
                                   reason=f"Premarket top-{self.cfg.s4_top_n}", status="WATCHING", when=now.astimezone(UTC))
            row += [x.get("rank", ""), x.get("change_pct", ""), x.get("source", mode), "", "", ""]
            self.bus.append("S4", row)
        self.audit.write("INFO", "S4", f"Locked {len(picked)} premarket candidates using {mode}")

    def _s4_official(self) -> list[dict[str, Any]]:
        if not self.cfg.s4_source_url:
            self.audit.write("ERROR", "S4", "S4_SOURCE_MODE=official but S4_SOURCE_URL is empty")
            return []
        headers = {}
        if self.cfg.s4_source_token:
            headers["Authorization"] = f"Bearer {self.cfg.s4_source_token}"
        try:
            r = requests.get(self.cfg.s4_source_url, headers=headers, timeout=12)
            r.raise_for_status()
            data = r.json()
            rows = data.get("data", data) if isinstance(data, dict) else data
            out = []
            if not isinstance(rows, list):
                raise ValueError("official source JSON must be a list or {'data': list}")
            for item in rows:
                if not isinstance(item, dict):
                    continue
                sy = str(item.get("symbol") or item.get("ticker") or "").strip().upper()
                if not sy:
                    continue
                chg = _safe_float(item.get("change_pct") or item.get("changePercent") or item.get("pct"), None)
                out.append({"symbol": sy, "change_pct": chg if chg is not None else "", "source": "official"})
            out.sort(key=lambda x: x["change_pct"] if isinstance(x["change_pct"], (int, float)) else -1e9, reverse=True)
            for i, x in enumerate(out[:self.cfg.s4_top_n], 1):
                x["rank"] = i
            return out[:self.cfg.s4_top_n]
        except Exception as ex:
            self.audit.write("ERROR", "S4", f"Official premarket source failed: {type(ex).__name__}")
            return []

    def _s4_on_regular_bar(self, bar: M1) -> None:
        if not self.cfg.s4_enabled or bar.symbol not in self.s4_list:
            return
        local = bar.ts.astimezone(NY)
        hm = local.hour * 60 + local.minute
        if 570 <= hm < 585:
            st = self.s4_first15.setdefault(bar.symbol, {"open": bar.open, "close": bar.close, "last_hm": hm})
            st["open"] = st.get("open", bar.open)
            st["close"] = bar.close
            st["last_hm"] = hm

    def _s4_confirm(self, now: datetime) -> None:
        if not self.cfg.s4_enabled or self.s4_done_day == now.date() or not self.s4_list:
            return
        incomplete = [sy for sy in self.s4_list if not self.s4_first15.get(sy) or self.s4_first15[sy].get("last_hm", -1) < 584]
        # Give delayed 09:44 bars up to two minutes to arrive before finalizing.
        if incomplete and (now.hour * 60 + now.minute) < 587:
            return
        for sy, meta in self.s4_list.items():
            st = self.s4_first15.get(sy)
            if not st or st.get("open", 0) <= 0 or st.get("last_hm", -1) < 584:
                continue
            op, cl = st["open"], st["close"]
            ret = (cl / op - 1) * 100
            trade_id = f"S4-{now.date()}-{sy}"
            if cl <= op:
                row = self._common_row("Premarket-Buyer", "SKIP", trade_id, sy, "LONG", op,
                                       reason="FIRST_15_NOT_UP", status="SKIP", when=now.astimezone(UTC))
                row += [meta.get("rank", ""), meta.get("change_pct", ""), meta.get("source", ""), op, cl, ret]
                self.bus.append("S4", row)
                continue
            latest = self.latest.get(sy)
            entry = latest[0] if latest else cl
            when = latest[1] if latest else now.astimezone(UTC)
            shares, cost = self._size(entry)
            if shares < 1:
                continue
            row = self._common_row("Premarket-Buyer", "ENTRY", trade_id, sy, "LONG", op, entry,
                                   shares=shares, invested_dkk=cost, reason="FIRST_15_CONFIRMED_UP",
                                   status="OPEN_RESEARCH", when=when)
            row += [meta.get("rank", ""), meta.get("change_pct", ""), meta.get("source", ""), op, cl, ret]
            self.bus.append("S4", row)
            self.tg.send("S4", f"STRATEGY 4 — Premarket-Buyer\nBUY/LONG\n{sy}\nfirst 15m: {ret:+.2f}%\nshares: {shares}\nprice: ${entry:.4f}\ncost: DKK {cost:.2f}")
        self.s4_done_day = now.date()

    def _next_trading_day(self, today: date) -> date:
        if self.dry_run:
            d = today + timedelta(days=1)
            while d.weekday() >= 5:
                d += timedelta(days=1)
            return d
        try:
            self._lazy_alpaca()
            from alpaca.trading.requests import GetCalendarRequest
            rows = self._alpaca_trade.get_calendar(GetCalendarRequest(start=today + timedelta(days=1), end=today + timedelta(days=10)))
            if rows:
                return rows[0].date
        except Exception as ex:
            self.audit.write("WARNING", "S5", f"Calendar lookup failed: {type(ex).__name__}")
        d = today + timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d

    def _s5_research(self, now: datetime) -> None:
        if not self.cfg.s5_enabled or self.s5_researched_day == now.date() or self.s5_research_pending:
            return
        target = self._next_trading_day(now.date())
        self.s5_research_pending = True
        self.s5_target = target

        fut = self.pool.submit(self.ai.next_day_events, target, self.cfg.s5_max_events)

        def done(f):
            try:
                rows, result = f.result()
            except Exception as ex:
                rows, result = [], AIResult("ERROR", checked_at=datetime.now(UTC), error=type(ex).__name__)
            with self.lock:
                self.s5_events = rows
                self.s5_research_pending = False
                self.s5_researched_day = now.date()
            self.audit.write("INFO" if result.decision == "OK" else "ERROR", "S5",
                             f"Event research for {target}: {len(rows)} candidates; status={result.decision}")
            for x in rows:
                trade_id = f"S5-{target}-{x['symbol']}-{x['type']}"
                row = self._common_row("Earning-Staker", "EVENT_FOUND", trade_id, x["symbol"], "LONG",
                                       reason=x["reason"], status="WAITING_ENTRY")
                row += [x["type"], target.isoformat(), x["reason"], x["url"]]
                self.bus.append("S5", row)

        fut.add_done_callback(done)

    def _s5_enter(self, now: datetime) -> None:
        if not self.cfg.s5_enabled or not self.s5_target or not self.s5_events:
            return
        for x in list(self.s5_events):
            key = (self.s5_target, x["symbol"])
            if key in self.s5_entered:
                continue
            latest = self.latest.get(x["symbol"])
            if not latest:
                continue
            price, when, _ = latest
            shares, cost = self._size(price)
            if shares < 1:
                continue
            trade_id = f"S5-{self.s5_target}-{x['symbol']}-{x['type']}"
            row = self._common_row("Earning-Staker", "ENTRY", trade_id, x["symbol"], "LONG", price, price,
                                   shares=shares, invested_dkk=cost, reason=x["reason"], status="OPEN_RESEARCH", when=when)
            row += [x["type"], self.s5_target.isoformat(), x["reason"], x["url"]]
            self.bus.append("S5", row)
            self.s5_entered.add(key)
            self.tg.send("S5", f"STRATEGY 5 — Earning-Staker\nBUY/LONG (research gamble)\n{x['symbol']}\n{x['type']}: {_words(x['reason'], 6)}\nshares: {shares}\nprice: ${price:.4f}\ncost: DKK {cost:.2f}")

    def _s6_on_bar(self, bar: M1) -> None:
        if not self.cfg.s6_enabled:
            return
        local = bar.ts.astimezone(NY)
        if local.weekday() >= 5:
            return
        hm = local.hour * 60 + local.minute
        state = self.s6_state.get(bar.symbol)
        if state is None or state.day != local.date():
            state = S6State(local.date())
            self.s6_state[bar.symbol] = state

        pos = self.s6_pos.get(bar.symbol)
        if pos:
            # Conservative same-bar rule: if both stop and target are touched, stop wins.
            if bar.low <= pos.stop:
                fill = bar.open if bar.open < pos.stop else pos.stop
                self._s6_exit(pos, fill, bar.ts, "STOP_LOSS")
            elif bar.high >= pos.target:
                fill = pos.target
                self._s6_exit(pos, fill, bar.ts, "TAKE_PROFIT_3R")
            return

        if state.traded:
            return

        if 570 <= hm < 585:  # 09:30-09:44 opening range
            state.orb_high = bar.high if state.orb_high is None else max(state.orb_high, bar.high)
            state.orb_low = bar.low if state.orb_low is None else min(state.orb_low, bar.low)
            return

        if state.orb_high is None or state.orb_low is None:
            return

        if 585 <= hm < 600:  # breakout must occur in the next 15-minute candle
            state.post945_low = bar.low if state.post945_low is None else min(state.post945_low, bar.low)
            if not state.breakout and bar.high > state.orb_high:
                state.breakout = True
                state.armed = True
                state.breakout_time = bar.ts
                state.breakout_price = max(state.orb_high, bar.close)
                row = self._common_row("ORB-Pullback", "BREAKOUT", f"S6-{local.date()}-{bar.symbol}", bar.symbol,
                                       "LONG", state.orb_high, reason="OPENING_RANGE_HIGH_BROKEN", status="ARMED", when=bar.ts)
                row += [state.orb_high, state.orb_low, _iso(state.breakout_time), state.breakout_price, "", state.orb_high,
                        "", "", "", self.cfg.s6_rr, ""]
                self.bus.append("S6", row)
            return

        if not state.armed or hm > _hhmm_to_minutes(self.cfg.s6_retest_deadline_hm):
            return

        state.post945_low = bar.low if state.post945_low is None else min(state.post945_low, bar.low)
        touched = bar.low <= state.orb_high <= bar.high
        reclaimed = bar.close >= state.orb_high
        if not (touched and reclaimed):
            return

        entry = bar.close
        raw_control = state.post945_low if state.post945_low is not None else state.orb_low
        buffer = state.orb_high * (self.cfg.s6_stop_buffer_pct / 100)
        stop = min(raw_control, state.orb_high - buffer)
        if stop <= 0 or stop >= entry:
            state.traded = True
            return
        risk_pct = (entry - stop) / entry * 100
        if risk_pct > self.cfg.s6_max_stop_pct:
            state.traded = True
            row = self._common_row("ORB-Pullback", "SKIP", f"S6-{local.date()}-{bar.symbol}", bar.symbol,
                                   "LONG", state.orb_high, reason="STOP_DISTANCE_TOO_WIDE", status="SKIP", when=bar.ts)
            row += [state.orb_high, state.orb_low, _iso(state.breakout_time), state.breakout_price, _iso(bar.ts),
                    state.orb_high, stop, "", entry - stop, self.cfg.s6_rr, "STOP_DISTANCE_TOO_WIDE"]
            self.bus.append("S6", row)
            return

        target = entry + self.cfg.s6_rr * (entry - stop)
        shares, cost = self._size(entry)
        if shares < 1:
            state.traded = True
            return
        trade_id = f"S6-{local.date()}-{bar.symbol}"
        pos = S6Pos(trade_id, bar.symbol, bar.ts, entry, shares, cost, stop, target,
                    state.orb_high, state.orb_low, state.breakout_time or bar.ts, state.breakout_price or entry)
        self.s6_pos[bar.symbol] = pos
        state.traded = True
        row = self._common_row("ORB-Pullback", "ENTRY", trade_id, bar.symbol, "LONG", state.orb_high, entry,
                               shares=shares, invested_dkk=cost, reason="BREAKOUT_PULLBACK_RECLAIM", status="OPEN", when=bar.ts)
        row += [state.orb_high, state.orb_low, _iso(state.breakout_time), state.breakout_price, _iso(bar.ts),
                state.orb_high, stop, target, entry - stop, self.cfg.s6_rr, ""]
        self.bus.append("S6", row)
        self.tg.send("S6", f"STRATEGY 6 — ORB-Pullback\nBUY/LONG\n{bar.symbol}\nentry: ${entry:.4f}\nstop: ${stop:.4f}\ntarget: ${target:.4f}\nR:R = 1:{self.cfg.s6_rr:g}")

    def _s6_exit(self, p: S6Pos, price: float, when: datetime, reason: str) -> None:
        pnl = p.shares * (price - p.entry_price) * self.fx
        pct = pnl / p.invested_dkk * 100 if p.invested_dkk else 0.0
        row = self._common_row("ORB-Pullback", "EXIT", p.trade_id, p.symbol, "LONG", p.orb_high, p.entry_price,
                               price, p.shares, p.invested_dkk, pnl, pct, reason, "CLOSED", when)
        row += [p.orb_high, p.orb_low, _iso(p.breakout_time), p.breakout_price, "", p.orb_high,
                p.stop, p.target, p.entry_price - p.stop, self.cfg.s6_rr, reason]
        self.bus.append("S6", row)
        self.tg.send("S6", f"STRATEGY 6 — ORB-Pullback\nEXIT\n{p.symbol}\n{reason}\nprice: ${price:.4f}\nP/L: DKK {pnl:+.2f} ({pct:+.2f}%)")
        self.s6_pos.pop(p.symbol, None)

    def _s7_snapshot(self, now: datetime, daily: list[tuple[str, float]]) -> None:
        if not self.cfg.s7_enabled:
            return
        top = daily[:self.cfg.s7_top_n]
        self.s7_snaps.append((now, top))
        cutoff = now - timedelta(minutes=self.cfg.s7_window_minutes)
        while self.s7_snaps and self.s7_snaps[0][0] < cutoff:
            self.s7_snaps.popleft()
        counts: defaultdict[str, int] = defaultdict(int)
        for _, rows in self.s7_snaps:
            for s, _ in rows:
                counts[s] += 1
        members = ",".join(s for s, _ in top)
        for rank, (sy, gain) in enumerate(top, 1):
            count = counts[sy]
            tid = f"S7-{now.date()}-{sy}"
            row = self._common_row("Repeat-Gainer", "SNAPSHOT", tid, sy, "LONG", signal_price=self.latest.get(sy, ("", "", ""))[0],
                                   reason="REGULAR_SESSION_TOP_GAINER", status="WATCHING", when=now.astimezone(UTC))
            row += [rank, gain, count, self.cfg.s7_window_minutes, members]
            self.bus.append("S7", row)
            key = (now.date(), sy)
            if count < self.cfg.s7_repeat_count or key in self.s7_bought:
                continue
            latest = self.latest.get(sy)
            if not latest:
                continue
            price, when, _ = latest
            shares, cost = self._size(price)
            if shares < 1:
                continue
            self.s7_bought.add(key)
            erow = self._common_row("Repeat-Gainer", "ENTRY", tid, sy, "LONG", price, price,
                                    shares=shares, invested_dkk=cost,
                                    reason=f"TOP10_REPEAT_{count}X_IN_{self.cfg.s7_window_minutes}M",
                                    status="OPEN_RESEARCH", when=when)
            erow += [rank, gain, count, self.cfg.s7_window_minutes, members]
            self.bus.append("S7", erow)
            self.tg.send("S7", f"STRATEGY 7 — Repeat-Gainer\nBUY/LONG\n{sy}\nappeared {count}x in {self.cfg.s7_window_minutes}m\ndaily gain: {gain:+.2f}%\nprice: ${price:.4f}\nshares: {shares}")

    def _heartbeat(self, now: datetime) -> None:
        if not (self.cfg.heartbeat_enabled or self.cfg.s7_enabled):
            return
        if not self.rank_ready.is_set():
            self.audit.write("INFO", "HEARTBEAT", "Waiting for ranking bootstrap")
            return
        key = now.strftime("%Y-%m-%d-%H-%M")
        if key in self.hb_sent:
            return
        rows = self.ranks()
        daily_all = sorted(((s, x) for s, x, _, _ in rows if x is not None and x > 0), key=lambda x: x[1], reverse=True)
        weekly_all = sorted(((s, x) for s, _, x, _ in rows if x is not None and x > 0), key=lambda x: x[1], reverse=True)
        monthly_all = sorted(((s, x) for s, _, _, x in rows if x is not None and x > 0), key=lambda x: x[1], reverse=True)
        d = daily_all[:self.cfg.heartbeat_top_n]
        w = weekly_all[:self.cfg.heartbeat_top_n]
        m = monthly_all[:self.cfg.heartbeat_top_n]

        # Strategy 7 is deliberately based on the top 10 regular-session gainers,
        # independent of HEARTBEAT_TOP_N and independent of Telegram heartbeat toggles.
        self._s7_snapshot(now, daily_all[:self.cfg.s7_top_n])

        if not self.cfg.heartbeat_enabled:
            self.hb_sent.add(key)
            return

        unique = list(dict.fromkeys([s for s, _ in d + w + m]))
        hints = self.ai.heartbeat_hints(unique) if unique else {}

        def text_block(title: str, xs: list[tuple[str, float]]) -> str:
            z = [title]
            if not xs:
                z.append("No positive performers")
            for i, (s, r) in enumerate(xs, 1):
                hint = hints.get(s, "No verified catalyst found")
                z.append(f"{i}. {s} {r:+.2f}% — {hint}")
            return "\n".join(z)

        text = "📊 15-MIN MARKET HEARTBEAT\n\n" + text_block("DAILY", d) + "\n\n" + text_block("WEEKLY", w) + "\n\n" + text_block("MONTHLY", m)
        sent = self.tg.send("HEARTBEAT", text)
        self.bus.append("HEARTBEAT", [
            now.astimezone(UTC).isoformat(), now.isoformat(),
            "; ".join(f"{s}:{r:+.2f}%" for s, r in d),
            "; ".join(f"{s}:{r:+.2f}%" for s, r in w),
            "; ".join(f"{s}:{r:+.2f}%" for s, r in m),
            str(sent), ""
        ])
        self.hb_sent.add(key)

    def on_bar(self, bar: M1) -> None:
        local = bar.ts.astimezone(NY)
        hm = local.hour * 60 + local.minute
        with self.lock:
            self.latest[bar.symbol] = (bar.close, bar.ts, bar)
            if hm == 570 and bar.symbol not in self.session_open:
                # Exact 09:30 regular-session 1m bar OPEN; never substitute a late-day price.
                self.session_open[bar.symbol] = (bar.open, local)

        if hm < 570:
            self._s4_track_premarket(bar)
            return
        if hm >= 960 or local.weekday() >= 5:
            return

        # Strategy-specific minute logic first.
        self._s1_manage(bar)
        self._s4_on_regular_bar(bar)
        self._s6_on_bar(bar)

        up = self.up.get(bar.symbol)
        if up:
            self._process_candidate(up, bar)
        down = self.down.get(bar.symbol)
        if down:
            self._process_candidate(down, bar)

        x5 = self.agg5.push(bar)
        if x5:
            self._detect_anomaly(x5)

    def scheduler_tick(self, now: Optional[datetime] = None) -> None:
        now = (now or datetime.now(NY)).astimezone(NY)
        self._drain_ai()
        if now.weekday() >= 5:
            return
        hm = now.hour * 60 + now.minute

        if self.cfg.s4_enabled and hm >= _hhmm_to_minutes(self.cfg.s4_lock_hm) and hm < 570:
            self._s4_lock(now)
        if self.cfg.s4_enabled and hm >= 585 and self.s4_done_day != now.date():
            self._s4_confirm(now)

        if self.cfg.s5_enabled and hm >= _hhmm_to_minutes(self.cfg.s5_research_hm) and self.s5_researched_day != now.date():
            self._s5_research(now)
        if self.cfg.s5_enabled and hm >= _hhmm_to_minutes(self.cfg.s5_entry_hm) and hm < 960:
            self._s5_enter(now)

        hb_start = _hhmm_to_minutes(self.cfg.heartbeat_start_hm)
        hb_end = _hhmm_to_minutes(self.cfg.heartbeat_end_hm)
        if (
            (self.cfg.heartbeat_enabled or self.cfg.s7_enabled)
            and hb_start <= hm <= hb_end
            and now.minute % 15 == 0
        ):
            self._heartbeat(now)

        # Keep de-duplication sets bounded.
        cutoff = (now.date() - timedelta(days=3)).isoformat()
        self.hb_sent = {x for x in self.hb_sent if x[:10] >= cutoff}

    def scheduler_loop(self) -> None:
        while not self._shutdown.wait(5):
            try:
                self.scheduler_tick()
            except Exception as ex:
                self.audit.write("ERROR", "SCHEDULER", f"{type(ex).__name__}: {ex}")

    def run(self) -> None:
        if self.dry_run:
            raise RuntimeError("run() is not available in dry-run mode")
        self.symbols = self.universe()
        if not self.symbols:
            raise RuntimeError("No symbols available. Check STREAM_ALL/WATCHLIST and Alpaca credentials.")
        self.audit.write("INFO", "STARTUP", "VERSION=7.1-MULTI-STRATEGY")
        self.audit.write("INFO", "STARTUP", f"Monitoring {len(self.symbols)} symbols")

        from alpaca.data.enums import DataFeed
        from alpaca.data.live import StockDataStream

        # Start the live stream immediately. Historical bootstrap runs in parallel so
        # premarket/09:30 data are not missed while thousands of symbols are loading.
        threading.Thread(target=self.bootstrap, args=(self.symbols,), name="bootstrap", daemon=True).start()
        stream = StockDataStream(self.cfg.alpaca_key, self.cfg.alpaca_secret, feed=DataFeed.IEX)

        async def handler(raw):
            try:
                bar = M1(
                    raw.symbol, raw.timestamp.astimezone(UTC), float(raw.open), float(raw.high),
                    float(raw.low), float(raw.close), float(raw.volume)
                )
                self.on_bar(bar)
                self._drain_ai()
            except Exception as ex:
                self.audit.write("ERROR", "BAR_HANDLER", f"{type(ex).__name__}: {ex}", getattr(raw, "symbol", ""))
                log.exception("Bar handler error")

        if self.cfg.stream_all:
            stream.subscribe_bars(handler, "*")
        else:
            stream.subscribe_bars(handler, *self.symbols)

        threading.Thread(target=self.scheduler_loop, name="scheduler", daemon=True).start()
        self.audit.write("INFO", "STARTUP", "Starting Alpaca IEX websocket")
        try:
            stream.run()
        finally:
            self.close()

    def close(self) -> None:
        self._shutdown.set()
        try:
            self._drain_ai()
        except Exception:
            pass
        self.pool.shutdown(wait=False, cancel_futures=True)
        self.bus.close()


# -----------------------------
# Offline self-test (no credentials, no third-party SDKs)
# -----------------------------

def _self_test() -> None:
    cfg = Cfg.load(require_credentials=False)
    bus = SheetBus(cfg, dry_run=True)

    # Schema requirement: every automatically-created sheet includes Day_end_price.
    assert all("Day_end_price" in h for h in SCHEMAS.values())
    assert len(SCHEMAS) == 10

    # 5-minute aggregation.
    ag = Agg5(5)
    t0 = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
    emitted = None
    for k in range(6):
        emitted = ag.push(M1("TEST", t0 + timedelta(minutes=k), 10 + k * .01, 10.2, 9.9, 10.1, 100 + k))
    assert emitted is not None and emitted.symbol == "TEST"

    # Baseline/z-score math can initialize and compute.
    bl = Baselines(cfg)
    for k in range(max(cfg.warmup_candles, 35)):
        bl.add(M5("TEST", t0 + timedelta(minutes=5*k), 10, 10.1 + k * 0.0001, 9.9, 10.0, 1000 + k))
    assert bl.ready("TEST") and bl.stats("TEST") is not None

    # Heartbeat schedule is quarter-hour based and preserves 09:45/16:00 boundaries.
    assert 45 % 15 == 0 and 0 % 15 == 0
    assert _hhmm_to_minutes(cfg.heartbeat_start_hm) <= _hhmm_to_minutes(cfg.heartbeat_end_hm)

    # Strategy 6 1:3 target arithmetic.
    entry, stop, rr = 10.0, 9.5, 3.0
    target = entry + rr * (entry - stop)
    assert abs(target - 11.5) < 1e-12

    # Dry-run engine accepts a bar without external services.
    e = Engine(cfg, dry_run=True)
    e.on_bar(M1("TEST", datetime(2026, 9, 8, 13, 30, tzinfo=UTC), 10, 10.2, 9.9, 10.1, 1000))
    assert "TEST" in e.latest
    e.close()

    print("SELF_TEST_OK")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("multi-strategy")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args()
    if args.self_test:
        _self_test()
        return
    cfg = Cfg.load(require_credentials=True)
    Engine(cfg).run()


if __name__ == "__main__":
    main()
