import argparse
import asyncio
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
from statistics import mean
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
STRATEGY_HEADERS = [
    "timestamp_et","strategy","event","trade_id","symbol","side",
    "signal_time_et","signal_price","entry_time_et","entry_price",
    "exit_time_et","exit_price","quantity","invested_dkk","pnl_dkk",
    "pnl_pct","stop_price","target_price","z_range","z_volume",
    "continuation_pct","news_decision","news_hint","reason","status",
    "meta","Day_end_price",
]
SYSTEM_HEADERS = [
    "timestamp_et","level","stage","symbol","message","meta","Day_end_price"
]
AI_HEADERS = [
    "timestamp_et","kind","symbol","decision","text","input_tokens",
    "output_tokens","total_tokens","cost","error","sources","Day_end_price"
]
RANK_HEADERS = [
    "timestamp_et","period","rank","symbol","change_pct","price","baseline",
    "reason","Day_end_price"
]
S4_INPUT_HEADERS = [
    "trade_date","rank","symbol","source","notes","Day_end_price"
]


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


class Cfg:
    def __init__(self, raw: dict[str, Any]):
        self.r = raw
        required = [
            "sa","wl","ci","sp","sv","zs","bc","wm","bb","bp","qm","mr",
            "mp","bd","pf","cd","tr","fx","fl","mx","gsr","ge","gm","gr",
            "gc","gt","gw","gwe","br","bh","bm","rk","rn","rr","rb","rbb",
            "rbp","s","n","nr","nb","ne","s2w","s3r","s4m","s5m","s5h",
            "s5n","s6t","s6b","s6p","s6r","hi","rows"
        ]
        miss = [k for k in required if k not in raw]
        if miss:
            raise RuntimeError("K08 missing config keys: " + ",".join(miss))
        if len(raw["s"]) != 7 or len(raw["n"]) != 7:
            raise RuntimeError("K08 keys 's' and 'n' must each contain 7 values")
        if len(raw["tr"]) != 4:
            raise RuntimeError("K08 key 'tr' must contain 4 trailing-stop values")
        if int(raw["hi"]) != 15:
            raise RuntimeError("This refactor expects hi=15 for 15-minute ranking cycles")

    def __getitem__(self, k):
        return self.r[k]

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
            self.x[b.s] = [st,b.o,b.h,b.l,b.c,b.v]
            return None
        if w[0] == st:
            w[2] = max(w[2], b.h)
            w[3] = min(w[3], b.l)
            w[4] = b.c
            w[5] += b.v
            return None
        z = M5(b.s, w[0], w[0] + timedelta(minutes=5), w[1], w[2], w[3], w[4], w[5])
        self.x[b.s] = [st,b.o,b.h,b.l,b.c,b.v]
        return z


class Roll:
    def __init__(self, n: int):
        self.n = n
        self.q: dict[str, deque[tuple[float,float]]] = defaultdict(lambda: deque(maxlen=n))
        self.sr = defaultdict(float)
        self.sr2 = defaultdict(float)
        self.sv = defaultdict(float)
        self.sv2 = defaultdict(float)

    def add(self, s: str, r: float, v: float):
        q = self.q[s]
        if len(q) == self.n:
            a,b = q[0]
            self.sr[s] -= a; self.sr2[s] -= a*a
            self.sv[s] -= b; self.sv2[s] -= b*b
        q.append((r,v))
        self.sr[s] += r; self.sr2[s] += r*r
        self.sv[s] += v; self.sv2[s] += v*v

    def stats(self, s: str):
        q = self.q[s]
        n = len(q)
        if n < 2:
            return None
        mr = self.sr[s] / n
        mv = self.sv[s] / n
        vr = max(0.0, (self.sr2[s] - n*mr*mr) / (n-1))
        vv = max(0.0, (self.sv2[s] - n*mv*mv) / (n-1))
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
    meta: dict[str,Any] = field(default_factory=dict)


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
    meta: dict[str,Any] = field(default_factory=dict)


class TG:
    def __init__(self, tok: str, chat: str):
        import requests
        self.rq = requests.Session()
        self.u = f"https://api.telegram.org/bot{tok}/sendMessage"
        self.c = chat

    def send(self, text: str):
        try:
            r = self.rq.post(self.u, json={"chat_id":self.c,"text":text}, timeout=10)
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
        self.headers = {SHEETS[i]:STRATEGY_HEADERS for i in range(1,8)}
        self.headers.update({
            "System_Log":SYSTEM_HEADERS,
            "AI_Log":AI_HEADERS,
            "Rankings":RANK_HEADERS,
            "S4_Input":S4_INPUT_HEADERS,
        })
        for name, headers in self.headers.items():
            self.ws[name] = self._sheet(name, headers)
        threading.Thread(target=self._loop, daemon=True).start()

    def _sheet(self, name, headers):
        try:
            w = self.book.worksheet(name)
        except self.gspread.WorksheetNotFound:
            w = self.book.add_worksheet(title=name, rows=int(self.cfg["rows"]), cols=len(headers))
        if _bool(self.cfg["gsr"]):
            w.clear()
            w.resize(rows=int(self.cfg["rows"]), cols=len(headers))
        else:
            try:
                w.resize(cols=len(headers))
            except Exception:
                pass
        vals = w.row_values(1)
        if vals != headers:
            if not vals:
                w.append_row(headers, value_input_option="USER_ENTERED")
            elif _bool(self.cfg["gsr"]):
                w.update([headers], "A1", value_input_option="USER_ENTERED")
            else:
                raise RuntimeError(f"Sheet {name} has incompatible headers. Use a fresh workbook or set gsr=1 once.")
        return w

    def put(self, name: str, row: list[Any]):
        h = self.headers[name]
        row = list(row[:len(h)]) + [""] * max(0, len(h)-len(row))
        with self.lock:
            self.q[name].append(row)

    def _loop(self):
        while not self.stop.wait(float(self.cfg["fl"])):
            self.flush()

    def flush(self):
        packs = {}
        mx = int(self.cfg["mx"])
        with self.lock:
            for k,v in self.q.items():
                if v:
                    packs[k] = [v.popleft() for _ in range(min(mx,len(v)))]
        for name, rows in packs.items():
            try:
                self.ws[name].append_rows(rows, value_input_option="USER_ENTERED")
            except Exception as ex:
                L.error("Google flush %s: %s", name, ex)
                with self.lock:
                    for r in reversed(rows):
                        self.q[name].appendleft(r)

    def read_s4(self, today: date, limit: int) -> list[str]:
        try:
            vals = self.ws["S4_Input"].get_all_records()
        except Exception as ex:
            L.error("S4 input read: %s", ex)
            return []
        out=[]
        for r in vals:
            if str(r.get("trade_date","")).strip() != today.isoformat():
                continue
            s = str(r.get("symbol","")).strip().upper()
            try:
                rk = int(r.get("rank",999))
            except Exception:
                rk = 999
            if s and rk <= limit:
                out.append((rk,s))
        return [s for _,s in sorted(out)[:limit]]

    def close(self):
        self.stop.set()
        self.flush()


class AI:
    def __init__(self, key: str, cfg: Cfg, gs: GS):
        import requests
        self.rq = requests.Session()
        self.k = key
        self.cfg = cfg
        self.gs = gs
        self.cache: dict[str,tuple[datetime,Any]] = {}
        self.lock = threading.Lock()
        self.off_until = datetime.min.replace(tzinfo=UTC)

    def _log(self, kind, symbol, decision, text, data=None, err=""):
        u = (data or {}).get("usage",{}) or {}
        src=[]
        try:
            anns=(data or {}).get("choices",[{}])[0].get("message",{}).get("annotations",[]) or []
            for a in anns:
                x=a.get("url_citation",{}) or {}
                if x.get("url") and x["url"] not in src:
                    src.append(x["url"])
        except Exception:
            pass
        self.gs.put("AI_Log",[
            _et(datetime.now(UTC)),kind,symbol,decision,text,
            u.get("prompt_tokens",u.get("input_tokens","")),
            u.get("completion_tokens",u.get("output_tokens","")),
            u.get("total_tokens",""),u.get("cost",""),err,"|".join(src),""
        ])
        return src

    def _call(self, kind, symbol, messages, max_tokens, web=True):
        if not _bool(self.cfg["ge"]) or not self.k:
            return None,"AI_DISABLED",[]
        if datetime.now(UTC) < self.off_until:
            return None,"AI_COOLDOWN",[]
        body={
            "model":self.cfg["gm"],
            "messages":messages,
            "temperature":0,
            "max_tokens":int(max_tokens),
        }
        if web:
            body["plugins"]=[{"id":"web","max_results":int(self.cfg["gr"])}]
        hdr={
            "Authorization":f"Bearer {self.k}",
            "Content-Type":"application/json",
            "HTTP-Referer":"https://github.com/",
            "X-Title":"research-runner",
        }
        last=""
        for i in range(3):
            try:
                r=self.rq.post("https://openrouter.ai/api/v1/chat/completions",headers=hdr,json=body,timeout=float(self.cfg["gt"]))
                if r.status_code in {401,403}:
                    self.off_until=datetime.now(UTC)+timedelta(minutes=60)
                    last=f"HTTP {r.status_code}: {r.text[:240]}"
                    self._log(kind,symbol,"ERROR","",None,last)
                    return None,last,[]
                if r.status_code==429:
                    last=f"HTTP 429: {r.text[:240]}"
                    time.sleep(2*(i+1)); continue
                if 500 <= r.status_code < 600:
                    last=f"HTTP {r.status_code}: {r.text[:240]}"
                    time.sleep(2*(i+1)); continue
                r.raise_for_status()
                data=r.json()
                msg=((data.get("choices") or [{}])[0].get("message") or {})
                content=msg.get("content")
                text=content.strip() if isinstance(content,str) else ""
                if not text:
                    last="empty assistant content"
                    if i<2:
                        time.sleep(1.5*(i+1)); continue
                    self._log(kind,symbol,"ERROR","",data,last)
                    return None,last,[]
                src=self._log(kind,symbol,"OK",text,data,"")
                return text,"",src
            except Exception as ex:
                last=f"{type(ex).__name__}: {ex}"
                if i<2:
                    time.sleep(1.5*(i+1)); continue
        self._log(kind,symbol,"ERROR","",None,last)
        return None,last,[]

    def signal(self, s: str, direction: str, t: datetime, p: float, zr: float, zv: float, vol: float, fresh: bool=False):
        day=t.astimezone(NY)
        hrs=int(self.cfg["gwe"] if day.weekday()==0 else self.cfg["gw"])
        key=f"sig:{s}:{direction}:{day.date()}"
        if not fresh:
            with self.lock:
                z=self.cache.get(key)
                if z and datetime.now(UTC)-z[0] < timedelta(hours=float(self.cfg["gc"])):
                    return z[1]
        move="rise" if direction=="U" else "fall"
        q=[
            {"role":"system","content":"You are a strict market-news gate. Use current web results. Output exactly NO or YES|a factual 5-6 word catalyst. YES requires a recent credible company/security-specific catalyst that plausibly explains the move. Do not use generic market commentary."},
            {"role":"user","content":f"Ticker {s}. Abnormal {move}. Time {t.isoformat()}. Price {p:.6f}. z-range {zr:.2f}; z-volume {zv:.2f}; volume {vol:.0f}. Search only the prior {hrs} hours."}
        ]
        text,err,src=self._call("SIGNAL",s,q,40,True)
        if not text:
            out=("ERROR","",src,err)
        else:
            raw=text.strip()
            if raw.upper().startswith("YES|"):
                out=("YES",raw.split("|",1)[1].strip(),src,"")
            elif raw.upper()=="YES":
                out=("YES","Verified recent catalyst",src,"")
            else:
                out=("NO","",src,"")
        if not fresh:
            with self.lock:
                self.cache[key]=(datetime.now(UTC),out)
        return out

    def next_day_events(self, target: date, limit: int):
        q=[
            {"role":"system","content":"Use current web sources. Return TSV only, one event per line: SYMBOL<TAB>EVENT<TAB>DATE<TAB>SOURCE_URL. Include only US-listed companies with a scheduled earnings release, Phase 2/3 clinical data readout, FDA decision, or PDUFA on the requested date. Skip uncertain dates and rows without a source URL."},
            {"role":"user","content":f"Find up to {limit} material scheduled events for {target.isoformat()}. Prefer events with a clearly verified date."}
        ]
        text,err,src=self._call("S5_EVENTS","",q,800,True)
        if not text:
            return [],err
        out=[]
        for line in text.splitlines():
            p=[x.strip() for x in line.split("\t")]
            if len(p)>=4 and p[0].replace(".","").replace("-","").isalnum() and p[3].startswith("http"):
                out.append({"symbol":p[0].upper(),"event":p[1],"date":p[2],"url":p[3]})
        return out[:limit],""

    def brief(self, today: date):
        q=[
            {"role":"system","content":"Produce a concise pre-market research brief using current web sources. No investment advice. Cover notable US earnings today, scheduled Phase 2/3 or FDA/PDUFA events, and energy catalysts including EIA/OPEC+/geopolitics/weather. Omit unverified items."},
            {"role":"user","content":f"New York date: {today.isoformat()}. Keep under 700 words."}
        ]
        return self._call("BRIEF","",q,900,True)[0] or ""

    def ranking_reasons(self, syms: list[str]):
        if not syms or not _bool(self.cfg["rr"]):
            return {}
        q=[
            {"role":"system","content":"Use current web results. Return one line per requested ticker: SYMBOL|4-5 word factual reason. If no verified reason, SYMBOL|No verified catalyst."},
            {"role":"user","content":"Tickers: "+", ".join(syms)}
        ]
        text,_,_=self._call("RANK_REASONS","",q,max(80,len(syms)*10),True)
        out={}
        for line in (text or "").splitlines():
            if "|" in line:
                s,h=line.split("|",1); out[s.strip().upper()]=h.strip()
        return out


class Core:
    def __init__(self, cfg: Cfg, gs: GS, tg: TG, ai: AI, key: str, sec: str):
        self.c=cfg; self.gs=gs; self.tg=tg; self.ai=ai; self.key=key; self.sec=sec
        self.lock=threading.RLock()
        self.a5=Agg5(); self.roll=Roll(int(cfg["bc"]))
        self.last: dict[str,tuple[float,datetime]]={}
        self.daily: dict[str,dict[date,dict[str,float]]]=defaultdict(dict)
        self.first: dict[str,tuple[float,datetime]]={}
        self.week: dict[str,float]={}; self.month: dict[str,float]={}
        self.sig: dict[str,Sig]={}; self.cool: dict[str,float]={}
        self.s2arm: dict[str,dict[str,Any]]={}; self.last_up: dict[str,tuple[datetime,float,float,float,float]]={}
        self.pos: dict[int,dict[str,Pos]]=defaultdict(dict)
        self.orb: dict[str,dict[str,Any]]={}
        self.rank_seen: dict[str,deque[datetime]]=defaultdict(deque)
        self.rank_bought: set[tuple[date,str]]=set()
        self.s4_list: list[str]=[]; self.s4_done:set[tuple[date,str]]=set(); self.s4_load_day:Optional[date]=None
        self.s5_today: list[dict[str,Any]]=[]; self.s5_scan_day:Optional[date]=None; self.s5_done:set[tuple[date,str]]=set()
        self.slots:set[str]=set(); self.brief_day:Optional[date]=None
        self.jobs=queue.Queue(); self.dead=threading.Event()
        threading.Thread(target=self._ai_worker,daemon=True).start()
        self.fx=self._fx()
        self._sys("INFO","BOOT","","core initialized",{"fx":self.fx})

    def _fx(self):
        x=float(self.c["fx"])
        if x>0: return x
        import requests, xml.etree.ElementTree as ET
        try:
            r=requests.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",timeout=10); r.raise_for_status()
            d={}
            for z in ET.fromstring(r.text).iter():
                if z.attrib.get("currency") and z.attrib.get("rate"):
                    d[z.attrib["currency"]]=float(z.attrib["rate"])
            return d["DKK"]/d["USD"]
        except Exception:
            return 6.5

    def _sys(self, level, stage, symbol, message, meta=None):
        fn={"INFO":L.info,"WARNING":L.warning,"ERROR":L.error}.get(level,L.info)
        fn("[%s] %s %s",stage,symbol or "-",message)
        self.gs.put("System_Log",[_et(datetime.now(UTC)),level,stage,symbol,message,_j(meta or {}),""])
        if level=="ERROR" and _bool(self.c["ne"]):
            self.tg.send(f"[SYSTEM ERROR]\n{stage}\n{symbol}\n{message[:500]}")

    def _row(self, st,event,symbol,side="",sig:Optional[Sig]=None,pos:Optional[Pos]=None,reason="",status="",meta=None,exit_t=None,exit_p=None,pnl=None):
        return [
            _et(datetime.now(UTC)),NAMES[st],event,(pos.id if pos else (sig.id if sig else "")),symbol,side,
            _et(sig.t) if sig else "",sig.p if sig else "",
            _et(pos.t) if pos else "",pos.p if pos else "",
            _et(exit_t),exit_p if exit_p is not None else "",
            pos.q if pos else "",pos.dkk if pos else "",
            pnl if pnl is not None else "",
            (pnl/pos.dkk*100 if pnl is not None and pos and pos.dkk else ""),
            pos.stop if pos and pos.stop else "",pos.target if pos and pos.target else "",
            sig.zr if sig else "",sig.zv if sig else "",sig.cont*100 if sig else "",
            sig.news if sig else "",sig.hint if sig else "",reason,status,_j(meta or (pos.meta if pos else {})),""
        ]

    def _logst(self, st, *args, **kwargs):
        self.gs.put(SHEETS[st], self._row(st,*args,**kwargs))

    def _note(self, st, text):
        if self.c.notify(st): self.tg.send(f"[S{st} {NAMES[st]}]\n{text}")

    def _qty(self,p):
        if p<float(self.c["pf"]): return 0,0.0
        q=math.floor((float(self.c["bd"])/self.fx)/p)
        return q,q*p*self.fx

    def _entry(self, st, s, side, p, t, sig=None, reason="", stop=0.0, target=0.0, meta=None):
        if s in self.pos[st]: return None
        q,dkk=self._qty(p)
        if q<1:
            self._logst(st,"IGNORE",s,side,sig=sig,reason="ONE_SHARE_EXCEEDS_BUDGET_OR_PENNY",status="IGNORED",meta=meta)
            return None
        z=Pos(uuid.uuid4().hex[:12],st,s,side,t,p,q,dkk,stop,target,p,p,meta or {})
        self.pos[st][s]=z
        self._logst(st,"ENTRY",s,side,sig=sig,pos=z,reason=reason,status="OPEN",meta=meta)
        self._note(st,f"{side} ENTRY\n{s}\nprice: ${p:.4f}\nshares: {q}\nDKK: {dkk:.2f}\n{reason}")
        return z

    def _exit(self, st, s, p, t, reason):
        z=self.pos[st].pop(s,None)
        if not z: return
        if z.side=="LONG": pnl=(p-z.p)*z.q*self.fx
        else: pnl=(z.p-p)*z.q*self.fx
        self._logst(st,"EXIT",s,z.side,pos=z,reason=reason,status="CLOSED",exit_t=t,exit_p=p,pnl=pnl)
        self._note(st,f"EXIT\n{s}\nprice: ${p:.4f}\nP/L: DKK {pnl:+.2f}\n{reason}")

    def _regular(self,t):
        x=t.astimezone(NY); m=x.hour*60+x.minute
        return x.weekday()<5 and 570<=m<960

    def bootstrap(self, ss):
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame,TimeFrameUnit
        cl=StockHistoricalDataClient(self.key,self.sec)
        st=datetime.now(UTC)-timedelta(days=5)
        bb=int(self.c["bb"])
        for i in range(0,len(ss),bb):
            z=ss[i:i+bb]
            try:
                r=cl.get_stock_bars(StockBarsRequest(symbol_or_symbols=z,timeframe=TimeFrame(5,TimeFrameUnit.Minute),start=st,feed=DataFeed.IEX))
                n=0
                for s,bars in r.data.items():
                    for b in bars:
                        if self._regular(b.timestamp):
                            self.roll.add(s,float(b.high)-float(b.low),float(b.volume)); n+=1
                self._sys("INFO","BOOTSTRAP","",f"batch {i//bb+1}: {n} bars")
            except Exception as ex:
                self._sys("ERROR","BOOTSTRAP","",str(ex))
            time.sleep(float(self.c["bp"]))

    def rank_bootstrap(self,ss):
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame,TimeFrameUnit
        cl=StockHistoricalDataClient(self.key,self.sec)
        st=datetime.now(UTC)-timedelta(days=int(self.c["rb"]))
        bb=int(self.c["rbb"]); ok=bad=0
        for i in range(0,len(ss),bb):
            z=ss[i:i+bb]
            try:
                r=cl.get_stock_bars(StockBarsRequest(symbol_or_symbols=z,timeframe=TimeFrame(1,TimeFrameUnit.Day),start=st,end=datetime.now(UTC),feed=DataFeed.IEX))
                for s,bars in r.data.items():
                    for b in bars:
                        d=b.timestamp.astimezone(NY).date()
                        self.daily[s][d]={"open":float(b.open),"close":float(b.close)}
                ok+=1
            except Exception as ex:
                bad+=1; self._sys("ERROR","RANK_BOOT","",str(ex))
            time.sleep(float(self.c["rbp"]))
        self._sys("INFO","RANK_BOOT","",f"done success={ok} failed={bad}")
        self._capture_open(ss)

    def _capture_open(self,ss):
        from alpaca.data.enums import DataFeed
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame,TimeFrameUnit
        now=datetime.now(NY); op=now.replace(hour=9,minute=30,second=0,microsecond=0)
        if now.weekday()>=5 or now<op: return
        end=min(now,op+timedelta(minutes=5))
        cl=StockHistoricalDataClient(self.key,self.sec); bb=int(self.c["rbb"])
        for i in range(0,len(ss),bb):
            try:
                r=cl.get_stock_bars(StockBarsRequest(symbol_or_symbols=ss[i:i+bb],timeframe=TimeFrame(1,TimeFrameUnit.Minute),start=op.astimezone(UTC),end=end.astimezone(UTC),feed=DataFeed.IEX))
                for s,bars in r.data.items():
                    if bars:
                        x=min(bars,key=lambda b:b.timestamp)
                        self.first[s]=(float(x.close),x.timestamp.astimezone(NY))
            except Exception as ex:
                self._sys("ERROR","RANK_OPEN","",str(ex))
            time.sleep(float(self.c["rbp"]))

    def on_bar(self,b:M1):
        x=b.t.astimezone(NY)
        with self.lock:
            self.last[b.s]=(b.c,b.t)
            if x.weekday()<5 and x.hour==9 and x.minute==30 and b.s not in self.first:
                self.first[b.s]=(b.c,x)
            self._manage_positions(b)
            self._orb(b)
            z=self.a5.push(b)
            if z:
                self._anomaly(z)
            self._continue(b)
            self._s2_expire(b.s,b.t)

    def _anomaly(self,z:M5):
        st=self.roll.stats(z.s)
        if not st or st[4] < int(self.c["wm"]):
            self.roll.add(z.s,z.rng,z.v); return
        mr,sr,mv,sv,_=st
        zr=(z.rng-mr)/sr if sr>0 else 0.0; zv=(z.v-mv)/sv if sv>0 else 0.0
        stage1=(mr>0 and z.rng>float(self.c["sp"])*mr) or (mv>0 and z.v>float(self.c["sv"])*mv)
        stage2=zr>float(self.c["zs"]) or zv>float(self.c["zs"])
        up=z.c>z.o; dn=z.c<z.o
        self.roll.add(z.s,z.rng,z.v)
        if not(stage1 and stage2 and (up or dn)): return
        direction="U" if up else "D"
        if direction=="D" and self.c.enabled(2) and z.s in self.s2arm:
            arm=self.s2arm.pop(z.s)
            self._logst(
                2,"CANCELLED",z.s,"SHORT",
                reason="NEXT_ANOMALY_NOT_UPWARD",status="IGNORED",
                meta={"first_price":arm["p"],"first_signal_time":_et(arm["t"]),"next_anomaly_direction":"DOWN"},
            )
        if direction=="U":
            self.last_up[z.s]=(z.end,z.c,zr,zv,z.v)
            if self.c.enabled(2):
                arm=self.s2arm.get(z.s)
                delay=float(self.c["s2w"])
                paired=False
                if arm:
                    dt=(z.end-arm["t"]).total_seconds()/60
                    if 0 < dt <= delay:
                        sg2=Sig(uuid.uuid4().hex[:12],z.s,"U",z.end,z.c,zr,zv,z.v,[z.c],[])
                        sg2.status="AI_PENDING"
                        self._logst(
                            2,"SECOND_ANOMALY",z.s,"SHORT",sig=sg2,
                            reason="SECOND_UP_ANOMALY_WITHIN_WINDOW_CHECK_NEWS",
                            status="AI_PENDING",
                            meta={"minutes":dt,"window_minutes":delay,"first_price":arm["p"],"first_signal_time":_et(arm["t"]),"first_signal_id":arm["id"]},
                        )
                        self.jobs.put(AIJob("S2",sg2,{"minutes":dt,"first_price":arm["p"],"first_signal_time":_et(arm["t"]),"first_signal_id":arm["id"]}))
                        self.s2arm.pop(z.s,None)
                        paired=True
                    elif dt>delay:
                        self._logst(
                            2,"EXPIRED",z.s,"SHORT",
                            reason="NO_SECOND_ANOMALY_WITHIN_WINDOW",
                            status="IGNORED",
                            meta={"window_minutes":delay,"elapsed_minutes":dt,"first_price":arm["p"],"first_signal_time":_et(arm["t"])},
                        )
                        self.s2arm.pop(z.s,None)
                if not paired and z.s not in self.s2arm:
                    sid=uuid.uuid4().hex[:12]
                    self.s2arm[z.s]={"t":z.end,"p":z.c,"id":sid}
                    first=Sig(sid,z.s,"U",z.end,z.c,zr,zv,z.v,[z.c],[])
                    self._logst(
                        2,"FIRST_ANOMALY",z.s,"SHORT",sig=first,
                        reason="FIRST_UP_ANOMALY_WAIT_SECOND_WITHIN_WINDOW",
                        status="ARMED",
                        meta={"window_minutes":delay,"ai_used_yet":False},
                    )
        nowm=time.monotonic()
        if nowm-self.cool.get(z.s,-1e12) < float(self.c["cd"])*60: return
        self.cool[z.s]=nowm
        if z.s in self.sig: return
        sg=Sig(uuid.uuid4().hex[:12],z.s,direction,z.end,z.c,zr,zv,z.v,[z.c],[])
        self.sig[z.s]=sg
        if direction=="U":
            if self.c.enabled(1): self._logst(1,"ANOMALY",z.s,"LONG",sig=sg,reason="UPWARD_5M_ANOMALY",status="WATCH")
        else:
            if self.c.enabled(3): self._logst(3,"ANOMALY",z.s,"LONG",sig=sg,reason="DOWNWARD_5M_ANOMALY",status="WATCH")

    def _continue(self,b:M1):
        sg=self.sig.get(b.s)
        if not sg or sg.status!="WATCH": return
        if b.t < sg.t: return
        if sg.mt and b.t<=sg.mt[-1]: return
        sg.mt.append(b.t); sg.px.append(b.c)
        if len(sg.px)<3: return
        a,b1,c=sg.px[0],sg.px[1],sg.px[2]
        if min(a,b1,c)<=0:
            self.sig.pop(sg.s,None); return
        r2=math.log(c/b1); total=math.log(c/a)
        if sg.direction=="U":
            ok=total>=float(self.c["mr"]) and max(0.0,-r2)<=float(self.c["mp"])
        else:
            ok=total<=-float(self.c["mr"]) and max(0.0,r2)<=float(self.c["mp"])
        sg.cont=total; sg.confirm_p=c
        if not ok:
            sg.status="FAIL"
            sts=[1] if sg.direction=="U" else [3]
            for st in sts:
                if self.c.enabled(st): self._logst(st,"CONTINUATION_FAIL",sg.s,"LONG" if st!=2 else "SHORT",sig=sg,reason="2_MIN_CONTINUATION_FAILED",status="IGNORED")
            self.sig.pop(sg.s,None); return
        if sg.direction=="U":
            if self.c.enabled(1):
                sg.status="AI_PENDING"
                self._logst(1,"CONTINUATION_PASS",sg.s,"LONG",sig=sg,reason="2_MIN_CONTINUATION_PASS",status="AI_PENDING")
                self.jobs.put(AIJob("S1",sg))
            else:
                self.sig.pop(sg.s,None)
        else:
            if self.c.enabled(3):
                sg.status="AI_PENDING"
                self._logst(3,"CONTINUATION_PASS",sg.s,"LONG",sig=sg,reason="2_MIN_CONTINUATION_PASS",status="AI_PENDING")
                self.jobs.put(AIJob("S3",sg))
            else:
                self.sig.pop(sg.s,None)

    def _ai_worker(self):
        while not self.dead.is_set():
            try:
                job=self.jobs.get(timeout=1)
            except queue.Empty:
                continue
            try:
                if isinstance(job, AIJob):
                    kind=job.kind
                    sg=job.sig
                    meta=job.meta
                else:
                    kind="S1" if getattr(job,"direction","")=="U" else "S3"
                    sg=job
                    meta={}

                if kind=="S2":
                    dec,hint,src,err=self.ai.signal(sg.s,"U",sg.t,sg.p,sg.zr,sg.zv,sg.vol,fresh=True)
                    with self.lock:
                        sg.news=dec; sg.hint=hint; sg.sources=src
                        if dec=="ERROR":
                            self._logst(2,"AI_ERROR",sg.s,"SHORT",sig=sg,reason=err,status="NO_TRADE",meta=meta)
                            continue
                        if dec=="YES":
                            self._logst(
                                2,"IGNORE",sg.s,"SHORT",sig=sg,
                                reason="VERIFIED_NEWS_FOUND_NO_SHORT",status="IGNORED",
                                meta={**meta,"news_hint":hint,"sources":src},
                            )
                            continue
                        p_now,t_now=self.last.get(sg.s,(sg.p,sg.t))
                        self._entry(
                            2,sg.s,"SHORT",p_now,t_now,sig=sg,
                            reason="TWO_UP_ANOMALIES_WITHIN_5M_NO_VERIFIED_NEWS",
                            meta={**meta,"second_alert_price":sg.p,"news_decision":"NO"},
                        )
                    continue

                dec,hint,src,err=self.ai.signal(sg.s,sg.direction,sg.t,sg.p,sg.zr,sg.zv,sg.vol)
                with self.lock:
                    cur=self.sig.get(sg.s)
                    if not cur or cur.id!=sg.id:
                        continue
                    sg.news=dec; sg.hint=hint; sg.sources=src
                    if dec=="ERROR":
                        st=1 if sg.direction=="U" else 3
                        if self.c.enabled(st):
                            self._logst(st,"AI_ERROR",sg.s,"LONG",sig=sg,reason=err,status="NO_TRADE")
                        self.sig.pop(sg.s,None); continue
                    if sg.direction=="U":
                        if dec=="YES" and self.c.enabled(1):
                            self._entry(1,sg.s,"LONG",sg.confirm_p,sg.mt[-1],sig=sg,reason=hint or "NEWS_CONFIRMED")
                        elif dec=="NO" and self.c.enabled(1):
                            self._logst(1,"IGNORE",sg.s,"LONG",sig=sg,reason="NO_VERIFIED_NEWS_CATALYST",status="IGNORED")
                    else:
                        if dec=="NO" and self.c.enabled(3):
                            self._entry(3,sg.s,"LONG",sg.confirm_p,sg.mt[-1],sig=sg,reason="FALL_WITHOUT_VERIFIED_CATALYST")
                        elif self.c.enabled(3):
                            self._logst(3,"IGNORE",sg.s,"LONG",sig=sg,reason="NEGATIVE_CATALYST_FOUND",status="IGNORED")
                    self.sig.pop(sg.s,None)
            except Exception as ex:
                self._sys("ERROR","AI_WORKER",getattr(locals().get("sg",None),"s",""),str(ex))

    def _s2_expire(self,s: str,t: datetime):
        arm=self.s2arm.get(s)
        if not arm:
            return
        delay=float(self.c["s2w"])
        dt=(t-arm["t"]).total_seconds()/60
        if dt>delay:
            if self.c.enabled(2):
                self._logst(
                    2,"EXPIRED",s,"SHORT",
                    reason="NO_SECOND_ANOMALY_WITHIN_WINDOW",
                    status="IGNORED",
                    meta={"window_minutes":delay,"elapsed_minutes":dt,"first_price":arm["p"],"first_signal_time":_et(arm["t"])},
                )
            self.s2arm.pop(s,None)

    def _poc(self,bars: list[tuple[float,float,float]],bins: int) -> Optional[float]:
        if not bars:
            return None
        lo=min(x[0] for x in bars)
        hi=max(x[1] for x in bars)
        if not math.isfinite(lo) or not math.isfinite(hi) or lo<=0 or hi<lo:
            return None
        if hi==lo:
            return lo
        n=max(8,int(bins))
        step=(hi-lo)/n
        vol=[0.0]*n
        for bl,bh,bv in bars:
            if bv<=0 or bh<bl:
                continue
            i0=max(0,min(n-1,int((bl-lo)/step)))
            i1=max(0,min(n-1,int((bh-lo)/step)))
            if i1<i0:
                i0,i1=i1,i0
            share=float(bv)/(i1-i0+1)
            for i in range(i0,i1+1):
                vol[i]+=share
        if not any(x>0 for x in vol):
            return None
        i=max(range(n),key=lambda k:vol[k])
        return lo+(i+0.5)*step

    def _manage_positions(self,b:M1):
        p=b.c; t=b.t
        z=self.pos[1].get(b.s)
        if z:
            z.peak=max(z.peak,p)
            gain=(z.peak/z.p-1)*100
            tr=self.c["tr"]; pct=float(tr[0] if gain<10 else tr[1] if gain<25 else tr[2] if gain<50 else tr[3])
            if (p/z.peak-1)*100 <= -pct: self._exit(1,b.s,p,t,"ADAPTIVE_TRAIL")
        z=self.pos[3].get(b.s)
        if z:
            z.trough=min(z.trough,p)
            if p>=z.trough*(1+float(self.c["s3r"])/100): self._exit(3,b.s,p,t,"REBOUND_FROM_POST_ENTRY_LOW")
        z=self.pos[6].get(b.s)
        if z:
            if b.l<=z.stop: self._exit(6,b.s,z.stop,t,"STRUCTURE_STOP")
            elif b.h>=z.target: self._exit(6,b.s,z.target,t,"THREE_R_TARGET")

    def _orb(self,b:M1):
        if not self.c.enabled(6): return
        x=b.t.astimezone(NY); m=x.hour*60+x.minute
        d=x.date(); k=f"{d}:{b.s}"
        q=self.orb.setdefault(k,{
            "h1":-math.inf,"l1":math.inf,"h2":-math.inf,"l2":math.inf,
            "c2":None,"armed":False,"done":False,"vp":[]
        })
        if 570<=m and not q["done"]:
            q["vp"].append((float(b.l),float(b.h),float(b.v)))
        if 570<=m<585:
            q["h1"]=max(q["h1"],b.h); q["l1"]=min(q["l1"],b.l); return
        if 585<=m<600:
            q["h2"]=max(q["h2"],b.h); q["l2"]=min(q["l2"],b.l); q["c2"]=b.c; return
        if m>=600 and not q["done"]:
            if not q["armed"]:
                if q["h1"]>-math.inf and q["c2"] is not None and q["h2"]>q["h1"] and q["c2"]>q["h1"]:
                    q["armed"]=True
                    self._logst(
                        6,"BREAKOUT_CONFIRMED",b.s,"LONG",
                        reason="SECOND_15M_CANDLE_BROKE_FIRST_HIGH",
                        status="WAIT_RETEST",
                        meta={"range_high":q["h1"],"range_low":q["l1"],"second_high":q["h2"],"second_close":q["c2"]},
                    )
                else:
                    q["done"]=True
                    self._logst(
                        6,"IGNORE",b.s,"LONG",
                        reason="NO_UPSIDE_STRUCTURE_BREAK",
                        status="IGNORED",
                        meta={"range_high":q["h1"],"range_low":q["l1"],"second_high":q["h2"],"second_close":q["c2"]},
                    )
                    return
            if q["armed"] and b.s not in self.pos[6]:
                tol=float(self.c["s6t"])/100
                if b.l<=q["h1"]*(1+tol) and b.c>=q["h1"]:
                    poc=self._poc(q["vp"],int(self.c["s6p"]))
                    if poc is None:
                        self._logst(6,"RETEST",b.s,"LONG",reason="POC_UNAVAILABLE",status="WAIT_RETEST",meta={"range_high":q["h1"],"retest_low":b.l})
                        return
                    buffer=float(self.c["s6b"])/100
                    stop=poc*(1-buffer)
                    if stop>=b.c:
                        self._logst(
                            6,"RETEST",b.s,"LONG",
                            reason="POC_NOT_BELOW_ENTRY",
                            status="WAIT_RETEST",
                            meta={"range_high":q["h1"],"retest_low":b.l,"poc":poc,"candidate_stop":stop,"entry":b.c},
                        )
                        return
                    risk=b.c-stop
                    target=b.c+float(self.c["s6r"])*risk
                    pos=self._entry(
                        6,b.s,"LONG",b.c,b.t,
                        reason="FIRST_RANGE_HIGH_RETEST_WITH_VOLUME_POC_STOP",
                        stop=stop,target=target,
                        meta={
                            "range_high":q["h1"],"range_low":q["l1"],"retest_low":b.l,
                            "poc":poc,"poc_bins":int(self.c["s6p"]),"poc_buffer_pct":float(self.c["s6b"]),
                            "risk_per_share":risk,"reward_r":float(self.c["s6r"]),
                        },
                    )
                    if pos: q["done"]=True

    def ranks(self):
        now=datetime.now(NY); td=now.date(); ws=td-timedelta(days=td.weekday()); ms=td.replace(day=1)
        out=[]
        for s,(p,_) in self.last.items():
            if p<=0: continue
            db=self.first.get(s,(None,None))[0]
            hist=self.daily.get(s,{})
            wd=sorted(d for d in hist if ws<=d<=td); md=sorted(d for d in hist if ms<=d<=td)
            wb=self.week.get(s) or (hist[wd[0]]["open"] if wd else None)
            mb=self.month.get(s) or (hist[md[0]]["open"] if md else None)
            dr=(p/db-1)*100 if db else None; wr=(p/wb-1)*100 if wb else None; mr=(p/mb-1)*100 if mb else None
            out.append((s,p,db,wb,mb,dr,wr,mr))
        return out

    def ranking_cycle(self):
        now=datetime.now(NY); rows=self.ranks(); n=int(self.c["rn"])
        blocks={
            "DAILY":sorted([x for x in rows if x[5] is not None and x[5]>0],key=lambda x:x[5],reverse=True)[:n],
            "WEEKLY":sorted([x for x in rows if x[6] is not None and x[6]>0],key=lambda x:x[6],reverse=True)[:n],
            "MONTHLY":sorted([x for x in rows if x[7] is not None and x[7]>0],key=lambda x:x[7],reverse=True)[:n],
        }
        uniq=[]
        for v in blocks.values():
            for x in v:
                if x[0] not in uniq: uniq.append(x[0])
        reasons=self.ai.ranking_reasons(uniq) if _bool(self.c["rr"]) else {}
        parts=["[15m Market Gainers]"]
        for period,idx,baseidx in [("DAILY",5,2),("WEEKLY",6,3),("MONTHLY",7,4)]:
            parts.append("\n"+period)
            for rank,x in enumerate(blocks[period],1):
                rs=reasons.get(x[0],"")
                parts.append(f"{rank}. {x[0]} {x[idx]:+.2f}%"+(f" — {rs}" if rs else ""))
                self.gs.put("Rankings",[_et(now),period,rank,x[0],x[idx],x[1],x[baseidx],rs,""])
        if _bool(self.c["nr"]): self.tg.send("\n".join(parts))
        if self.c.enabled(7): self._s7(blocks["DAILY"],now)

    def _s7(self,daily,now):
        top={x[0]:i+1 for i,x in enumerate(daily)}
        for s,rank in top.items():
            dq=self.rank_seen[s]; dq.append(now)
            cut=now-timedelta(minutes=60)
            while dq and dq[0]<cut: dq.popleft()
            self._logst(7,"TOP10_APPEARANCE",s,"LONG",reason="REGULAR_SESSION_TOP_GAINER",status="WATCH",meta={"rank":rank,"count_60m":len(dq)})
            k=(now.date(),s)
            if len(dq)>=2 and k not in self.rank_bought:
                p=self.last.get(s,(None,None))[0]
                if p:
                    z=self._entry(7,s,"LONG",p,now.astimezone(UTC),reason="TOP10_REPEATED_WITHIN_60M",meta={"rank":rank,"count_60m":len(dq)})
                    if z:self.rank_bought.add(k)

    def s4_load(self):
        td=datetime.now(NY).date()
        if self.s4_load_day==td:
            return
        self.s4_list=self.gs.read_s4(td,int(self.c["s4m"]))
        self.s4_load_day=td
        self._sys("INFO","S4_INPUT","",f"loaded {len(self.s4_list)} symbols",{"symbols":self.s4_list})

    def s4_eval(self):
        if not self.c.enabled(4): return
        now=datetime.now(NY); td=now.date()
        for s in self.s4_list:
            if (td,s) in self.s4_done: continue
            p=self.last.get(s,(None,None))[0]; b=self.first.get(s,(None,None))[0]
            if p is None or b is None: continue
            if p>b:
                self._entry(4,s,"LONG",p,datetime.now(UTC),reason="PREMARKET_TOP10_AND_FIRST_15M_UP",meta={"first_regular_price":b,"change_pct":(p/b-1)*100})
            else:
                self._logst(4,"IGNORE",s,"LONG",reason="FIRST_15M_NOT_UP",status="IGNORED",meta={"first_regular_price":b,"price":p})
            self.s4_done.add((td,s))

    def _next_trade_day(self,d:date):
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import GetCalendarRequest
        cl=TradingClient(self.key,self.sec,paper=True)
        xs=cl.get_calendar(GetCalendarRequest(start=d+timedelta(days=1),end=d+timedelta(days=10)))
        return xs[0].date if xs else d+timedelta(days=1)

    def s5_scan(self):
        if not self.c.enabled(5): return
        td=datetime.now(NY).date()
        if self.s5_scan_day==td:return
        try: target=self._next_trade_day(td)
        except Exception as ex:
            self._sys("ERROR","S5_CAL","",str(ex)); return
        rows,err=self.ai.next_day_events(target,int(self.c["s5m"]))
        if err:
            self._sys("ERROR","S5_SCAN","",err); return
        self.s5_today=rows; self.s5_scan_day=td
        for r in rows:
            self._logst(5,"EVENT_CANDIDATE",r["symbol"],"LONG",reason=r["event"],status="WAIT_OPEN",meta={"event_date":r["date"],"source":r["url"]})
        if self.c.notify(5): self.tg.send(f"[S5 {NAMES[5]}]\n{len(rows)} next-trading-day event candidates found.")

    def s5_buy(self):
        if not self.c.enabled(5):return
        td=datetime.now(NY).date()
        for r in self.s5_today:
            s=r["symbol"]; k=(td,s)
            if k in self.s5_done:continue
            p=self.last.get(s,(None,None))[0]
            if p:
                self._entry(5,s,"LONG",p,datetime.now(UTC),reason="BUY_DAY_BEFORE_SCHEDULED_EVENT",meta={"event":r["event"],"event_date":r["date"],"source":r["url"]})
                self.s5_done.add(k)

    def brief(self):
        if not _bool(self.c["br"]):return
        td=datetime.now(NY).date()
        if self.brief_day==td:return
        text=self.ai.brief(td)
        if text:
            if _bool(self.c["nb"]):self.tg.send("[Pre-market Research Brief]\n"+text)
            self.brief_day=td

    def scheduler(self):
        while not self.dead.is_set():
            try:
                now=datetime.now(NY); m=now.hour*60+now.minute
                if now.weekday()<5:
                    if m==9*60+25: self.s4_load()
                    if m==int(self.c["s5h"])*60+int(self.c["s5n"]): self.s5_scan()
                    if m==int(self.c["bh"])*60+int(self.c["bm"]): self.brief()
                    if m==9*60+31: self.s5_buy()
                    if m==9*60+45: self.s4_eval()
                    if _bool(self.c["rk"]) and 585<=m<=945 and now.minute%int(self.c["hi"])==0:
                        k=now.strftime("%Y-%m-%d-%H-%M")
                        if k not in self.slots:
                            self.ranking_cycle(); self.slots.add(k)
                    if len(self.slots)>500:
                        cut=(now.date()-timedelta(days=5)).isoformat(); self.slots={x for x in self.slots if x[:10]>=cut}
                time.sleep(5)
            except Exception as ex:
                self._sys("ERROR","SCHED","",str(ex)); time.sleep(10)

    def close(self):
        self.dead.set(); self.gs.close()


def _universe(k,s,cfg):
    from alpaca.trading.client import TradingClient
    from alpaca.trading.enums import AssetClass,AssetStatus
    from alpaca.trading.requests import GetAssetsRequest
    wl=[x.strip().upper() for x in str(cfg["wl"]).split(",") if x.strip()]
    if not _bool(cfg["sa"]): return wl
    x=TradingClient(k,s,paper=True).get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY,status=AssetStatus.ACTIVE))
    return sorted({a.symbol for a in x if a.tradable})


def live():
    k1=_need("K01"); k2=_need("K02"); k3=_need("K03"); k4=_need("K04")
    k5=_need("K05"); k6=_need("K06"); k7=os.getenv("K07","").strip(); cfg=Cfg(json.loads(_need("K08")))
    gs=GS(k5,k6,cfg); tg=TG(k3,k4); ai=AI(k7,cfg,gs); core=Core(cfg,gs,tg,ai,k1,k2)
    ss=_universe(k1,k2,cfg)
    core._sys("INFO","START","",f"universe={len(ss)}; strategies={[i for i in range(1,8) if cfg.enabled(i)]}")
    core.bootstrap(ss); core.rank_bootstrap(ss)
    threading.Thread(target=core.scheduler,daemon=True).start()
    from alpaca.data.enums import DataFeed
    from alpaca.data.live import StockDataStream
    from alpaca.data.models import Bar
    stream=StockDataStream(k1,k2,feed=DataFeed.IEX)
    async def h(bar:Bar):
        try:
            x=bar.timestamp.astimezone(NY); m=x.hour*60+x.minute
            if x.weekday()<5 and 570<=m<960:
                core.on_bar(M1(bar.symbol,bar.timestamp,float(bar.open),float(bar.high),float(bar.low),float(bar.close),float(bar.volume)))
        except Exception as ex:
            core._sys("ERROR","BAR",getattr(bar,"symbol",""),str(ex))
            L.exception("bar")
    if _bool(cfg["sa"]): stream.subscribe_bars(h,"*")
    else: stream.subscribe_bars(h,*ss)
    try: stream.run()
    finally: core.close()


def self_test():
    a=Agg5(); t=datetime(2026,9,8,13,30,tzinfo=UTC)
    out=None
    for i in range(6):
        z=a.push(M1("X",t+timedelta(minutes=i),10+i*.1,10.2+i*.1,9.9+i*.1,10.1+i*.1,100+i))
        if z: out=z
    assert out is not None and out.start.minute==30 and out.end.minute==35
    r=Roll(3); r.add("X",1,10); r.add("X",2,20); r.add("X",3,30)
    st=r.stats("X"); assert st and abs(st[0]-2)<1e-9 and abs(st[2]-20)<1e-9
    up=[100,100.3,100.8]; total=math.log(up[2]/up[0]); r2=math.log(up[2]/up[1]); assert total>=0.005 and max(0,-r2)<=0.0025
    dn=[100,99.7,99.2]; total=math.log(dn[2]/dn[0]); r2=math.log(dn[2]/dn[1]); assert total<=-0.005 and max(0,r2)<=0.0025
    entry=101; stop=99.8; target=entry+3*(entry-stop); assert abs(target-104.6)<1e-9
    peak=120; p=115; dd=(p/peak-1)*100; assert dd<0
    base=datetime(2026,9,8,14,0,tzinfo=UTC)
    for mins in (1,3,5):
        second=base+timedelta(minutes=mins)
        assert 0 < (second-base).total_seconds()/60 <= 5.0
    assert (base+timedelta(minutes=6)-base).total_seconds()/60 > 5.0
    fake=object.__new__(Core)
    poc=fake._poc([(99.8,100.0,100),(100.0,100.2,5000),(100.2,100.4,100)],24)
    assert poc is not None and 99.95 < poc < 100.25
    print("SELF_TEST_OK: aggregation, rolling stats, S1/S3 continuation logic, S2 consecutive-within-5m timing + fresh news gate, volume POC, 3R math, trailing math")


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--self-test",action="store_true"); x=ap.parse_args()
    if x.self_test:self_test()
    else:live()
