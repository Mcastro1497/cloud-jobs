# on_hd_ytm_duration.py (one-line-per-cycle)
# -*- coding: utf-8 -*-

import os, time
from math import isfinite
from datetime import datetime, timezone, time as dtime, date as dtdate
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# ===== Zona horaria =====
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def _get_local_tz():
    tzname = os.getenv("LOCAL_TZ", "America/Argentina/Cordoba")
    if ZoneInfo is not None:
        try:
            return ZoneInfo(tzname)
        except Exception:
            pass
    return timezone.utc

LOCAL_TZ = _get_local_tz()
def _now_local_str() -> str:
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

# ===== Config =====
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqllthcnlioujctfcseh.supabase.co")
SERVICE_KEY  = os.getenv("SERVICE_KEY",  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxbGx0aGNubGlvdWpjdGZjc2VoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzM4NTYxMywiZXhwIjoyMDcyOTYxNjEzfQ.p64ysy4DZ2w-QvpBgLogWFcD2qXF_TfTlartQ4BoKMI")
INTERVAL_SEC = float(os.getenv("INTERVAL_SEC", "60"))
PAGE_SIZE    = int(os.getenv("PAGE_SIZE", "1000"))
DEBUG_TICKER = (os.getenv("DEBUG_TICKER") or "").strip().upper() or None

ONS_FLOWS_TABLE       = os.getenv("ONS_FLOWS_TABLE", "ons_flows")
SOBERANOS_FLOWS_TABLE = os.getenv("SOBERANOS_FLOWS_TABLE", "soberanos_flows")

FILTER_FLOWS_BY_ALL_TICKERS = (os.getenv("FILTER_FLOWS_BY_ALL_TICKERS", "true").lower() == "true")
IN_CHUNK_SIZE = int(os.getenv("IN_CHUNK_SIZE", "300"))

sb = create_client(SUPABASE_URL, SERVICE_KEY)

# ===== Calendario =====
def load_holidays_dates_from_supabase() -> Set[dtdate]:
    hols: Set[dtdate] = set()
    try:
        rows = sb.table("holidays").select("holiday_date").execute().data or []
        for r in rows:
            dt = pd.to_datetime(r.get("holiday_date"), errors="coerce")
            if pd.notna(dt):
                hols.add(dt.date())
    except Exception:
        pass
    return hols

def next_business_day(local_date: pd.Timestamp, holidays: Set[dtdate]) -> pd.Timestamp:
    d = local_date + pd.Timedelta(days=1)
    while d.weekday() >= 5 or d.date() in holidays:
        d += pd.Timedelta(days=1)
    return d

def t1_eod_cutoff_utc(now_utc: datetime, holidays: Set[dtdate]) -> Tuple[datetime, pd.Timestamp, datetime]:
    today_local = now_utc.astimezone(LOCAL_TZ).date()
    t1_local_date = next_business_day(pd.Timestamp(today_local), holidays).date()
    t1_local_start = datetime.combine(t1_local_date, dtime(0, 0, 0, 0), tzinfo=LOCAL_TZ)
    return t1_local_start.astimezone(timezone.utc), pd.Timestamp(t1_local_date), t1_local_start.astimezone(timezone.utc)

# ===== Finanzas =====
def _to_dt_aware_utc(x):
    if isinstance(x, tuple): x = x[0]
    if isinstance(x, pd.Timestamp): x = x.to_pydatetime()
    if isinstance(x, datetime):
        if x.tzinfo is None: x = x.replace(tzinfo=timezone.utc)
        return x.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    x = pd.to_datetime(x, utc=True).to_pydatetime()
    return x.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def _yearfrac_365(d0: datetime, d1: datetime) -> float:
    d0 = _to_dt_aware_utc(d0); d1 = _to_dt_aware_utc(d1)
    return (d1 - d0).days / 365.0

def _xirr_f_and_df(rate: float, cashflows: List[Tuple[datetime, float]]):
    d0 = cashflows[0][0]
    one = 1.0 + rate
    if one <= 0:  return float("inf"), float("inf")
    f = 0.0; df = 0.0
    for (di, cfi) in cashflows:
        ti = _yearfrac_365(d0, di)
        denom = one ** ti
        f  += cfi / denom
        df += cfi * (-ti) * (one ** (-ti - 1.0))
    return f, df

def xirr_excel_style(cashflows: List[Tuple[datetime, float]], guess: float = 0.10):
    cashflows = sorted(cashflows, key=lambda x: x[0])
    r = guess
    for _ in range(80):
        f, df = _xirr_f_and_df(r, cashflows)
        if not (df and abs(df) > 1e-18): break
        r_next = r - (f/df)
        if r_next <= -0.9999: break
        if abs(r_next - r) < 1e-12: return r_next
        r = r_next
    def fval(x): return _xirr_f_and_df(x, cashflows)[0]
    grid = [-0.9,-0.5,-0.1,0.0,0.02,0.05,0.08,0.10,0.15,0.25,0.5,1.0,2.0,5.0,10.0]
    a = b = None
    last_x = last_y = None
    for x in grid:
        y = fval(x)
        if last_x is not None and pd.notna(last_y) and pd.notna(y) and last_y * y <= 0:
            a, b = last_x, x; break
        last_x, last_y = x, y
    if a is None:
        a, b = 0.0, 10.0
        fa, fb = fval(a), fval(b)
        tries = 0
        while fa * fb > 0 and b < 200 and tries < 12:
            b *= 2; fb = fval(b); tries += 1
        if fa * fb > 0: return None
    lo, hi = a, b
    flo, fhi = fval(lo), fval(hi)
    for _ in range(200):
        m = 0.5*(lo+hi); fm = fval(m)
        if abs(fm) < 1e-10 or (hi-lo) < 1e-12: return m
        if flo*fm <= 0: hi, fhi = m, fm
        else:           lo, flo = m, fm
    return m

def macaulay_duration(cashflows_pos: List[Tuple[float, float]], r: float):
    if r <= -0.9999: return None
    pv  = sum(cf / (1.0 + r)**t for t, cf in cashflows_pos)
    if pv <= 0: return None
    num = sum(t * (cf / (1.0 + r)**t) for t, cf in cashflows_pos)
    return num / pv

# ===== Helpers =====
def _chunks(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def _norm_key(k: str) -> str:
    return "".join(ch for ch in k.lower() if ch.isalnum())

def _norm_type_str(s: str) -> str:
    s = (s or "").strip().lower()
    if s.startswith("obligac"): return "ON"
    if "hard" in s or s.startswith("sober"): return "HD"
    return s or "UNKNOWN"

# ===== IO Supabase =====
def load_all_tickers_with_type() -> Tuple[List[str], Dict[str, str]]:
    rows = sb.table("all_tickers").select("*").execute().data or []
    out: Set[str] = set()
    type_map: Dict[str, str] = {}
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
        if not sym: continue
        is_active = r.get("is_active")
        if is_active is not None and (is_active is False or str(is_active).lower()=="false"):
            continue
        instr_type_val = None
        for k, v in r.items():
            nk = _norm_key(str(k))
            if nk in {"instrumenttype","tipoinstrumento","tipo","tipodeinstrumento"}:
                instr_type_val = v; break
        instr_type = _norm_type_str(str(instr_type_val) if instr_type_val is not None else "")
        out.add(sym); type_map[sym] = instr_type
    return sorted(out), type_map

def _normalize_flows_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], utc=True, errors="coerce")
    df["total"] = (
        df["total"].astype(str)
                    .str.replace(",", ".", regex=False)
                    .replace({"": "0", "None": "0"})
                    .astype(float)
    )
    return df

def fetch_future_flows_from_table(table_name: str, cutoff_utc: datetime,
                                  tickers_filter: Optional[List[str]] = None) -> pd.DataFrame:
    cols = "ticker, fecha_pago, total, moneda_pago"
    if tickers_filter and len(tickers_filter) > 0 and (os.getenv("FILTER_FLOWS_BY_ALL_TICKERS","true").lower()=="true"):
        frames = []
        for chunk in _chunks(tickers_filter, IN_CHUNK_SIZE):
            start = 0
            while True:
                q = (sb.table(table_name)
                       .select(cols)
                       .gt("fecha_pago", cutoff_utc.isoformat())
                       .in_("ticker", chunk)
                       .order("fecha_pago", desc=False)
                       .range(start, start + PAGE_SIZE - 1))
                data = q.execute().data or []
                if not data: break
                frames.append(pd.DataFrame(data))
                if len(data) < PAGE_SIZE: break
                start += PAGE_SIZE
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["ticker","fecha_pago","total","moneda_pago"])
        return _normalize_flows_df(df).sort_values(["ticker","fecha_pago"]).reset_index(drop=True)
    else:
        start = 0; out = []
        while True:
            q = (sb.table(table_name)
                   .select(cols)
                   .gt("fecha_pago", cutoff_utc.isoformat())
                   .order("fecha_pago", desc=False)
                   .range(start, start + PAGE_SIZE - 1))
            chunk = q.execute().data or []
            out.extend(chunk)
            if len(chunk) < PAGE_SIZE: break
            start += PAGE_SIZE
        df = pd.DataFrame(out) if out else pd.DataFrame(columns=["ticker","fecha_pago","total","moneda_pago"])
        return _normalize_flows_df(df).sort_values(["ticker","fecha_pago"]).reset_index(drop=True)

def load_last_prices() -> Tuple[Dict[str, float], Dict[str, float]]:
    price_usd_map: Dict[str, float] = {}
    last_map: Dict[str, float] = {}
    try:
        rows = sb.table("last_prices").select("symbol, price_usd, last").execute().data or []
    except Exception:
        rows = sb.table("last_prices").select("symbol, price_usd").execute().data or []
    for r in rows or []:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym: continue
        pu = r.get("price_usd"); lv = r.get("last")
        try:
            pu = float(pu) if pu is not None else None
            if pu and pu > 0: price_usd_map[sym] = pu
        except Exception: pass
        try:
            lv = float(lv) if lv is not None else None
            if lv and lv > 0: last_map[sym] = lv
        except Exception: pass
    return price_usd_map, last_map

def upsert_metrics(ticker: str, ytm: float, duration_y: Optional[float]):
    sb.table("last_prices").upsert({
        "symbol": ticker,
        "ytm": ytm,
        "duration_y": duration_y,
        "ts": datetime.now(timezone.utc).isoformat()
    }).execute()

# ===== Ciclo principal =====
def once() -> int:
    now_utc = datetime.now(timezone.utc)
    holidays = load_holidays_dates_from_supabase()
    cutoff_utc, t1_local_date, valuation_utc = t1_eod_cutoff_utc(now_utc, holidays)

    base_tickers, type_map = load_all_tickers_with_type()
    if not base_tickers:
        return 0

    on_tickers = [t for t in base_tickers if type_map.get(t) in {"ON","UNKNOWN"}]
    hd_tickers = [t for t in base_tickers if type_map.get(t) == "HD"]

    flows_on = fetch_future_flows_from_table(ONS_FLOWS_TABLE, cutoff_utc, on_tickers)
    flows_hd = fetch_future_flows_from_table(SOBERANOS_FLOWS_TABLE, cutoff_utc, hd_tickers)
    price_usd_map, last_map = load_last_prices()

    flows = pd.concat([flows_on, flows_hd], ignore_index=True) if (not flows_on.empty or not flows_hd.empty) else pd.DataFrame(columns=["ticker","fecha_pago","total","moneda_pago"])
    if flows.empty or not (price_usd_map or last_map):
        return 0

    candidates = sorted(set(base_tickers) & set(flows["ticker"].unique()))
    if not candidates:
        return 0

    updated = 0
    for tk in candidates:
        instr_type = type_map.get(tk, "UNKNOWN")
        price = last_map.get(tk) if instr_type == "HD" else price_usd_map.get(tk)
        if price is None or price <= 0:
            continue

        g = (flows.loc[flows["ticker"] == tk, ["fecha_pago","total"]]
                 .groupby("fecha_pago", as_index=False, sort=True)
                 .agg(total=("total","sum"))
                 .sort_values("fecha_pago"))

        cf_series = [(valuation_utc, -float(price))]
        for _, row in g.iterrows():
            dtp = row["fecha_pago"].to_pydatetime()
            amt = float(row["total"])
            if amt != 0.0: cf_series.append((dtp, amt))
        if len(cf_series) < 2:
            continue

        r = xirr_excel_style(cf_series, guess=0.10)
        if r is None or not isfinite(r):
            continue

        cfs_pos = []
        for _, row in g.iterrows():
            dtp = row["fecha_pago"].to_pydatetime()
            amt = float(row["total"])
            t = _yearfrac_365(valuation_utc, dtp)
            cfs_pos.append((t, amt))
        duration_y = macaulay_duration(cfs_pos, r)

        upsert_metrics(tk, float(r), None if duration_y is None else float(duration_y))
        updated += 1

    return updated

def main():
    while True:
        try:
            n = once()
            if n > 0:
                print(f"[{_now_local_str()}] UPDATED {n} tickers")
        except Exception as e:
            print(f"[{_now_local_str()}] [ERROR]", e)
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
