# -*- coding: utf-8 -*-
"""
Calcula TIR (XIRR), Duration (Macaulay) y TNA (solo Fija cupón 0) para soberanos ARS.
- Flujos: public.soberanos_ars_flows
- Tipo: columna 'tipo' (Fija / CER) en soberanos_ars_flows
- Precio:
    * Fija: last_prices.last (ARS)
    * CER : last_prices.last / (CER_t10 / CER_emision)
            - CER_emision: soberanos_ars_details.cer_emision
            - CER_t10: cer_historico.valor_cer en fecha = (T+1 hábil) - 10 días hábiles
- Fecha de valuación para TIR/Duration/TNA: T+1 hábil fin de día local
- Upsert en last_prices: ytm (decimal anual), duration_y (años), tna (decimal anual), ts
"""

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

# ===== Config =====
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqllthcnlioujctfcseh.supabase.co")
SERVICE_KEY  = os.getenv("SERVICE_KEY",  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxbGx0aGNubGlvdWpjdGZjc2VoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzM4NTYxMywiZXhwIjoyMDcyOTYxNjEzfQ.p64ysy4DZ2w-QvpBgLogWFcD2qXF_TfTlartQ4BoKMI")
INTERVAL_SEC = float(os.getenv("INTERVAL_SEC", "60"))
PAGE_SIZE    = int(os.getenv("PAGE_SIZE", "1000"))
DEBUG_TICKER = (os.getenv("DEBUG_TICKER") or "").strip().upper() or None

# Tablas
FLOWS_TABLE    = os.getenv("SOBERANOS_ARS_FLOWS_TABLE", "soberonos_ars_flows").replace("soberonos", "soberanos")  # safe fix
DETAILS_TABLE  = os.getenv("SOBERANOS_ARS_DETAILS_TABLE", "soberanos_ars_details")
CER_HIST_TABLE = os.getenv("CER_HISTORICO_TABLE", "cer_historico")

IN_CHUNK_SIZE = int(os.getenv("IN_CHUNK_SIZE", "300"))

sb = create_client(SUPABASE_URL, SERVICE_KEY)

# ===== Calendario / hábiles =====
def load_holidays_dates_from_supabase() -> Set[dtdate]:
    hols: Set[dtdate] = set()
    try:
        rows = sb.table("holidays").select("holiday_date").execute().data or []
        for r in rows:
            dt = pd.to_datetime(r.get("holiday_date"), errors="coerce")
            if pd.notna(dt):
                hols.add(dt.date())
    except Exception as e:
        print("[WARN] No se pudieron leer feriados de Supabase:", e)
    return hols

def next_business_day(local_date: pd.Timestamp, holidays: Set[dtdate]) -> pd.Timestamp:
    d = local_date + pd.Timedelta(days=1)
    while d.weekday() >= 5 or d.date() in holidays:
        d += pd.Timedelta(days=1)
    return d

def prev_business_day(local_date: pd.Timestamp, holidays: Set[dtdate]) -> pd.Timestamp:
    d = local_date - pd.Timedelta(days=1)
    while d.weekday() >= 5 or d.date() in holidays:
        d -= pd.Timedelta(days=1)
    return d

def minus_n_business_days(local_date: pd.Timestamp, n: int, holidays: Set[dtdate]) -> pd.Timestamp:
    d = pd.Timestamp(local_date.date())
    for _ in range(n):
        d = prev_business_day(d, holidays)
    return d

def t1_eod_cutoff_utc(now_utc: datetime, holidays: Set[dtdate]) -> Tuple[datetime, pd.Timestamp, datetime]:
    """(cutoff_utc, t1_local_date, valuation_utc=T+1 EOD local en UTC)"""
    today_local = now_utc.astimezone(LOCAL_TZ).date()
    t1_local_date = next_business_day(pd.Timestamp(today_local), holidays).date()
    t1_local_eod = datetime.combine(t1_local_date, dtime(23, 59, 59, 999000), tzinfo=LOCAL_TZ)
    return t1_local_eod.astimezone(timezone.utc), pd.Timestamp(t1_local_date), t1_local_eod.astimezone(timezone.utc)

# ===== Finanzas =====
def _yearfrac_365(d0: datetime, d1: datetime) -> float:
    if d0.tzinfo is None: d0 = d0.replace(tzinfo=timezone.utc)
    if d1.tzinfo is None: d1 = d1.replace(tzinfo=timezone.utc)
    return (d1 - d0).total_seconds() / (365.0 * 24 * 3600)

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

def xirr_excel_style(cfs: List[Tuple[datetime, float]], guess: float = 0.10):
    cfs = sorted(cfs, key=lambda x: x[0])
    r = guess
    for _ in range(80):
        f, df = _xirr_f_and_df(r, cfs)
        if not (df and abs(df) > 1e-18): break
        r_next = r - (f/df)
        if r_next <= -0.9999: break
        if abs(r_next - r) < 1e-12: return r_next
        r = r_next
    def fval(x): return _xirr_f_and_df(x, cfs)[0]
    grid = [-0.9, -0.5, -0.1, 0.0, 0.02, 0.05, 0.08, 0.10, 0.15, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]
    a = b = None
    last_x, last_y = None, None
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

def _normalize_flows_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    # columnas: ticker, fecha_pago, total, tipo
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], utc=True, errors="coerce")
    df["total"] = (
        df["total"].astype(str)
                    .str.replace(",", ".", regex=False)
                    .replace({"": "0", "None": "0"})
                    .astype(float)
    )
    df["tipo"] = df["tipo"].astype(str).str.strip().str.title()  # "Fija"/"Cer"
    return df

def fetch_future_flows(cutoff_utc: datetime) -> pd.DataFrame:
    cols = "ticker, fecha_pago, total, tipo"
    start = 0
    frames = []
    while True:
        q = (sb.table(FLOWS_TABLE)
               .select(cols)
               .gt("fecha_pago", cutoff_utc.isoformat())
               .order("fecha_pago", desc=False)
               .range(start, start + PAGE_SIZE - 1))
        data = q.execute().data or []
        if not data: break
        frames.append(pd.DataFrame(data))
        if len(data) < PAGE_SIZE: break
        start += PAGE_SIZE
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols.split(", "))
    return _normalize_flows_df(df).sort_values(["ticker","fecha_pago"]).reset_index(drop=True)

def load_last_prices_last_map() -> Dict[str, float]:
    out: Dict[str, float] = {}
    rows = sb.table("last_prices").select("symbol, last").execute().data or []
    for r in rows:
        sym = (r.get("symbol") or "").strip().upper()
        try:
            lv = float(r.get("last")) if r.get("last") is not None else None
            if sym and lv and lv > 0:
                out[sym] = lv
        except Exception:
            pass
    return out

def load_cer_emision_map(tickers: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not tickers: return out
    for chunk in _chunks(sorted(set(tickers)), IN_CHUNK_SIZE):
        rows = (sb.table(DETAILS_TABLE)
                  .select("ticker, cer_emision")
                  .in_("ticker", chunk)
                  .execute().data or [])
        for r in rows:
            tk = (r.get("ticker") or "").strip().upper()
            try:
                v = float(r.get("cer_emision")) if r.get("cer_emision") is not None else None
                if tk and v and v > 0:
                    out[tk] = v
            except Exception:
                pass
    return out

def load_cer_value_for_date(d: pd.Timestamp) -> Optional[float]:
    try:
        date_str = d.strftime("%Y-%m-%d")
        rows = sb.table(CER_HIST_TABLE).select("fecha, valor_cer").eq("fecha", date_str).limit(1).execute().data or []
        if not rows:
            rows = (sb.table(CER_HIST_TABLE)
                      .select("fecha, valor_cer")
                      .lte("fecha", date_str)
                      .order("fecha", desc=True)
                      .limit(1).execute().data or [])
        if rows:
            v = float(rows[0].get("valor_cer"))
            return v if v > 0 else None
    except Exception as e:
        print("[WARN] No se pudo leer CER para", d, ":", e)
    return None

def upsert_metrics(ticker: str, ytm: float, duration_y: Optional[float], tna: Optional[float]):
    sb.table("last_prices").upsert({
        "symbol": ticker,
        "ytm": ytm,
        "duration_y": duration_y,
        "tna": tna,
        "ts": datetime.now(timezone.utc).isoformat()
    }).execute()

# ===== Ciclo principal =====
def once() -> int:
    now_utc = datetime.now(timezone.utc)

    # 1) Cutoff y fecha de valuación = T+1 EOD local
    holidays = load_holidays_dates_from_supabase()
    cutoff_utc, t1_local_date, valuation_utc = t1_eod_cutoff_utc(now_utc, holidays)
    cer_tminus10_date = minus_n_business_days(t1_local_date, 10, holidays)

    print(f"[INFO] Cutoff (T+1 hábil, fin de día local) = {cutoff_utc.astimezone(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"[INFO] Fecha CER t-10 hábiles = {cer_tminus10_date.strftime('%Y-%m-%d')} (respecto T+1={t1_local_date.strftime('%Y-%m-%d')})")

    # 2) Flujos
    flows = fetch_future_flows(cutoff_utc)
    if flows.empty:
        print("[WARN] sin flujos futuros en soberanos_ars_flows."); return 0

    # 3) Tipo por ticker (prioriza CER si hay mezcla)
    tipo_by_ticker: Dict[str, str] = {}
    for tk, g in flows.groupby("ticker"):
        tipos = set(g["tipo"].dropna().unique().tolist())
        if "Cer" in tipos or "CER" in tipos:
            tipo_by_ticker[tk] = "CER"
        elif "Fija" in tipos:
            tipo_by_ticker[tk] = "Fija"
        else:
            tipo_by_ticker[tk] = (list(tipos)[0] if tipos else "Fija")

    tickers = sorted(tipo_by_ticker.keys())

    # 4) Precios
    last_map = load_last_prices_last_map()
    if not last_map:
        print("[WARN] last_prices vacío (sin 'last')."); return 0

    # 5) Datos para CER
    cer_tickers = [tk for tk in tickers if tipo_by_ticker.get(tk) == "CER"]
    cer_emision_map = load_cer_emision_map(cer_tickers) if cer_tickers else {}
    cer_t10_value = load_cer_value_for_date(cer_tminus10_date) if cer_tickers else None
    if cer_tickers and (cer_t10_value is None):
        print("[WARN] No se pudo determinar CER(t-10). Se omitirán los CER hasta contar con el valor.")

    # 6) Loop
    updated = 0
    skipped = []
    issues = []

    for tk, g in flows.groupby("ticker", sort=True):
        instr_type = tipo_by_ticker.get(tk, "Fija")
        base_last = last_map.get(tk)

        if base_last is None or base_last <= 0:
            skipped.append((tk, instr_type, "sin last")); continue

        # Precio efectivo
        if instr_type.upper() == "CER":
            cer_em = cer_emision_map.get(tk)
            if cer_em is None or cer_em <= 0 or cer_t10_value is None or cer_t10_value <= 0:
                skipped.append((tk, "CER", "sin cer_emision o sin CER(t-10)")); continue
            coef = cer_t10_value / cer_em
            if coef <= 0:
                skipped.append((tk, "CER", f"coef inválido={coef:.6f}")); continue
            price = base_last / coef
            price_src = f"last / (CER_t10/CER_emision) = {base_last:.6f} / ({cer_t10_value:.6f}/{cer_em:.6f})"
        else:
            price = base_last
            price_src = "last"

        # Consolidar CFs por fecha
        gi = (g[["fecha_pago","total"]]
              .groupby("fecha_pago", as_index=False, sort=True)
              .agg(total=("total","sum"))
              .sort_values("fecha_pago"))

        # === Valuación en T+1 EOD ===
        cf_series = [(valuation_utc, -float(price))]
        for _, row in gi.iterrows():
            dtpay = row["fecha_pago"].to_pydatetime()
            amt = float(row["total"])
            if amt != 0.0:
                cf_series.append((dtpay, amt))
        if len(cf_series) < 2:
            skipped.append((tk, instr_type, "sin CFs positivos")); continue

        # XIRR (TEA)
        r = xirr_excel_style(cf_series, guess=0.10)
        if r is None or not isfinite(r):
            issues.append((tk, price, price_src, sum(a for _, a in cf_series[1:]),
                           [(d.strftime('%Y-%m-%d'), a) for d, a in cf_series[:6]]))
            if DEBUG_TICKER and tk == DEBUG_TICKER:
                print(f"[DEBUG] {tk} sin raíz | {price_src} | cf_sum={sum(a for _, a in cf_series[1:]):.6f}")
            continue

        # Duration (Macaulay)
        cfs_pos = []
        for _, row in gi.iterrows():
            dtpay = row["fecha_pago"].to_pydatetime()
            amt = float(row["total"])
            t = _yearfrac_365(valuation_utc, dtpay)
            cfs_pos.append((t, amt))
        duration_y = macaulay_duration(cfs_pos, r)

        # TNA:
        #   - CER: None
        #   - Fija:
        #       * si UN solo flujo positivo futuro => cupón 0:
        #           TNA = ((pago_final/precio) - 1) * (365 / días) = ((payoff/price)-1) / t_years
        #       * si más de uno => con cupones => TNA = None
        if instr_type.upper() == "FIJA":
            pos_rows = gi.loc[gi["total"].astype(float) != 0.0].copy()
            if len(pos_rows) == 1:
                payoff = float(pos_rows.iloc[0]["total"])
                dtpay  = pos_rows.iloc[0]["fecha_pago"].to_pydatetime()
                t_years = _yearfrac_365(valuation_utc, dtpay)
                if payoff > 0 and price > 0 and t_years > 0:
                    tna_val = ((payoff / price) - 1.0) / t_years
                else:
                    tna_val = None
            else:
                tna_val = None
        else:
            tna_val = None

        upsert_metrics(tk, float(r), None if duration_y is None else float(duration_y), tna_val)
        updated += 1

        if DEBUG_TICKER and tk == DEBUG_TICKER:
            print(f"[DEBUG] {tk} ({instr_type}) price={price:,.6f} ({price_src})  XIRR={r:.6%}  Duration={duration_y}  TNA={tna_val}")
        else:
            dur_txt = None if duration_y is None else round(duration_y, 4)
            tna_txt = None if tna_val is None else round(tna_val, 6)
            reason = "" if (instr_type.upper() != "FIJA") else (f"  [{'cupón 0' if tna_val is not None else 'con cupones => sin TNA'}]")
            print(f"[OK] {tk:8s} ({instr_type}) price={price:,.6f}  XIRR={r:.6%}  duration_y={dur_txt}  tna={tna_txt}{reason}")

    if skipped:
        sample = ", ".join([f"{tk}({t}:{why})" for tk,t,why in skipped[:10]])
        print(f"[INFO] saltados ({len(skipped)}): {sample}")

    if issues:
        print("[INFO] casos sin raíz/convergencia (muestra máx 5):")
        for tk, p, src, s, c in issues[:5]:
            print(f"  - {tk}: price={p:.6f} src={src} sum_cf={s:.6f} cfs(muestra)={c}")

    return updated

def main():
    while True:
        try:
            n = once()
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Tickers actualizados: {n}")
        except Exception as e:
            print("[ERROR]", e)
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
