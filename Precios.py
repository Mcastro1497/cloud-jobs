# ws_ingestor_last_prices_usd.py
# -*- coding: utf-8 -*-

import os, time, signal, threading
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

import requests
import pyRofex
from supabase import create_client

# ========= Config =========
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqllthcnlioujctfcseh.supabase.co")
SERVICE_KEY  = os.getenv("SERVICE_KEY",  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxbGx0aGNubGlvdWpjdGZjc2VoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzM4NTYxMywiZXhwIjoyMDcyOTYxNjEzfQ.p64ysy4DZ2w-QvpBgLogWFcD2qXF_TfTlartQ4BoKMI")

PRIMARY_USER = os.getenv("ECO_USER", "20402019396")
PRIMARY_PASS = os.getenv("ECO_PASS", "Mercado14$")
PRIMARY_ACCT = os.getenv("ECO_ACCT", "222958")

DEFAULT_SEG  = os.getenv("DEFAULT_SEGMENT", "24hs").strip()
PUSH_INTERVAL_SEC = float(os.getenv("PUSH_INTERVAL_SEC", 10.0))

ECO_BASE = os.getenv("ECO_BASE", "https://api.eco.xoms.com.ar")
ECO_WS   = os.getenv("ECO_WS",   "wss://api.eco.xoms.com.ar")

ONLY_ACTIVE = str(os.getenv("ALL_TICKERS_ONLY_ACTIVE", "true")).lower() in ("1","true","yes","y")

sb = create_client(SUPABASE_URL, SERVICE_KEY)

# ========= Helpers =========
def _clean_symbol(s: str) -> str:
    return (s or "").strip().upper()

def extract_ticker(full_symbol: str) -> str:
    # "MERV - XMEV - AL30 - 24hs" -> "AL30"
    s = (full_symbol or "").strip()
    parts = [p.strip() for p in s.split(" - ")]
    if len(parts) >= 3 and parts[2]:
        return parts[2]
    if len(parts) >= 1 and parts[0]:
        return parts[0]
    return s

def format_byma_symbol(ticker: str, segmento: str = None) -> str:
    seg = (segmento or DEFAULT_SEG).strip()
    return f"MERV - XMEV - {ticker} - {seg}"

def _norm_key(k: str) -> str:
    return "".join(ch for ch in str(k).lower() if ch.isalnum())

def init_connection_eco():
    pyRofex._set_environment_parameter("url", ECO_BASE + "/", pyRofex.Environment.LIVE)
    pyRofex._set_environment_parameter("ws",  ECO_WS   + "/", pyRofex.Environment.LIVE)
    r = requests.post(ECO_BASE + "/login",
                      json={"username": PRIMARY_USER, "password": PRIMARY_PASS},
                      timeout=8)
    print("[DEBUG] ECO /login ->", r.status_code)
    r.raise_for_status()
    pyRofex.initialize(user=PRIMARY_USER, password=PRIMARY_PASS,
                       account=PRIMARY_ACCT, environment=pyRofex.Environment.LIVE)
    print(f"[OK] Conectado a ECO {ECO_BASE} (Environment.LIVE)")

# ========= Tickers desde public.all_tickers =========
def get_all_tickers_from_all_tickers():
    """
    Devuelve:
      - wanted: lista de tickers únicos (str)
      - seg_pref: dict {ticker -> segmento preferido}
    Soporta columnas: symbol/ticker, segment/preferred_segment/seg/Segmento, is_active.
    Agrega EXTRA_TICKERS y asegura AL30/AL30D para FX MEP.
    """
    rows = sb.table("all_tickers").select("*").execute().data or []
    uniques = set()
    seg_pref = {}

    seg_keys = {"segment", "preferred_segment", "seg", "segmento"}  # normalizados
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
        if not sym:
            continue
        if ONLY_ACTIVE and ("is_active" in r) and (str(r["is_active"]).lower() == "false"):
            continue

        chosen_seg = None
        for k, v in r.items():
            nk = _norm_key(k)
            if nk in seg_keys:
                chosen_seg = (v or "").strip()
                break
        if not chosen_seg:
            chosen_seg = DEFAULT_SEG

        uniques.add(sym)
        seg_pref[sym] = chosen_seg

    extras = [_clean_symbol(x) for x in os.getenv("EXTRA_TICKERS","").split(",") if x.strip()]
    for x in extras:
        uniques.add(x)
        seg_pref.setdefault(x, DEFAULT_SEG)

    uniques.update({"AL30", "AL30D"})
    seg_pref.setdefault("AL30", DEFAULT_SEG)
    seg_pref.setdefault("AL30D", DEFAULT_SEG)

    syms = sorted(uniques)
    print(f"[all_tickers] {len(rows)} filas leídas -> {len(syms)} tickers únicos (ONLY_ACTIVE={ONLY_ACTIVE})")
    if syms:
        print("[all_tickers] muestra:", syms[:20], ("..." if len(syms)>20 else ""))
    return syms, seg_pref

def fetch_merv_instruments_symbols():
    """Lista oficial de instrumentos MERV y mapeo ticker -> símbolos."""
    try:
        resp = pyRofex.get_instruments('by_segments',
                                       market=pyRofex.Market.ROFEX,
                                       market_segment=[pyRofex.MarketSegment.MERV])
        data = resp.get("instruments") or []
    except Exception:
        print("[info] fallback get_all_instruments() y filtro 'MERV - '")
        data = (pyRofex.get_all_instruments() or {}).get("instruments") or []

    symbols = set()
    by_ticker = {}
    for it in data:
        sym = (it.get("symbol") or "").strip()
        if not sym.startswith("MERV - "):
            continue
        symbols.add(sym)
        tk = extract_ticker(sym)
        by_ticker.setdefault(tk, []).append(sym)
    print(f"[instr] MERV symbols disponibles: {len(symbols)}")
    return symbols, by_ticker

# ========= Supabase upsert =========
def upsert_last_prices_row(ticker: str, price_ars, bid=None, ask=None,
                           fx_mep=None, price_usd=None, closing_price=None, change=None):
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "symbol":        ticker,     # PK limpio
        "last":          price_ars,  # precio ARS
        "bid":           bid,
        "ask":           ask,
        "price_ars":     price_ars,
        "fx_mep":        fx_mep,
        "price_usd":     price_usd,
        "closing_price": closing_price,
        "change":        change,
        "ts":            now_iso
    }
    sb.table("last_prices").upsert(payload).execute()

# ========= Estado =========
_lock = threading.Lock()
_stop_event = threading.Event()

_latest_by_symbol = {}               # full_symbol -> {ticker,last_ars,bid,ask,closing_price,seen_ts}
_ref_prices = {"AL30": None, "AL30D": None}
_last_push_ts = {}                   # por ticker limpio

# ========= WS Handlers =========
def market_data_handler(message: dict):
    full_symbol = (message.get("instrumentId") or {}).get("symbol")
    md = message.get("marketData") or {}

    last = (md.get("LA") or {}).get("price")

    bid = md.get("BI")[0].get("price") if isinstance(md.get("BI"), list) and md["BI"] else None
    ask = md.get("OF")[0].get("price") if isinstance(md.get("OF"), list) and md["OF"] else None

    # Closing Price
    cl_entry = md.get("CL")
    closing = None
    if isinstance(cl_entry, dict):
        closing = cl_entry.get("price")
    elif isinstance(cl_entry, (int, float)):
        closing = cl_entry

    if not full_symbol or last is None:
        return

    ticker = extract_ticker(full_symbol)
    with _lock:
        prev = _latest_by_symbol.get(full_symbol, {})
        _latest_by_symbol[full_symbol] = {
            "ticker": ticker,
            "last_ars": float(last),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            # persistimos el último closing conocido si en este mensaje no viene
            "closing_price": float(closing) if closing is not None else prev.get("closing_price"),
            "seen_ts": time.time()
        }
        if ticker in _ref_prices:
            _ref_prices[ticker] = float(last)

def error_handler(msg):       print("WS Error:", msg)
def exception_handler(e):     print("WS Exception:", getattr(e, "msg", str(e)))

# ========= Push loop =========
def compute_fx_and_usd(price_ars):
    al30  = _ref_prices.get("AL30")
    al30d = _ref_prices.get("AL30D")
    if al30 and al30d and al30 > 0:
        fx = al30d / al30
        return fx, (price_ars * fx)
    return None, None

def pusher_loop():
    while not _stop_event.is_set():
        now = time.time()
        with _lock:
            for full_sym, data in list(_latest_by_symbol.items()):
                tk   = data["ticker"]
                ars  = data["last_ars"]
                bid, ask = data.get("bid"), data.get("ask")
                clp  = data.get("closing_price")  # puede ser None

                # Variación = last / closing - 1 (sólo si closing válido)
                chg = None
                if clp is not None:
                    try:
                        if float(clp) > 0:
                            chg = (ars / float(clp)) - 1.0
                    except Exception:
                        chg = None

                last_push = _last_push_ts.get(tk, 0.0)
                if (now - last_push) >= PUSH_INTERVAL_SEC:
                    fx_mep, usd = compute_fx_and_usd(ars)
                    upsert_last_prices_row(
                        ticker=tk,
                        price_ars=ars,
                        bid=bid,
                        ask=ask,
                        fx_mep=fx_mep,
                        price_usd=usd,
                        closing_price=clp,
                        change=chg
                    )
                    _last_push_ts[tk] = now
                    chg_txt = f"{chg:.2%}" if chg is not None else "N/A"
                    print(f"[PUSH] {tk:8s} ARS={ars:,.6f}  CL={clp}  CHG={chg_txt}  FX={fx_mep}  USD={usd}")
        _stop_event.wait(1.0)

# ========= Main =========
if __name__ == "__main__":
    running = True
    def stop_connection(sig, frame):
        global running
        print("\nDeteniendo...")
        running = False
        _stop_event.set()
    signal.signal(signal.SIGINT, stop_connection)

    # 1) ECO
    init_connection_eco()

    # 2) Tickers deseados desde ALL_TICKERS (+ segmento preferido si existe)
    wanted, seg_pref = get_all_tickers_from_all_tickers()

    # 3) Símbolos canónicos MERV (REST) y mapeo por ticker
    symbols_set, by_ticker = fetch_merv_instruments_symbols()

    # 4) Resolver símbolo por ticker (preferimos seg_pref[ticker] o DEFAULT_SEG)
    symbols_ws = []
    not_found = []
    for tk in wanted:
        candidates = by_ticker.get(tk, [])
        if not candidates:
            not_found.append(tk); continue
        target_seg = (seg_pref.get(tk) or DEFAULT_SEG).strip()
        pick = None
        for c in candidates:
            if c.endswith(f" - {target_seg}"):
                pick = c; break
        if pick is None:
            for c in candidates:
                if c.endswith(f" - {DEFAULT_SEG}"):
                    pick = c; break
        if pick is None:
            pick = candidates[0]
        symbols_ws.append(pick)

    print(f"[ws] segmento default = {DEFAULT_SEG}")
    print(f"[ws] suscribiendo {len(symbols_ws)} símbolos (por all_tickers):")
    for s in symbols_ws[:25]:
        print("  -", s)
    if len(symbols_ws) > 25:
        print("  ...", len(symbols_ws)-25, "más")
    if not_found:
        print("[warn] tickers sin símbolo MERV:", not_found[:20], ("..." if len(not_found)>20 else ""))

    # 5) WS con Closing Price incluido
    entries = [pyRofex.MarketDataEntry.LAST,
               pyRofex.MarketDataEntry.BIDS,
               pyRofex.MarketDataEntry.OFFERS,
               pyRofex.MarketDataEntry.CLOSING_PRICE]
    pyRofex.init_websocket_connection(
        market_data_handler=market_data_handler,
        error_handler=error_handler,
        exception_handler=exception_handler
    )
    pyRofex.market_data_subscription(tickers=symbols_ws, entries=entries)

    # 6) loop de push
    t = threading.Thread(target=pusher_loop, daemon=True)
    t.start()
    try:
        while running:
            time.sleep(0.5)
    finally:
        _stop_event.set()
        t.join(timeout=2.0)
        pyRofex.close_websocket_connection()
        print("WS cerrado.")
