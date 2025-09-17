# precios_ws.py
import threading, time, signal
from importlib import import_module
from src.app.utils import log

ws_mod = import_module("Precios_v4")  # tu Precios_v4.py renómbralo así o ajusta el import

def _run_ws_blocking(stop_event: threading.Event):
    """
    Reutilizamos la lógica original del __main__ de tu script.
    Como allí se usa un evento global interno, acá simulamos la misma
    secuencia de arranque y usamos 'stop_event' para pedir corte.
    """
    # 1) ECO + login + init
    ws_mod.init_connection_eco()
    # 2) Tickers all_tickers y símbolos MERV
    wanted, seg_pref = ws_mod.get_all_tickers_from_all_tickers()
    symbols_set, by_ticker = ws_mod.fetch_merv_instruments_symbols()
    symbols_ws = []
    for tk in wanted:
        candidates = by_ticker.get(tk, [])
        if not candidates: continue
        target_seg = (seg_pref.get(tk) or ws_mod.DEFAULT_SEG).strip()
        pick = next((c for c in candidates if c.endswith(f" - {target_seg}")), None) or \
               next((c for c in candidates if c.endswith(f" - {ws_mod.DEFAULT_SEG}")), None) or \
               candidates[0]
        symbols_ws.append(pick)

    entries = [ws_mod.pyRofex.MarketDataEntry.LAST,
               ws_mod.pyRofex.MarketDataEntry.BIDS,
               ws_mod.pyRofex.MarketDataEntry.OFFERS,
               ws_mod.pyRofex.MarketDataEntry.CLOSING_PRICE]
    ws_mod.pyRofex.init_websocket_connection(
        market_data_handler=ws_mod.market_data_handler,
        error_handler=ws_mod.error_handler,
        exception_handler=ws_mod.exception_handler
    )
    ws_mod.pyRofex.market_data_subscription(tickers=symbols_ws, entries=entries)

    # Lanza el pusher en thread como hace tu script
    t = threading.Thread(target=ws_mod.pusher_loop, daemon=True)
    t.start()
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        ws_mod._stop_event.set()
        t.join(timeout=2.0)
        ws_mod.pyRofex.close_websocket_connection()
        print("[precios_ws] WS cerrado.")

class WSManager:
    def __init__(self):
        self._stop = threading.Event()
        self._th = None
    def start(self):
        if self._th and self._th.is_alive():
            return
        self._stop.clear()
        self._th = threading.Thread(target=_run_ws_blocking, args=(self._stop,), daemon=True)
        self._th.start()
        print("[precios_ws] WS iniciado.")
    def stop(self):
        self._stop.set()
        if self._th:
            self._th.join(timeout=5.0)
            print("[precios_ws] WS detenido.")

# API pública para el runner:
ws_manager = WSManager()

async def run_start():
    """Arranca el WS (no bloquea)."""
    ws_manager.start()

async def run_stop():
    """Detiene el WS."""
    ws_manager.stop()
