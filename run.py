# run_all_prices_then_metrics.py
# -*- coding: utf-8 -*-
"""
Orquestador único:
1) Lanza Precios (WebSocket) por PRICES_RUN_SECONDS
2) Mata Precios
3) Espera DELAY_AFTER_PRICES_SEC (default: 15s)
4) Corre CER once()
5) Corre TIR once()
6) Repite cada CYCLE_INTERVAL_SEC
"""

import os, time, signal, subprocess, sys
from datetime import datetime, timezone

# ===== Config =====
PRICES_SCRIPT       = os.getenv("PRICES_SCRIPT", "Precios.py")
PRICES_RUN_SECONDS  = int(os.getenv("PRICES_RUN_SECONDS", "25"))
DELAY_AFTER_PRICES_SEC = int(os.getenv("DELAY_AFTER_PRICES_SEC", "15"))  # <<< NUEVO
CYCLE_INTERVAL_SEC  = int(os.getenv("CYCLE_INTERVAL_SEC", "120"))
STATUS_INTERVAL_SEC = int(os.getenv("STATUS_INTERVAL_SEC", "60"))
PYTHON_EXEC         = os.getenv("PYTHON_EXEC", sys.executable or "python")

# ===== Import CER/TIR once() =====
import importlib.util

def _import_module_from_path(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar {module_name} desde {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

CER_PATH = os.getenv("CER_PATH", "CER.py")
TIR_PATH = os.getenv("TIR_PATH", "ON.py")

cer_mod = _import_module_from_path(CER_PATH, "cer_mod")
tir_mod = _import_module_from_path(TIR_PATH, "tir_mod")

if not hasattr(cer_mod, "once"):
    raise RuntimeError("El módulo CER no expone once()")
if not hasattr(tir_mod, "once"):
    raise RuntimeError("El módulo TIR no expone once()")

_last_status_ts = 0.0
def status_print():
    global _last_status_ts
    now = time.time()
    if now - _last_status_ts >= STATUS_INTERVAL_SEC:
        print(f"datos actualizados hora {datetime.now().strftime('%H:%M:%S')}")
        _last_status_ts = now

def run_prices_once():
    """
    Lanza el script de Precios como subproceso por PRICES_RUN_SECONDS.
    Se confía en que en ese lapso el WS reciba ticks y haga upsert en last_prices.
    Luego lo cerramos con SIGINT y SIGTERM si hiciera falta.
    """
    env = os.environ.copy()
    env.setdefault("STATUS_INTERVAL_SEC", str(STATUS_INTERVAL_SEC))

    proc = subprocess.Popen([PYTHON_EXEC, PRICES_SCRIPT],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, env=env)

    start = time.time()
    try:
        while time.time() - start < PRICES_RUN_SECONDS:
            line = proc.stdout.readline() if proc.stdout else ""
            if not line:
                time.sleep(0.2); continue
            status_print()
    except Exception:
        pass

    # Cierre suave
    try: proc.send_signal(signal.SIGINT)
    except Exception: pass
    try: proc.wait(timeout=5)
    except Exception:
        try:
            proc.terminate(); proc.wait(timeout=5)
        except Exception:
            try: proc.kill()
            except Exception: pass

def run_cycle():
    # 1) Export de precios (WS) por un tramo corto
    run_prices_once()

    # 2) Espera adicional para asegurar writes a DB (default 15s)
    if DELAY_AFTER_PRICES_SEC > 0:
        time.sleep(DELAY_AFTER_PRICES_SEC)

    # 3) Calcular CER (una pasada)
    try:
        n_cer = cer_mod.once()
    except Exception as e:
        n_cer = 0
        print(f"datos actualizados hora {datetime.now().strftime('%H:%M:%S')}  [CER error: {e}]")

    # 4) Calcular TIR/Duration (una pasada)
    try:
        n_tir = tir_mod.once()
    except Exception as e:
        n_tir = 0
        print(f"datos actualizados hora {datetime.now().strftime('%H:%M:%S')}  [TIR error: {e}]")

    # 5) Único print de resumen del ciclo
    print(f"datos actualizados hora {datetime.now().strftime('%H:%M:%S')}  [CER={n_cer} | TIR={n_tir}]")

def main():
    while True:
        t0 = time.time()
        run_cycle()
        elapsed = time.time() - t0
        sleep_left = max(0, CYCLE_INTERVAL_SEC - elapsed)
        if sleep_left > 0:
            time.sleep(sleep_left)

if __name__ == "__main__":
    main()
