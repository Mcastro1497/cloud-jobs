import argparse, asyncio, time
from src.app.utils import log, in_window
from src.app.config import settings
from src.app.jobs import cer_metrics, tir_hd_on, precios_ws

ALL_ONESHOT = {   # corren y terminan
  "cer": cer_metrics.run,
  "tir": tir_hd_on.run,
}
ALL_WS = {        # viven dentro de ventana
  "ws": (precios_ws.run_start, precios_ws.run_stop),
}

async def run_oneshot_all():
    # ejecuta CER y luego TIR (secuencial); podés paralelizar si querés
    await cer_metrics.run()
    await tir_hd_on.run()

def cli():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("run-all")         # corre CER + TIR una vez
    sub.add_parser("run-cer")         # sólo CER
    sub.add_parser("run-tir")         # sólo TIR
    sub.add_parser("ws-guard")        # mantiene WS entre START y STOP

    args = p.parse_args()
    if args.cmd == "run-all":
        asyncio.run(run_oneshot_all()); return
    if args.cmd == "run-cer":
        asyncio.run(cer_metrics.run()); return
    if args.cmd == "run-tir":
        asyncio.run(tir_hd_on.run()); return
    if args.cmd == "ws-guard":
        # se queda “vivo” iniciando/parando el WS según ventana
        started = False
        while True:
            inside = in_window(settings.START_HHMM, settings.STOP_HHMM, settings.TZ)
            if inside and not started:
                asyncio.run(ALL_WS["ws"][0]())  # start
                started = True
            elif (not inside) and started:
                asyncio.run(ALL_WS["ws"][1]())  # stop
                started = False
            time.sleep(5)
    p.print_help()
