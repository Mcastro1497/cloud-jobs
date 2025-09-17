# cer_metrics.py
from datetime import datetime, timezone
from importlib import import_module
from src.app.utils import log

# Importa tu script (asegurate que esté en el PYTHONPATH o instala como paquete)
cer_mod = import_module("Cer_v2")  # nombre de tu archivo .py

async def run():
    """
    Ejecuta una corrida única (equivalente a 'once()' en Cer_v2).
    Ideal para llamarlo cada X minutos dentro de la ventana.
    """
    # Cer_v2 ya expone once() y su main() hace un loop; usamos once() directo.
    updated = cer_mod.once()
    print(f"[cer_metrics] {datetime.now(timezone.utc).isoformat()} updated={updated}")
