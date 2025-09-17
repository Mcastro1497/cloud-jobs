# tir_hd_on.py
from datetime import datetime, timezone
from importlib import import_module
from src.app.utils import log

tir_mod = import_module("TIR_v3")

async def run():
    """Corre una valuación única (ON + HD)."""
    updated = tir_mod.once()
    print(f"[tir_hd_on] {datetime.now(timezone.utc).isoformat()} updated={updated}")
