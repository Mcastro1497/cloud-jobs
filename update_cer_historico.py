# -*- coding: utf-8 -*-
"""
Trae los últimos 70 datos del CER (API BCRA v4.0) y hace upsert en Supabase.
Tabla destino: public.cer_historico(fecha DATE UNIQUE, valor_cer NUMERIC)

ENV requeridas:
  - SUPABASE_URL
  - SERVICE_KEY  (service_role o key con permiso de escritura)

Uso:
  python update_cer_historico.py --cer-id 123           # CER por ID explícito
  python update_cer_historico.py                         # usando CER_ID constante
  python update_cer_historico.py --cer-id 123 --insecure # si el TLS del BCRA molesta
"""

import os
import sys
import argparse
from typing import Any, Dict, List
from datetime import datetime

import requests
try:
    import certifi
    CERT_PATH = certifi.where()
except Exception:
    CERT_PATH = True  # default de requests

from supabase import create_client, Client

# ⬇️ Si preferís hardcodear el ID del CER, ponelo acá:
CER_ID = 30  # ej.: 999  (si lo dejás en None, usá --cer-id por CLI)

BASE = "https://api.bcra.gob.ar/estadisticas/v4.0"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_KEY  = os.getenv("SERVICE_KEY")
if not SUPABASE_URL or not SERVICE_KEY:
    print("ERROR: faltan SUPABASE_URL o SERVICE_KEY en variables de entorno.", file=sys.stderr)
    sys.exit(1)

def fetch_cer_last_70(cer_id: int, insecure: bool = False) -> List[Dict[str, Any]]:
    """Obtiene la serie del CER por ID y devuelve las últimas 70 filas normalizadas."""
    url = f"{BASE}/monetarias/{cer_id}?limit=3000&offset=0"
    r = requests.get(url, timeout=45, verify=(False if insecure else CERT_PATH))
    r.raise_for_status()
    payload = r.json()
    results = payload.get("results", [])
    if not results or not isinstance(results[0].get("detalle"), list):
        raise RuntimeError("Respuesta inesperada del endpoint de series del BCRA.")
    detalle = results[0]["detalle"]

    rows = []
    for pt in detalle:
        fecha = str(pt.get("fecha"))[:10]
        val   = pt.get("valor")
        if fecha and val is not None:
            try:
                rows.append({"fecha": fecha, "valor_cer": float(val)})
            except Exception:
                pass

    rows.sort(key=lambda x: x["fecha"])
    return rows[-70:]

def upsert_supabase(rows: List[Dict[str, Any]]) -> None:
    client: Client = create_client(SUPABASE_URL, SERVICE_KEY)
    # requiere UNIQUE(fecha) para idempotencia real
    client.table("cer_historico").upsert(rows, on_conflict="fecha").execute()

def main():
    ap = argparse.ArgumentParser(description="Actualiza cer_historico en Supabase (últimos 70 del CER).")
    ap.add_argument("--cer-id", type=int, help="idVariable del CER en la API BCRA v4.0")
    ap.add_argument("--insecure", action="store_true", help="Desactiva verificación TLS (solo si falla)")
    args = ap.parse_args()

    cer_id = args.cer_id if args.cer_id is not None else CER_ID
    if cer_id is None:
        print("ERROR: no se especificó CER_ID (pasa --cer-id N o setea la constante CER_ID).", file=sys.stderr)
        sys.exit(2)

    try:
        rows = fetch_cer_last_70(cer_id, insecure=args.insecure)
        if not rows:
            raise RuntimeError("No se obtuvieron filas del CER.")
        upsert_supabase(rows)
        print(f"OK: Upsert {len(rows)} filas en cer_historico. Última fecha: {rows[-1]['fecha']}")
    except requests.exceptions.SSLError as e:
        if not args.insecure:
            print("ERROR TLS con BCRA. Probá agregar --insecure (solo para sortear el certificado).", file=sys.stderr)
        print(f"Detalle SSL: {e}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"FALLÓ: {e}", file=sys.stderr)
        sys.exit(4)

if __name__ == "__main__":
    main()
