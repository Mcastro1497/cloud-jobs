import os
from dotenv import load_dotenv
load_dotenv()

class Settings:
    TZ = os.getenv("TZ", "America/Argentina/Cordoba")
    START_HHMM = os.getenv("START_HHMM", "09:00")
    STOP_HHMM  = os.getenv("STOP_HHMM",  "18:30")
    # Si querés granularidad de “cada X min”, definilo en el CRON del proveedor
settings = Settings()
