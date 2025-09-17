import logging, asyncio, time
from zoneinfo import ZoneInfo
from datetime import datetime

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("app")

def now_tz(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))

def in_window(start_hhmm: str, stop_hhmm: str, tz: str) -> bool:
    now = now_tz(tz)
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, stop_hhmm.split(":"))
    start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    stop  = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= now <= stop
