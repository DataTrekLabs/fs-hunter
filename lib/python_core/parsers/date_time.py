from contextlib import suppress
from datetime import datetime, time, timedelta

import re

from lib.python_core.constants.enums import Date_STUCT, Time_STUCT


def parse_date(date_str: str) -> datetime:
    """Smart date parser that auto-completes partial dates.

    '2024'          -> 2024-01-01
    '2024-06'       -> 2024-06-01
    '2024-06-15'    -> 2024-06-15
    """
    s = date_str.strip().replace("/", "-")
    for f in Date_STUCT:
        with suppress(ValueError):
            return datetime.strptime(s, f.value)
    raise ValueError(f"Cannot parse date: '{date_str}'. Use YYYY[-MM[-DD]]")


def parse_time(time_str: str) -> time:
    """Smart time parser that auto-completes partial times.

    '14'       -> 14:00:00
    '14:30'    -> 14:30:00
    '14:30:45' -> 14:30:45
    """
    s = time_str.strip()
    for f in Time_STUCT:
        with suppress(ValueError):
            return datetime.strptime(s, f.value).time()
    raise ValueError(f"Cannot parse time: '{time_str}'. Use HH[:MM[:SS]]")


def parse_date_time(date_time_str: str) -> datetime:
    """Parse combined date and time string.

    '2024-06-15 14:30:45' -> as-is
    '2024-06-15 14:30'    -> 2024-06-15 14:30:00
    '2024-06-15 14'       -> 2024-06-15 14:00:00
    '2024-06-15'          -> 2024-06-15 00:00:00
    '2024-06'             -> 2024-06-01 00:00:00
    '2024'                -> 2024-01-01 00:00:00
    """
    s = date_time_str.strip().replace("/", "-")
    for date_fmt in Date_STUCT:
        for time_fmt in Time_STUCT:
            fmt = f"{date_fmt.value} {time_fmt.value}"
            with suppress(ValueError):
                return datetime.strptime(s, fmt)
        with suppress(ValueError):
            return datetime.strptime(s, date_fmt.value)
    raise ValueError(
        f"Cannot parse date/time: '{date_time_str}'. Use YYYY[-MM[-DD[ HH[:MM[:SS]]]]]"
    )


_DURATION_VALID = re.compile(r"^(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:\d+H)?(?:\d+m)?(?:\d+s)?$")
_DURATION_PARTS = re.compile(r"(\d+)([YMDHms])")


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string into timedelta. Supports combined units.

    Case-sensitive: M = Month (30 days), m = minute, s = second.

    '1Y'        -> 365 days
    '6M'        -> 180 days (6 months)
    '7D'        -> 7 days
    '2H'        -> 2 hours
    '30m'       -> 30 minutes
    '45s'       -> 45 seconds
    '1Y6M'      -> 545 days
    '1D12H'     -> 1 day 12 hours
    '2D6H30m'   -> 2 days 6 hours 30 minutes
    '1H30m15s'  -> 1 hour 30 minutes 15 seconds
    '1Y3M15D'   -> 470 days
    """
    s = duration_str.strip()
    if not s or not _DURATION_VALID.fullmatch(s):
        raise ValueError(f"Cannot parse duration: '{duration_str}'. Use e.g. 1Y, 6M, 7D, 2H, 30m, 45s (M=Month, m=minute, s=second)")
    parts = {unit: int(val) for val, unit in _DURATION_PARTS.findall(s)}
    days = parts.get("Y", 0) * 365 + parts.get("M", 0) * 30 + parts.get("D", 0)
    return timedelta(days=days, hours=parts.get("H", 0), minutes=parts.get("m", 0), seconds=parts.get("s", 0))
