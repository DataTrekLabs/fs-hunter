import re
from datetime import datetime, time, timedelta


def parse_date(date_str: str) -> datetime:
    """Smart date parser that auto-completes partial dates.

    '2024'          -> 2024-01-01 00:00:00
    '2024-06'       -> 2024-06-01 00:00:00
    '2024-06-15'    -> 2024-06-15 00:00:00
    '2024-06-15 14' -> 2024-06-15 14:00:00
    '2024-06-15 14:30:00' -> as-is
    """
    s = date_str.strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H",
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: '{date_str}'. Use YYYY[-MM[-DD[ HH[:MM[:SS]]]]]")


def parse_time(time_str: str) -> time:
    """Smart time parser that auto-completes partial times.

    '14'       -> 14:00:00
    '14:30'    -> 14:30:00
    '14:30:45' -> as-is
    """
    s = time_str.strip()
    formats = [
        "%H:%M:%S",
        "%H:%M",
        "%H",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse time: '{time_str}'. Use HH[:MM[:SS]]")


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string into timedelta. Supports combined units.

    '7D'      -> 7 days
    '2H'      -> 2 hours
    '30M'     -> 30 minutes
    '1D12H'   -> 1 day 12 hours
    '2D6H30M' -> 2 days 6 hours 30 minutes
    """
    s = duration_str.strip().upper()
    pattern = re.compile(r"^(?:(\d+)D)?(?:(\d+)H)?(?:(\d+)M)?$")
    match = pattern.match(s)
    if not match or not any(match.groups()):
        raise ValueError(f"Cannot parse duration: '{duration_str}'. Use e.g. 7D, 2H, 30M, 1D12H30M")
    days = int(match.group(1) or 0)
    hours = int(match.group(2) or 0)
    minutes = int(match.group(3) or 0)
    return timedelta(days=days, hours=hours, minutes=minutes)


def format_date(dt: datetime) -> str:
    """Format datetime to display string."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_time(t: time) -> str:
    """Format time to display string."""
    return t.strftime("%H:%M:%S")
