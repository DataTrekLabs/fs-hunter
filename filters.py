from __future__ import annotations

import re
import fnmatch
from typing import Callable
from metadata import FileMetadata
from datetime import datetime
from formatters import parse_date, parse_time, parse_duration
from utils import name_to_pattern


def filter_by_name_pattern(pattern: str) -> Callable[[FileMetadata], bool]:
    """Filter files by regex pattern on filename."""
    regex = re.compile(pattern)
    return lambda m: regex.search(m.name) is not None


def filter_by_path_pattern(pattern: str) -> Callable[[FileMetadata], bool]:
    """Filter files by glob pattern on relative path. e.g. 'derived/*.parq'"""
    # normalize to forward slashes so patterns work cross-platform
    return lambda m: fnmatch.fnmatch(m.relative_path.replace("\\", "/"), pattern)


def filter_by_date_range(
    after: str | None = None, before: str | None = None
) -> Callable[[FileMetadata], bool]:
    """Filter by mtime date range. Supports partial dates (YYYY, YYYY-MM, etc)."""
    after_dt = parse_date(after) if after else None
    before_dt = parse_date(before) if before else None

    def check(m: FileMetadata) -> bool:
        if after_dt and m.mtime < after_dt:
            return False
        if before_dt and m.mtime > before_dt:
            return False
        return True

    return check


def filter_by_time_range(
    start: str, end: str
) -> Callable[[FileMetadata], bool]:
    """Filter by time-of-day window. e.g. '09:00'-'17:00' matches any day."""
    start_t = parse_time(start)
    end_t = parse_time(end)

    def check(m: FileMetadata) -> bool:
        file_time = m.mtime.time()
        if start_t <= end_t:
            return start_t <= file_time <= end_t
        # wraps midnight: e.g. 22:00 -> 02:00
        return file_time >= start_t or file_time <= end_t

    return check


def filter_by_size_range(
    min_size: int | None = None, max_size: int | None = None
) -> Callable[[FileMetadata], bool]:
    """Filter by file size range in bytes."""

    def check(m: FileMetadata) -> bool:
        if min_size is not None and m.size_bytes < min_size:
            return False
        if max_size is not None and m.size_bytes > max_size:
            return False
        return True

    return check


def filter_by_past_duration(duration: str) -> Callable[[FileMetadata], bool]:
    """Filter files by mtime within the past duration. e.g. '7D', '2H', '1D12H30M'"""
    delta = parse_duration(duration)
    cutoff = datetime.now() - delta

    def check(m: FileMetadata) -> bool:
        return m.mtime >= cutoff

    return check


def filter_unique(base: str = "hash") -> Callable[[FileMetadata], bool]:
    """Deduplicate filter. Only yields first occurrence.

    base='hash'        — unique by SHA256 (same content = duplicate)
    base='namepattern' — unique by name_to_pattern (same structure = duplicate)
                         e.g. report_20250601.csv and report_20240115.csv
                         both match report_\\d{4}\\d{2}\\d{2}\\.csv
    """
    seen: set[str] = set()

    def check(m: FileMetadata) -> bool:
        if base == "hash":
            key = m.sha256
        else:
            key = name_to_pattern(m.name)
        if not key or key in seen:
            return False
        seen.add(key)
        return True

    return check


def build_filter_chain(
    filters: list[Callable[[FileMetadata], bool]],
) -> Callable[[FileMetadata], bool]:
    """Combine multiple filter functions into a single predicate."""
    if not filters:
        return lambda m: True
    return lambda m: all(f(m) for f in filters)
