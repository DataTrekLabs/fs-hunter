from __future__ import annotations

import os
import sys
import subprocess
import fnmatch
import re
from pathlib import Path
from typing import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from metadata import FileMetadata, extract_metadata_stat, enrich_metadata
from formatters import parse_duration


def _duration_to_minutes(duration_str: str) -> int:
    """Convert a duration string like '24H' or '1D12H30M' to total minutes."""
    delta = parse_duration(duration_str)
    return int(delta.total_seconds() // 60)


def _build_find_cmd(
    target: str,
    name_pattern: str | None = None,
    pattern_type: str = "glob",
    lookback: str | None = None,
    scan_start: str | None = None,
    scan_end: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
) -> list[str]:
    """Build a find command list from filter parameters.

    Pushes name, date, and size filtering to the kernel level
    so Python never sees non-matching files.
    """
    cmd = ["find", target, "-type", "f"]

    # Name filter
    if name_pattern:
        if pattern_type == "regex":
            # find -regex matches the FULL path, so prefix with '.*/'
            full_regex = f".*/{name_pattern}"
            cmd += ["-regextype", "posix-extended", "-regex", full_regex]
        else:
            cmd += ["-name", name_pattern]

    # Date filter: lookback (relative) or range (absolute)
    if lookback:
        minutes = _duration_to_minutes(lookback)
        cmd += ["-mmin", f"-{minutes}"]
    else:
        if scan_start:
            cmd += ["-newermt", scan_start]
        if scan_end:
            cmd += ["!", "-newermt", scan_end]

    # Size filters
    if min_size is not None:
        cmd += ["-size", f"+{min_size}c"]
    if max_size is not None:
        cmd += ["-size", f"-{max_size}c"]

    cmd += ["-print0"]
    return cmd


def _run_find(
    target: str,
    base_dir: Path,
    name_pattern: str | None = None,
    pattern_type: str = "glob",
    lookback: str | None = None,
    scan_start: str | None = None,
    scan_end: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
) -> list[tuple[Path, Path]]:
    """Run find on a single target and return list of (file_path, base_dir) tuples."""
    cmd = _build_find_cmd(target, name_pattern, pattern_type, lookback, scan_start, scan_end, min_size, max_size)
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        print(f"Warning: find failed for '{target}': {e}", file=sys.stderr)
        return []

    if not result.stdout:
        return []

    paths = []
    for entry in result.stdout.split(b"\0"):
        if entry:
            paths.append((Path(os.fsdecode(entry)), base_dir))
    return paths


def _enrich_batch(
    batch: list[tuple[Path, Path]],
    path_pattern: str | None,
    pattern_type: str,
    time_filter: Callable[[FileMetadata], bool] | None,
    need_hash: bool,
) -> list[FileMetadata]:
    """Process a batch of (file_path, base_dir) tuples.

    Applies remaining Python-side filters (path_pattern, time-of-day)
    then enriches with owner + MIME for matches.
    """
    path_regex = re.compile(path_pattern) if path_pattern and pattern_type == "regex" else None
    results = []
    for file_path, base_dir in batch:
        # Path pattern filter (relative path — can't push to find)
        if path_pattern:
            try:
                rel = str(file_path.relative_to(base_dir)).replace("\\", "/")
            except ValueError:
                rel = file_path.name
            if pattern_type == "regex":
                if not path_regex.search(rel):
                    continue
            else:
                if not fnmatch.fnmatch(rel, path_pattern):
                    continue

        # Stat for metadata (find already filtered by date/size,
        # but we need the stat values for FileMetadata fields)
        try:
            file_stat = file_path.stat()
            metadata = extract_metadata_stat(file_path, base_dir, file_stat)
        except (PermissionError, OSError):
            continue

        # Time-of-day filter (can't push to find)
        if time_filter and not time_filter(metadata):
            continue

        # Tier 2: Expensive enrichment (owner, MIME)
        try:
            enrich_metadata(metadata, file_path)
        except (PermissionError, OSError):
            pass

        if need_hash:
            metadata.compute_sha256()

        results.append(metadata)
    return results


def scan_directories(
    targets: list[str],
    name_pattern: str | None = None,
    pattern_type: str = "glob",
    lookback: str | None = None,
    scan_start: str | None = None,
    scan_end: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    time_filter: Callable[[FileMetadata], bool] | None = None,
    path_pattern: str | None = None,
    unique_filter: Callable[[FileMetadata], bool] | None = None,
    need_hash: bool = False,
    workers: int = 4,
    verbose: bool = False,
) -> Generator[FileMetadata, None, None]:
    """Walk target directories and yield filtered FileMetadata.

    Phase 1 — find: pushes name, date, size filtering to the kernel.
    Phase 2 — Python: path_pattern, time-of-day, owner/MIME enrichment
              in parallel batches across workers.

    verbose=True: Rich progress bar showing find + enrichment progress.
    """
    # Phase 1: Run find to collect matching paths
    all_found: list[tuple[Path, Path]] = []

    if verbose:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            transient=True,
        ) as progress:
            tid = progress.add_task("[cyan]Running find...[/cyan]", total=None)

            if workers <= 1 or len(targets) == 1:
                for target in targets:
                    base_dir = Path(target).resolve()
                    found = _run_find(
                        target, base_dir, name_pattern, pattern_type,
                        lookback, scan_start, scan_end, min_size, max_size,
                    )
                    all_found.extend(found)
                    progress.update(tid, description=f"[cyan]find[/cyan] [dim]{len(all_found)} candidates[/dim]")
            else:
                with ThreadPoolExecutor(max_workers=min(workers, len(targets))) as executor:
                    futures = {
                        executor.submit(
                            _run_find, target, Path(target).resolve(),
                            name_pattern, pattern_type, lookback,
                            scan_start, scan_end, min_size, max_size,
                        ): target
                        for target in targets
                    }
                    for future in as_completed(futures):
                        all_found.extend(future.result())
                        progress.update(tid, description=f"[cyan]find[/cyan] [dim]{len(all_found)} candidates[/dim]")
    else:
        if workers <= 1 or len(targets) == 1:
            for target in targets:
                base_dir = Path(target).resolve()
                all_found.extend(_run_find(
                    target, base_dir, name_pattern, pattern_type,
                    lookback, scan_start, scan_end, min_size, max_size,
                ))
        else:
            with ThreadPoolExecutor(max_workers=min(workers, len(targets))) as executor:
                futures = {
                    executor.submit(
                        _run_find, target, Path(target).resolve(),
                        name_pattern, lookback, scan_start, scan_end,
                        min_size, max_size,
                    ): target
                    for target in targets
                }
                for future in as_completed(futures):
                    all_found.extend(future.result())

    if not all_found:
        return

    # Phase 2: Batch enrichment across workers
    if workers <= 1:
        batches = [all_found]
    else:
        batch_size = max(1, len(all_found) // workers)
        batches = [
            all_found[i:i + batch_size]
            for i in range(0, len(all_found), batch_size)
        ]

    if verbose:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            transient=False,
        ) as progress:
            tid = progress.add_task(
                f"[cyan]Enriching[/cyan] [dim]0/{len(all_found)} files[/dim]",
                total=len(all_found),
            )
            matched = 0
            processed = 0

            if workers <= 1:
                for metadata in _enrich_batch(all_found, path_pattern, pattern_type, time_filter, need_hash):
                    matched += 1
                    processed += 1
                    progress.update(tid, completed=processed,
                                    description=f"[cyan]Enriching[/cyan] [dim]{processed}/{len(all_found)} — matched:{matched}[/dim]")
                    if unique_filter and not unique_filter(metadata):
                        continue
                    yield metadata
                # update for files that didn't match
                progress.update(tid, completed=len(all_found),
                                description=f"[green]Done[/green] [dim]{len(all_found)} processed — {matched} matched[/dim]")
            else:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(
                            _enrich_batch, batch, path_pattern, pattern_type, time_filter, need_hash,
                        ): len(batch)
                        for batch in batches
                    }
                    for future in as_completed(futures):
                        batch_results = future.result()
                        batch_len = futures[future]
                        processed += batch_len
                        matched += len(batch_results)
                        progress.update(tid, completed=processed,
                                        description=f"[cyan]Enriching[/cyan] [dim]{processed}/{len(all_found)} — matched:{matched}[/dim]")
                        for metadata in batch_results:
                            if unique_filter and not unique_filter(metadata):
                                continue
                            yield metadata

                progress.update(tid, completed=len(all_found),
                                description=f"[green]Done[/green] [dim]{len(all_found)} processed — {matched} matched[/dim]")
    else:
        # Non-verbose mode
        if workers <= 1:
            for metadata in _enrich_batch(all_found, path_pattern, pattern_type, time_filter, need_hash):
                if unique_filter and not unique_filter(metadata):
                    continue
                yield metadata
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(
                        _enrich_batch, batch, path_pattern, pattern_type, time_filter, need_hash,
                    )
                    for batch in batches
                ]
                for future in as_completed(futures):
                    for metadata in future.result():
                        if unique_filter and not unique_filter(metadata):
                            continue
                        yield metadata
