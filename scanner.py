from __future__ import annotations

import sys
import fnmatch
import re
from pathlib import Path
from typing import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from metadata import FileMetadata, extract_metadata


def _expand_targets(targets: list[str], workers: int) -> list[tuple[str, Path, bool]]:
    """Expand targets into (scan_path, base_dir, recursive) tuples.

    When workers > 1, splits each target into immediate subdirectories
    so they can be scanned in parallel.
    """
    if workers <= 1:
        return [(t, Path(t).resolve(), True) for t in targets]

    expanded = []
    for target in targets:
        base_dir = Path(target).resolve()
        if not base_dir.is_dir():
            expanded.append((target, base_dir, True))
            continue
        # Direct files in root (non-recursive)
        expanded.append((target, base_dir, False))
        # Each subdirectory scanned recursively in parallel
        try:
            for child in sorted(base_dir.iterdir()):
                if child.is_dir():
                    expanded.append((str(child), base_dir, True))
        except PermissionError:
            pass
    return expanded


def _apply_filters(
    metadata: FileMetadata,
    date_filter: Callable[[FileMetadata], bool] | None,
    time_filter: Callable[[FileMetadata], bool] | None,
    size_filter: Callable[[FileMetadata], bool] | None,
    path_pattern: str | None,
    name_regex: re.Pattern | None,
) -> bool:
    """Return True if the file passes all cheap filters."""
    if date_filter and not date_filter(metadata):
        return False
    if time_filter and not time_filter(metadata):
        return False
    if size_filter and not size_filter(metadata):
        return False
    if path_pattern:
        rel = metadata.relative_path.replace("\\", "/")
        if not fnmatch.fnmatch(rel, path_pattern):
            return False
    if name_regex and not name_regex.search(metadata.name):
        return False
    return True


def _scan_single_target(
    target: str,
    base_dir: Path,
    name_regex: re.Pattern | None,
    date_filter: Callable[[FileMetadata], bool] | None,
    time_filter: Callable[[FileMetadata], bool] | None,
    size_filter: Callable[[FileMetadata], bool] | None,
    path_pattern: str | None,
    need_hash: bool = False,
    recursive: bool = True,
    progress: Progress | None = None,
    task_id=None,
) -> list[FileMetadata]:
    """Scan a single directory with filters. Returns list of matching metadata."""
    results = []
    scan_dir = Path(target).resolve()
    if not scan_dir.exists():
        print(f"Warning: '{target}' does not exist, skipping.", file=sys.stderr)
        return results
    if not scan_dir.is_dir():
        print(f"Warning: '{target}' is not a directory, skipping.", file=sys.stderr)
        return results

    scanned = 0
    matched = 0
    display_name = scan_dir.name
    glob_pattern = "**/*" if recursive else "*"

    for file_path in scan_dir.glob(glob_pattern):
        if not file_path.is_file():
            continue

        scanned += 1

        try:
            metadata = extract_metadata(file_path, base_dir)
        except (PermissionError, OSError):
            if progress and task_id is not None:
                progress.update(task_id, completed=scanned,
                                description=f"[cyan]{display_name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")
            continue

        if _apply_filters(metadata, date_filter, time_filter, size_filter, path_pattern, name_regex):
            if need_hash:
                metadata.compute_sha256()
            matched += 1
            results.append(metadata)

        if progress and task_id is not None:
            progress.update(task_id, completed=scanned,
                            description=f"[cyan]{display_name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")

    if progress and task_id is not None:
        progress.update(task_id, completed=scanned,
                        description=f"[green]{display_name}[/green] [dim]done — scanned:{scanned} matched:{matched}[/dim]")

    return results


def scan_directories(
    targets: list[str],
    recursive: bool = True,
    date_filter: Callable[[FileMetadata], bool] | None = None,
    time_filter: Callable[[FileMetadata], bool] | None = None,
    size_filter: Callable[[FileMetadata], bool] | None = None,
    path_pattern: str | None = None,
    name_pattern: str | None = None,
    unique_filter: Callable[[FileMetadata], bool] | None = None,
    need_hash: bool = False,
    workers: int = 4,
    verbose: bool = False,
) -> Generator[FileMetadata, None, None]:
    """Walk target directories and yield filtered FileMetadata.

    Filter order (all optional):
      1. date_range OR past_duration
      2. time_range
      3. size_range
      4. path_pattern
      5. name_pattern
      6. SHA256 (only when need_hash=True)
      7. unique_filter (last)

    When workers > 1, each target's subdirectories are scanned in parallel.
    verbose=True: Rich progress bars per scan unit
    """
    name_regex = re.compile(name_pattern) if name_pattern else None
    expanded = _expand_targets(targets, workers)

    if not verbose:
        if workers <= 1:
            for scan_path, base_dir, rec in expanded:
                for metadata in _scan_single_target(
                    scan_path, base_dir, name_regex, date_filter, time_filter,
                    size_filter, path_pattern, need_hash, rec,
                ):
                    if unique_filter and not unique_filter(metadata):
                        continue
                    yield metadata
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _scan_single_target, scan_path, base_dir, name_regex,
                        date_filter, time_filter, size_filter, path_pattern,
                        need_hash, rec,
                    ): scan_path
                    for scan_path, base_dir, rec in expanded
                }
                for future in as_completed(futures):
                    for metadata in future.result():
                        if unique_filter and not unique_filter(metadata):
                            continue
                        yield metadata
        return

    # verbose mode — Rich progress bars
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=False,
    ) as progress:

        if workers <= 1:
            for scan_path, base_dir, rec in expanded:
                task_id = progress.add_task(
                    f"[cyan]{Path(scan_path).name}[/cyan] [dim]starting...[/dim]",
                    total=None,
                )
                for metadata in _scan_single_target(
                    scan_path, base_dir, name_regex, date_filter, time_filter,
                    size_filter, path_pattern, need_hash, rec, progress, task_id,
                ):
                    if unique_filter and not unique_filter(metadata):
                        continue
                    yield metadata
        else:
            task_ids = {}
            for scan_path, base_dir, rec in expanded:
                tid = progress.add_task(
                    f"[cyan]{Path(scan_path).name}[/cyan] [dim]queued[/dim]",
                    total=None,
                )
                task_ids[scan_path] = (tid, base_dir, rec)

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _scan_single_target, scan_path, base_dir, name_regex,
                        date_filter, time_filter, size_filter, path_pattern,
                        need_hash, rec, progress, tid,
                    ): scan_path
                    for scan_path, (tid, base_dir, rec) in task_ids.items()
                }
                for future in as_completed(futures):
                    for metadata in future.result():
                        if unique_filter and not unique_filter(metadata):
                            continue
                        yield metadata
