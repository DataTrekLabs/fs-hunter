from __future__ import annotations

import sys
import fnmatch
import re
from pathlib import Path
from typing import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from metadata import FileMetadata, extract_metadata


def _scan_single_target(
    target: str,
    name_regex: re.Pattern | None,
    date_filter: Callable[[FileMetadata], bool] | None,
    time_filter: Callable[[FileMetadata], bool] | None,
    size_filter: Callable[[FileMetadata], bool] | None,
    path_pattern: str | None,
    need_hash: bool = False,
    progress: Progress | None = None,
    task_id=None,
) -> list[FileMetadata]:
    """Scan a single directory with filters. Returns list of matching metadata."""
    results = []
    base_dir = Path(target).resolve()
    if not base_dir.exists():
        print(f"Warning: '{target}' does not exist, skipping.", file=sys.stderr)
        return results
    if not base_dir.is_dir():
        print(f"Warning: '{target}' is not a directory, skipping.", file=sys.stderr)
        return results

    scanned = 0
    matched = 0

    for file_path in base_dir.glob("**/*"):
        if not file_path.is_file():
            continue

        scanned += 1

        try:
            metadata = extract_metadata(file_path, base_dir)
        except (PermissionError, OSError):
            continue

        if date_filter and not date_filter(metadata):
            continue
        if time_filter and not time_filter(metadata):
            continue
        if size_filter and not size_filter(metadata):
            continue

        if path_pattern:
            rel = metadata.relative_path.replace("\\", "/")
            if not fnmatch.fnmatch(rel, path_pattern):
                continue

        if name_regex and not name_regex.search(metadata.name):
            continue

        if need_hash:
            metadata.compute_sha256()

        matched += 1
        results.append(metadata)

        if progress and task_id is not None:
            progress.update(task_id, completed=scanned,
                            description=f"[cyan]{base_dir.name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")

    if progress and task_id is not None:
        progress.update(task_id, completed=scanned,
                        description=f"[green]{base_dir.name}[/green] [dim]done — scanned:{scanned} matched:{matched}[/dim]")

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
      6. unique_filter (last)

    verbose=True: Rich progress bars per directory
    """
    name_regex = re.compile(name_pattern) if name_pattern else None

    if not verbose:
        # silent mode — no progress
        if workers <= 1 or len(targets) == 1:
            for target in targets:
                for metadata in _scan_single_target(
                    target, name_regex, date_filter, time_filter,
                    size_filter, path_pattern, need_hash,
                ):
                    if unique_filter and not unique_filter(metadata):
                        continue
                    yield metadata
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _scan_single_target, target, name_regex,
                        date_filter, time_filter, size_filter, path_pattern,
                        need_hash,
                    ): target
                    for target in targets
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

        if workers <= 1 or len(targets) == 1:
            for target in targets:
                task_id = progress.add_task(
                    f"[cyan]{Path(target).name}[/cyan] [dim]starting...[/dim]",
                    total=None,
                )
                for metadata in _scan_single_target(
                    target, name_regex, date_filter, time_filter,
                    size_filter, path_pattern, need_hash, progress, task_id,
                ):
                    if unique_filter and not unique_filter(metadata):
                        continue
                    yield metadata
        else:
            task_ids = {}
            for target in targets:
                tid = progress.add_task(
                    f"[cyan]{Path(target).name}[/cyan] [dim]queued[/dim]",
                    total=None,
                )
                task_ids[target] = tid

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _scan_single_target, target, name_regex,
                        date_filter, time_filter, size_filter, path_pattern,
                        need_hash, progress, task_ids[target],
                    ): target
                    for target in targets
                }
                for future in as_completed(futures):
                    for metadata in future.result():
                        if unique_filter and not unique_filter(metadata):
                            continue
                        yield metadata
