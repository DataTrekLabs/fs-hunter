from __future__ import annotations

import os
import sys
import fnmatch
import re
from pathlib import Path
from typing import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from metadata import FileMetadata, extract_metadata_stat, enrich_metadata


def _dir_mtime(path: str) -> float:
    """Get directory mtime, returning 0 on error."""
    try:
        return os.stat(path).st_mtime
    except (PermissionError, OSError):
        return 0


def _walk_files(root: Path, dir_cutoff: float | None) -> Generator[Path, None, None]:
    """Walk directory tree yielding file paths, pruning old directories.

    When dir_cutoff is set, skips subdirectories whose mtime is older
    than the cutoff — they can't contain recently ingested files.
    """
    for dirpath, dirnames, filenames in os.walk(root):
        if dir_cutoff is not None:
            dirnames[:] = [
                d for d in dirnames
                if _dir_mtime(os.path.join(dirpath, d)) >= dir_cutoff
            ]
        for fname in filenames:
            yield Path(os.path.join(dirpath, fname))


def _list_files(directory: Path) -> Generator[Path, None, None]:
    """List files in a single directory (non-recursive)."""
    try:
        for entry in os.scandir(directory):
            if entry.is_file(follow_symlinks=False):
                yield Path(entry.path)
    except PermissionError:
        pass


def _expand_dir(
    directory: Path,
    base_dir: Path,
    expanded: list[tuple[str, Path, bool]],
    depth: int,
    max_depth: int,
) -> None:
    """Recursively split a directory into scan units for parallelism.

    At each level:
    - Adds a non-recursive entry for files directly in this directory
    - If depth < max_depth, recurses into child directories that themselves
      contain subdirectories; leaf directories are added as recursive entries
    """
    # Direct files at this level (non-recursive scan)
    expanded.append((str(directory), base_dir, False))

    try:
        children = sorted(directory.iterdir())
    except PermissionError:
        return

    child_dirs = [c for c in children if c.is_dir()]

    for child in child_dirs:
        if depth < max_depth:
            # Check if child has any subdirectories worth splitting further
            try:
                has_subdirs = any(gc.is_dir() for gc in child.iterdir())
            except PermissionError:
                has_subdirs = False

            if has_subdirs:
                _expand_dir(child, base_dir, expanded, depth + 1, max_depth)
            else:
                # Leaf directory — scan recursively as a single unit
                expanded.append((str(child), base_dir, True))
        else:
            # Max depth reached — scan recursively as a single unit
            expanded.append((str(child), base_dir, True))


def _expand_targets(targets: list[str], workers: int) -> list[tuple[str, Path, bool]]:
    """Expand targets into (scan_path, base_dir, recursive) tuples.

    When workers > 1, splits each target up to 2 levels deep so large
    subdirectories don't bottleneck a single thread.
    """
    if workers <= 1:
        return [(t, Path(t).resolve(), True) for t in targets]

    expanded: list[tuple[str, Path, bool]] = []
    for target in targets:
        base_dir = Path(target).resolve()
        if not base_dir.is_dir():
            expanded.append((target, base_dir, True))
            continue
        _expand_dir(base_dir, base_dir, expanded, depth=0, max_depth=2)
    return expanded


def _apply_stat_filters(
    metadata: FileMetadata,
    date_filter: Callable[[FileMetadata], bool] | None,
    time_filter: Callable[[FileMetadata], bool] | None,
    size_filter: Callable[[FileMetadata], bool] | None,
) -> bool:
    """Return True if the file passes stat-based filters (Tier 1).

    Name and path filters are handled at Tier 0 before metadata is built.
    """
    if date_filter and not date_filter(metadata):
        return False
    if time_filter and not time_filter(metadata):
        return False
    if size_filter and not size_filter(metadata):
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
    dir_cutoff: float | None = None,
) -> list[FileMetadata]:
    """Scan a single directory with three-tier filter cascade.

    Tier 0 — Free (zero I/O): name_regex, path_pattern
    Tier 1 — Cheap (1 stat syscall): date, time, size filters
    Tier 2 — Expensive (file I/O + OS calls): owner, MIME enrichment

    dir_cutoff: skip subdirectories with mtime older than this timestamp.
    """
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

    if recursive:
        file_iter = _walk_files(scan_dir, dir_cutoff)
    else:
        file_iter = _list_files(scan_dir)

    for file_path in file_iter:
        scanned += 1

        # --- Tier 0: Free checks (no I/O) ---
        if name_regex and not name_regex.search(file_path.name):
            if progress and task_id is not None:
                progress.update(task_id, completed=scanned,
                                description=f"[cyan]{display_name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")
            continue

        if path_pattern:
            try:
                rel = str(file_path.relative_to(base_dir)).replace("\\", "/")
            except ValueError:
                rel = file_path.name
            if not fnmatch.fnmatch(rel, path_pattern):
                if progress and task_id is not None:
                    progress.update(task_id, completed=scanned,
                                    description=f"[cyan]{display_name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")
                continue

        # --- Tier 1: Cheap checks (stat syscall) ---
        try:
            file_stat = file_path.stat()
            metadata = extract_metadata_stat(file_path, base_dir, file_stat)
        except (PermissionError, OSError):
            if progress and task_id is not None:
                progress.update(task_id, completed=scanned,
                                description=f"[cyan]{display_name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")
            continue

        if not _apply_stat_filters(metadata, date_filter, time_filter, size_filter):
            if progress and task_id is not None:
                progress.update(task_id, completed=scanned,
                                description=f"[cyan]{display_name}[/cyan] [dim]scanned:{scanned} matched:{matched}[/dim]")
            continue

        # --- Tier 2: Expensive enrichment (owner, MIME) ---
        try:
            enrich_metadata(metadata, file_path)
        except (PermissionError, OSError):
            pass  # keep placeholder values

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
    dir_cutoff: float | None = None,
) -> Generator[FileMetadata, None, None]:
    """Walk target directories and yield filtered FileMetadata.

    Filter cascade (all optional):
      Tier 0 — name_pattern, path_pattern (zero I/O, rejects early)
      Tier 1 — date, time, size (one stat call)
      Tier 2 — owner, MIME type (expensive I/O)
      Post  — SHA256, unique_filter

    dir_cutoff: skip subdirectories with mtime older than this timestamp.
    When workers > 1, targets are split up to 2 levels deep for parallelism.
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
                    dir_cutoff=dir_cutoff,
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
                        need_hash, rec, None, None, dir_cutoff,
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
                    dir_cutoff,
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
                        need_hash, rec, progress, tid, dir_cutoff,
                    ): scan_path
                    for scan_path, (tid, base_dir, rec) in task_ids.items()
                }
                for future in as_completed(futures):
                    for metadata in future.result():
                        if unique_filter and not unique_filter(metadata):
                            continue
                        yield metadata
