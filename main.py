from __future__ import annotations

import time
import typer
from typing import Optional
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console

from scanner import scan_directories, process_file_list
from utils import (
    parse_delta_csv,
    results_to_dataframe,
    create_output_dir,
    write_results,
    write_summary,
    enrich_with_delta,
    write_metrics,
)
from filters import (
    filter_by_time_range,
    filter_unique,
)

app = typer.Typer(help="fs-hunter: Scan directories and extract file metadata.")
console = Console()


def _yesterday_midnight() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _resolve_targets(base_path, paths, path_list, files, file_list, delta_csv) -> tuple[list[str], list | None, bool]:
    """Resolve scan targets from one of six input modes.

    Returns: (targets, delta_records or None, is_file_list)
    """
    provided = sum(1 for x in [base_path, paths, path_list, files, file_list, delta_csv] if x)
    if provided == 0:
        console.print("[red]Error:[/red] Provide --base-path, --paths, --path-list, --files, --file-list, or --delta-csv.")
        raise typer.Exit(1)
    if provided > 1:
        console.print("[red]Error:[/red] Input modes are mutually exclusive.")
        raise typer.Exit(1)

    if base_path:
        return [base_path], None, False

    if paths:
        return [p.strip() for p in paths.split(",") if p.strip()], None, False

    if path_list:
        p = Path(path_list)
        if not p.is_file():
            console.print(f"[red]Error:[/red] Path list file not found: {path_list}")
            raise typer.Exit(1)
        lines = p.read_text(encoding="utf-8").strip().splitlines()
        targets = [line.strip() for line in lines if line.strip()]
        if not targets:
            console.print(f"[red]Error:[/red] Path list file is empty: {path_list}")
            raise typer.Exit(1)
        return targets, None, False

    if files:
        return [f.strip() for f in files.split(",") if f.strip()], None, True

    if file_list:
        p = Path(file_list)
        if not p.is_file():
            console.print(f"[red]Error:[/red] File list not found: {file_list}")
            raise typer.Exit(1)
        lines = p.read_text(encoding="utf-8").strip().splitlines()
        file_paths = [line.strip() for line in lines if line.strip()]
        if not file_paths:
            console.print(f"[red]Error:[/red] File list is empty: {file_list}")
            raise typer.Exit(1)
        return file_paths, None, True

    # delta_csv
    try:
        unique_paths, delta_records = parse_delta_csv(delta_csv)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    return unique_paths, delta_records, False


@app.command()
def scan(
    base_path: Optional[str] = typer.Option(None, "--base-path", help="Single directory to scan"),
    paths: Optional[str] = typer.Option(None, "--paths", help="Directories to scan (comma-separated)"),
    path_list: Optional[str] = typer.Option(None, "--path-list", help="Text file with paths (one per line)"),
    files: Optional[str] = typer.Option(None, "--files", help="Specific file paths (comma-separated)"),
    file_list: Optional[str] = typer.Option(None, "--file-list", help="Text file with specific file paths (one per line)"),
    delta_csv: Optional[str] = typer.Option(None, "--delta-csv", help="CSV file with Directory column (pandas)"),
    scan_start: str = typer.Option(None, "--scan-start", help="Date range start (default: yesterday 00:00:00)"),
    scan_end: str = typer.Option(None, "--scan-end", help="Date range end (default: now)"),
    lookback: str = typer.Option("1H", "--lookback", help="Relative duration e.g. 7D, 2H, 1D12H30M (replaces --scan-start/--scan-end)"),
    day_start: str = typer.Option("00:00:00", "--day-start", help="Time-of-day start"),
    day_end: str = typer.Option("23:59:59", "--day-end", help="Time-of-day end"),
    file_pattern: tuple[str, str] = typer.Option(("glob", "*.parq*"), "--file-pattern", help="Pattern on filename: glob|regex PATTERN"),
    path_pattern: Optional[tuple[str, str]] = typer.Option(None, "--path-pattern", help="Pattern on relative path: glob|regex PATTERN"),
    min_size: Optional[int] = typer.Option(None, "--min-size", help="Min file size in bytes"),
    max_size: Optional[int] = typer.Option(None, "--max-size", help="Max file size in bytes"),
    unique: str = typer.Option("namepattern", "--unique", help="Deduplicate by 'hash' or 'namepattern'"),
    output_format: str = typer.Option("csv", "--output-format", help="Output format: 'csv' or 'jsonl'"),
    output_folder: str = typer.Option("~", "-o", help="Output folder (default: ~)"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel threads (default: 4)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show scan progress details"),
    no_metrics: bool = typer.Option(False, "--no-metrics", help="Skip metrics.json generation"),
    metrics_interval: int = typer.Option(30, "--metrics-interval", help="Time bucket size in minutes"),
):
    """Scan directories and extract file metadata with filters."""

    targets, delta_records, is_file_list = _resolve_targets(base_path, paths, path_list, files, file_list, delta_csv)

    use_date_range = scan_start is not None or scan_end is not None

    if unique not in ("hash", "namepattern"):
        console.print("[red]Error:[/red] --unique must be 'hash' or 'namepattern'.")
        raise typer.Exit(1)

    if output_format not in ("jsonl", "csv"):
        console.print("[red]Error:[/red] --output-format must be 'jsonl' or 'csv'.")
        raise typer.Exit(1)

    if use_date_range:
        if scan_start is None:
            scan_start = _yesterday_midnight()
        if scan_end is None:
            scan_end = _now()

    # Unpack file_pattern tuple (type, pattern)
    fp_type, fp_value = file_pattern
    if fp_type not in ("glob", "regex"):
        console.print("[red]Error:[/red] --file-pattern type must be 'glob' or 'regex'.")
        raise typer.Exit(1)

    # Unpack path_pattern tuple if provided
    pp_type, pp_value = path_pattern if path_pattern else (fp_type, None)

    uniq_filter = filter_unique(base=unique)
    need_hash = unique == "hash"

    # collect scan results
    scan_start_time = time.time()
    results = []

    if is_file_list:
        # Direct file processing — no find, just enrich the given files
        scanner = process_file_list(
            file_paths=targets,
            unique_filter=uniq_filter,
            need_hash=need_hash,
            workers=workers,
            verbose=verbose,
        )
    else:
        time_filter = filter_by_time_range(day_start, day_end)
        scanner = scan_directories(
            targets=targets,
            name_pattern=fp_value,
            pattern_type=fp_type,
            lookback=lookback if not use_date_range else None,
            scan_start=scan_start if use_date_range else None,
            scan_end=scan_end if use_date_range else None,
            min_size=min_size,
            max_size=max_size,
            time_filter=time_filter,
            path_pattern=pp_value,
            unique_filter=uniq_filter,
            need_hash=need_hash,
            workers=workers,
            verbose=verbose,
        )

    for metadata in scanner:
        console.print(f"{metadata.relative_path}")
        results.append(metadata)

    scan_duration = time.time() - scan_start_time
    console.print(f"\n[green]{len(results)} files found.[/green]")

    if not results:
        return

    # build DataFrame
    df = results_to_dataframe(results)

    # enrich with delta CSV details if provided
    if delta_records:
        df = enrich_with_delta(df, delta_records)

    # write output files
    out_dir = create_output_dir(output_folder)
    results_file = write_results(df, out_dir, fmt=output_format)
    summary_file = write_summary(df, out_dir, targets, scan_start, scan_end)

    if not no_metrics:
        metrics_file = write_metrics(df, out_dir, scan_duration, metrics_interval)
        console.print(f"[green]Metrics:[/green]  {metrics_file}")

    console.print(f"[green]Results:[/green]  {results_file}")
    console.print(f"[green]Summary:[/green]  {summary_file}")


if __name__ == "__main__":
    app()
