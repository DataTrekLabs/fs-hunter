from __future__ import annotations

import typer
from typing import Optional
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console

from scanner import scan_directories
from utils import (
    parse_delta_csv,
    results_to_dataframe,
    create_output_dir,
    write_results,
    write_summary,
    enrich_with_delta,
)
from filters import (
    filter_by_date_range,
    filter_by_past_duration,
    filter_by_time_range,
    filter_by_size_range,
    filter_unique,
)

app = typer.Typer(help="fs-hunter: Scan directories and extract file metadata.")
console = Console()


def _yesterday_midnight() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _resolve_targets(base_path, paths, path_list, delta_csv) -> tuple[list[str], list | None]:
    """Resolve scan targets from one of four input modes.

    Returns: (targets, delta_records or None)
    """
    provided = sum(1 for x in [base_path, paths, path_list, delta_csv] if x)
    if provided == 0:
        console.print("[red]Error:[/red] Provide --base-path, --paths, --path-list, or --delta-csv.")
        raise typer.Exit(1)
    if provided > 1:
        console.print("[red]Error:[/red] Input modes are mutually exclusive.")
        raise typer.Exit(1)

    if base_path:
        return [base_path], None

    if paths:
        return list(paths), None

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
        return targets, None

    # delta_csv
    try:
        unique_paths, delta_records = parse_delta_csv(delta_csv)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    return unique_paths, delta_records


@app.command()
def scan(
    base_path: Optional[str] = typer.Option(None, "--base-path", help="Single directory to scan"),
    paths: Optional[list[str]] = typer.Option(None, "--paths", help="Multiple directories to scan"),
    path_list: Optional[str] = typer.Option(None, "--path-list", help="Text file with paths (one per line)"),
    delta_csv: Optional[str] = typer.Option(None, "--delta-csv", help="CSV file with Directory column (pandas)"),
    scan_start: str = typer.Option(None, "--scan-start", help="Date range start (default: yesterday 00:00:00)"),
    scan_end: str = typer.Option(None, "--scan-end", help="Date range end (default: now)"),
    lookback: str = typer.Option("1H", "--lookback", help="Relative duration e.g. 7D, 2H, 1D12H30M (replaces --scan-start/--scan-end)"),
    day_start: str = typer.Option("00:00:00", "--day-start", help="Time-of-day start"),
    day_end: str = typer.Option("23:59:59", "--day-end", help="Time-of-day end"),
    file_pattern: str = typer.Option(r".*\.parq(uet)?$", "--file-pattern", help="Regex on filename"),
    path_pattern: Optional[str] = typer.Option(None, "--path-pattern", help="Glob on relative path"),
    min_size: Optional[int] = typer.Option(None, "--min-size", help="Min file size in bytes"),
    max_size: Optional[int] = typer.Option(None, "--max-size", help="Max file size in bytes"),
    unique: str = typer.Option("namepattern", "--unique", help="Deduplicate by 'hash' or 'namepattern'"),
    output_format: str = typer.Option("csv", "--output-format", help="Output format: 'csv' or 'jsonl'"),
    output_folder: str = typer.Option("~", "-o", help="Output folder (default: ~)"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel threads (default: 4)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show scan progress details"),
):
    """Scan directories and extract file metadata with filters."""

    targets, delta_records = _resolve_targets(base_path, paths, path_list, delta_csv)

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
        date_filter = filter_by_date_range(after=scan_start, before=scan_end)
    else:
        date_filter = filter_by_past_duration(lookback)
    time_filter = filter_by_time_range(day_start, day_end)

    size_filter = None
    if min_size is not None or max_size is not None:
        size_filter = filter_by_size_range(min_size=min_size, max_size=max_size)

    uniq_filter = filter_unique(base=unique)
    need_hash = unique == "hash"

    # collect scan results
    results = []
    for metadata in scan_directories(
        targets=targets,
        date_filter=date_filter,
        time_filter=time_filter,
        size_filter=size_filter,
        path_pattern=path_pattern,
        name_pattern=file_pattern,
        unique_filter=uniq_filter,
        need_hash=need_hash,
        workers=workers,
        verbose=verbose,
    ):
        console.print(f"{metadata.relative_path}")
        results.append(metadata)

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

    console.print(f"[green]Results:[/green]  {results_file}")
    console.print(f"[green]Summary:[/green]  {summary_file}")


if __name__ == "__main__":
    app()
