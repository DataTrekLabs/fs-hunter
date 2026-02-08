from __future__ import annotations

import os
import time
import typer
from typing import Optional
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

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
from compare import compute_delta, write_compare_summary, write_delta_metrics
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


def _parse_input(value: str) -> list[str]:
    """Auto-detect input: if value ends with .txt and the file exists, read lines;
    otherwise split on comma."""
    if value.endswith(".txt") and Path(value).is_file():
        lines = Path(value).read_text(encoding="utf-8").strip().splitlines()
        entries = [line.strip() for line in lines if line.strip()]
        if not entries:
            console.print(f"[red]Error:[/red] File is empty: {value}")
            raise typer.Exit(1)
        return entries
    return [v.strip() for v in value.split(",") if v.strip()]


@app.command()
def scan(
    dirs: Optional[str] = typer.Option(None, "--dirs", "-d", help="Directories (comma-separated or .txt file)"),
    files: Optional[str] = typer.Option(None, "--files", "-f", help="Specific files (comma-separated or .txt file)"),
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
    off_hash: bool = typer.Option(False, "--off-hash", help="Disable MD5 hash computation"),
    output_format: str = typer.Option("csv", "--output-format", help="Output format: 'csv' or 'jsonl'"),
    output_folder: str = typer.Option(os.getenv("FS_HUNTER_OUTPUT_DIR", "~"), "-o", help="Output folder"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel threads (default: 4)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show scan progress details"),
    no_metrics: bool = typer.Option(False, "--no-metrics", help="Skip metrics.json generation"),
    metrics_interval: int = typer.Option(30, "--metrics-interval", help="Time bucket size in minutes"),
):
    """Scan directories or specific files and extract file metadata with filters."""

    if not dirs and not files:
        console.print("[red]Error:[/red] Provide -d/--dirs or -f/--files.")
        raise typer.Exit(1)
    if dirs and files:
        console.print("[red]Error:[/red] -d/--dirs and -f/--files are mutually exclusive.")
        raise typer.Exit(1)

    is_file_list = files is not None
    targets = _parse_input(files if is_file_list else dirs)

    if not targets:
        console.print("[red]Error:[/red] No targets resolved from input.")
        raise typer.Exit(1)

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
    enable_hash = os.getenv("ENABLE_HASH", "true").lower() not in ("false", "0", "no")
    need_hash = (not off_hash) and enable_hash

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

    # write output files
    out_dir = create_output_dir(output_folder, "scan")
    results_file = write_results(df, out_dir, fmt=output_format)
    summary_file = write_summary(df, out_dir, targets, scan_start, scan_end)

    if not no_metrics:
        metrics_file = write_metrics(df, out_dir, scan_duration, metrics_interval)
        console.print(f"[green]Metrics:[/green]  {metrics_file}")

    console.print(f"[green]Results:[/green]  {results_file}")
    console.print(f"[green]Summary:[/green]  {summary_file}")


@app.command()
def delta(
    delta_csv: str = typer.Argument(..., help="Path to delta CSV file with Directory column"),
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
    off_hash: bool = typer.Option(False, "--off-hash", help="Disable MD5 hash computation"),
    output_format: str = typer.Option("csv", "--output-format", help="Output format: 'csv' or 'jsonl'"),
    output_folder: str = typer.Option(os.getenv("FS_HUNTER_OUTPUT_DIR", "~"), "-o", help="Output folder"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel threads (default: 4)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show scan progress details"),
    no_metrics: bool = typer.Option(False, "--no-metrics", help="Skip metrics.json generation"),
    metrics_interval: int = typer.Option(30, "--metrics-interval", help="Time bucket size in minutes"),
):
    """Scan directories from a delta CSV manifest and enrich with delta metadata."""

    try:
        targets, delta_records = parse_delta_csv(delta_csv)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

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

    fp_type, fp_value = file_pattern
    if fp_type not in ("glob", "regex"):
        console.print("[red]Error:[/red] --file-pattern type must be 'glob' or 'regex'.")
        raise typer.Exit(1)

    pp_type, pp_value = path_pattern if path_pattern else (fp_type, None)

    uniq_filter = filter_unique(base=unique)
    enable_hash = os.getenv("ENABLE_HASH", "true").lower() not in ("false", "0", "no")
    need_hash = (not off_hash) and enable_hash
    time_filter = filter_by_time_range(day_start, day_end)

    scan_start_time = time.time()
    results = []

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

    df = results_to_dataframe(results)
    df = enrich_with_delta(df, delta_records)

    out_dir = create_output_dir(output_folder, "delta")
    results_file = write_results(df, out_dir, fmt=output_format)
    summary_file = write_summary(df, out_dir, targets, scan_start, scan_end)

    if not no_metrics:
        metrics_file = write_metrics(df, out_dir, scan_duration, metrics_interval)
        console.print(f"[green]Metrics:[/green]  {metrics_file}")

    console.print(f"[green]Results:[/green]  {results_file}")
    console.print(f"[green]Summary:[/green]  {summary_file}")


@app.command()
def compare(
    source_prefix: str = typer.Option(..., "--source-prefix", help="Source (baseline) base path"),
    target_prefix: str = typer.Option(..., "--target-prefix", help="Target (current) base path"),
    subdirs: Optional[str] = typer.Option(None, "--subdirs", help="Subdirectory names (comma-separated or .txt file)"),
    files: Optional[str] = typer.Option(None, "--files", help="File paths relative to prefix (comma-separated or .txt file)"),
    scan_start: str = typer.Option(None, "--scan-start", help="Date range start"),
    scan_end: str = typer.Option(None, "--scan-end", help="Date range end"),
    lookback: str = typer.Option("1H", "--lookback", help="Relative duration e.g. 7D, 2H, 1D12H30M"),
    day_start: str = typer.Option("00:00:00", "--day-start", help="Time-of-day start"),
    day_end: str = typer.Option("23:59:59", "--day-end", help="Time-of-day end"),
    file_pattern: tuple[str, str] = typer.Option(("glob", "*.parq*"), "--file-pattern", help="Pattern on filename: glob|regex PATTERN"),
    path_pattern: Optional[tuple[str, str]] = typer.Option(None, "--path-pattern", help="Pattern on relative path: glob|regex PATTERN"),
    min_size: Optional[int] = typer.Option(None, "--min-size", help="Min file size in bytes"),
    max_size: Optional[int] = typer.Option(None, "--max-size", help="Max file size in bytes"),
    unique: str = typer.Option("namepattern", "--unique", help="Deduplicate by 'hash' or 'namepattern'"),
    off_hash: bool = typer.Option(False, "--off-hash", help="Disable MD5 hash computation"),
    output_folder: str = typer.Option(os.getenv("FS_HUNTER_OUTPUT_DIR", "~"), "-o", help="Output folder"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel threads (default: 4)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show scan progress details"),
    no_metrics: bool = typer.Option(False, "--no-metrics", help="Skip metrics.json generation"),
    metrics_interval: int = typer.Option(30, "--metrics-interval", help="Time bucket size in minutes"),
):
    """Compare two directory trees: scan both with same filters, then diff results."""

    if not subdirs and not files:
        console.print("[red]Error:[/red] Provide --subdirs or --files.")
        raise typer.Exit(1)
    if subdirs and files:
        console.print("[red]Error:[/red] --subdirs and --files are mutually exclusive.")
        raise typer.Exit(1)

    entries = _parse_input(subdirs if subdirs else files)
    if not entries:
        console.print("[red]Error:[/red] No entries resolved from input.")
        raise typer.Exit(1)

    # Build full paths for source and target
    source_paths = [os.path.join(source_prefix, e) for e in entries]
    target_paths = [os.path.join(target_prefix, e) for e in entries]

    is_file_mode = files is not None

    use_date_range = scan_start is not None or scan_end is not None

    if unique not in ("hash", "namepattern"):
        console.print("[red]Error:[/red] --unique must be 'hash' or 'namepattern'.")
        raise typer.Exit(1)

    if use_date_range:
        if scan_start is None:
            scan_start = _yesterday_midnight()
        if scan_end is None:
            scan_end = _now()

    fp_type, fp_value = file_pattern
    if fp_type not in ("glob", "regex"):
        console.print("[red]Error:[/red] --file-pattern type must be 'glob' or 'regex'.")
        raise typer.Exit(1)

    pp_type, pp_value = path_pattern if path_pattern else (fp_type, None)
    time_filter = filter_by_time_range(day_start, day_end)
    enable_hash = os.getenv("ENABLE_HASH", "true").lower() not in ("false", "0", "no")
    need_hash = (not off_hash) and enable_hash

    def _run_scan(dir_paths: list[str], label: str, ufilter) -> list:
        console.print(f"\n[bold cyan]Scanning {label}:[/bold cyan] {', '.join(dir_paths)}")
        results = []

        if is_file_mode:
            scanner = process_file_list(
                file_paths=dir_paths,
                unique_filter=ufilter,
                need_hash=need_hash,
                workers=workers,
                verbose=verbose,
            )
        else:
            scanner = scan_directories(
                targets=dir_paths,
                name_pattern=fp_value,
                pattern_type=fp_type,
                lookback=lookback if not use_date_range else None,
                scan_start=scan_start if use_date_range else None,
                scan_end=scan_end if use_date_range else None,
                min_size=min_size,
                max_size=max_size,
                time_filter=time_filter,
                path_pattern=pp_value,
                unique_filter=ufilter,
                need_hash=need_hash,
                workers=workers,
                verbose=verbose,
            )

        for metadata in scanner:
            if verbose:
                console.print(f"  {metadata.relative_path}")
            results.append(metadata)
        console.print(f"[green]{len(results)} files found in {label}.[/green]")
        return results

    scan_start_time = time.time()

    source_results = _run_scan(source_paths, "source", filter_unique(base=unique))
    target_results = _run_scan(target_paths, "target", filter_unique(base=unique))

    scan_duration = time.time() - scan_start_time

    source_df = results_to_dataframe(source_results) if source_results else pd.DataFrame()
    target_df = results_to_dataframe(target_results) if target_results else pd.DataFrame()

    # Compute delta
    if source_df.empty and target_df.empty:
        console.print("[yellow]Both directories empty — nothing to compare.[/yellow]")
        return

    delta_df, added_count, removed_count = compute_delta(
        source_df if not source_df.empty else pd.DataFrame(columns=["full_path"]),
        target_df if not target_df.empty else pd.DataFrame(columns=["full_path"]),
    )

    console.print(f"\n[bold]Delta:[/bold] [green]+{added_count} added[/green], [red]-{removed_count} removed[/red]")

    # Create output directory
    out_dir = create_output_dir(output_folder, "compare")

    # Write source and target results
    if not source_df.empty:
        s_file = out_dir / "s_result.csv"
        source_df.to_csv(s_file, index=False)
        console.print(f"[green]Source results:[/green]  {s_file}")

    if not target_df.empty:
        t_file = out_dir / "t_result.csv"
        target_df.to_csv(t_file, index=False)
        console.print(f"[green]Target results:[/green]  {t_file}")

    # Write comparison summary
    summary_file = write_compare_summary(
        source_df if not source_df.empty else pd.DataFrame(),
        target_df if not target_df.empty else pd.DataFrame(),
        delta_df, out_dir, source_prefix, target_prefix,
    )
    console.print(f"[green]Summary:[/green]         {summary_file}")

    # Write delta CSV if there are changes
    if not delta_df.empty:
        delta_file = out_dir / "delta.csv"
        delta_df.to_csv(delta_file, index=False)
        console.print(f"[green]Delta:[/green]           {delta_file}")

    # Write delta metrics
    if not no_metrics:
        dm_file = write_delta_metrics(delta_df, out_dir)
        console.print(f"[green]Delta metrics:[/green]   {dm_file}")

        # Combined metrics on all scanned files
        combined_df = pd.concat([source_df, target_df], ignore_index=True) if not source_df.empty or not target_df.empty else pd.DataFrame()
        if not combined_df.empty:
            metrics_file = write_metrics(combined_df, out_dir, scan_duration, metrics_interval)
            console.print(f"[green]Metrics:[/green]         {metrics_file}")


if __name__ == "__main__":
    app()
