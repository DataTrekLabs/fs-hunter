from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import typer
from dotenv import load_dotenv
from rich.console import Console

from utils import create_output_dir

load_dotenv()

app = typer.Typer(help="Compare two fs-hunter scan results and output added/removed files.")
console = Console()


@app.command()
def delta(
    source: str = typer.Option(..., "--source", help="Path to source (baseline) results CSV"),
    target: str = typer.Option(..., "--target", help="Path to target (current) results CSV"),
    output_folder: str = typer.Option(
        os.getenv("FS_HUNTER_OUTPUT_DIR", "~"), "-o", help="Output folder"
    ),
    verbose: bool = typer.Option(False, "-v", help="Show summary to console"),
):
    """Compare two scan result CSVs and produce a delta CSV of added/removed files."""

    source_path = Path(source)
    target_path = Path(target)

    if not source_path.is_file():
        console.print(f"[red]Error:[/red] Source file not found: {source}")
        raise typer.Exit(1)
    if not target_path.is_file():
        console.print(f"[red]Error:[/red] Target file not found: {target}")
        raise typer.Exit(1)

    source_df = pd.read_csv(source_path)
    target_df = pd.read_csv(target_path)

    if "full_path" not in source_df.columns:
        console.print("[red]Error:[/red] Source CSV missing 'full_path' column.")
        raise typer.Exit(1)
    if "full_path" not in target_df.columns:
        console.print("[red]Error:[/red] Target CSV missing 'full_path' column.")
        raise typer.Exit(1)

    source_paths = set(source_df["full_path"])
    target_paths = set(target_df["full_path"])

    added_paths = target_paths - source_paths
    removed_paths = source_paths - target_paths

    added_df = target_df[target_df["full_path"].isin(added_paths)].copy()
    added_df.insert(0, "change", "+")

    removed_df = source_df[source_df["full_path"].isin(removed_paths)].copy()
    removed_df.insert(0, "change", "-")

    delta_df = pd.concat([added_df, removed_df], ignore_index=True)

    if verbose:
        console.print(f"[green]Added (+):[/green]   {len(added_df)} files")
        console.print(f"[red]Removed (-):[/red] {len(removed_df)} files")
        console.print(f"[bold]Total delta:[/bold]  {len(delta_df)} files")

    if delta_df.empty:
        console.print("[yellow]No differences found.[/yellow]")
        return

    out_dir = create_output_dir(output_folder)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    delta_file = out_dir / f"delta_{timestamp}.csv"
    delta_df.to_csv(delta_file, index=False)

    console.print(f"[green]Delta:[/green] {delta_file}")


if __name__ == "__main__":
    app()
