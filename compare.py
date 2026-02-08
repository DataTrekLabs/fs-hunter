from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def compute_delta(
    source_df: pd.DataFrame, target_df: pd.DataFrame
) -> tuple[pd.DataFrame, int, int]:
    """Compare source (baseline) vs target (current) scan results on full_path.

    Returns:
        (delta_df, added_count, removed_count)
        delta_df has a 'change' column: '+' for added, '-' for removed.
    """
    source_paths = set(source_df["full_path"])
    target_paths = set(target_df["full_path"])

    added_paths = target_paths - source_paths
    removed_paths = source_paths - target_paths

    added_df = target_df[target_df["full_path"].isin(added_paths)].copy()
    added_df.insert(0, "change", "+")

    removed_df = source_df[source_df["full_path"].isin(removed_paths)].copy()
    removed_df.insert(0, "change", "-")

    delta_df = pd.concat([added_df, removed_df], ignore_index=True)
    return delta_df, len(added_df), len(removed_df)


def write_compare_summary(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    delta_df: pd.DataFrame,
    out_dir: Path,
    source_path: str,
    target_path: str,
) -> Path:
    """Write _summary.csv with comparison statistics."""
    added_count = len(delta_df[delta_df["change"] == "+"]) if not delta_df.empty else 0
    removed_count = len(delta_df[delta_df["change"] == "-"]) if not delta_df.empty else 0

    summary = {
        "source_dir": source_path,
        "target_dir": target_path,
        "source_files": len(source_df),
        "target_files": len(target_df),
        "added": added_count,
        "removed": removed_count,
        "unchanged": len(source_df) - removed_count,
    }
    summary_file = out_dir / "_summary.csv"
    pd.DataFrame([summary]).to_csv(summary_file, index=False)
    return summary_file


def write_delta_metrics(delta_df: pd.DataFrame, out_dir: Path) -> Path:
    """Write delta_metrics.json with breakdown of added/removed files."""
    metrics: dict = {}

    # by_change_type
    added = delta_df[delta_df["change"] == "+"] if not delta_df.empty else pd.DataFrame()
    removed = delta_df[delta_df["change"] == "-"] if not delta_df.empty else pd.DataFrame()

    added_size = int(added["size_bytes"].sum()) if "size_bytes" in added.columns and len(added) > 0 else 0
    removed_size = int(removed["size_bytes"].sum()) if "size_bytes" in removed.columns and len(removed) > 0 else 0

    metrics["by_change_type"] = {
        "added": {"count": len(added), "total_bytes": added_size},
        "removed": {"count": len(removed), "total_bytes": removed_size},
    }

    # by_extension
    by_ext: dict = {}
    if "extension" in delta_df.columns and len(delta_df) > 0:
        for ext, group in delta_df.groupby("extension", dropna=False):
            key = ext if ext else "(none)"
            ext_added = group[group["change"] == "+"]
            ext_removed = group[group["change"] == "-"]
            by_ext[key] = {
                "added": len(ext_added),
                "removed": len(ext_removed),
            }
    metrics["by_extension"] = by_ext

    # totals
    metrics["total_size_added_bytes"] = added_size
    metrics["total_size_removed_bytes"] = removed_size

    metrics_file = out_dir / "delta_metrics.json"
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)
    return metrics_file
