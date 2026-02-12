from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
from loguru import logger


def format_time_delta(seconds: float) -> str:
    """Format a time delta in seconds to a human-readable string.

    0          -> ""
    abs >= 3600 -> +/-HH:MM:SS
    abs < 3600  -> +/-MM:SS
    + = target newer, - = source newer
    """
    if seconds == 0 or math.isnan(seconds):
        return ""
    sign = "+" if seconds > 0 else "-"
    total = int(abs(seconds))
    h, remainder = divmod(total, 3600)
    m, s = divmod(remainder, 60)
    if h > 0:
        return f"{sign}{h:02d}:{m:02d}:{s:02d}"
    return f"{sign}{m:02d}:{s:02d}"


def compute_comparison(
    source_df: pd.DataFrame, target_df: pd.DataFrame
) -> pd.DataFrame:
    """Join source and target on relative_path and compute per-file status/deltas.

    Returns a DataFrame with columns:
        relative_path, status,
        source_mtime, target_mtime, mtime_delta,
        source_ctime, target_ctime, ctime_delta,
        source_size, target_size, size_delta,
        checksum,
        source_full_path, target_full_path
    """
    # Rename columns before merge to avoid ambiguity
    s = source_df.rename(columns={
        "full_path": "source_full_path",
        "mtime": "source_mtime",
        "ctime": "source_ctime",
        "size_bytes": "source_size",
        "md5": "source_md5",
    })[["relative_path", "source_full_path", "source_mtime", "source_ctime", "source_size", "source_md5"]]

    t = target_df.rename(columns={
        "full_path": "target_full_path",
        "mtime": "target_mtime",
        "ctime": "target_ctime",
        "size_bytes": "target_size",
        "md5": "target_md5",
    })[["relative_path", "target_full_path", "target_mtime", "target_ctime", "target_size", "target_md5"]]

    merged = pd.merge(s, t, on="relative_path", how="outer")
    logger.info("compute_comparison | source_rows={} target_rows={} merged_rows={}", len(s), len(t), len(merged))

    # Parse timestamps to datetime for delta computation
    for col in ("source_mtime", "target_mtime", "source_ctime", "target_ctime"):
        merged[col] = pd.to_datetime(merged[col], errors="coerce")

    # Derive status
    def _status(row):
        s_missing = pd.isna(row.get("source_full_path"))
        t_missing = pd.isna(row.get("target_full_path"))
        if s_missing:
            return "missing_in_source"
        if t_missing:
            return "missing_in_target"
        # Both present — check for differences
        size_diff = row.get("source_size") != row.get("target_size")
        md5_diff = (
            row.get("source_md5") != row.get("target_md5")
            and row.get("source_md5") not in (None, "", "nan")
            and row.get("target_md5") not in (None, "", "nan")
        )
        mtime_diff = row.get("source_mtime") != row.get("target_mtime")
        if size_diff or md5_diff or mtime_diff:
            return "differ"
        return "match"

    merged["status"] = merged.apply(_status, axis=1)
    _status_counts = merged["status"].value_counts()
    logger.info("compute_comparison status | {}", _status_counts.to_dict())

    # Compute time deltas (target - source, in seconds)
    def _time_delta_seconds(row, src_col, tgt_col):
        s_val = row.get(src_col)
        t_val = row.get(tgt_col)
        if pd.isna(s_val) or pd.isna(t_val):
            return float("nan")
        return (t_val - s_val).total_seconds()

    merged["mtime_delta"] = merged.apply(
        lambda r: _time_delta_seconds(r, "source_mtime", "target_mtime"), axis=1
    )
    merged["ctime_delta"] = merged.apply(
        lambda r: _time_delta_seconds(r, "source_ctime", "target_ctime"), axis=1
    )

    # Size delta
    merged["size_delta"] = merged["target_size"] - merged["source_size"]

    # Checksum status
    def _checksum_status(row):
        s_md5 = row.get("source_md5")
        t_md5 = row.get("target_md5")
        if pd.isna(s_md5) or pd.isna(t_md5) or s_md5 in ("", None) or t_md5 in ("", None):
            return "N/A"
        return "Match" if s_md5 == t_md5 else "Mismatch"

    merged["checksum"] = merged.apply(_checksum_status, axis=1)

    # Format time deltas for display
    merged["mtime_delta"] = merged["mtime_delta"].apply(
        lambda v: format_time_delta(v) if not pd.isna(v) else ""
    )
    merged["ctime_delta"] = merged["ctime_delta"].apply(
        lambda v: format_time_delta(v) if not pd.isna(v) else ""
    )

    # Format timestamps back to strings
    for col in ("source_mtime", "target_mtime", "source_ctime", "target_ctime"):
        merged[col] = merged[col].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("N/A")

    # Fill missing-side columns
    merged["source_full_path"] = merged["source_full_path"].fillna("N/A")
    merged["target_full_path"] = merged["target_full_path"].fillna("N/A")
    merged["source_size"] = merged["source_size"].fillna(0).astype(int)
    merged["target_size"] = merged["target_size"].fillna(0).astype(int)
    merged["size_delta"] = merged["size_delta"].fillna(0).astype(int)

    # Select and order output columns
    out_cols = [
        "relative_path", "status",
        "source_mtime", "target_mtime", "mtime_delta",
        "source_ctime", "target_ctime", "ctime_delta",
        "source_size", "target_size", "size_delta",
        "checksum",
        "source_full_path", "target_full_path",
    ]
    return merged[out_cols]


def write_compare_summary(
    comparison_df: pd.DataFrame,
    out_dir: Path,
    source_path: str,
    target_path: str,
    source_count: int,
    target_count: int,
) -> Path:
    """Write _summary.csv with comparison statistics."""
    status_counts = comparison_df["status"].value_counts()
    summary = {
        "source_dir": source_path,
        "target_dir": target_path,
        "total_source": source_count,
        "total_target": target_count,
        "total_compared": len(comparison_df),
        "total_matched": int(status_counts.get("match", 0)),
        "total_differ": int(status_counts.get("differ", 0)),
        "missing_in_source": int(status_counts.get("missing_in_source", 0)),
        "missing_in_target": int(status_counts.get("missing_in_target", 0)),
    }
    summary_file = out_dir / "_summary.csv"
    pd.DataFrame([summary]).to_csv(summary_file, index=False)
    logger.info("write_compare_summary | path={}", summary_file)
    return summary_file


def write_delta_metrics(comparison_df: pd.DataFrame, out_dir: Path) -> Path:
    """Write delta_metrics.json with overview, by_status, by_extension, latency."""
    metrics: dict = {}
    status_counts = comparison_df["status"].value_counts()
    total = len(comparison_df)
    matched = int(status_counts.get("match", 0))

    # overview
    metrics["overview"] = {
        "total_compared": total,
        "matched": matched,
        "differ": int(status_counts.get("differ", 0)),
        "missing_in_source": int(status_counts.get("missing_in_source", 0)),
        "missing_in_target": int(status_counts.get("missing_in_target", 0)),
        "match_rate": round(matched / total, 4) if total > 0 else 0,
    }

    # by_status
    by_status: dict = {}
    for status_val, group in comparison_df.groupby("status"):
        source_bytes = int(group["source_size"].sum())
        target_bytes = int(group["target_size"].sum())
        by_status[status_val] = {
            "count": len(group),
            "source_bytes": source_bytes,
            "target_bytes": target_bytes,
        }
    metrics["by_status"] = by_status

    # by_extension
    by_ext: dict = {}
    comparison_df = comparison_df.copy()
    comparison_df["_ext"] = comparison_df["relative_path"].apply(
        lambda p: Path(p).suffix if pd.notna(p) else "(none)"
    )
    for ext, group in comparison_df.groupby("_ext", dropna=False):
        key = ext if ext else "(none)"
        ext_status = group["status"].value_counts()
        by_ext[key] = {
            "match": int(ext_status.get("match", 0)),
            "differ": int(ext_status.get("differ", 0)),
            "missing_in_source": int(ext_status.get("missing_in_source", 0)),
            "missing_in_target": int(ext_status.get("missing_in_target", 0)),
        }
    metrics["by_extension"] = by_ext

    # latency — compute from mtime_delta strings for rows with both sides present
    both_present = comparison_df[comparison_df["status"].isin(["match", "differ"])]
    latency_seconds = []
    for val in both_present["mtime_delta"]:
        if not val or val == "":
            latency_seconds.append(0.0)
            continue
        # Parse back from format_time_delta format
        sign = 1 if val.startswith("+") else -1
        parts = val[1:].split(":")
        if len(parts) == 3:
            secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            secs = int(parts[0]) * 60 + int(parts[1])
        else:
            secs = 0
        latency_seconds.append(sign * secs)

    if latency_seconds:
        metrics["latency"] = {
            "avg_mtime_delta_seconds": round(sum(latency_seconds) / len(latency_seconds), 2),
            "max_mtime_delta_seconds": max(latency_seconds),
            "min_mtime_delta_seconds": min(latency_seconds),
        }
    else:
        metrics["latency"] = {
            "avg_mtime_delta_seconds": 0,
            "max_mtime_delta_seconds": 0,
            "min_mtime_delta_seconds": 0,
        }

    metrics_file = out_dir / "delta_metrics.json"
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)
    logger.info("write_delta_metrics | path={}", metrics_file)
    return metrics_file


def write_metrics_jsonl(
    comparison_df: pd.DataFrame, out_dir: Path, interval_minutes: int = 30
) -> Path:
    """Write metrics.jsonl — one JSON line per time bucket.

    Buckets by source_mtime (falls back to target_mtime for missing-in-source rows).
    Floor to nearest interval_minutes.
    """
    df = comparison_df.copy()

    # Pick the best timestamp for bucketing
    def _bucket_time(row):
        ts = row.get("source_mtime")
        if ts == "N/A" or pd.isna(ts):
            ts = row.get("target_mtime")
        return ts

    df["_bucket_ts"] = df.apply(_bucket_time, axis=1)
    df["_bucket_ts"] = pd.to_datetime(df["_bucket_ts"], errors="coerce")

    # Drop rows with no usable timestamp
    df = df.dropna(subset=["_bucket_ts"])

    if df.empty:
        metrics_file = out_dir / "metrics.jsonl"
        metrics_file.touch()
        return metrics_file

    # Floor to interval
    df["_bucket"] = df["_bucket_ts"].dt.floor(f"{interval_minutes}min")
    df["_bucket_label"] = df["_bucket"].dt.strftime("%Y%m%d_%H%M")

    lines = []
    for bucket_label, group in df.groupby("_bucket_label"):
        source_files = group[group["source_full_path"] != "N/A"]["relative_path"].tolist()
        target_files = group[group["target_full_path"] != "N/A"]["relative_path"].tolist()
        status_counts = group["status"].value_counts()

        # Parse latency for this bucket
        latencies = []
        for val in group["mtime_delta"]:
            if not val or val == "":
                continue
            sign = 1 if val.startswith("+") else -1
            parts = val[1:].split(":")
            if len(parts) == 3:
                secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                secs = int(parts[0]) * 60 + int(parts[1])
            else:
                secs = 0
            latencies.append(sign * secs)

        record = {
            "bucket": bucket_label,
            "source_count": len(source_files),
            "target_count": len(target_files),
            "match": int(status_counts.get("match", 0)),
            "differ": int(status_counts.get("differ", 0)),
            "missing_source": int(status_counts.get("missing_in_source", 0)),
            "missing_target": int(status_counts.get("missing_in_target", 0)),
            "avg_latency_sec": round(sum(latencies) / len(latencies), 2) if latencies else 0,
            "source_files": source_files,
            "target_files": target_files,
        }
        lines.append(json.dumps(record, default=str))

    metrics_file = out_dir / "metrics.jsonl"
    with open(metrics_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return metrics_file
