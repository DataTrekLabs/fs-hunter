from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
import pandas as pd
from loguru import logger
from metadata import FileMetadata, DeltaInfo, DELTA_REQUIRED_COLUMNS

# CSV column order matching FileMetadata.to_dict() keys
_CSV_COLUMNS = [
    "name", "extension", "full_path", "relative_path", "size_bytes",
    "ctime", "mtime", "permissions", "owner", "mime_type", "md5",
]


class StreamingCSVWriter:
    """Writes FileMetadata rows to CSV incrementally as they arrive."""

    def __init__(self, out_dir: Path, fmt: str = "csv"):
        self.fmt = fmt
        self._row_count = 0
        if fmt == "jsonl":
            self.path = out_dir / "results.jsonl"
            self._fh = open(self.path, "w", encoding="utf-8", newline="")
            self._writer = None
        else:
            self.path = out_dir / "results.csv"
            self._fh = open(self.path, "w", encoding="utf-8", newline="")
            self._writer = csv.DictWriter(self._fh, fieldnames=_CSV_COLUMNS)
            self._writer.writeheader()
            self._fh.flush()
        logger.info("StreamingCSVWriter opened | path={} fmt={}", self.path, fmt)

    def write_row(self, metadata: FileMetadata) -> None:
        row = metadata.to_dict()
        if self.fmt == "jsonl":
            self._fh.write(json.dumps(row, default=str) + "\n")
        else:
            self._writer.writerow(row)
        self._fh.flush()
        self._row_count += 1
        if self._row_count % 100 == 0:
            logger.debug("StreamingCSVWriter progress | rows={}", self._row_count)

    def close(self) -> None:
        self._fh.close()
        logger.info("StreamingCSVWriter closed | path={} rows={}", self.path, self._row_count)


def _is_valid_date(digits: str) -> bool:
    """Check if 8-digit string is a plausible YYYYMMDD date."""
    if len(digits) != 8:
        return False
    y, m, d = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
    return 1900 <= y <= 2099 and 1 <= m <= 12 and 1 <= d <= 31


def parse_delta_csv(csv_path: str) -> tuple[list[str], list[DeltaInfo]]:
    """Parse delta CSV file using pandas.

    Validates required columns, extracts unique directories for scanning,
    and returns DeltaInfo records for each row.

    Returns:
        (unique_paths, delta_records)
    """
    p = Path(csv_path)
    if not p.is_file():
        raise FileNotFoundError(f"Delta CSV not found: {csv_path}")

    df = pd.read_csv(p)

    # validate required columns
    missing = [col for col in DELTA_REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Delta CSV missing columns: {missing}")

    # extract unique directories for scanning
    unique_paths = df["Directory"].dropna().unique().tolist()

    # build DeltaInfo records
    delta_records = []
    for _, row in df.iterrows():
        delta_records.append(DeltaInfo(
            directory=str(row.get("Directory", "")),
            dataset_repo=str(row.get("Dataset Repo", "")),
            sf_table=str(row.get("SF Table", "")),
            filename=str(row.get("Filename", "")),
        ))

    logger.info("parse_delta_csv | path={} rows={} unique_dirs={}", csv_path, len(delta_records), len(unique_paths))
    return unique_paths, delta_records


def results_to_dataframe(results: list[FileMetadata]) -> pd.DataFrame:
    """Convert list of FileMetadata to a pandas DataFrame."""
    rows = [m.to_dict() for m in results]
    return pd.DataFrame(rows)


def create_output_dir(output_folder: str = "~", subcommand: str = "scan") -> Path:
    """Create timestamped output directory: fs_hunter/{subcommand}/YYYYMMDD_HHMMSS/"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_folder).expanduser() / "fs_hunter" / subcommand / timestamp
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("create_output_dir | path={}", out_dir)
    return out_dir


def write_results(df: pd.DataFrame, out_dir: Path, fmt: str = "jsonl") -> Path:
    """Write scan results DataFrame in chosen format."""
    if fmt == "jsonl":
        out_file = out_dir / "results.jsonl"
        df.to_json(out_file, orient="records", lines=True)
    else:
        out_file = out_dir / "results.csv"
        df.to_csv(out_file, index=False)
    logger.info("write_results | path={} rows={}", out_file, len(df))
    return out_file


def write_summary(df: pd.DataFrame, out_dir: Path, targets: list[str],
                   scan_start: str, scan_end: str) -> Path:
    """Write _summary.csv with scan stats."""
    summary = {
        "scan_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_start": scan_start,
        "scan_end": scan_end,
        "targets": "; ".join(targets),
        "total_files": len(df),
        "total_size_bytes": int(df["size_bytes"].sum()) if "size_bytes" in df.columns else 0,
        "unique_extensions": df["extension"].nunique() if "extension" in df.columns else 0,
    }
    summary_file = out_dir / "_summary.csv"
    pd.DataFrame([summary]).to_csv(summary_file, index=False)
    logger.info("write_summary | path={}", summary_file)
    return summary_file


def enrich_with_delta(df: pd.DataFrame, delta_records: list[DeltaInfo]) -> pd.DataFrame:
    """Enrich scan results DataFrame with delta CSV details.

    Matches scanned files to delta rows by: full_path starts with directory.
    Adds columns: dataset_repo, sf_table, filename_pattern.
    """
    delta_df = pd.DataFrame([d.to_dict() for d in delta_records])

    # normalize trailing slashes
    delta_df["directory"] = delta_df["directory"].str.rstrip("/") + "/"

    def _match_delta(full_path: str) -> pd.Series:
        # normalize path separators
        fp = full_path.replace("\\", "/")
        for _, row in delta_df.iterrows():
            if fp.startswith(row["directory"]):
                return pd.Series({
                    "dataset_repo": row["dataset_repo"],
                    "sf_table": row["sf_table"],
                    "filename_pattern": row["filename"],
                })
        return pd.Series({
            "dataset_repo": "",
            "sf_table": "",
            "filename_pattern": "",
        })

    enriched = df["full_path"].apply(_match_delta)
    return pd.concat([df, enriched], axis=1)


def name_to_pattern(filename: str) -> str:
    r"""Convert a filename to a regex pattern for dedup grouping.

    report_20250601.csv   -> report_\d{4}\d{2}\d{2}\.csv
    data_2024-01-15.parq  -> data_\d{4}-\d{2}-\d{2}\.parq
    log_123.txt           -> log_\d{3}\.txt
    backup_v2.tar.gz      -> backup_v\d{1}\.tar\.gz
    """
    parts = re.split(r"(\d+)", filename)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            result.append(re.escape(part))
        else:
            if _is_valid_date(part):
                result.append(r"\d{4}\d{2}\d{2}")
            else:
                result.append(rf"\d{{{len(part)}}}")
    return "".join(result)


def write_metrics(df: pd.DataFrame, out_dir: Path, scan_duration: float,
                  interval_minutes: int = 30) -> Path:
    """Build and write metrics.json with scan performance stats and breakdowns."""
    metrics: dict = {}

    # --- scan_performance ---
    metrics["scan_performance"] = {
        "total_matched": len(df),
        "scan_duration_seconds": round(scan_duration, 3),
    }

    # --- size_stats ---
    if "size_bytes" in df.columns and len(df) > 0:
        metrics["size_stats"] = {
            "total_bytes": int(df["size_bytes"].sum()),
            "avg_bytes": int(df["size_bytes"].mean()),
            "min_bytes": int(df["size_bytes"].min()),
            "max_bytes": int(df["size_bytes"].max()),
        }
    else:
        metrics["size_stats"] = {
            "total_bytes": 0, "avg_bytes": 0, "min_bytes": 0, "max_bytes": 0,
        }

    # --- by_extension ---
    by_ext: dict = {}
    if "extension" in df.columns and "size_bytes" in df.columns and len(df) > 0:
        grouped = df.groupby("extension", dropna=False)
        for ext, group in grouped:
            key = ext if ext else "(none)"
            by_ext[key] = {
                "count": len(group),
                "total_bytes": int(group["size_bytes"].sum()),
            }
    metrics["by_extension"] = by_ext

    # --- by_directory (first path component of full_path relative to base) ---
    by_dir: dict = {}
    if "full_path" in df.columns and "size_bytes" in df.columns and len(df) > 0:
        def _top_dir(full_path: str) -> str:
            parts = Path(full_path).parts
            # Use the parent directory name (second to last component)
            if len(parts) >= 2:
                return parts[-2]
            return parts[0] if parts else "(root)"

        df_copy = df.copy()
        df_copy["_top_dir"] = df_copy["full_path"].apply(_top_dir)
        grouped = df_copy.groupby("_top_dir", dropna=False)
        for dirname, group in grouped:
            by_dir[dirname] = {
                "count": len(group),
                "total_bytes": int(group["size_bytes"].sum()),
            }
    metrics["by_directory"] = by_dir

    # --- time_buckets (24h split into N-minute intervals by mtime) ---
    buckets_per_day = (24 * 60) // interval_minutes
    time_buckets: list[dict] = []

    if "mtime" in df.columns and len(df) > 0:
        # Ensure mtime is datetime
        mod_col = pd.to_datetime(df["mtime"], errors="coerce")

        for i in range(buckets_per_day):
            start_min = i * interval_minutes
            end_min = start_min + interval_minutes
            start_h, start_m = divmod(start_min, 60)
            end_h, end_m = divmod(end_min, 60)
            label = f"{start_h:02d}:{start_m:02d}-{end_h:02d}:{end_m:02d}"

            # Filter files whose mtime time-of-day falls in this bucket
            time_of_day = mod_col.dt.hour * 60 + mod_col.dt.minute
            mask = (time_of_day >= start_min) & (time_of_day < end_min)
            bucket_df = df[mask]

            bucket = {
                "interval": label,
                "count": len(bucket_df),
                "total_bytes": int(bucket_df["size_bytes"].sum()) if "size_bytes" in bucket_df.columns else 0,
                "files": bucket_df["full_path"].tolist() if len(bucket_df) > 0 else [],
            }
            time_buckets.append(bucket)

        # Find peak bucket
        peak = max(time_buckets, key=lambda b: b["count"])
        empty_count = sum(1 for b in time_buckets if b["count"] == 0)
    else:
        peak = {"interval": "N/A", "count": 0}
        empty_count = buckets_per_day

    metrics["time_buckets"] = {
        "interval_minutes": interval_minutes,
        "buckets": time_buckets,
        "peak_bucket": peak["interval"],
        "peak_count": peak["count"],
        "empty_buckets": empty_count,
    }

    # Write to file
    metrics_file = out_dir / "metrics.json"
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)
    logger.info("write_metrics | path={}", metrics_file)
    return metrics_file
