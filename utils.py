import re
from datetime import datetime
from pathlib import Path
import pandas as pd
from metadata import FileMetadata, DeltaInfo, DELTA_REQUIRED_COLUMNS


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

    return unique_paths, delta_records


def results_to_dataframe(results: list[FileMetadata]) -> pd.DataFrame:
    """Convert list of FileMetadata to a pandas DataFrame."""
    rows = [m.to_dict() for m in results]
    return pd.DataFrame(rows)


def create_output_dir(output_folder: str = "~") -> Path:
    """Create timestamped output directory: fs_hunter_YYYYMMDD_HHMMSS/"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_folder).expanduser() / f"fs_hunter_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def write_results(df: pd.DataFrame, out_dir: Path, fmt: str = "jsonl") -> Path:
    """Write scan results DataFrame in chosen format."""
    if fmt == "jsonl":
        out_file = out_dir / "results.jsonl"
        df.to_json(out_file, orient="records", lines=True)
    else:
        out_file = out_dir / "results.csv"
        df.to_csv(out_file, index=False)
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
