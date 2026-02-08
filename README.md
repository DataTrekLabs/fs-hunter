# fs-hunter

A Python CLI tool for scanning directories, extracting extended file metadata, applying powerful filters, and exporting structured results. Built for data engineering workflows where you need to inventory, deduplicate, and catalog files across large directory trees.

## Features

- **Extended metadata extraction** — name, extension, size, created/modified dates, permissions, owner, MIME type, SHA256 checksum
- **Smart filtering** — lookback duration, date ranges, time-of-day windows, file size, regex on filenames, glob on paths, deduplication
- **4 input modes** — single path, multiple paths, path list file, or delta CSV
- **Delta CSV enrichment** — map scanned files back to dataset/table metadata from a CSV manifest
- **Multi-threaded scanning** — parallel directory walks with configurable worker count
- **Rich progress bars** — real-time scan progress per directory in verbose mode
- **Flexible output** — JSONL (default) or CSV, with automatic summary stats

## Installation

```bash
# Clone the repository
git clone git@git.codewilling.com:alchmy/fs-hunter.git
cd fs-hunter

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

- `typer` — CLI framework
- `rich` — terminal formatting and progress bars
- `pandas` — DataFrame operations and CSV/JSONL output

## Quick Start

```bash
# Scan /data for parquet files modified in the last hour (all defaults)
python main.py scan --base-path /data

# Scan with 7-day lookback
python main.py scan --base-path /data --lookback 7D

# Scan with verbose progress and CSV output
python main.py scan --base-path /data --lookback 1D -v --output-format csv

# Compare two directories (diff added/removed files)
python main.py compare --source /data/baseline --target /data/current --lookback 7D -v
```

## Commands

fs-hunter has two subcommands:

| Command | Description |
|---|---|
| `scan` | Scan directories and extract file metadata with filters |
| `compare` | Scan two directories with the same filters, then diff results |

```bash
python main.py scan [OPTIONS]
python main.py compare [OPTIONS]
```

## Scan Command

You must provide exactly one input mode: `--base-path`, `--paths`, `--path-list`, `--files`, `--file-list`, or `--delta-csv`.

## Scan CLI Reference

| Flag | Short | Default | Description |
|---|---|---|---|
| `--base-path` | | | Single directory to scan |
| `--paths` | | | Multiple directories (comma-separated) |
| `--path-list` | | | Text file with paths (one per line) |
| `--files` | | | Specific file paths (comma-separated) |
| `--file-list` | | | Text file with specific file paths (one per line) |
| `--delta-csv` | | | CSV manifest with Directory column |
| `--lookback` | | `1H` | Relative duration: `7D`, `2H`, `1D12H30M` |
| `--scan-start` | | | Absolute date range start (overrides lookback) |
| `--scan-end` | | | Absolute date range end (overrides lookback) |
| `--day-start` | | `00:00:00` | Time-of-day filter start |
| `--day-end` | | `23:59:59` | Time-of-day filter end |
| `--file-pattern` | | `glob *.parq*` | Pattern on filename: `glob\|regex PATTERN` |
| `--path-pattern` | | | Pattern on relative path: `glob\|regex PATTERN` |
| `--min-size` | | | Minimum file size in bytes |
| `--max-size` | | | Maximum file size in bytes |
| `--unique` | | `namepattern` | Dedup mode: `hash` or `namepattern` |
| `--output-format` | | `csv` | Output format: `csv` or `jsonl` |
| | `-o` | `~` | Output folder |
| `--workers` | `-w` | `4` | Parallel scan threads |
| `--verbose` | `-v` | `false` | Show Rich progress bars |
| `--no-metrics` | | `false` | Skip metrics.json generation |
| `--metrics-interval` | | `30` | Time bucket size in minutes |

## Input Modes

Exactly one must be provided. They are mutually exclusive.

### Single directory (`--base-path`)

```bash
python main.py scan --base-path /data/warehouse
```

### Multiple directories (`--paths`)

```bash
python main.py scan --paths "/data/raw,/data/derived,/data/archive"
```

### Path list file (`--path-list`)

```bash
python main.py scan --path-list paths.txt
```

Where `paths.txt` contains one directory per line:

```
/data/raw
/data/derived
/data/archive
```

### Delta CSV (`--delta-csv`)

```bash
python main.py scan --delta-csv manifest.csv
```

The CSV must contain these columns: `Directory`, `Dataset Repo`, `SF Table`, `Filename`. The tool extracts unique directories for scanning and enriches output with the dataset/table metadata.

## Date and Time Filters

### Lookback duration (`--lookback`)

The **default** date filter. Scans files modified within the given duration from now.

```bash
# Last hour (default)
python main.py scan --base-path /data

# Last 7 days
python main.py scan --base-path /data --lookback 7D

# Last 2 hours
python main.py scan --base-path /data --lookback 2H

# Last 1 day, 12 hours, 30 minutes
python main.py scan --base-path /data --lookback 1D12H30M
```

**Supported units:** `D` (days), `H` (hours), `M` (minutes). Can be combined: `1D12H30M`.

**Default:** `1H` (last 1 hour).

### Date range (`--scan-start`, `--scan-end`)

Absolute date range filter. When either `--scan-start` or `--scan-end` is provided, it **overrides** `--lookback`.

Supports partial dates with smart auto-complete:

| Input | Interpreted as |
|---|---|
| `2024` | `2024-01-01 00:00:00` |
| `2024-06` | `2024-06-01 00:00:00` |
| `2024-06-15` | `2024-06-15 00:00:00` |
| `2024-06-15 14` | `2024-06-15 14:00:00` |
| `2024-06-15 14:30` | `2024-06-15 14:30:00` |
| `2024-06-15 14:30:45` | `2024-06-15 14:30:45` |

When using date range mode: `--scan-start` defaults to yesterday 00:00:00, `--scan-end` defaults to now.

```bash
# Files modified in 2024
python main.py scan --base-path /data --scan-start 2024 --scan-end 2025

# Files modified since a specific date
python main.py scan --base-path /data --scan-start "2026-02-01"

# Files modified in a specific window
python main.py scan --base-path /data --scan-start "2026-02-01" --scan-end "2026-02-07 23:59:59"
```

### Time-of-day window (`--day-start`, `--day-end`)

Filter by time-of-day regardless of date. Works alongside both lookback and date range. Supports midnight wrapping.

```bash
# Files modified during business hours only
python main.py scan --base-path /data --day-start 09:00 --day-end 17:00

# Files modified overnight (wraps midnight)
python main.py scan --base-path /data --day-start 22:00 --day-end 06:00
```

Supports partial time input: `14` becomes `14:00:00`, `14:30` becomes `14:30:00`.

**Defaults:** `--day-start` = `00:00:00`, `--day-end` = `23:59:59` (all times).

## File Filters

### Filename regex (`--file-pattern`)

Regex pattern matched against the filename.

```bash
# Default: parquet files
python main.py scan --base-path /data --file-pattern ".*\.parq(uet)?$"

# CSV files
python main.py scan --base-path /data --file-pattern ".*\.csv$"

# Files starting with "report_"
python main.py scan --base-path /data --file-pattern "^report_"
```

**Default:** `.*\.parq(uet)?$` (parquet files).

### Path glob (`--path-pattern`)

Glob pattern matched against the file's relative path from the scan root.

```bash
# Only files under "derived/" subdirectories
python main.py scan --base-path /data --path-pattern "derived/*"

# Parquet files in any "daily" folder
python main.py scan --base-path /data --path-pattern "*/daily/*.parq"
```

### File size (`--min-size`, `--max-size`)

Filter by file size in bytes.

```bash
# Files larger than 1MB
python main.py scan --base-path /data --min-size 1048576

# Files between 1KB and 100MB
python main.py scan --base-path /data --min-size 1024 --max-size 104857600
```

## Deduplication (`--unique`)

Controls how duplicate files are identified. Only the first occurrence is kept.

| Mode | Behavior |
|---|---|
| `namepattern` | Groups files by structural name pattern. `report_20250601.csv` and `report_20240115.csv` both match `report_\d{4}\d{2}\d{2}\.csv` — only one is kept. |
| `hash` | Groups by SHA256 checksum — identical content = duplicate. |

**Default:** `namepattern`.

```bash
# Deduplicate by content hash
python main.py scan --base-path /data --unique hash

# Deduplicate by name structure (default)
python main.py scan --base-path /data --unique namepattern
```

## Output

### Output format (`--output-format`)

```bash
# JSONL output (default)
python main.py scan --base-path /data --output-format jsonl

# CSV output
python main.py scan --base-path /data --output-format csv
```

### Output folder (`-o`)

The tool creates a timestamped directory `fs_hunter_YYYYMMDD_HHMMSS/` inside the specified folder.

```bash
# Output to home directory (default)
python main.py scan --base-path /data

# Output to specific folder
python main.py scan --base-path /data -o /reports
```

**Output structure:**

```
/reports/fs_hunter_20260207_143022/
  results.jsonl    # or results.csv
  _summary.csv     # scan statistics
  metrics.json     # scan performance and breakdown stats
```

The `_summary.csv` contains: scan time, start/end range, targets scanned, total files, total size, unique extensions.

### Metrics (`metrics.json`)

Generated by default alongside results. Contains scan performance stats and file breakdowns:

| Section | Contents |
|---|---|
| `scan_performance` | `total_matched`, `scan_duration_seconds` |
| `size_stats` | `total_bytes`, `avg_bytes`, `min_bytes`, `max_bytes` |
| `by_extension` | Count and total bytes per file extension |
| `by_directory` | Count and total bytes per parent directory |
| `time_buckets` | 24h split into N-minute intervals with count, total bytes, file list, peak bucket, and empty bucket count |

```bash
# Default: metrics.json with 30-minute buckets
python main.py scan --base-path /data --lookback 1D

# Custom 60-minute buckets
python main.py scan --base-path /data --lookback 1D --metrics-interval 60

# Skip metrics generation
python main.py scan --base-path /data --no-metrics
```

## Performance

### Workers (`--workers` / `-w`)

Number of parallel threads for scanning multiple directories.

```bash
# Use 8 threads
python main.py scan --paths /data/a --paths /data/b --paths /data/c -w 8
```

**Default:** 4.

### Verbose mode (`--verbose` / `-v`)

Shows real-time Rich progress bars per directory with scanned/matched file counts.

```bash
python main.py scan --base-path /data -v
```

## Metadata Fields

Each scanned file produces the following fields:

| Field | Description |
|---|---|
| `name` | Filename (e.g., `report_20250601.parq`) |
| `extension` | Full extension including compound (e.g., `.tar.gz`) |
| `full_path` | Absolute resolved path |
| `size_bytes` | File size in bytes |
| `created` | Creation timestamp (`YYYY-MM-DD HH:MM:SS`) |
| `modified` | Last modified timestamp (`YYYY-MM-DD HH:MM:SS`) |
| `permissions` | Unix-style permission string (e.g., `-rw-r--r--`) |
| `owner` | File owner (or `N/A` if unavailable) |
| `mime_type` | Detected MIME type (or `unknown`) |
| `sha256` | SHA256 checksum (only computed when `--unique hash`; empty otherwise) |

When using `--delta-csv`, three additional columns are enriched:

| Field | Description |
|---|---|
| `dataset_repo` | Dataset repository from the CSV manifest |
| `sf_table` | Snowflake table name from the CSV manifest |
| `filename_pattern` | Filename pattern from the CSV manifest |

## Filter Execution Order

Filters are applied in this order during scanning:

1. **Lookback / Date range** — skip files outside the modified date window
2. **Time-of-day** — skip files outside the time window
3. **Size range** — skip files outside the size bounds
4. **Path pattern** — glob match on relative path
5. **Name pattern** — regex match on filename
6. **SHA256 hash** — computed only for files that pass all above filters, and only when `--unique hash` is used
7. **Unique filter** — deduplicate (applied last, after all other filters)

## Compare Command

The `compare` command scans two directories with the same filter flags, then diffs the results to show added and removed files.

```bash
python main.py compare --source /data/baseline --target /data/current [OPTIONS]
```

### Compare CLI Reference

| Flag | Short | Default | Description |
|---|---|---|---|
| `--source` | | **(required)** | Source (baseline) directory path |
| `--target` | | **(required)** | Target (current) directory path |
| `--lookback` | | `1H` | Relative duration: `7D`, `2H`, `1D12H30M` |
| `--scan-start` | | | Absolute date range start (overrides lookback) |
| `--scan-end` | | | Absolute date range end (overrides lookback) |
| `--day-start` | | `00:00:00` | Time-of-day filter start |
| `--day-end` | | `23:59:59` | Time-of-day filter end |
| `--file-pattern` | | `glob *.parq*` | Pattern on filename: `glob\|regex PATTERN` |
| `--path-pattern` | | | Pattern on relative path: `glob\|regex PATTERN` |
| `--min-size` | | | Minimum file size in bytes |
| `--max-size` | | | Maximum file size in bytes |
| `--unique` | | `namepattern` | Dedup mode: `hash` or `namepattern` |
| | `-o` | `~` | Output folder |
| `--workers` | `-w` | `4` | Parallel scan threads |
| `--verbose` | `-v` | `false` | Show Rich progress bars |
| `--no-metrics` | | `false` | Skip metrics.json generation |
| `--metrics-interval` | | `30` | Time bucket size in minutes |

### Compare Output

```
fs_hunter/{YYYYMMDD_HHMMSS}/compare/
    _summary.csv         — source_dir, target_dir, file counts, added/removed/unchanged
    s_result.csv         — full source scan results
    t_result.csv         — full target scan results
    delta.csv            — added (+) and removed (-) files with change column
    delta_metrics.json   — breakdown by extension and change type, size totals
    metrics.json         — combined scan metrics (time buckets, size stats)
```

### Compare Examples

```bash
# Compare two directories for parquet file changes in the last 7 days
python main.py compare \
  --source /data/baseline \
  --target /data/current \
  --lookback 7D -v

# Compare with specific file pattern and 8 threads
python main.py compare \
  --source /data/v1 \
  --target /data/v2 \
  --file-pattern glob "*.csv" \
  -w 8 -v

# Compare with output to specific folder
python main.py compare \
  --source /data/old \
  --target /data/new \
  --lookback 1D \
  -o /reports
```

## Architecture

```
fs-hunter/
  main.py         # CLI entry point — typer app with scan + compare subcommands
  scanner.py      # Multi-threaded recursive directory walking
  metadata.py     # FileMetadata/DeltaInfo dataclasses + extraction
  filters.py      # Filter functions (date, time, size, pattern, unique, lookback)
  formatters.py   # Smart date/time/duration parsers
  utils.py        # Delta CSV, DataFrame I/O, output writing, enrichment
  compare.py      # Compare logic: compute_delta, write_compare_summary, write_delta_metrics
```

**Data pipeline:**
- **scan:** `main.py` orchestrates: scan directories -> extract metadata -> apply filters -> build DataFrame -> enrich with delta (optional) -> write output files.
- **compare:** `main.py` scans source + target with same filters -> builds DataFrames -> `compute_delta()` diffs on `full_path` -> writes s_result, t_result, delta, summary, and metrics.

## Examples

### Scan Examples

```bash
# Default: scan for parquet files modified in the last hour
python main.py scan --base-path /data

# Last 7 days, verbose progress
python main.py scan --base-path /data --lookback 7D -v

# Absolute date range for all of 2024
python main.py scan --base-path /data --scan-start 2024 --scan-end 2025

# Multiple dirs, CSV files, dedup by hash, 8 threads
python main.py scan --paths "/data/raw,/data/derived" \
  --file-pattern regex ".*\.csv$" \
  --lookback 30D \
  --unique hash \
  -w 8 -v

# Delta CSV manifest with enrichment, CSV output
python main.py scan --delta-csv manifest.csv -v --output-format csv -o /reports

# Large files only, business hours
python main.py scan --base-path /warehouse \
  --min-size 1048576 \
  --day-start 09:00 --day-end 17:00 \
  --lookback 7D -v

# Path glob filter for specific subdirectories
python main.py scan --base-path /data \
  --path-pattern glob "*/daily/*.parq" \
  --lookback 1D
```

### Compare Examples

```bash
# Compare two directories for changes in the last 7 days
python main.py compare \
  --source /data/baseline \
  --target /data/current \
  --lookback 7D -v

# Compare with custom file pattern
python main.py compare \
  --source /data/v1 \
  --target /data/v2 \
  --file-pattern glob "*.csv" \
  -w 8 -v -o /reports
```

## License

Internal use.
