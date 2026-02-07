# fs-hunter

A Python CLI tool for scanning directories, extracting extended file metadata, applying powerful filters, and exporting structured results. Built for data engineering workflows where you need to inventory, deduplicate, and catalog files across large directory trees.

## Features

- **Extended metadata extraction** — name, extension, size, created/modified dates, permissions, owner, MIME type, SHA256 checksum
- **Smart filtering** — date ranges, time-of-day windows, file size, regex on filenames, glob on paths, deduplication
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
venv\Scripts\activate      # Windows
source venv/bin/activate   # Linux/macOS

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

- `typer` — CLI framework
- `rich` — terminal formatting and progress bars
- `pandas` — DataFrame operations and CSV/JSONL output

## Usage

```bash
python main.py [OPTIONS]
```

You must provide exactly one input mode: `--base-path`, `--paths`, `--path-list`, or `--delta-csv`.

### Input Modes

#### Single directory

```bash
python main.py --base-path /data/warehouse
```

#### Multiple directories

```bash
python main.py --paths /data/raw --paths /data/derived --paths /data/archive
```

#### Path list file

```bash
python main.py --path-list paths.txt
```

Where `paths.txt` contains one directory per line:

```
/data/raw
/data/derived
/data/archive
```

#### Delta CSV

```bash
python main.py --delta-csv manifest.csv
```

The CSV must contain these columns: `Directory`, `Dataset Repo`, `SF Table`, `Filename`. The tool extracts unique directories for scanning and enriches output with the dataset/table metadata.

### Date and Time Filters

#### Date range (`--scan-start`, `--scan-end`)

Filter files by last modified date. Supports partial dates with smart auto-complete:

| Input | Interpreted as |
|---|---|
| `2024` | `2024-01-01 00:00:00` |
| `2024-06` | `2024-06-01 00:00:00` |
| `2024-06-15` | `2024-06-15 00:00:00` |
| `2024-06-15 14` | `2024-06-15 14:00:00` |
| `2024-06-15 14:30` | `2024-06-15 14:30:00` |
| `2024-06-15 14:30:45` | `2024-06-15 14:30:45` |

**Defaults:** `--scan-start` = yesterday 00:00:00, `--scan-end` = now.

```bash
# Files modified in 2024
python main.py --base-path /data --scan-start 2024 --scan-end 2025

# Files modified in the last week
python main.py --base-path /data --scan-start "2026-02-01" --scan-end "2026-02-07 23:59:59"
```

#### Time-of-day window (`--day-start`, `--day-end`)

Filter by time-of-day regardless of date. Supports midnight wrapping.

```bash
# Files modified during business hours only
python main.py --base-path /data --day-start 09:00 --day-end 17:00

# Files modified overnight (wraps midnight)
python main.py --base-path /data --day-start 22:00 --day-end 06:00
```

**Defaults:** `--day-start` = `00:00:00`, `--day-end` = `23:59:59` (all times).

### File Filters

#### Filename regex (`--file-pattern`)

Regex pattern matched against the filename.

```bash
# Default: parquet files
python main.py --base-path /data --file-pattern ".*\.parq(uet)?$"

# CSV files
python main.py --base-path /data --file-pattern ".*\.csv$"

# Files starting with "report_"
python main.py --base-path /data --file-pattern "^report_"
```

**Default:** `.*\.parq(uet)?$` (parquet files).

#### Path glob (`--path-pattern`)

Glob pattern matched against the file's relative path from the scan root.

```bash
# Only files under "derived/" subdirectories
python main.py --base-path /data --path-pattern "derived/*"

# Parquet files in any "daily" folder
python main.py --base-path /data --path-pattern "*/daily/*.parq"
```

#### File size (`--min-size`, `--max-size`)

Filter by file size in bytes.

```bash
# Files larger than 1MB
python main.py --base-path /data --min-size 1048576

# Files between 1KB and 100MB
python main.py --base-path /data --min-size 1024 --max-size 104857600
```

### Deduplication (`--unique`)

Controls how duplicate files are identified. Only the first occurrence is kept.

| Mode | Behavior |
|---|---|
| `namepattern` | Groups files by structural name pattern. `report_20250601.csv` and `report_20240115.csv` both match `report_\d{4}\d{2}\d{2}\.csv` — only one is kept. |
| `hash` | Groups by SHA256 checksum — identical content = duplicate. |

**Default:** `namepattern`.

```bash
# Deduplicate by content hash
python main.py --base-path /data --unique hash

# Deduplicate by name structure (default)
python main.py --base-path /data --unique namepattern
```

### Output Options

#### Output format (`--output-format`)

```bash
# JSONL output (default)
python main.py --base-path /data --output-format jsonl

# CSV output
python main.py --base-path /data --output-format csv
```

#### Output folder (`-o`)

The tool creates a timestamped directory `fs_hunter_YYYYMMDD_HHMMSS/` inside the specified folder.

```bash
# Output to home directory (default)
python main.py --base-path /data

# Output to specific folder
python main.py --base-path /data -o /reports
```

**Output structure:**

```
/reports/fs_hunter_20260207_143022/
  results.jsonl    # or results.csv
  _summary.csv     # scan statistics
```

The `_summary.csv` contains: scan time, start/end range, targets scanned, total files, total size, unique extensions.

### Performance Options

#### Workers (`--workers`)

Number of parallel threads for scanning multiple directories.

```bash
# Use 8 threads
python main.py --paths /data/a --paths /data/b --paths /data/c --workers 8
```

**Default:** 4.

#### Verbose mode (`-v`)

Shows real-time Rich progress bars per directory with file counts.

```bash
python main.py --base-path /data -v
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
| `sha256` | SHA256 checksum (empty string on permission error) |

When using `--delta-csv`, three additional columns are added:

| Field | Description |
|---|---|
| `dataset_repo` | Dataset repository from the CSV manifest |
| `sf_table` | Snowflake table name from the CSV manifest |
| `filename_pattern` | Filename pattern from the CSV manifest |

## Filter Execution Order

Filters are applied in this order during scanning for optimal performance:

1. **Date range** — skip files outside the modified date window
2. **Time-of-day** — skip files outside the time window
3. **Size range** — skip files outside the size bounds
4. **Path pattern** — glob match on relative path
5. **Name pattern** — regex match on filename
6. **Unique filter** — deduplicate (applied last, after all other filters)

## Architecture

```
fs-hunter/
  main.py         # CLI entry point — typer app, wires the pipeline
  scanner.py      # Multi-threaded recursive directory walking
  metadata.py     # FileMetadata/DeltaInfo dataclasses + extraction
  filters.py      # Filter functions (date, time, size, pattern, unique)
  formatters.py   # Smart date/time/duration parsers
  utils.py        # Delta CSV, DataFrame I/O, output writing, enrichment
```

**Data pipeline:** `main.py` orchestrates: scan directories -> extract metadata -> apply filters -> build DataFrame -> enrich with delta (optional) -> write output files.

## Examples

```bash
# Scan /data for parquet files modified since yesterday, output as JSONL
python main.py --base-path /data

# Scan multiple dirs for CSV files modified in 2024, deduplicate by hash
python main.py --paths /data/raw --paths /data/derived \
  --file-pattern ".*\.csv$" \
  --scan-start 2024 --scan-end 2025 \
  --unique hash

# Scan from delta CSV manifest, verbose progress, CSV output
python main.py --delta-csv manifest.csv -v --output-format csv -o /reports

# Large files only, business hours, 8 threads
python main.py --base-path /warehouse \
  --min-size 1048576 \
  --day-start 09:00 --day-end 17:00 \
  --workers 8 -v
```

## License

Internal use.
