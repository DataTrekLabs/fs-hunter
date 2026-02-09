# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fs-hunter is a Python CLI tool that scans directories, extracts extended file metadata (name, size, MD5 checksum, MIME type, owner, permissions, dates), applies filters, and outputs results as CSV or JSONL. It supports comparing two directory trees to find added/removed files and re-scanning from delta CSV manifests. Flat module structure (no packages). **v1.0.0 released** under MIT License.

## Running

```bash
# Activate venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Unix

# Install dependencies
pip install -r requirements.txt

# Scan directories
python main.py scan -d /data --lookback 7D -v
python main.py scan -d /dir1,/dir2 --file-pattern glob "*.csv" -w 30
python main.py scan -d paths.txt -w 30 -v --lookback 1D
python main.py scan -f file1.parq,file2.parq

# Scan all file types (default is *.parq*)
python main.py scan -d /data --file-pattern glob "*" -v

# Delta (re-scan from CSV manifest)
python main.py delta manifest.csv --lookback 7D -v

# Compare two directory trees
python main.py compare --source-prefix /data/v1 --target-prefix /data/v2 --subdirs 20260206,20260207 -v
```

## Architecture

Three subcommands (`scan`, `delta`, `compare`) sharing the same scan pipeline.

- **main.py** — Typer CLI with 3 subcommands, `_parse_input()` auto-detects `.txt` file vs comma-separated input
- **scanner.py** — `_run_find()` file discovery + `_enrich_batch()` parallel enrichment via ThreadPoolExecutor; `scan_directories()` for dir scanning, `process_file_list()` for file-mode
- **metadata.py** — `FileMetadata` dataclass (11 fields: name, extension, full_path, relative_path, size_bytes, ctime, mtime, permissions, owner, mime_type, md5) + `extract_metadata_stat()` / `enrich_metadata()` / `compute_hash()`
- **filters.py** — `build_filter_chain()` returns a combined predicate; `filter_by_time_range()`, `filter_unique()`
- **formatters.py** — `parse_date()`, `parse_time()`, `parse_duration()` smart parsers
- **utils.py** — `parse_delta_csv()`, `results_to_dataframe()`, `create_output_dir(subcommand)`, `write_results()`, `write_summary()`, `enrich_with_delta()`, `write_metrics()`
- **compare.py** — `compute_delta()`, `write_compare_summary()`, `write_delta_metrics()`

## Key Design Decisions

- **Dependencies**: `typer`, `rich`, `pandas`, `python-dotenv`
- **`FileMetadata` is a dataclass** so `to_dict()` feeds CSV/JSONL seamlessly
- **Two-phase metadata**: cheap `extract_metadata_stat()` first, expensive `enrich_metadata()` (owner/MIME) only for filter-passing files
- **Three-tier filter cascade**: Tier 0 (free string checks: name_regex, path_pattern) → Tier 1 (stat: date/time/size) → Tier 2 (expensive: owner + MIME enrichment)
- **MD5 hash by default**: computed for every file via `hashlib.md5()` in 8KB chunks; disable with `--off-hash` or `ENABLE_HASH=false`
- **Generator pipeline**: scanner yields `FileMetadata` lazily, results collected only for output
- **Auto-detecting input**: `_parse_input()` checks if value ends with `.txt` → read lines, else split on comma
- **Compare uses prefix + subdirs/files**: builds full paths as `prefix/entry` for source and target
- **Output organized by subcommand**: `fs_hunter/{scan,delta,compare}/YYYYMMDD_HHMMSS/`
- **`--file-pattern` takes two values**: type (`glob` or `regex`) and pattern (default: `glob *.parq*`)

## CLI Quick Reference

### scan
| Flag | Default | Description |
|---|---|---|
| `-d`/`--dirs` | — | Directories (comma-separated or .txt) |
| `-f`/`--files` | — | Specific files (comma-separated or .txt) |
| `--lookback` | `1H` | Relative duration (e.g. 7D, 2H, 1D12H30M) |
| `--file-pattern` | `glob *.parq*` | Two values: type pattern |
| `--off-hash` | false | Disable MD5 computation |
| `-w`/`--workers` | 4 | Parallel threads |
| `-v`/`--verbose` | false | Show progress |
| `-o` | `FS_HUNTER_OUTPUT_DIR` | Output folder |

### delta
- Positional arg: path to delta CSV file
- Same filter/output flags as scan

### compare
| Flag | Description |
|---|---|
| `--source-prefix` | Source base path (required) |
| `--target-prefix` | Target base path (required) |
| `--subdirs` | Subdirectory names (comma-sep or .txt) |
| `--files` | File paths relative to prefix (comma-sep or .txt) |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FS_HUNTER_OUTPUT_DIR` | `~` | Output folder |
| `ENABLE_HASH` | `true` | Compute MD5 hash for each file |

## Output Structure

```
fs_hunter/
├── scan/YYYYMMDD_HHMMSS/
│   ├── results.csv
│   ├── _summary.csv
│   └── metrics.json
├── delta/YYYYMMDD_HHMMSS/
│   ├── results.csv
│   ├── _summary.csv
│   └── metrics.json
└── compare/YYYYMMDD_HHMMSS/
    ├── s_result.csv
    ├── t_result.csv
    ├── _summary.csv
    ├── delta.csv
    ├── delta_metrics.json
    └── metrics.json
```

## Server Deployment

- Server: `ip-10-74-229-135` (user: `bthanujan.mahendran`)
- `.env`: `FS_HUNTER_OUTPUT_DIR=/sf/home/bthanujan.mahendran/output`
- `path.txt`: 24 dirs across factset, bloomberg, cw, apptopia, lseg_refinitiv
- Tested: `python main.py scan -d path.txt -w 30 -v` → 145 files found
- Git remote: `codewilling` (server pulls from here, proxy blocks GitHub)
