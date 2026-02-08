# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fs-hunter is a Python CLI tool that scans directories, extracts extended file metadata (name, size, checksums, MIME type, owner, permissions, dates), applies filters, and outputs results as a table or CSV. It also supports comparing two directory trees to find added/removed files. Flat module structure (no packages).

## Running

```bash
# Activate venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Unix

# Install dependencies
pip install -r requirements.txt

# Scan command
python main.py scan -d /data --lookback 7D -v
python main.py scan -d /dir1,/dir2 --file-pattern glob "*.csv"

# Delta command (CSV manifest)
python main.py delta manifest.csv --lookback 7D -v

# Compare command
python main.py compare --source-prefix /data/v1 --target-prefix /data/v2 --subdirs 20260206,20260207 -v
```

## Architecture

Three subcommands (`scan`, `delta`, and `compare`) sharing the same scan pipeline.

- **main.py** — typer CLI with `scan` + `delta` + `compare` subcommands, wires the pipeline
- **scanner.py** — `find`-based file discovery + parallel enrichment batches via ThreadPoolExecutor
- **metadata.py** — `FileMetadata` dataclass (11 fields) + `extract_metadata_stat()` / `enrich_metadata()`
- **filters.py** — `build_filter_chain()` returns a combined predicate `Callable[[FileMetadata], bool]`
- **formatters.py** — `parse_date()`, `parse_time()`, `parse_duration()` smart parsers
- **utils.py** — Delta CSV parsing, DataFrame I/O, output writing, metrics generation
- **compare.py** — `compute_delta()`, `write_compare_summary()`, `write_delta_metrics()`

## Key Design Decisions

- **Dependencies**: `typer`, `rich`, `pandas`, `python-dotenv`
- **`FileMetadata` is a dataclass** so `to_dict()` feeds CSV/JSONL seamlessly
- **Two-phase metadata**: cheap stat-only first, expensive owner/MIME only for filter-passing files
- **Generator pipeline**: scanner yields lazily, results collected only for display
- **Auto-detecting input**: `_parse_input()` checks if value ends with `.txt` → read lines, else split on comma
- **Compare uses prefix + subdirs/files**: builds full paths as `prefix/entry` for source and target
