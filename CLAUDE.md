# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fs-hunter is a Python CLI tool that scans directories, extracts extended file metadata (name, size, checksums, MIME type, owner, permissions, dates), applies filters, and outputs results as a table or CSV. Designed for Windows with a flat module structure (no packages).

## Running

```bash
# Activate venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Unix

# Install dependencies
pip install -r requirements.txt

# Run the tool
python main.py <dir1> [dir2 ...] [options]
python main.py . --ext .py
python main.py ./src --min-size 1KB --after 2024-01-01 --output results.csv
```

## Architecture

Data flows as a pipeline: `main.py` orchestrates scanner -> metadata -> filters -> formatters.

- **main.py** — argparse CLI entry point, wires the pipeline together
- **scanner.py** — Generator-based directory walking via pathlib; yields `(file_path, base_dir)` tuples lazily
- **metadata.py** — `FileMetadata` dataclass (13 fields) + `extract_metadata()`. Uses ctypes/advapi32 for Windows file owner, single-pass MD5+SHA256 checksums
- **filters.py** — `build_filter_chain()` returns a combined predicate `Callable[[FileMetadata], bool]` from CLI filter args
- **formatters.py** — `print_table()` (tabulate grid) and `export_csv()` (csv.DictWriter). Consumes `FileMetadata.asdict()`
- **utils.py** — Size parsing/formatting ("1KB" <-> 1024) and date parsing/formatting

## Key Design Decisions

- **Only external dependency is `tabulate`** — everything else is stdlib
- **`FileMetadata` is a dataclass** so `asdict()` feeds CSV/future Google Sheets seamlessly
- **Windows-specific**: file owner uses ctypes advapi32.dll directly (no subprocess)
- **Generator pipeline**: scanner yields lazily, results collected only for display
