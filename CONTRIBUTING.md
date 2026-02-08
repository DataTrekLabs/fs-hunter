# Contributing to fs-hunter

Thank you for your interest in contributing to fs-hunter! This guide will help you get started.

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/DataTrekLabs/fs-hunter.git
cd fs-hunter
```

### 2. Set up the development environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Verify setup

```bash
python main.py --help
python main.py scan --help
python main.py delta --help
python main.py compare --help
```

## Project Structure

```
fs-hunter/
  main.py         CLI entry point (scan, delta, compare subcommands)
  scanner.py      Multi-threaded file discovery and enrichment
  metadata.py     FileMetadata dataclass and extraction functions
  filters.py      Filter predicates (date, time, size, pattern, unique)
  formatters.py   Smart date/time/duration parsers
  utils.py        DataFrame I/O, output writing, metrics generation
  compare.py      Delta computation and compare output
```

## Development Guidelines

### Code Style

- Python 3.10+ with `from __future__ import annotations`
- Flat module structure — no packages, all `.py` files at root level
- Use type hints for function signatures
- Keep functions focused and single-purpose

### Key Patterns

- **Two-phase metadata**: `extract_metadata_stat()` (cheap) then `enrich_metadata()` (expensive) — only enrich files that pass all filters
- **Three-tier filter cascade**: Tier 0 (free string checks) → Tier 1 (stat call) → Tier 2 (file I/O) — short-circuit early
- **Generator pipeline**: scanner yields `FileMetadata` lazily, results collected only for output
- **Auto-detecting input**: `_parse_input()` checks `.txt` extension → read lines, else split on comma

### Dependencies

Only add new dependencies if absolutely necessary. Current stack:

| Package | Purpose |
|---|---|
| `typer` | CLI framework |
| `rich` | Terminal formatting and progress bars |
| `pandas` | DataFrame operations and CSV/JSONL output |
| `python-magic` | MIME type detection |
| `python-dotenv` | Environment configuration |

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FS_HUNTER_OUTPUT_DIR` | `~` | Output folder |
| `ENABLE_HASH` | `true` | Compute MD5 hash for each file |

## Making Changes

### 1. Create a branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make your changes

- Keep changes focused on a single feature or fix
- Test locally before committing
- Update `README.md` if you change CLI flags or behavior
- Update `CLAUDE.md` if you change architecture or running instructions

### 3. Test your changes

```bash
# Test scan command
python main.py scan -d /some/test/dir --lookback 7D -v

# Test delta command
python main.py delta test_manifest.csv -v

# Test compare command
python main.py compare --source-prefix /dir1 --target-prefix /dir2 --subdirs subdir1,subdir2 -v

# Verify help output
python main.py --help
python main.py scan --help
python main.py delta --help
python main.py compare --help
```

### 4. Commit

```bash
git add <files>
git commit -m "type: short description"
```

**Commit message prefixes:**

| Prefix | Use for |
|---|---|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `refactor:` | Code restructuring (no behavior change) |
| `docs:` | Documentation only |
| `perf:` | Performance improvement |

### 5. Push

```bash
git push origin feature/your-feature-name
```

## Reporting Issues

When reporting a bug, include:

- Command you ran (with flags)
- Expected behavior
- Actual behavior
- Error output (if any)
- Python version (`python --version`)
- OS (Linux/macOS/Windows)

## Questions

If you have questions about the codebase or how to implement something, open an issue or reach out to the maintainers.
