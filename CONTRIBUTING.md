# Contributing to fs-hunter

Thank you for your interest in contributing to fs-hunter!

## Getting Started

See the [README Setup section](README.md#setup) for clone, install, and configuration instructions.

## Making Changes

### 1. Create a branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Development guidelines

**Code style:**
- Python 3.10+ with `from __future__ import annotations`
- Flat module structure — no packages, all `.py` files at root level
- Use type hints for function signatures
- Keep functions focused and single-purpose

**Key patterns to follow:**
- **Two-phase metadata**: `extract_metadata_stat()` (cheap) then `enrich_metadata()` (expensive) — only enrich files that pass all filters
- **Three-tier filter cascade**: Tier 0 (free string checks) → Tier 1 (stat call) → Tier 2 (file I/O) — short-circuit early to avoid unnecessary work
- **Generator pipeline**: scanner yields `FileMetadata` lazily, results collected only for output
- **Auto-detecting input**: `_parse_input()` checks `.txt` extension → read lines, else split on comma

**Dependencies:** only add new packages if absolutely necessary. Discuss in an issue first.

### 3. What to update

| If you change... | Also update... |
|---|---|
| CLI flags or subcommands | `README.md` (CLI reference tables, examples) |
| Architecture or module roles | `CLAUDE.md` |
| Environment variables | `README.md`, `.env.example` |
| Output file structure | `README.md` (output section, mermaid diagrams) |

### 4. Test your changes

```bash
# Verify all subcommands load
python main.py --help
python main.py scan --help
python main.py delta --help
python main.py compare --help

# Test scan
python main.py scan -d /some/test/dir --lookback 7D -v

# Test delta
python main.py delta test_manifest.csv -v

# Test compare
python main.py compare --source-prefix /dir1 --target-prefix /dir2 --subdirs sub1,sub2 -v
```

### 5. Commit and push

```bash
git add <files>
git commit -m "type: short description"
git push origin feature/your-feature-name
```

**Commit prefixes:**

| Prefix | Use for |
|---|---|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `refactor:` | Code restructuring (no behavior change) |
| `docs:` | Documentation only |
| `perf:` | Performance improvement |

## Reporting Issues

When reporting a bug, include:

- Command you ran (with all flags)
- Expected vs actual behavior
- Error output (if any)
- Python version (`python --version`)
- OS (Linux/macOS/Windows)
