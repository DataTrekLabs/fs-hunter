# GEMINI.md

This file provides context and guidelines for the **fs-hunter** project.

## Project Overview

**fs-hunter** is a Python CLI tool designed to scan directories, extract comprehensive file metadata (size, dates, ownership, etc.), filter the results, and export the data to targets like Google Sheets.

## Technical Stack

- **Language:** Python 3.9+
- **CLI Framework:** [Typer](https://typer.tiangolo.com/)
- **Terminal UI:** [Rich](https://rich.readthedocs.io/)
- **Data Manipulation:** [Pandas](https://pandas.pydata.org/)
- **Integration:** Google Sheets (via `gspread` / `google-auth` - *to be implemented*)

## Project Structure

The project currently follows a flat structure.

- **`main.py`**: The entry point for the CLI application.
- **`requirements.txt`**: Python dependencies.
- **`.gitignore`**: Git ignore rules.

### Intended Modules (To Be Implemented)

- **Scanner**: Module to walk directory trees and extract file attributes.
- **Filters**: Logic to filter files based on patterns, size, date, etc.
- **Exporter**: Logic to handle data export (CSV, Google Sheets).

## Setup & Running

1.  **Environment Setup**:
    ```bash
    # Create virtual environment
    python -m venv venv
    
    # Activate (Windows)
    .\venv\Scripts\activate
    
    # Install dependencies
    pip install -r requirements.txt
    ```

2.  **Running the Tool**:
    ```bash
    python main.py [COMMAND] [ARGS]
    ```

## Development Guidelines

- **Dependencies**: Manage via `pip` and `requirements.txt`. Ensure compatibility with Python 3.9.
- **Style**: Follow PEP 8. Use `typer` for all CLI commands.
- **Windows Support**: The tool is primarily being developed on/for Windows (`win32`). Ensure file path handling (e.g., `pathlib`) and metadata extraction (e.g., file owners) works on Windows.

## Notes

- The project previously considered a "stdlib-only" approach (referenced in `CLAUDE.md`), but has shifted to using modern libraries like `Typer`, `Rich`, and `Pandas` for better developer experience and capabilities.
- **Immediate Goal**: Implement the scanner and Google Sheets integration.
