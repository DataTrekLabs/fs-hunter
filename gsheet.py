from __future__ import annotations

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from loguru import logger

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_worksheet(sheet_id: str, key_path: str):
    """Authenticate and return the first worksheet of a Google Sheet."""
    creds = Credentials.from_service_account_file(key_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.sheet1


def check_connection(sheet_id: str, key_path: str) -> None:
    """Validate Google Sheet connectivity before scanning.

    Raises on auth failure, bad sheet ID, or permission issues.
    """
    ws = _get_worksheet(sheet_id, key_path)
    ws.row_count  # force an API call to confirm access
    logger.info("gsheet connection OK | sheet_id={}", sheet_id)


def append_to_sheet(df: pd.DataFrame, sheet_id: str, key_path: str) -> int:
    """Append DataFrame rows to the first worksheet of a Google Sheet.

    If the sheet is empty, writes the header row first.
    Returns the number of rows appended.
    """
    ws = _get_worksheet(sheet_id, key_path)

    # Check if sheet is empty → write header
    existing = ws.get_all_values()
    if not existing:
        header = df.columns.tolist()
        ws.append_row(header, value_input_option="RAW")
        logger.debug("gsheet header written | columns={}", len(header))

    # Convert DataFrame rows to list of lists (all strings for Sheets)
    rows = df.astype(str).values.tolist()
    ws.append_rows(rows, value_input_option="RAW")

    logger.info("gsheet append complete | rows={} sheet_id={}", len(rows), sheet_id)
    return len(rows)
