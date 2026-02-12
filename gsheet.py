from __future__ import annotations

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from loguru import logger

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def append_to_sheet(df: pd.DataFrame, sheet_id: str, key_path: str) -> int:
    """Append DataFrame rows to the first worksheet of a Google Sheet.

    If the sheet is empty, writes the header row first.
    Returns the number of rows appended.
    """
    creds = Credentials.from_service_account_file(key_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1

    # Check if sheet is empty → write header
    existing = worksheet.get_all_values()
    if not existing:
        header = df.columns.tolist()
        worksheet.append_row(header, value_input_option="RAW")
        logger.debug("gsheet header written | columns={}", len(header))

    # Convert DataFrame rows to list of lists (all strings for Sheets)
    rows = df.astype(str).values.tolist()
    worksheet.append_rows(rows, value_input_option="RAW")

    logger.info("gsheet append complete | rows={} sheet_id={}", len(rows), sheet_id)
    return len(rows)
