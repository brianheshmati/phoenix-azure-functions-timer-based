import os
import json
import logging
import requests
import azure.functions as func
from typing import Dict, Any, List

# ================================================================
# CONFIGURATION
# ================================================================

SS_API_BASE = "https://api.smartsheet.com/2.0"
SMARTSHEET_TOKEN = os.environ["SMARTSHEET_ACCESS_TOKEN"]

# Hardcoded source sheet and columns
SOURCE_SHEET_ID = 639499383033732
SRC_COL_TANK = 6122430917201796
SRC_COL_CITY = 2744731196673924
SRC_COL_STATE = 7248330824044420

# Destination sheet mappings loaded from environment variable
DEST_SHEETS = json.loads(os.environ["DEST_SHEETS_JSON"])

logging.info(f"🔧 Loaded {len(DEST_SHEETS)} destination sheets from environment")
logging.debug(f"🔧 DEST_SHEETS: {DEST_SHEETS}")

# Optional DRY_RUN flag (set in Azure App Settings)
DRY_RUN = os.getenv("DRY_RUN_MISSING_PROJECT", "false").lower() == "true"

HEADERS = {
    "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
    "Content-Type": "application/json"
}

MAX_BATCH = 500  # Smartsheet bulk update limit per request

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def get_all_rows(sheet_id: int) -> List[Dict[str, Any]]:
    """Fetch all rows from a Smartsheet sheet (bulk GET)."""
    url = f"{SS_API_BASE}/sheets/{sheet_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    return data.get("rows", [])


def extract_key(row: Dict[str, Any], tank_col: int, city_col: int, state_col: int) -> str:
    """Return normalized composite key tank|city|state."""
    cells = {c["columnId"]: str(c.get("value") or "").strip().lower() for c in row.get("cells", [])}
    return f"{cells.get(tank_col)}|{cells.get(city_col)}|{cells.get(state_col)}"


def bulk_update(sheet_id: int, updates: List[Dict[str, Any]]) -> int:
    """Bulk PUT updates to Smartsheet, chunked to 500 rows max."""
    total = 0
    for i in range(0, len(updates), MAX_BATCH):
        chunk = updates[i:i + MAX_BATCH]
        url = f"{SS_API_BASE}/sheets/{sheet_id}/rows"
        if DRY_RUN:
            logging.info(f"🟡 [DRY RUN] Would update {len(chunk)} rows in sheet {sheet_id}")
            total += len(chunk)
            continue

        resp = requests.put(url, headers=HEADERS, data=json.dumps(chunk))
        resp.raise_for_status()
        total += len(chunk)
    return total


# ================================================================
# MAIN FUNCTION
# ================================================================

def main(mytimer: func.TimerRequest) -> None:
    mode = "DRY RUN" if DRY_RUN else "LIVE RUN"
    logging.info(f"⏱️ Starting Project Missing check ({mode}, every 1 min)...")

    try:
        # 1️⃣ Load source keys
        src_rows = get_all_rows(SOURCE_SHEET_ID)
        src_keys = {
            extract_key(r, SRC_COL_TANK, SRC_COL_CITY, SRC_COL_STATE)
            for r in src_rows
            if extract_key(r, SRC_COL_TANK, SRC_COL_CITY, SRC_COL_STATE) != "||"
        }
        logging.info(f"✅ Loaded {len(src_keys)} source keys from Sheet {SOURCE_SHEET_ID}")

        total_updates = 0
        report = []

        # 2️⃣ Loop through destination sheets
        for dest in DEST_SHEETS:
            sid = dest["sheet_id"]
            cols = dest["cols"]
            logging.info(f"➡️ Checking destination sheet {sid}...")

            dest_rows = get_all_rows(sid)
            updates = []

            for row in dest_rows:
                key = extract_key(row, cols["tank"], cols["city"], cols["state"])
                if not key or key == "||":
                    continue
                if key not in src_keys:
                    updates.append({
                        "id": row["id"],
                        "cells": [{"columnId": cols["missing"], "value": True}]
                    })

            if updates:
                count = bulk_update(sid, updates)
                total_updates += count
                log_msg = f"✅ Sheet {sid}: {count} rows {'would be' if DRY_RUN else 'were'} marked Project Missing"
                report.append(log_msg)
                logging.info(log_msg)
            else:
                report.append(f"✔️ Sheet {sid}: No missing rows found")
                logging.info(f"✔️ Sheet {sid}: No missing rows found")

        summary = f"🏁 Completed {mode}. Total rows {'to update' if DRY_RUN else 'updated'}: {total_updates}"
        logging.info(summary)
        for line in report:
            logging.info(line)

    except Exception as e:
        logging.exception(f"❌ Error during scheduled Project Missing check: {e}")
