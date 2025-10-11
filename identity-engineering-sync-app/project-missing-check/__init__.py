import os
import json
import time
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
SRC_COL_TANK = 3633417232797572
SRC_COL_CITY = 818667465691012
SRC_COL_STATE = 5322267093061508

# Destination sheet mappings loaded from environment variable
try:
    DEST_SHEETS = json.loads(os.environ["DEST_SHEETS_JSON"])
    logging.info(f"🔧 Loaded {len(DEST_SHEETS)} destination sheets from environment")
except Exception as e:
    logging.error(f"❌ Invalid DEST_SHEETS_JSON format: {e}")
    DEST_SHEETS = []

# Dry-run toggle
DRY_RUN = os.getenv("DRY_RUN_MISSING_PROJECT", "false").lower() == "true"

HEADERS = {
    "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
    "Content-Type": "application/json"
}

MAX_BATCH = 500   # Smartsheet bulk update limit per request
RETRY_DELAY = 3   # seconds to wait before retrying a 429 response

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def normalize_tank(value: Any) -> str:
    """Normalize tank number as integer-like string (e.g., 010 → 10, 1000.0 → 1000)."""
    if value is None or str(value).strip() == "":
        return ""
    try:
        return str(int(float(str(value).strip())))
    except ValueError:
        return str(value).strip().lower()


def extract_key(row: Dict[str, Any], tank_col: int, city_col: int, state_col: int) -> str:
    """Return normalized composite key tank|city|state, skipping incomplete rows."""
    cells = {c["columnId"]: c.get("value") for c in row.get("cells", [])}

    tank = normalize_tank(cells.get(tank_col))
    city = str(cells.get(city_col) or "").strip().lower()
    state = str(cells.get(state_col) or "").strip().lower()

    # 🚫 Skip rows missing any key component
    if not tank or not city or not state:
        logging.debug(
            f"⚠️ Skipping incomplete row: Tank={tank}, City={city}, State={state}"
        )
        return ""

    return f"{tank}|{city}|{state}"


def get_all_rows(sheet_id: int) -> List[Dict[str, Any]]:
    """Fetch all rows from a Smartsheet sheet (bulk GET)."""
    url = f"{SS_API_BASE}/sheets/{sheet_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    return data.get("rows", [])


def bulk_update(sheet_id: int, updates: List[Dict[str, Any]]) -> int:
    """Bulk PUT updates to Smartsheet, chunked to 500 rows max, with retry on 429."""
    total = 0
    for i in range(0, len(updates), MAX_BATCH):
        chunk = updates[i:i + MAX_BATCH]
        url = f"{SS_API_BASE}/sheets/{sheet_id}/rows"

        if DRY_RUN:
            row_ids = [u["id"] for u in chunk]
            logging.info(f"🟡 [DRY RUN] Would update {len(chunk)} rows in sheet {sheet_id}: {row_ids[:10]}...")
            total += len(chunk)
            continue

        for attempt in range(2):  # One retry if rate limited
            resp = requests.put(url, headers=HEADERS, data=json.dumps(chunk))
            if resp.status_code == 429:
                logging.warning(f"⚠️ Rate limited on sheet {sheet_id}, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                continue
            resp.raise_for_status()
            break

        total += len(chunk)
    return total

# ================================================================
# MAIN FUNCTION
# ================================================================

def main(mytimer: func.TimerRequest) -> None:
    mode = "DRY RUN" if DRY_RUN else "LIVE RUN"
    logging.info(f"⏱️ Starting Project Missing check ({mode}, every 1 min)...")

    if not DEST_SHEETS:
        logging.warning("⚠️ No destination sheets configured. Exiting.")
        return

    try:
        # 1️⃣ Load normalized source keys
        src_rows = get_all_rows(SOURCE_SHEET_ID)
        src_keys = set()
        for r in src_rows:
            key = extract_key(r, SRC_COL_TANK, SRC_COL_CITY, SRC_COL_STATE)
            if key:
                src_keys.add(key)

        logging.info(f"✅ Loaded {len(src_keys)} valid source keys from Sheet {SOURCE_SHEET_ID}")
        logging.info(f"🔑 Sample Source Keys: {list(src_keys)[:5]}...")

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
                if not key:
                    continue
                if key not in src_keys:
                    logging.info(f"❗ Row {row['id']} in sheet {sid} is missing project (key: {key})")
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
                msg = f"✔️ Sheet {sid}: No missing rows found"
                report.append(msg)
                logging.info(msg)

        summary = f"🏁 Completed {mode}. Total rows {'to update' if DRY_RUN else 'updated'}: {total_updates}"
        logging.info(summary)
        for line in report:
            logging.info(line)

    except Exception as e:
        logging.exception(f"❌ Error during scheduled Project Missing check: {e}")
