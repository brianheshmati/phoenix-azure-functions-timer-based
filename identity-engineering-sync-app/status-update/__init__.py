import os
import json
import time
import logging
import requests
import csv
import azure.functions as func
from typing import Dict, Any, List

# ================================================================
# CONFIGURATION
# ================================================================

SS_API_BASE = "https://api.smartsheet.com/2.0"
SMARTSHEET_TOKEN = os.environ["SMARTSHEET_ACCESS_TOKEN"]

SOURCE_SHEET_ID = 639499383033732
SRC_COL_TANK = 3633417232797572
SRC_COL_CITY = 818667465691012
SRC_COL_STATE = 5322267093061508
SRC_COL_STATUS = 1917042186473348  # ✅ Actual source Status column ID

# Load destination sheets from environment
try:
    DEST_SHEETS = json.loads(os.environ["DEST_SHEETS_JSON"])
except Exception as e:
    logging.error(f"❌ Invalid DEST_SHEETS_JSON format: {e}")
    DEST_SHEETS = []

DRY_RUN = os.getenv("DRY_RUN_STATUS_UPDATE", "false").lower() == "true"

HEADERS = {
    "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
    "Content-Type": "application/json"
}

MAX_BATCH = 500
RETRY_DELAY = 3
CSV_LOG_PATH = "/tmp/status_changes.csv"  # optional audit log

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def normalize_tank(value: Any) -> str:
    """Normalize tank number (e.g., 010 → 10)."""
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
    if not tank or not city or not state:
        return ""
    return f"{tank}|{city}|{state}"


def get_all_rows(sheet_id: int) -> List[Dict[str, Any]]:
    """Fetch all rows from a Smartsheet sheet."""
    url = f"{SS_API_BASE}/sheets/{sheet_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json().get("rows", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"⚠️  Failed to fetch rows for sheet {sheet_id}: {e}")
        return []


def bulk_update(sheet_id: int, updates: List[Dict[str, Any]]) -> int:
    """Bulk PUT updates to Smartsheet with retry on 429."""
    total = 0
    for i in range(0, len(updates), MAX_BATCH):
        chunk = updates[i:i + MAX_BATCH]
        url = f"{SS_API_BASE}/sheets/{sheet_id}/rows"

        if DRY_RUN:
            total += len(chunk)
            continue

        for attempt in range(2):
            resp = requests.put(url, headers=HEADERS, data=json.dumps(chunk))
            if resp.status_code == 429:
                logging.warning(f"⏳ Rate limited on {sheet_id}, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                continue
            try:
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"❌ Failed updating sheet {sheet_id}: {e} | {resp.text[:150]}")
            break

        total += len(chunk)
    return total


def validate_dest_sheet(dest: Dict[str, Any]) -> bool:
    """Ensure destination sheet has all required column IDs and they are integers."""
    required_keys = {"tank", "city", "state", "status"}
    cols = dest.get("cols", {})
    missing = [k for k in required_keys if k not in cols or not isinstance(cols[k], int)]
    if missing:
        logging.error(f"❌ {dest.get('sheet_name','?')} missing columns: {missing}")
        return False
    return True


# ================================================================
# MAIN FUNCTION
# ================================================================

def main(mytimer: func.TimerRequest) -> None:
    mode = "DRY RUN" if DRY_RUN else "LIVE RUN"
    logging.info("───────────────────────────────────────────────")
    logging.info(f"[START] Status Sync ({mode})")
    logging.info("───────────────────────────────────────────────")

    if not DEST_SHEETS:
        logging.warning("⚠️  No destination sheets configured. Exiting.")
        return

    try:
        # 1️⃣ Load all source rows and build lookup: key → status_value
        src_rows = get_all_rows(SOURCE_SHEET_ID)
        src_map = {}
        for r in src_rows:
            key = extract_key(r, SRC_COL_TANK, SRC_COL_CITY, SRC_COL_STATE)
            if not key:
                continue
            cells = {c["columnId"]: c.get("value") for c in r.get("cells", [])}
            src_map[key] = cells.get(SRC_COL_STATUS, "")

        logging.info(f"✅ Loaded {len(src_map)} source project status values from Sheet {SOURCE_SHEET_ID}")

        total_updates = 0
        all_changes = []  # for CSV
        csv_header = ["Sheet Name", "Row ID", "Tank", "City", "State", "Old Status", "New Status"]

        # 2️⃣ Process each destination sheet
        for dest in DEST_SHEETS:
            sid = dest.get("sheet_id")
            name = dest.get("sheet_name", str(sid))
            cols = dest.get("cols", {})

            if not validate_dest_sheet(dest):
                logging.warning(f"⚠️  {name}: Skipped (invalid mapping)")
                continue

            try:
                logging.info(f"🔍 Processing sheet: {name} (ID: {sid})")
                dest_rows = get_all_rows(sid)
                if not dest_rows:
                    logging.warning(f"⚠️  {name}: No data or fetch error")
                    continue

                updates = []
                for row in dest_rows:
                    cells = {c["columnId"]: c.get("value") for c in row.get("cells", [])}
                    tank = normalize_tank(cells.get(cols["tank"]))
                    city = str(cells.get(cols["city"]) or "").strip()
                    state = str(cells.get(cols["state"]) or "").strip()
                    dest_status = (cells.get(cols["status"]) or "").strip() if cells.get(cols["status"]) else ""

                    key = f"{tank}|{city.lower()}|{state.lower()}"
                    src_status = (src_map.get(key) or "").strip()

                    # Skip if both statuses are blank or None
                    if (not src_status) and (not dest_status):
                        continue

                    # Skip if no key or no identifying data
                    if not key or (tank == "" and city == "" and state == ""):
                        continue

                    # ✅ Only update when different
                    if src_status != dest_status:
                        logging.info(f"🟡 {name}: Row {row['id']} ({tank}, {city}, {state}) "
                                     f"will change from '{dest_status}' → '{src_status}'")
                        updates.append({
                            "id": row["id"],
                            "cells": [{"columnId": cols["status"], "value": src_status}]
                        })
                        all_changes.append([name, row["id"], tank, city, state, dest_status, src_status])

                if updates:
                    count = bulk_update(sid, updates)
                    total_updates += count
                    logging.info(f"✅ {name}: {count} rows {'would be' if DRY_RUN else 'were'} updated")
                else:
                    logging.info(f"✔️  {name}: No status changes needed")

            except Exception as ex:
                logging.error(f"❌ {name}: {ex}")

        # 3️⃣ Optional CSV audit
        if all_changes:
            try:
                with open(CSV_LOG_PATH, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(csv_header)
                    writer.writerows(all_changes)
                logging.info(f"🗂️  Logged {len(all_changes)} changes to {CSV_LOG_PATH}")
            except Exception as e:
                logging.error(f"⚠️  Failed to write CSV log: {e}")

        # 4️⃣ Summary
        logging.info("───────────────────────────────────────────────")
        logging.info(f"🏁 Completed {mode}: {total_updates} rows {'to update' if DRY_RUN else 'updated'}")
        logging.info("───────────────────────────────────────────────")

    except Exception as e:
        logging.exception(f"❌ Fatal error in Status Sync: {e}")
