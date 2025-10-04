# Silence noisy Azure SDK HTTP logging
# logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

import os
import json
import logging
import datetime as dt
import functools
from typing import Dict, Any, List, Tuple

import azure.functions as func
import requests
from dateutil import tz
from azure.storage.blob import BlobServiceClient
from collections import defaultdict

# ---------- Config ----------
SS_API_BASE = "https://api.smartsheet.com/2.0"

def safe_int_env(key: str, default: int = None) -> int:
    val = os.environ.get(key)
    if val is None:
        if default is not None:
            return int(default)
        raise RuntimeError(f"Missing required env var: {key}")
    try:
        return int(val)
    except Exception:
        raise RuntimeError(f"Invalid int for env var {key}: {val}")

SMARTSHEET_TOKEN = os.environ.get("SMARTSHEET_ACCESS_TOKEN")
SOURCE_SHEET_ID = 639499383033732   # 02-Identity Scope - hardcoded
DEST_SHEET_ID   = 436476211842948  # 09-Subcontracts - hardcoded

# Source column IDs
SRC_TANK_COL        = 3633417232797572
SRC_ROW_COL         = 537192488980356
SRC_ORDER_COL       = 8699966813589380 # columnId for "Order" here
SRC_INSULATION_COL = 959404954046340 # Insulation Column on 02 sheet
SRC_NTP_DATE_COL  = 3844523465330564
SRC_CONTRACT_DAYS_COL = 8348123092701060
SRC_NTP_COMPLETION_DATE_COL = 1029773698224004

# Destination column IDs
DEST_TANK_COL = 7584488200294276
DEST_ROW_COL  = 1136952015015812
DEST_NTP_DATE_COL  = 1673513689370500
DEST_CONTRACT_DAYS_COL = 6177113316740996
DEST_NTP_COMPLETION_DATE_COL = 3925313503055748
DEST_INSULATION_COL = 3388751828701060 # Insulation column on 09-Subcontracts sheet
DEST_PRIMERY_COL = 8710388107136900 # Primary column on 09 sheet
DEST_ORDER_COL = 6766451549228932 # Order column on 09 sheet
DEST_ORDER_VAL = "0008 - Insulation"


ROW_VALUE_PROJECT     = "Project"
ROW_VALUE_INSULATION = "Insulation"
ORDER_VALUE_PROJECT   = "0000 - Project"

SRC_DEST_COLUMN_MAP : Dict[int, int] = {
    3633417232797572: 7584488200294276,  # Tank #
    8137016860168068: 1954988666081156,  # Site name
    818667465691012:  6458588293451652,  # City
    5322267093061508: 4206788479766404,  # State
    2155673605066628: 5051213409898372,  # Size
    6659273232437124: 2799413596213124,  # Type
    4618579651284868: 547613782527876,   # Project manager
    5885217046482820: 11052108173188,  # Estimator
    6448166999904132: 7303013223583620,  # Contract - date
    3844523465330564: 1673513689370500,  # NTP date
    8348123092701060: 6177113316740996,  # Contract days
    1029773698224004: 3925313503055748,  # NTP completion date
    5533373325594500: 8428913130426244,   # LDs
    4407473418751876: 4488263456477060,  # Engineering firm
    8911073046122372: 8991863083847556,  # Owner
    1381617419112324: 4514651735543684,  # Bid #
}

COLUMN_MAP: Dict[int, int] = {int(k): int(v) for k, v in SRC_DEST_COLUMN_MAP.items()}

STATE_CONTAINER = os.environ.get("STATE_CONTAINER")
STATE_BLOB      = os.environ.get("STATE_BLOB")
BLOB_CS         = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

DRY_RUN = os.getenv("DRY_RUN_INSULATION", "false").lower() == "true"

HEADERS = {
    "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
    "Content-Type": "application/json"
}

# ---------- Utilities ----------
def to_iso_z(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz.UTC)
    return d.astimezone(tz.UTC).isoformat().replace("+00:00", "Z")

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def blob_client():
    svc = BlobServiceClient.from_connection_string(BLOB_CS)
    container = svc.get_container_client(STATE_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
    return container.get_blob_client(STATE_BLOB)

def load_last_run() -> dt.datetime:
    bc = blob_client()
    default = dt.datetime.utcnow().replace(tzinfo=tz.UTC) - dt.timedelta(days=1)
    try:
        blob_data = bc.download_blob().readall().decode("utf-8")
        data = json.loads(blob_data)
        last_run_str = data.get("lastRun")
        if not last_run_str:
            return default
        return dt.datetime.fromisoformat(last_run_str.replace("Z", "+00:00"))
    except Exception:
        return default

def save_last_run(ts: dt.datetime):
    bc = blob_client()
    payload = {"lastRun": to_iso_z(ts)}
    bc.upload_blob(json.dumps(payload), overwrite=True)

def ss_get(url: str, params: Dict[str, Any] = None) -> requests.Response:
    logging.debug(f">>>>> SS_GET called with url={url} params={params}")

    if not SMARTSHEET_TOKEN:
        raise RuntimeError("SMARTSHEET_ACCESS_TOKEN is not set")
    
    logging.info(f"Smartsheet GET {url} params={params}")
    
    resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
    # logging.info(f"Smartsheet GET {url} response: {resp.json()}")

    resp.raise_for_status()
    return resp

def ss_post(url: str, body: Any) -> requests.Response:
    resp = requests.post(url, headers=HEADERS, data=json.dumps(body), timeout=60)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"Smartsheet POST {url} failed: {e}, response: {resp.text}")
        # You can either re-raise (to stop), or return resp anyway
        # raise
        return resp  # <- if you want the caller to decide

    logging.info(f"Smartsheet POST {url}, body: {body}, response: {resp.json()}")
    return resp
    # logging.info(f"Smartsheet POST {url}, body: {body}, response: {resp.json()}")
    # resp.raise_for_status()
    # return resp

def ss_put(url: str, body: Any) -> requests.Response:
    resp = requests.put(url, headers=HEADERS, data=json.dumps(body), timeout=60)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"Smartsheet PUT {url} failed: {e}, response: {resp.text}")
        return resp  # still return so caller can inspect the response

    logging.info(f"Smartsheet PUT {url}, response: {resp.json()}")
    return resp
    # logging.info(f"Smartsheet PUT {url}, body: {body}") #, response: {resp.json()}")   
    # resp.raise_for_status()
    # return resp

def cells_array_to_dict(cells: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out = {}
    for c in cells or []:
        cid = int(c["columnId"])
        out[cid] = {"value": c.get("value"), "displayValue": c.get("displayValue")}
    return out

@functools.lru_cache(maxsize=2)
def get_column_titles(sheet_id: int) -> Dict[int, str]:
    """
    Return {columnId: title} using the correct endpoint:
    GET /sheets/{sheetId}
    """
    url = f"{SS_API_BASE}/sheets/{sheet_id}"
    r = ss_get(url, params={"pageSize": 1})  # tiny page, we only need columns
    data = r.json()
    return {col["id"]: col["title"] for col in data.get("columns", [])}

def normalize(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dt.datetime):
        return to_iso_z(val)
    return str(val)

def log_source_row_changes(srow: Dict[str, Any], src_titles: Dict[int, str]):
    """
    Print all non-empty source cell values in a readable format for debugging.
    """
    scells = cells_array_to_dict(srow.get("cells", []))
    lines = []
    for col_id, cell in scells.items():
        val = cell.get("value")
        if val not in (None, "", []):
            col_name = src_titles.get(col_id, str(col_id))
            lines.append(f"{col_name}: {val}")
    logging.info("[SOURCE ROW] " + " | ".join(lines))

# ---------- Fetching ----------
def list_all_source_project_rows() -> List[Dict[str, Any]]:
    """
    Fetch ALL rows from source with Row='Project' and Order='0000 - Project'
    using the correct list endpoint: GET /sheets/{sheetId} with paging.
    """
    logging.info(f"[SmartsheetSync] Getting all source rows from sheet {SOURCE_SHEET_ID}") 

    rows: List[Dict[str, Any]] = []
    page = 1
    page_size = 500

    logging.info(f"[SmartsheetSync] Fetching all source rows from sheet {SOURCE_SHEET_ID} with Row='{ROW_VALUE_PROJECT}' and Order='{ORDER_VALUE_PROJECT}'")

    #while True:
    url = f"{SS_API_BASE}/sheets/{SOURCE_SHEET_ID}"
    params = {"include": "rowPermalink", "page": page, "pageSize": page_size}
    
    logging.info(f"fetching source ==>> {url}, params ==>> {params}")

    r = ss_get(url, params=params)
    data = r.json()
    batch = data.get("rows", [])
    logging.info(f">>>>> fetched {len(data)} from source")
    # logging.info(f"<<<<< fetched rows: {batch}")

    for row in batch:
        scells = cells_array_to_dict(row.get("cells", []))
        src_row_val   = str((scells.get(SRC_ROW_COL)   or {}).get("value") or "").strip()
        src_order_val = str((scells.get(SRC_ORDER_COL) or {}).get("value") or "").strip()
        #src_shaft_val = str((scells.get(SRC_INSULATION_COL) or {}).get("value") or "").strip()
        if src_row_val == ROW_VALUE_PROJECT and src_order_val == ORDER_VALUE_PROJECT: # and (src_shaft_val != ""):
            rows.append(row)
    # if len(batch) < page_size:
    #     break
    page += 1
    return rows

def index_dest_by_tank_and_row() -> Dict[str, Dict[str, Any]]:
    """
    Index destination rows by Tank#, but keep ALL rows per tank in a list.
    We only include rows whose 'Row' column is 'Insulation' so
    later filtering by DEST_ROW_COL is trivial or unnecessary.
    """
    idx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    page = 1
    page_size = 500
    while True:
        url = f"{SS_API_BASE}/sheets/{DEST_SHEET_ID}"
        params = {"include": "rowPermalink", "page": page, "pageSize": page_size}
        r = ss_get(url, params=params)
        data = r.json()
        batch = data.get("rows", [])
        for row in batch:
            cdict = cells_array_to_dict(row.get("cells", []))
            row_val  = str((cdict.get(DEST_ROW_COL)  or {}).get("value") or "").strip()
            tank_val =     (cdict.get(DEST_TANK_COL) or {}).get("value")
            if row_val == ROW_VALUE_INSULATION and tank_val not in (None, ""):
                idx[str(tank_val).strip()].append(row)
        if len(batch) < page_size:
            break
        page += 1
    return dict(idx)

# ---------- Diff / Planning ----------
def find_column_diffs(
    src_cells: Dict[int, Dict[str, Any]],
    dest_cells: Dict[int, Dict[str, Any]],
    src_titles: Dict[int, str],
    dest_titles: Dict[int, str]
) -> List[str]:
    diffs: List[str] = []
    for src_col, dest_col in COLUMN_MAP.items():
        src_val  = normalize((src_cells.get(src_col)  or {}).get("value"))
        dest_val = normalize((dest_cells.get(dest_col) or {}).get("value"))
        if src_val != dest_val:
            diffs.append(f"{src_titles.get(src_col, str(src_col))}"
                         f"->{dest_titles.get(dest_col, str(dest_col))}: "
                         f"'{src_val}' vs '{dest_val}'")
    return diffs
def build_operations(
    source_rows: List[Dict[str, Any]],
    dest_index: Dict[str, Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    inserts: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []

    src_titles  = get_column_titles(SOURCE_SHEET_ID)
    dest_titles = get_column_titles(DEST_SHEET_ID)

    for srow in source_rows:
        scells = cells_array_to_dict(srow.get("cells", []))
        # logging.info(f"[Plan] Source row: {scells}")
        
        src_row_val   = str((scells.get(SRC_ROW_COL)   or {}).get("value") or "").strip()
        src_order_val = str((scells.get(SRC_ORDER_COL) or {}).get("value") or "").strip()
        src_tank_val  =     (scells.get(SRC_TANK_COL)  or {}).get("value")
        src_insulation_val = str((scells.get(SRC_INSULATION_COL) or {}).get("value") or "").strip()
        src_ntp_date_val = (scells.get(SRC_NTP_DATE_COL) or {}).get("value")
        src_contract_days_val = (scells.get(SRC_CONTRACT_DAYS_COL) or {}).get("value")
        
        logging.info(f"[Plan] Source row Tank={src_tank_val}, Shaft={src_insulation_val}, NTP Date={src_ntp_date_val}, Contract Days={src_contract_days_val}")

        src_ntp_completion_date_val = (scells.get(SRC_NTP_COMPLETION_DATE_COL) or {}).get("value")


        # Must be a Project row
        if src_row_val != ROW_VALUE_PROJECT or src_order_val != ORDER_VALUE_PROJECT:
            continue
        if src_tank_val in (None, ""):
            continue

        tank_key = str(src_tank_val).strip()

        candidates = dest_index.get(tank_key, [])
        if isinstance(candidates, dict):
            candidates = [candidates]

        # logging.info(f"[Plan] Candidates {candidates}")

        dest_row = None
        for row in candidates:
            cdict = cells_array_to_dict(row.get("cells", []))
            val = (cdict.get(DEST_ROW_COL) or {}).get("value")
            if val == ROW_VALUE_INSULATION:   # all indexed rows should already match
                dest_row = row
                break
        
        #logging.info(f"[Plan] Processing tank={tank_key}: dest_row found={dest_row is not None}")

        dest_cells = cells_array_to_dict(dest_row.get("cells", [])) if dest_row else {}
        
        dest_insulation_val = dest_cells.get(DEST_INSULATION_COL, {}).get('value')
        
        mapped_cells: List[Dict[str, Any]] = []
        
        if dest_row is None:
            # INSERT only if source "Insulation" is "Phoenix or Subcontractor"
            if src_insulation_val == "Phoenix" or src_insulation_val == "Subcontractor":
                 # Build mapped cell payload        
                for src_col, dest_col in COLUMN_MAP.items():
                    if src_col in scells:
                        mapped_cells.append({"columnId": dest_col, "value": scells[src_col].get("value")})
                
                mapped_cells.append({"columnId": DEST_PRIMERY_COL, "value": ROW_VALUE_INSULATION}) # Primary column
                mapped_cells.append({"columnId": DEST_ORDER_COL, "value": DEST_ORDER_VAL}) # Order
                # Force Row column in destination to Shaft"
                mapped_cells.append({"columnId": DEST_ROW_COL, "value": ROW_VALUE_INSULATION})
                mapped_cells.append({"columnId": DEST_INSULATION_COL, "value": src_insulation_val}) # Insulation column on 09 sheet with the value from 02 sheet

                inserts.append({"toBottom": True, "cells": mapped_cells})
                logging.info(f"[Plan] INSERT tank={tank_key} (Insulation = {src_insulation_val})")
            else:
                logging.info(f"[Plan] SKIP insert tank={tank_key} (Insulation= {src_insulation_val})")
        else:
            # UPDATE always if there are diffs
            
            dest_insulation_val = dest_cells.get(DEST_INSULATION_COL, {}).get('value')
            
            if(src_insulation_val != dest_insulation_val):
                mapped_cells.append({"columnId": DEST_INSULATION_COL, "value": src_insulation_val}) # update the Insulation column on 09 sheet with the value from 02 sheet
                logging.info(f"[Plan] UPDATE tank={tank_key} (Turning Insulation from {dest_insulation_val} to {src_insulation_val})")

            if(src_ntp_date_val != dest_cells.get(DEST_NTP_DATE_COL, {}).get("value")):
                mapped_cells.append({"columnId": DEST_NTP_DATE_COL, "value": src_ntp_date_val})      # update the NTP Date column on 09 sheet with the value from 02 sheet
                mapped_cells.append({"columnId": DEST_CONTRACT_DAYS_COL, "value": src_contract_days_val})      # update the Contract Days column on 09 sheet with the value from 02 sheet
                mapped_cells.append({"columnId": DEST_NTP_COMPLETION_DATE_COL, "value": src_ntp_completion_date_val})      # update the NTP Completion Date column on 09 sheet with the value from 02 sheet
                logging.info(f"[Plan] UPDATE tank={tank_key} (NTP Date = {src_ntp_date_val})")

            if mapped_cells:
                updates.append({"id": dest_row["id"], "cells": mapped_cells})

            # if diffs:
            #     updates.append({"id": dest_row["id"], "cells": mapped_cells})
            #     logging.info(f"[Plan] UPDATE tank={tank_key} – diffs: {', '.join(diffs)}")
            # else:
            #     logging.info(f"[Plan] SKIP update tank={tank_key} (no differences)")

    return inserts, updates

# ---------- Bulk Ops ----------
def bulk_insert(rows: List[Dict[str, Any]]):
    if not rows:
        return
    url = f"{SS_API_BASE}/sheets/{DEST_SHEET_ID}/rows"
    for batch in chunked(rows, 500):
        resp = ss_post(url, batch)
        if resp.status_code >= 400:
            logging.warning(f"[SmartsheetSync] Bulk insert failed for batch of {len(batch)} rows – retrying individually.")
            for row in batch:
                r = ss_post(url, [row])
                if r.status_code >= 400:
                    logging.error(f"[SmartsheetSync] Row insert failed: {row}, response={r.text}")
        else:
            logging.info(f"[SmartsheetSync] Inserted batch of {len(batch)} rows")
def bulk_update(rows: List[Dict[str, Any]]):
    if not rows:
        return
    url = f"{SS_API_BASE}/sheets/{DEST_SHEET_ID}/rows"
    for batch in chunked(rows, 500):
        resp = ss_put(url, batch)
        if resp.status_code >= 400:
            logging.warning(f"[SmartsheetSync] Bulk update failed for batch of {len(batch)} rows – retrying individually.")
            for row in batch:
                r = ss_put(url, [row])
                if r.status_code >= 400:
                    logging.error(f"[SmartsheetSync] Row update failed: {row}, response={r.text}")
        else:
            logging.info(f"[SmartsheetSync] Updated batch of {len(batch)} rows")

# ---------- Azure Function Entry ----------
def main(mytimer: func.TimerRequest) -> None:
    start_ts = dt.datetime.utcnow().replace(tzinfo=tz.UTC)
    logging.info(f"=>[SmartsheetSync] Triggered at {to_iso_z(start_ts)}")
    logging.info(f"=>[SmartsheetSync] DRY_RUN mode is {'ON' if DRY_RUN else 'OFF'}")

    #last_run = load_last_run()
    #logging.info(f"[SmartsheetSync] Last run: {to_iso_z(last_run)}")

    try:
        # Always full scan for correctness; paging uses /sheets/{id}
        source_rows = list_all_source_project_rows()
        logging.info(f"[SmartsheetSync] Source candidate rows: {len(source_rows)}")

        if not source_rows:
            save_last_run(start_ts)
            logging.info("[SmartsheetSync] Nothing to do.")
            return

        dest_index = index_dest_by_tank_and_row()
        logging.info(f"[SmartsheetSync] Indexed destination rows (Row='Insulation'): {len(dest_index)}")

        inserts, updates = build_operations(source_rows, dest_index)
        logging.info(f"[SmartsheetSync] Plan => inserts: {len(inserts)} | updates: {len(updates)}")

        if DRY_RUN:
            logging.warning("[SmartsheetSync] DRY_RUN mode ON – no changes will be written.")
        else:
            bulk_insert(inserts)
            bulk_update(updates) 
            logging.info("[SmartsheetSync] Changes committed to Smartsheet.")

        save_last_run(start_ts)
        logging.info("[SmartsheetSync] Done.")
    except Exception as ex:
        logging.exception(f"[identity-shaft-sync] FAILED: {ex}")
        raise
