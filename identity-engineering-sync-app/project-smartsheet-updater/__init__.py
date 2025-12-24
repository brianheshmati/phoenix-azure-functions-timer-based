import json
import os
import logging
import uuid
import requests
import azure.functions as func
import mssql
from typing import Dict

# ================================================================
# CONFIG
# ================================================================
SS_API_BASE = "https://api.smartsheet.com/2.0"
SMARTSHEET_TOKEN = os.environ["SMARTSHEET_ACCESS_TOKEN"]

DRY_RUN = os.getenv("DRY_RUN_SMARTSHEET_UPDATER", "false").lower() == "true"

HEADERS = {
    "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
    "Content-Type": "application/json"
}

# ================================================================
# SHEET ID MAPPING (AUTHORITATIVE)
# ================================================================
DEPARTMENT_SHEET_MAP = {
    "Sales":        639499383033732,
    "Engineering":  639499383033732,
    "Shaft":        5148656698085252,
    "Erection":     1936716945379204,
    "Coatings":     5695766275248004,
    "Subcontracts": 5695766275248004,
    "Foundation":   4814574961250180,
    "Punch List":   2176504579444612
}

# ================================================================
# APP INSIGHTS STRUCTURED LOGGER
# ================================================================
def ai_log(level: str, message: str, **props):
    extra = {"customDimensions": props}

    if level == "info":
        logging.info(message, extra=extra)
    elif level == "warning":
        logging.warning(message, extra=extra)
    elif level == "error":
        logging.error(message, extra=extra)
    else:
        logging.debug(message, extra=extra)

# ================================================================
# SQL CONNECTION + AUDIT LOGGING
# ================================================================
def get_sql_conn():
    return mssql.connect(
        server=os.environ["SQL_SERVER"],
        user=os.environ["SQL_USERNAME"],
        password=os.environ["SQL_PASSWORD"],
        database=os.environ["SQL_DB"],
        encrypt=True
    )

def log_audit(**kwargs):
    """Audit logging must NEVER throw"""
    try:
        conn = get_sql_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.ProjectSmartsheetAuditLog (
                Direction, Operation, SheetId, RowId,
                JobNumber, Department, City, State,
                Success, Message, PerformedBy
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                kwargs.get("direction"),
                kwargs.get("operation"),
                kwargs.get("sheet_id"),
                kwargs.get("row_id"),
                kwargs.get("job_number"),
                kwargs.get("department"),
                kwargs.get("city"),
                kwargs.get("state"),
                kwargs.get("success"),
                kwargs.get("message"),
                kwargs.get("user")
            )
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"[AUDIT] Failed to log audit row: {e}")

# ================================================================
# HELPERS
# ================================================================
def normalize(v):
    return str(v).strip().lower()

def resolve_candidate_sheets(obj: dict):
    dept = obj.get("department")
    if not dept or dept not in DEPARTMENT_SHEET_MAP:
        raise ValueError(f"Invalid or missing department: {dept}")
    return [DEPARTMENT_SHEET_MAP[dept]]

def get_sheet(sheet_id: int):
    resp = requests.get(f"{SS_API_BASE}/sheets/{sheet_id}", headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()

def get_column_map(sheet: dict) -> Dict[str, int]:
    return {c["title"]: c["id"] for c in sheet["columns"]}

def row_matcher(row: dict, obj: dict, col_map: dict) -> bool:
    def cell(col):
        cid = col_map.get(col)
        for c in row["cells"]:
            if c["columnId"] == cid:
                return normalize(c.get("displayValue") or c.get("value"))
        return ""

    return (
        cell("Tank #") == normalize(obj["jobNumber"]) and
        cell("City") == normalize(obj["city"]) and
        cell("State") == normalize(obj["state"])
    )

def build_smartsheet_updates(obj: dict, col_map: dict):
    MS_TO_SS = {
        "PM": "Project Manager",
        "ENG": "Assigned To",
        "FDN": "Assigned To",
        "Foreman/Sub": "Foreman/Sub"
    }

    cells = []
    for src, tgt in MS_TO_SS.items():
        if src in obj.get("updates", {}) and tgt in col_map:
            cells.append({
                "columnId": col_map[tgt],
                "value": obj["updates"][src]
            })
    return cells

def update_row(sheet_id: int, row_id: int, cells: list):
    if DRY_RUN:
        return

    payload = {"rows": [{"id": row_id, "cells": cells}]}
    resp = requests.put(
        f"{SS_API_BASE}/sheets/{sheet_id}/rows",
        headers=HEADERS,
        json=payload,
        timeout=20
    )
    resp.raise_for_status()

def extract_return_values(row: dict, col_map: dict):
    wanted = ["Duration", "Start Date", "End Date"]
    out = {}
    for c in row["cells"]:
        for name in wanted:
            if c["columnId"] == col_map.get(name):
                out[name] = c.get("value")
    return out

# ================================================================
# MAIN
# ================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = req.headers.get("X-Correlation-ID", str(uuid.uuid4()))

    caller = (
        req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        or req.headers.get("X-User")
        or "unknown"
    )

    ai_log(
        "info",
        "Integration request started",
        correlationId=correlation_id,
        caller=caller,
        dryRun=DRY_RUN
    )

    try:
        payload = req.get_json()
        results = []

        for obj in payload:
            ai_log(
                "info",
                "Processing job",
                correlationId=correlation_id,
                jobNumber=obj.get("jobNumber"),
                department=obj.get("department")
            )

            matched = False

            for sheet_id in resolve_candidate_sheets(obj):
                sheet = get_sheet(sheet_id)
                col_map = get_column_map(sheet)

                for row in sheet["rows"]:
                    if row_matcher(row, obj, col_map):
                        matched = True
                        row_id = row["id"]

                        ai_log(
                            "info",
                            "Matched Smartsheet row",
                            correlationId=correlation_id,
                            sheetId=sheet_id,
                            rowId=row_id
                        )

                        try:
                            cells = build_smartsheet_updates(obj, col_map)
                            update_row(sheet_id, row_id, cells)

                            log_audit(
                                direction="MSP_TO_SS",
                                operation="UPDATE_ROW",
                                success=True,
                                user=caller,
                                sheet_id=sheet_id,
                                row_id=row_id,
                                job_number=obj["jobNumber"],
                                department=obj["department"],
                                city=obj["city"],
                                state=obj["state"]
                            )

                            return_vals = extract_return_values(row, col_map)

                            log_audit(
                                direction="SS_TO_MSP",
                                operation="RETURN_VALUES",
                                success=True,
                                user=caller,
                                sheet_id=sheet_id,
                                row_id=row_id,
                                job_number=obj["jobNumber"],
                                department=obj["department"],
                                city=obj["city"],
                                state=obj["state"]
                            )

                            results.append({
                                **obj,
                                "smartsheet": {
                                    "sheetId": sheet_id,
                                    "rowId": row_id,
                                    "values": return_vals
                                }
                            })

                        except Exception as e:
                            ai_log(
                                "error",
                                "Smartsheet update failed",
                                correlationId=correlation_id,
                                sheetId=sheet_id,
                                rowId=row_id,
                                error=str(e)
                            )

                            log_audit(
                                direction="MSP_TO_SS",
                                operation="UPDATE_ROW",
                                success=False,
                                user=caller,
                                sheet_id=sheet_id,
                                row_id=row_id,
                                job_number=obj["jobNumber"],
                                department=obj["department"],
                                city=obj["city"],
                                state=obj["state"],
                                message=str(e)
                            )
                            raise

                        break

                if matched:
                    break

            if not matched:
                ai_log(
                    "warning",
                    "No matching Smartsheet row found",
                    correlationId=correlation_id,
                    jobNumber=obj.get("jobNumber")
                )
                results.append({**obj, "error": "No matching Smartsheet row found"})

        ai_log(
            "info",
            "Integration request completed",
            correlationId=correlation_id,
            processed=len(results)
        )

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "dryRun": DRY_RUN,
                "correlationId": correlation_id,
                "results": results
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("[FATAL] Integration failed")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "correlationId": correlation_id,
                "message": str(e)
            }),
            mimetype="application/json",
            status_code=500
        )
