"""ISP Recharge History Dashboard — standalone Flask app.

Lets PTL look up a customer and see all recharge history with
corresponding ISP ticket status.
"""

import json
from pathlib import Path

import requests as http_requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config — reuse breach_tracker settings
# ---------------------------------------------------------------------------
SETTINGS_FILE = Path(__file__).resolve().parent.parent / "breach_tracker" / "settings.json"


def _load_settings() -> dict:
    with open(SETTINGS_FILE) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Metabase client (minimal, reuses same pattern as breach_tracker)
# ---------------------------------------------------------------------------
class MetabaseClient:
    def __init__(self, url: str, database_id: str, api_key: str = ""):
        self.url = url.rstrip("/")
        self.database_id = int(database_id) if database_id else None
        self.api_key = api_key.strip()

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
        }

    def run_native_query(self, sql: str) -> list[dict]:
        """Execute a native SQL query and return list of row dicts."""
        payload = {
            "database": self.database_id,
            "type": "native",
            "native": {"query": sql},
            "parameters": [],
        }
        resp = http_requests.post(
            f"{self.url}/api/dataset",
            headers=self._headers(),
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise Exception(f"Metabase query error: {data['error']}")

        cols = [c["name"] for c in data["data"]["cols"]]
        rows = data["data"]["rows"]
        return [dict(zip(cols, row)) for row in rows]


def _get_client() -> MetabaseClient:
    cfg = _load_settings()
    return MetabaseClient(
        url=cfg["metabase_url"],
        database_id=cfg.get("metabase_database_id", ""),
        api_key=cfg.get("metabase_api_key", ""),
    )


# ---------------------------------------------------------------------------
# SQL query
# ---------------------------------------------------------------------------
def _build_lookup_sql(mobile: str = "", nas_id: str = "", account_id: str = "") -> str:
    """Build the SQL for recharge history + ISP ticket lookup.

    Returns future upcoming ISP ticket (if any) + last 5 recharges,
    ordered by time descending.
    """
    # Build WHERE clause from provided search params
    conditions = []
    if mobile:
        conditions.append(f"hrpi.MOBILE = '{mobile}'")
    if nas_id:
        conditions.append(f"hrpi.NAS_ID = {nas_id}")
    if account_id:
        conditions.append(f"hrpi.ACCOUNT_ID = {account_id}")

    if not conditions:
        raise ValueError("At least one search parameter is required")

    where = " AND ".join(conditions)

    sql = f"""
WITH recharges AS (
    SELECT
        hrpi.TRANSACTION_ID,
        hrpi.PLAN_START_TIME,
        hrpi.TIME_PLAN,
        hrpi.NAS_ID,
        hrpi.ACCOUNT_ID,
        hrpi.MOBILE,
        hrpi.PLAN_AMOUNT,
        hrpi.PLAN_END_TIME,
        hrpi.RECHARGE_ISP
    FROM DYNAMODB_READ.HOME_ROUTER_PLAN_INFO hrpi
    WHERE {where}
      AND hrpi._FIVETRAN_ACTIVE = TRUE
    ORDER BY hrpi.PLAN_START_TIME DESC
),
ranked AS (
    SELECT
        r.*,
        rt.RECHARGE_TICKETS_ID   AS ISP_TICKET_ID,
        rt.START_DATE             AS ISP_START_DATE,
        rt.EXPIRY_DATE            AS ISP_EXPIRY_DATE,
        rt.STATUS                 AS ISP_TICKET_STATUS,
        rt.CREATED_AT             AS ISP_TICKET_CREATED_AT,
        rt.UPDATED_AT             AS ISP_TICKET_SHOWN_DATE,
        rr.REQUEST_STATUS         AS ISP_REQUEST_STATUS,
        rr.RECHARGE_REQUEST_ID    AS ISP_REQUEST_ID,
        CASE
            WHEN rt.EXPIRY_DATE > CURRENT_TIMESTAMP() THEN 1
            ELSE 0
        END AS IS_FUTURE_TICKET,
        ROW_NUMBER() OVER (
            PARTITION BY r.TRANSACTION_ID
            ORDER BY rt.CREATED_AT DESC NULLS LAST
        ) AS ticket_rn
    FROM recharges r
    LEFT JOIN PARTNER_ISP_INTEGRATION_SERVICE_PUBLIC.RECHARGE_REQUESTS rr
        ON r.NAS_ID = rr.NAS_ID
        AND (
            r.TRANSACTION_ID = rr.CUSTOMER_REFERENCE_ID
            OR ABS(DATEDIFF('minute', r.PLAN_START_TIME, rr.CUSTOMER_PLAN_START)) <= 5
        )
    LEFT JOIN PARTNER_ISP_INTEGRATION_SERVICE_PUBLIC.RECHARGE_TICKETS rt
        ON rr.RECHARGE_REQUEST_ID = rt.RECHARGE_REQUEST_ID
)
SELECT
    TRANSACTION_ID,
    PLAN_START_TIME    AS RECHARGE_TIMESTAMP,
    TIME_PLAN          AS RECHARGE_DURATION_SEC,
    PLAN_AMOUNT        AS RECHARGE_AMOUNT,
    NAS_ID,
    ACCOUNT_ID,
    MOBILE,
    PLAN_END_TIME,
    RECHARGE_ISP,
    ISP_TICKET_ID,
    ISP_START_DATE,
    ISP_EXPIRY_DATE,
    ISP_TICKET_STATUS,
    ISP_TICKET_SHOWN_DATE,
    ISP_REQUEST_STATUS,
    ISP_REQUEST_ID,
    IS_FUTURE_TICKET
FROM ranked
WHERE ticket_rn = 1
ORDER BY IS_FUTURE_TICKET DESC, RECHARGE_TIMESTAMP DESC
LIMIT 20
"""
    return sql


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/lookup", methods=["POST"])
def lookup():
    body = request.get_json(silent=True) or {}
    mobile = str(body.get("mobile", "")).strip()
    nas_id = str(body.get("nas_id", "")).strip()
    account_id = str(body.get("account_id", "")).strip()

    if not any([mobile, nas_id, account_id]):
        return jsonify({"error": "Provide at least one of: Mobile Number, NAS ID, or Account ID"}), 400

    # Basic input validation — numeric only
    for label, val in [("Mobile", mobile), ("NAS ID", nas_id), ("Account ID", account_id)]:
        if val and not val.isdigit():
            return jsonify({"error": f"{label} must be numeric"}), 400

    try:
        sql = _build_lookup_sql(mobile=mobile, nas_id=nas_id, account_id=account_id)
        client = _get_client()
        rows = client.run_native_query(sql)
        return jsonify({"rows": rows, "count": len(rows)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        app.logger.error(f"Lookup error: {e}")
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5001)
