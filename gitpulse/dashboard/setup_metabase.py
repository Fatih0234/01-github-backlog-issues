"""
Phase 5: Metabase provisioning script.

Creates (idempotently):
  - Collection "GitPulse V1"
  - 16 SQL saved questions
  - Dashboard "GitPulse — Open Issue Backlog Monitor"
  - All 16 cards wired into the dashboard

Usage:
    uv run python src/dashboard/setup_metabase.py
"""

import logging
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
METABASE_EMAIL = os.environ.get("METABASE_EMAIL", "admin@gitpulse.com")
METABASE_PASSWORD = os.environ.get("METABASE_PASSWORD", "password")
REPO_OWNER = os.environ.get("REPO_OWNER", "apache")
REPO_NAME = os.environ.get("REPO_NAME", "airflow")

COLLECTION_NAME = "GitPulse V1"
DASHBOARD_NAME = "GitPulse — Open Issue Backlog Monitor"

W = f"repo_owner = '{REPO_OWNER}' AND repo_name = '{REPO_NAME}'"

CARDS = [
    {
        "name": "kpi_repo_name",
        "display_name": "Repository",
        "display": "scalar",
        "sql": f"SELECT DISTINCT repo_name FROM gold_mart_issue_lifecycle WHERE {W} LIMIT 1",
    },
    {
        "name": "kpi_opened_in_window",
        "display_name": "Opened (Last 180 Days)",
        "display": "scalar",
        "sql": (
            f"SELECT COUNT(*) FROM gold_mart_issue_lifecycle "
            f"WHERE {W} AND created_date >= CURRENT_DATE - INTERVAL 180 DAY"
        ),
    },
    {
        "name": "kpi_closed_in_window",
        "display_name": "Closed (Last 180 Days)",
        "display": "scalar",
        "sql": (
            f"SELECT COUNT(*) FROM gold_mart_issue_lifecycle "
            f"WHERE {W} AND is_closed = true AND closed_date >= CURRENT_DATE - INTERVAL 180 DAY"
        ),
    },
    {
        "name": "kpi_net_change_in_window",
        "display_name": "Net Change (Last 180 Days)",
        "display": "scalar",
        "sql": (
            f"SELECT SUM(net_change) FROM gold_mart_issue_daily_flow "
            f"WHERE {W} AND calendar_date >= CURRENT_DATE - INTERVAL 180 DAY"
        ),
    },
    {
        "name": "kpi_backlog_direction_signal",
        "display_name": "Backlog Direction",
        "display": "scalar",
        "sql": (
            f"SELECT CASE WHEN SUM(net_change) > 10 THEN 'Worsening' "
            f"WHEN SUM(net_change) < -10 THEN 'Improving' ELSE 'Flat' END "
            f"FROM gold_mart_issue_daily_flow "
            f"WHERE {W} AND calendar_date >= CURRENT_DATE - INTERVAL 180 DAY"
        ),
    },
    {
        "name": "ts_cumulative_backlog_direction",
        "display_name": "Cumulative Backlog Direction",
        "display": "line",
        "sql": (
            f"SELECT calendar_date, cumulative_net_change AS \"Cumulative Net Change\" "
            f"FROM gold_mart_issue_daily_flow "
            f"WHERE {W} AND calendar_date >= CURRENT_DATE - INTERVAL 180 DAY "
            f"ORDER BY calendar_date"
        ),
        "viz_settings": {
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Cumulative Net Change",
            "series_settings": {
                "Cumulative Net Change": {"color": "#F9844A"},
            },
        },
    },
    {
        "name": "ts_7d_opened_vs_closed",
        "display_name": "7d Opened vs Closed (Rolling)",
        "display": "line",
        "sql": (
            f"SELECT calendar_date, "
            f"rolling_7d_opened AS \"7d Opened\", "
            f"rolling_7d_closed AS \"7d Closed\" "
            f"FROM gold_mart_issue_daily_flow "
            f"WHERE {W} AND calendar_date >= CURRENT_DATE - INTERVAL 180 DAY "
            f"ORDER BY calendar_date"
        ),
        "viz_settings": {
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Issues (7d Rolling)",
            "series_settings": {
                "7d Opened": {"color": "#EF4444"},
                "7d Closed": {"color": "#22C55E"},
            },
        },
    },
    {
        "name": "ts_7d_backlog_velocity",
        "display_name": "7d Backlog Velocity (Rolling)",
        "display": "line",
        "sql": (
            f"SELECT calendar_date, rolling_7d_net_change AS \"7d Net Change\" "
            f"FROM gold_mart_issue_daily_flow "
            f"WHERE {W} AND calendar_date >= CURRENT_DATE - INTERVAL 180 DAY "
            f"ORDER BY calendar_date"
        ),
        "viz_settings": {
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Net Change (7d Rolling)",
            "series_settings": {
                "7d Net Change": {"color": "#A855F7"},
            },
        },
    },
    {
        "name": "tbl_top_positive_swing_days",
        "display_name": "Top 10 Positive Swing Days",
        "display": "table",
        "sql": (
            f"SELECT calendar_date, "
            f"opened_count AS \"Opened\", "
            f"closed_count AS \"Closed\", "
            f"net_change AS \"Net Change\" "
            f"FROM gold_mart_issue_swing_days "
            f"WHERE {W} AND swing_rank_positive <= 10 "
            f"ORDER BY swing_rank_positive"
        ),
    },
    {
        "name": "tbl_top_negative_swing_days",
        "display_name": "Top 10 Negative Swing Days",
        "display": "table",
        "sql": (
            f"SELECT calendar_date, "
            f"opened_count AS \"Opened\", "
            f"closed_count AS \"Closed\", "
            f"net_change AS \"Net Change\" "
            f"FROM gold_mart_issue_swing_days "
            f"WHERE {W} AND swing_rank_negative <= 10 "
            f"ORDER BY swing_rank_negative"
        ),
    },
    {
        "name": "ts_monthly_closure_age",
        "display_name": "Monthly Closure Age",
        "display": "line",
        "sql": (
            f"SELECT year_month, "
            f"median_closure_age_days AS \"Median Days\", "
            f"p90_closure_age_days AS \"P90 Days\" "
            f"FROM gold_mart_issue_closure_age_monthly "
            f"WHERE {W} AND year_month >= CURRENT_DATE - INTERVAL 2 YEAR ORDER BY year_month"
        ),
        "viz_settings": {
            "graph.x_axis.title_text": "Month",
            "graph.y_axis.title_text": "Days to Close",
            "graph.y_axis.auto_split": False,
            "series_settings": {
                "Median Days": {"color": "#38BDF8"},
                "P90 Days": {"color": "#F97316"},
            },
        },
    },
    {
        "name": "kpi_share_closed_within_7d",
        "display_name": "Avg Share Closed Within 7 Days",
        "display": "scalar",
        "sql": (
            f"SELECT AVG(share_closed_within_7d) "
            f"FROM gold_mart_issue_closure_age_monthly WHERE {W}"
        ),
    },
    {
        "name": "kpi_share_closed_after_90d",
        "display_name": "Avg Share Closed After 90 Days",
        "display": "scalar",
        "sql": (
            f"SELECT AVG(share_closed_after_90d) "
            f"FROM gold_mart_issue_closure_age_monthly WHERE {W}"
        ),
    },
    {
        "name": "bar_opened_by_weekday",
        "display_name": "Opened by Weekday",
        "display": "bar",
        "sql": (
            f"SELECT weekday_name, issue_count AS \"Issue Count\" "
            f"FROM gold_mart_issue_weekday_rhythm "
            f"WHERE {W} AND event_type = 'opened' ORDER BY weekday_num"
        ),
        "viz_settings": {
            "graph.x_axis.title_text": "Day of Week",
            "graph.y_axis.title_text": "Issue Count",
            "series_settings": {
                "Issue Count": {"color": "#EF4444"},
            },
        },
    },
    {
        "name": "bar_closed_by_weekday",
        "display_name": "Closed by Weekday",
        "display": "bar",
        "sql": (
            f"SELECT weekday_name, issue_count AS \"Issue Count\" "
            f"FROM gold_mart_issue_weekday_rhythm "
            f"WHERE {W} AND event_type = 'closed' ORDER BY weekday_num"
        ),
        "viz_settings": {
            "graph.x_axis.title_text": "Day of Week",
            "graph.y_axis.title_text": "Issue Count",
            "series_settings": {
                "Issue Count": {"color": "#22C55E"},
            },
        },
    },
    {
        "name": "tbl_issue_detail",
        "display_name": "Issue Detail (Last 500)",
        "display": "table",
        "sql": (
            f"SELECT issue_number AS \"Issue #\", title, created_at, closed_at, state, "
            f"closure_age_days AS \"Closure Age (days)\", "
            f"author_association AS \"Author Role\", "
            f"label_group AS \"Label Group\", "
            f"html_url AS \"URL\" "
            f"FROM gold_mart_issue_lifecycle "
            f"WHERE {W} ORDER BY created_at DESC LIMIT 500"
        ),
    },
]


def _session(email: str, password: str) -> str:
    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": email, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["id"]
    log.info("Authenticated as %s", email)
    return token


def _headers(token: str) -> dict:
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}


def _find_duckdb_db(token: str) -> int:
    resp = requests.get(f"{METABASE_URL}/api/database", headers=_headers(token), timeout=30)
    resp.raise_for_status()
    dbs = resp.json()
    # Handle both list and {"data": [...]} shapes
    items = dbs if isinstance(dbs, list) else dbs.get("data", [])
    for db in items:
        if db.get("engine") == "duckdb":
            log.info("Found DuckDB database: id=%d name=%s", db["id"], db["name"])
            return db["id"]
    raise RuntimeError("No DuckDB database found in Metabase. Add it via the UI first.")


def _get_or_create_collection(token: str) -> int:
    resp = requests.get(f"{METABASE_URL}/api/collection", headers=_headers(token), timeout=30)
    resp.raise_for_status()
    items = resp.json()
    collections = items if isinstance(items, list) else items.get("data", [])
    for col in collections:
        if col.get("name") == COLLECTION_NAME:
            log.info("Collection already exists: id=%d", col["id"])
            return col["id"]

    resp = requests.post(
        f"{METABASE_URL}/api/collection",
        headers=_headers(token),
        json={"name": COLLECTION_NAME, "color": "#509EE3"},
        timeout=30,
    )
    resp.raise_for_status()
    col_id = resp.json()["id"]
    log.info("Created collection '%s': id=%d", COLLECTION_NAME, col_id)
    return col_id


def _get_existing_cards(token: str, collection_id: int) -> dict[str, int]:
    """Return {name: card_id} for all cards in the collection."""
    resp = requests.get(
        f"{METABASE_URL}/api/card",
        headers=_headers(token),
        params={"f": "all"},
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json()
    cards = items if isinstance(items, list) else items.get("data", [])
    return {
        c["name"]: c["id"]
        for c in cards
        if c.get("collection_id") == collection_id
    }


def _create_cards(token: str, db_id: int, collection_id: int) -> dict[str, int]:
    existing = _get_existing_cards(token, collection_id)
    card_ids: dict[str, int] = {}

    for card in CARDS:
        name = card["name"]
        display_name = card.get("display_name", name)
        payload = {
            "name": display_name,
            "collection_id": collection_id,
            "display": card["display"],
            "dataset_query": {
                "type": "native",
                "database": db_id,
                "native": {"query": card["sql"]},
            },
            "visualization_settings": card.get("viz_settings", {}),
        }
        # Look up by display_name first (new), then by old internal name (backwards compat)
        cid = existing.get(display_name) or existing.get(name)
        if cid:
            resp = requests.put(
                f"{METABASE_URL}/api/card/{cid}",
                headers=_headers(token),
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            log.info("Updated card '%s': id=%d", display_name, cid)
        else:
            resp = requests.post(
                f"{METABASE_URL}/api/card",
                headers=_headers(token),
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            cid = resp.json()["id"]
            log.info("Created card '%s': id=%d", display_name, cid)
        card_ids[name] = cid

    return card_ids


def _get_or_create_dashboard(token: str, collection_id: int) -> int:
    resp = requests.get(f"{METABASE_URL}/api/dashboard", headers=_headers(token), timeout=30)
    resp.raise_for_status()
    items = resp.json()
    dashboards = items if isinstance(items, list) else items.get("data", [])
    for d in dashboards:
        if d.get("name") == DASHBOARD_NAME:
            log.info("Dashboard already exists: id=%d", d["id"])
            return d["id"]

    resp = requests.post(
        f"{METABASE_URL}/api/dashboard",
        headers=_headers(token),
        json={"name": DASHBOARD_NAME, "collection_id": collection_id},
        timeout=30,
    )
    resp.raise_for_status()
    did = resp.json()["id"]
    log.info("Created dashboard '%s': id=%d", DASHBOARD_NAME, did)
    return did


def _get_dashboard(token: str, dashboard_id: int) -> dict:
    resp = requests.get(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}",
        headers=_headers(token),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


_DASHCARD_PUT_FIELDS = {"id", "card_id", "row", "col", "size_x", "size_y", "parameter_mappings", "visualization_settings", "series", "action_id"}


def _wire_cards_to_dashboard(
    token: str, dashboard_id: int, card_ids: dict[str, int]
) -> None:
    # Metabase v0.49+ uses PUT /api/dashboard/:id with full dashcards list.
    # The PUT does DELETE+re-INSERT for all dashcards, so every dashcard must
    # have a valid dashboard_tab_id (Metabase auto-creates a default tab).
    dashboard = _get_dashboard(token, dashboard_id)
    existing_dashcards = dashboard.get("dashcards", [])
    tabs = dashboard.get("tabs", [])
    log.info("Dashboard tabs from GET: %s", tabs)
    log.info("Existing dashcard tab_ids: %s", [dc.get("dashboard_tab_id") for dc in existing_dashcards])
    # Use the first tab's id; required FK on REPORT_DASHBOARDCARD.DASHBOARD_TAB_ID
    tab_id = tabs[0]["id"] if tabs else None
    log.info("Using tab_id=%s for all dashcards", tab_id)

    existing_card_ids = {dc["card_id"] for dc in existing_dashcards if dc.get("card_id")}

    def _strip(dc: dict) -> dict:
        out = {k: v for k, v in dc.items() if k in _DASHCARD_PUT_FIELDS}
        out["visualization_settings"] = {}
        if tab_id is not None:
            out["dashboard_tab_id"] = tab_id
        return out

    new_dashcards = [_strip(dc) for dc in existing_dashcards]

    next_row = max((dc["row"] + dc.get("size_y", 4) for dc in existing_dashcards), default=0)
    tmp_id = -1

    for card in CARDS:
        cid = card_ids[card["name"]]
        if cid in existing_card_ids:
            log.info("Card '%s' already on dashboard.", card["name"])
        else:
            entry: dict = {
                "id": tmp_id,
                "card_id": cid,
                "row": next_row,
                "col": 0,
                "size_x": 18,
                "size_y": 4,
                "parameter_mappings": [],
                "visualization_settings": {},
            }
            if tab_id is not None:
                entry["dashboard_tab_id"] = tab_id
            new_dashcards.append(entry)
            log.info("Queued card '%s' (id=%d) at row=%d", card["name"], cid, next_row)
            tmp_id -= 1
            next_row += 4

    # Must include tabs in the payload; without it Metabase deletes the existing
    # tabs before re-inserting dashcards, causing a FK violation on dashboard_tab_id.
    tabs_payload = [{"id": t["id"], "name": t["name"], "position": t["position"]} for t in tabs]
    resp = requests.put(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}",
        headers=_headers(token),
        json={"dashcards": new_dashcards, "tabs": tabs_payload},
        timeout=30,
    )
    if not resp.ok:
        log.error("Dashboard PUT failed %d: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    log.info("Dashboard updated with %d total dashcards.", len(new_dashcards))


def main() -> None:
    token = _session(METABASE_EMAIL, METABASE_PASSWORD)
    db_id = _find_duckdb_db(token)
    collection_id = _get_or_create_collection(token)
    card_ids = _create_cards(token, db_id, collection_id)
    dashboard_id = _get_or_create_dashboard(token, collection_id)
    _wire_cards_to_dashboard(token, dashboard_id, card_ids)
    log.info(
        "Done. Collection=%d  Dashboard=%d  Cards=%d",
        collection_id,
        dashboard_id,
        len(card_ids),
    )


if __name__ == "__main__":
    main()
