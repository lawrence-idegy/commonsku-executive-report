"""
export_executive_csvs.py
========================
Downloads BOTH Sales Rep and Sales Dashboard CSVs from CommonSKU
and upserts to Supabase.

Workflow 2: Executive Sales Report (David Brown)
-------------------------------------------------
For each date range (This Week / This Month / This Year), downloads
two reports:

  1. Sales Rep report (/report/sales-rep, Form Type = Sales Order)
     -> commonsku_sr_weekly / commonsku_sr_monthly / commonsku_sr_ytd
     Contains individual order rows with subtotal and booked_margin
     for GP% calculations.

  2. Sales Dashboard report (/report/sales-dashboard)
     -> commonsku_exec_dash_weekly / commonsku_exec_dash_monthly / commonsku_exec_dash_ytd
     Contains per-rep summary rows with total_in_production,
     total_sales_orders, and total_invoices aggregates.

The downstream Zapier step merges both datasets:
  - Sales Rep data for GP% (booked_margin on individual orders)
  - Dashboard data for In Production dollar totals per rep

Each CSV row is augmented with export_date (today, YYYY-MM-DD)
and inserted using a delete-then-insert strategy keyed on export_date.

Usage:
  python export_executive_csvs.py --scope all
  python export_executive_csvs.py --scope weekly
  python export_executive_csvs.py --scope monthly
  python export_executive_csvs.py --scope ytd
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

COMMONSKU_URL = os.getenv("COMMONSKU_URL", "https://idegy.commonsku.com")
COMMONSKU_EMAIL = os.getenv("COMMONSKU_EMAIL", "")
COMMONSKU_PASSWORD = os.getenv("COMMONSKU_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://oascilobkhxpmrayftar.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5000"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

REPORT_JOBS = {
    "weekly": {
        "date_filter": "This Week",
        "sr_table": "commonsku_sr_weekly",
        "dash_table": "commonsku_exec_dash_weekly",
    },
    "monthly": {
        "date_filter": "This Month",
        "sr_table": "commonsku_sr_monthly",
        "dash_table": "commonsku_exec_dash_monthly",
    },
    "ytd": {
        "date_filter": "This Year",
        "sr_table": "commonsku_sr_ytd",
        "dash_table": "commonsku_exec_dash_ytd",
    },
}

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / "executive_export.log"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------
import urllib.request
import urllib.error


def supabase_request(endpoint: str, method: str = "GET", body=None, extra_headers=None):
    """Low-level Supabase REST API call using urllib (no extra deps)."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)

    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as resp:
            response_body = resp.read().decode("utf-8")
            return json.loads(response_body) if response_body.strip() else None
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8") if exc.fp else ""
        logger.error("Supabase %s %s -> %s: %s", method, endpoint, exc.code, error_body)
        raise


def delete_existing_rows(table_name: str, export_date: str):
    """Delete all rows for the given export_date before upserting fresh data."""
    endpoint = f"{table_name}?export_date=eq.{export_date}"
    supabase_request(endpoint, method="DELETE", extra_headers={"Prefer": "return=minimal"})
    logger.info("Deleted existing rows for %s on %s", table_name, export_date)


def upsert_csv_to_supabase(table_name: str, csv_content: str, export_date: str):
    """
    Parse a CSV string and upsert all rows into the Supabase table.

    Strategy: delete-then-insert for the given export_date. This avoids
    conflict-key issues when CommonSKU changes column order.
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = []

    for raw_row in reader:
        row = {}
        row["export_date"] = export_date

        for csv_col, value in raw_row.items():
            if csv_col is None:
                continue
            # Convert "Sales Rep First Name" -> "sales_rep_first_name"
            db_col = csv_col.strip().lower().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace("%", "").strip("_")
            # Clean value
            clean_value = value.strip().strip('"').strip("'") if value else None

            # Try to cast numeric-looking values
            # Covers both Sales Rep and Sales Dashboard CSV columns
            if clean_value and db_col in (
                "subtotal", "taxes", "total", "booked_margin",
                "booked_margin_amount", "project_budget",
                "sales_order_total", "in_production_total",
                "invoice_total", "total_in_production_margin",
                "margin", "margin_amount", "amount",
            ):
                try:
                    clean_value = float(clean_value.replace("$", "").replace(",", ""))
                except (ValueError, AttributeError):
                    pass

            row[db_col] = clean_value

        rows.append(row)

    if not rows:
        logger.warning("No rows parsed from CSV for %s", table_name)
        return 0

    # Delete existing data for this date, then bulk insert
    delete_existing_rows(table_name, export_date)

    # Insert in batches of 100
    inserted = 0
    for i in range(0, len(rows), 100):
        batch = rows[i : i + 100]
        supabase_request(
            table_name,
            method="POST",
            body=batch,
            extra_headers={"Prefer": "return=minimal"},
        )
        inserted += len(batch)
        logger.info("Inserted batch %d-%d into %s", i, i + len(batch), table_name)

    logger.info("Total rows upserted into %s: %d", table_name, inserted)
    return inserted


# ---------------------------------------------------------------------------
# Playwright: CommonSKU login + CSV downloads
# ---------------------------------------------------------------------------
def login_to_commonsku(page):
    """Log into CommonSKU with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Login attempt %d of %d", attempt, MAX_RETRIES)
            page.goto(COMMONSKU_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            current_url = page.url
            logger.info("Current URL: %s", current_url)

            # Check if already logged in
            if "/login" not in current_url and "/signin" not in current_url:
                page.goto(f"{COMMONSKU_URL}/report/sales-dashboard", wait_until="domcontentloaded", timeout=15000)
                page.wait_for_timeout(2000)
                if "/report" in page.url:
                    logger.info("Already logged in")
                    return True

            # Navigate to login page
            if "/login" not in page.url:
                page.goto(f"{COMMONSKU_URL}/login", wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000)

            # Fill email field (try multiple selectors)
            email_selectors = [
                'input[type="email"]',
                'input[name="email"]',
                'input[name="username"]',
                'input[placeholder*="email" i]',
                'input[placeholder*="Email" i]',
            ]
            email_filled = False
            for selector in email_selectors:
                element = page.query_selector(selector)
                if element:
                    element.fill(COMMONSKU_EMAIL)
                    email_filled = True
                    logger.info("Filled email field with selector: %s", selector)
                    break

            if not email_filled:
                raise Exception("Could not find email input field")

            # Fill password field
            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
            ]
            password_filled = False
            for selector in password_selectors:
                element = page.query_selector(selector)
                if element:
                    element.fill(COMMONSKU_PASSWORD)
                    password_filled = True
                    logger.info("Filled password field")
                    break

            if not password_filled:
                raise Exception("Could not find password input field")

            # Click submit
            submit_selectors = [
                'button:has-text("Login")',
                'button:has-text("Log In")',
                'button:has-text("Sign In")',
                'button[type="submit"]',
                'input[type="submit"]',
            ]
            submit_clicked = False
            for selector in submit_selectors:
                element = page.query_selector(selector)
                if element and element.is_visible():
                    element.click()
                    submit_clicked = True
                    logger.info("Clicked submit button with selector: %s", selector)
                    break

            if not submit_clicked:
                logger.error("Could not find any submit button on login page")
                raise Exception("No submit button found on login page")

            page.wait_for_timeout(5000)

            # Verify login
            if "/login" not in page.url and "/signin" not in page.url:
                logger.info("Login successful, URL: %s", page.url)
                return True

            logger.warning("Login may have failed, URL still: %s", page.url)

        except Exception as exc:
            logger.error("Login attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY / 1000)

    raise Exception(f"Failed to log into CommonSKU after {MAX_RETRIES} attempts")


def download_sr_report(page, date_filter: str, download_dir: str) -> str:
    """
    Navigate to CommonSKU Sales Rep report, set Form Type = Sales Order,
    apply date filter, and download the CSV.
    Returns the CSV file content as a string.

    The Sales Rep report gives individual order rows with subtotal and
    booked_margin for GP% calculations.
    """
    logger.info("Downloading Sales Rep report with date filter: %s", date_filter)

    # Navigate to Sales Rep report
    page.goto(f"{COMMONSKU_URL}/report/sales-rep", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(5000)

    # Select Form Type = Sales Order
    logger.info("Setting Form Type to: Sales Order")
    try:
        form_type_selector = page.query_selector('select[name="form_type"], select#form_type')
        if form_type_selector:
            form_type_selector.select_option(label="Sales Order")
            logger.info("Selected Form Type: Sales Order via <select>")
            page.wait_for_timeout(1000)
        else:
            # Try clicking a dropdown-style Form Type control
            form_type_labels = page.query_selector_all('text="Form Type"')
            for label in form_type_labels:
                parent = label.query_selector('xpath=..')
                if parent:
                    select_el = parent.query_selector('select')
                    if select_el:
                        select_el.select_option(label="Sales Order")
                        logger.info("Selected Form Type: Sales Order via parent select")
                        page.wait_for_timeout(1000)
                        break
    except Exception as exc:
        logger.warning("Could not set Form Type: %s", exc)

    # Set date range
    logger.info("Setting date range to: %s", date_filter)
    try:
        date_input = page.query_selector('input[readonly][type="text"]')
        if date_input:
            date_input.click()
            page.wait_for_timeout(2000)

            date_options = page.query_selector_all(f'text="{date_filter}"')
            for option in date_options:
                if option.is_visible():
                    option.click()
                    logger.info("Selected date range: %s", date_filter)
                    page.wait_for_timeout(1000)
                    break
        else:
            logger.warning("Date input not found")
    except Exception as exc:
        logger.warning("Could not set date range to %s: %s", date_filter, exc)

    # Click Get Report
    logger.info("Clicking Get Report button...")
    report_button_selectors = [
        "#get-report-btn",
        'button:has-text("Get Report")',
        'button:has-text("Generate Report")',
        'button:has-text("Run Report")',
    ]
    for selector in report_button_selectors:
        element = page.query_selector(selector)
        if element:
            element.click()
            logger.info("Clicked Get Report")
            break

    # Wait for report to generate
    logger.info("Waiting for report generation...")
    page.wait_for_timeout(10000)

    # Set up download listener, then click Export
    with page.expect_download(timeout=45000) as download_info:
        # Open Actions dropdown
        actions_selectors = [
            'button:has-text("Actions")',
            'button.btn-default:has-text("Actions")',
            'button[aria-haspopup="true"]:has-text("Actions")',
            'button:has-text("Export")',
        ]
        actions_opened = False
        for selector in actions_selectors:
            try:
                element = page.query_selector(selector)
                if element and element.is_visible():
                    element.click()
                    actions_opened = True
                    logger.info("Opened Actions dropdown")
                    page.wait_for_timeout(1500)
                    break
            except Exception:
                continue

        if not actions_opened:
            for btn in page.query_selector_all("button"):
                text = btn.text_content() or ""
                if "action" in text.lower():
                    btn.click()
                    actions_opened = True
                    logger.info("Opened Actions via text search")
                    page.wait_for_timeout(1500)
                    break

        # Click Export Report
        export_selectors = [
            'text="Export Report"',
            'a:has-text("Export Report")',
            'button:has-text("Export Report")',
            '[role="menuitem"]:has-text("Export")',
            'text="Export"',
        ]
        export_clicked = False
        for selector in export_selectors:
            try:
                elements = page.query_selector_all(selector)
                for element in elements:
                    if element.is_visible():
                        element.click()
                        export_clicked = True
                        logger.info("Clicked Export Report")
                        break
                if export_clicked:
                    break
            except Exception:
                continue

        if not export_clicked:
            screenshot_path = os.path.join(download_dir, f"error_sr_{date_filter}_{int(time.time())}.png")
            page.screenshot(path=screenshot_path, full_page=True)
            raise Exception(f"Could not click Export Report for SR {date_filter}")

    download = download_info.value
    csv_filename = f"sr-{date_filter.lower().replace(' ', '-')}-{TODAY}.csv"
    csv_path = os.path.join(download_dir, csv_filename)
    download.save_as(csv_path)
    logger.info("Downloaded CSV: %s", csv_path)

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        content = f.read()

    logger.info("CSV has %d lines", content.count("\n"))
    return content


def download_dashboard_report(page, date_filter: str, download_dir: str) -> str:
    """
    Navigate to CommonSKU Sales Dashboard report, apply date filter,
    and download the CSV. Returns the CSV file content as a string.

    The Sales Dashboard includes both Sales Order and In Production
    rows â no Form Type filter needed (unlike the Sales Rep report).
    """
    logger.info("Downloading Dashboard report with date filter: %s", date_filter)

    # Navigate to Sales Dashboard report
    page.goto(f"{COMMONSKU_URL}/report/sales-dashboard", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(5000)

    # Set date range
    logger.info("Setting date range to: %s", date_filter)
    try:
        date_input = page.query_selector('input[readonly][type="text"]')
        if date_input:
            date_input.click()
            page.wait_for_timeout(2000)

            date_options = page.query_selector_all(f'text="{date_filter}"')
            for option in date_options:
                if option.is_visible():
                    option.click()
                    logger.info("Selected date range: %s", date_filter)
                    page.wait_for_timeout(1000)
                    break
        else:
            logger.warning("Date input not found")
    except Exception as exc:
        logger.warning("Could not set date range to %s: %s", date_filter, exc)

    # Click Get Report
    logger.info("Clicking Get Report button...")
    report_button_selectors = [
        "#get-report-btn",
        'button:has-text("Get Report")',
        'button:has-text("Generate Report")',
        'button:has-text("Run Report")',
    ]
    for selector in report_button_selectors:
        element = page.query_selector(selector)
        if element:
            element.click()
            logger.info("Clicked Get Report")
            break

    # Wait for report to generate
    logger.info("Waiting for report generation...")
    page.wait_for_timeout(10000)

    # Set up download listener, then click Export
    with page.expect_download(timeout=45000) as download_info:
        # Open Actions dropdown
        actions_selectors = [
            'button:has-text("Actions")',
            'button.btn-default:has-text("Actions")',
            'button[aria-haspopup="true"]:has-text("Actions")',
            'button:has-text("Export")',
        ]
        actions_opened = False
        for selector in actions_selectors:
            try:
                element = page.query_selector(selector)
                if element and element.is_visible():
                    element.click()
                    actions_opened = True
                    logger.info("Opened Actions dropdown")
                    page.wait_for_timeout(1500)
                    break
            except Exception:
                continue

        if not actions_opened:
            # Fallback: search all buttons
            for btn in page.query_selector_all("button"):
                text = btn.text_content() or ""
                if "action" in text.lower():
                    btn.click()
                    actions_opened = True
                    logger.info("Opened Actions via text search")
                    page.wait_for_timeout(1500)
                    break

        # Click Export Report
        export_selectors = [
            'text="Export Report"',
            'a:has-text("Export Report")',
            'button:has-text("Export Report")',
            '[role="menuitem"]:has-text("Export")',
            'text="Export"',
        ]
        export_clicked = False
        for selector in export_selectors:
            try:
                elements = page.query_selector_all(selector)
                for element in elements:
                    if element.is_visible():
                        element.click()
                        export_clicked = True
                        logger.info("Clicked Export Report")
                        break
                if export_clicked:
                    break
            except Exception:
                continue

        if not export_clicked:
            screenshot_path = os.path.join(download_dir, f"error_dashboard_{date_filter}_{int(time.time())}.png")
            page.screenshot(path=screenshot_path, full_page=True)
            raise Exception(f"Could not click Export Report for {date_filter}")

    download = download_info.value
    csv_filename = f"dashboard-{date_filter.lower().replace(' ', '-')}-{TODAY}.csv"
    csv_path = os.path.join(download_dir, csv_filename)
    download.save_as(csv_path)
    logger.info("Downloaded CSV: %s", csv_path)

    # Read and return content
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        content = f.read()

    logger.info("CSV has %d lines", content.count("\n"))
    return content


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Export executive report CSVs from CommonSKU to Supabase")
    parser.add_argument(
        "--scope",
        choices=["all", "weekly", "monthly", "ytd"],
        default="all",
        help="Which report(s) to download and upload",
    )
    args = parser.parse_args()

    # Determine which jobs to run
    if args.scope == "all":
        jobs_to_run = list(REPORT_JOBS.items())
    else:
        jobs_to_run = [(args.scope, REPORT_JOBS[args.scope])]

    logger.info("=" * 60)
    logger.info("EXECUTIVE REPORT CSV EXPORT")
    logger.info("Date: %s", TODAY)
    logger.info("Scope: %s", args.scope)
    logger.info("Jobs: %s", [j[0] for j in jobs_to_run])
    logger.info("Supabase: %s", SUPABASE_URL)
    logger.info("=" * 60)

    # Ensure download directory
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    results = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            accept_downloads=True,
        )
        page = context.new_page()

        # Login
        login_to_commonsku(page)

        # Download and upsert each report (both SR and Dashboard per date range)
        for job_name, job_config in jobs_to_run:
            date_filter = job_config["date_filter"]
            sr_table = job_config["sr_table"]
            dash_table = job_config["dash_table"]

            # --- Sales Rep report (individual orders with GP%) ---
            sr_key = f"{job_name}_sr"
            logger.info("-" * 40)
            logger.info("JOB: %s SR (filter=%s, table=%s)", job_name, date_filter, sr_table)

            try:
                sr_csv = download_sr_report(page, date_filter, DOWNLOAD_DIR)

                if not sr_csv or not sr_csv.strip():
                    logger.warning("Empty SR CSV for %s, skipping", job_name)
                    results[sr_key] = {"status": "empty", "rows": 0}
                else:
                    sr_rows = upsert_csv_to_supabase(sr_table, sr_csv, TODAY)
                    results[sr_key] = {"status": "success", "rows": sr_rows}
                    logger.info("SUCCESS: %s SR -> %d rows to %s", job_name, sr_rows, sr_table)

            except Exception as exc:
                logger.error("FAILED: %s SR -> %s", job_name, exc)
                results[sr_key] = {"status": "error", "error": str(exc)}
                try:
                    screenshot_path = os.path.join(DOWNLOAD_DIR, f"error_{job_name}_sr_{int(time.time())}.png")
                    page.screenshot(path=screenshot_path, full_page=True)
                    logger.info("Error screenshot: %s", screenshot_path)
                except Exception:
                    pass

            # --- Dashboard report (per-rep In Production totals) ---
            dash_key = f"{job_name}_dash"
            logger.info("-" * 40)
            logger.info("JOB: %s DASH (filter=%s, table=%s)", job_name, date_filter, dash_table)

            try:
                dash_csv = download_dashboard_report(page, date_filter, DOWNLOAD_DIR)

                if not dash_csv or not dash_csv.strip():
                    logger.warning("Empty Dashboard CSV for %s, skipping", job_name)
                    results[dash_key] = {"status": "empty", "rows": 0}
                else:
                    dash_rows = upsert_csv_to_supabase(dash_table, dash_csv, TODAY)
                    results[dash_key] = {"status": "success", "rows": dash_rows}
                    logger.info("SUCCESS: %s DASH -> %d rows to %s", job_name, dash_rows, dash_table)

            except Exception as exc:
                logger.error("FAILED: %s DASH -> %s", job_name, exc)
                results[dash_key] = {"status": "error", "error": str(exc)}
                try:
                    screenshot_path = os.path.join(DOWNLOAD_DIR, f"error_{job_name}_dash_{int(time.time())}.png")
                    page.screenshot(path=screenshot_path, full_page=True)
                    logger.info("Error screenshot: %s", screenshot_path)
                except Exception:
                    pass

        browser.close()

    # Summary
    logger.info("=" * 60)
    logger.info("EXPORT COMPLETE")
    for job_name, result in results.items():
        logger.info("  %s: %s", job_name, result)
    logger.info("=" * 60)

    # Exit with error if any job failed
    if any(r.get("status") == "error" for r in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
