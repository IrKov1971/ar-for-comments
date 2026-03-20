"""
AR for comments — QBO to Google Sheets sync
"""
import os, json, base64, httpx
from datetime import date, datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_API_BASE = "https://quickbooks.api.intuit.com/v3/company"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_TAB = "AR"
DATA_START_ROW = 2
DAYS_AHEAD = 30
def get_qbo_access_token(client_id, client_secret, refresh_token):
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    r = httpx.post(QBO_TOKEN_URL, headers={"Authorization": f"Basic {auth}", "Accept": "application/json"}, data={"grant_type": "refresh_token", "refresh_token": refresh_token}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]
def qbo_query(realm_id, access_token, query):
    r = httpx.get(f"{QBO_API_BASE}/{realm_id}/query", headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, params={"query": query}, timeout=60)
    r.raise_for_status()
    return r.json()
def fetch_unpaid_invoices(realm_id, access_token):
    invoices, start, page = [], 1, 1000
    while True:
        data = qbo_query(realm_id, access_token, f"SELECT * FROM Invoice WHERE Balance > '0' STARTPOSITION {start} MAXRESULTS {page}")
        batch = data.get("QueryResponse", {}).get("Invoice", []) or []
        if not batch: break
        invoices.extend(batch)
        if len(batch) < page: break
        start += page
    return invoices
def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").date()
def compute_status(due_date, today):
    delta = (due_date - today).days
    if delta < 0: return f"overdue {abs(delta)} days"
    elif delta == 0: return "due in 0 days"
    else: return f"due in {delta} days"
def filter_and_sort(invoices):
    today = date.today()
    result = [inv for inv in invoices if float(inv.get("Balance", 0) or 0) > 0 and inv.get("DueDate") and (parse_date(inv["DueDate"]) - today).days <= DAYS_AHEAD]
    result.sort(key=lambda inv: (0, (parse_date(inv["DueDate"]) - today).days) if (parse_date(inv["DueDate"]) - today).days < 0 else (1, (parse_date(inv["DueDate"]) - today).days))
    return result
def format_row(inv):
    today = date.today()
    balance = float(inv.get("Balance", 0) or 0)
    due = parse_date(inv.get("DueDate", ""))
    return [inv.get("CustomerRef", {}).get("name", ""), f"${balance:,.2f}", f"{due.month}/{due.day}/{due.year}", compute_status(due, today), inv.get("DocNumber", "")]
def get_sheets_service(sa_json):
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)
def read_sheet(service, spreadsheet_id):
    return service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!A:H").execute().get("values", [])
def update_sheet(service, spreadsheet_id, invoices):
    current_rows = read_sheet(service, spreadsheet_id)
    existing = {row[4]: i for i, row in enumerate(current_rows) if i > 0 and len(row) >= 5 and row[4]}
    new_map = {inv["DocNumber"]: inv for inv in invoices}
    new_nums, existing_nums = set(new_map), set(existing)
    to_delete = existing_nums - new_nums
    kept_rows = []
    for i, row in enumerate(current_rows):
        if i == 0: continue
        inv_num = row[4] if len(row) >= 5 else ""
        if inv_num in to_delete: continue
        if inv_num in new_map:
            fgh = row[5:8] if len(row) >= 6 else []
            while len(fgh) < 3: fgh.append("")
            kept_rows.append((inv_num, format_row(new_map[inv_num]) + fgh))
        else:
            while len(row) < 8: row.append("")
            kept_rows.append((None, row[:8]))
    for inv in invoices:
        if inv["DocNumber"] in (new_nums - existing_nums):
            kept_rows.append((inv["DocNumber"], format_row(inv) + ["", "", ""]))
    today = date.today()
    def sort_key(item):
        try:
            due = datetime.strptime(item[1][2], "%m/%d/%Y").date()
            d = (due - today).days
            return (0, d) if d < 0 else (1, d)
        except: return (2, 0)
    kept_rows.sort(key=sort_key)
    rows_to_clear = max(len(current_rows) - 1, len(kept_rows)) + 5
    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!A{DATA_START_ROW}:H{DATA_START_ROW + rows_to_clear}").execute()
    if kept_rows:
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!A{DATA_START_ROW}", valueInputOption="USER_ENTERED", body={"values": [r for _, r in kept_rows]}).execute()
    return len(kept_rows), len(to_delete), len(new_nums - existing_nums)
def main():
    print(f"🚀 AR sync — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    access_token = get_qbo_access_token(os.environ["QBO_CLIENT_ID"], os.environ["QBO_CLIENT_SECRET"], os.environ["QBO_REFRESH_TOKEN"])
    all_invoices = fetch_unpaid_invoices(os.environ["QBO_REALM_ID"], access_token)
    filtered = filter_and_sort(all_invoices)
    print(f"✅ {len(all_invoices)} unpaid, {len(filtered)} after filter")
    service = get_sheets_service(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    kept, deleted, added = update_sheet(service, os.environ["SPREADSHEET_ID"], filtered)
    print(f"✅ Done — {kept} rows, {added} added, {deleted} removed")
if __name__ == "__main__":
    main()
