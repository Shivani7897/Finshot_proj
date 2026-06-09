import pandas as pd
import os

# Set your localized source and destination directory locations
SRC  = r"C:\Users\shisingh77\Downloads\temporal_graphrag_dataset"
DEST = r"C:\Users\shisingh77\Downloads\temporal_graphrag_dataset\dataset with timestamp"
os.makedirs(DEST, exist_ok=True)

# ── TIMESTAMP COMPLIANCE HELPERS ─────────────────────────────────────────────
def ts(date_str, time="00:00:00"):
    """Converts a standard YYYY-MM-DD date string into a strict ISO-8601 UTC format."""
    if not date_str or pd.isna(date_str) or str(date_str).strip() == "" or str(date_str).strip() == "nan":
        return ""
    # Strip any existing times or spaces and append strict UTC format
    return str(date_str).strip().split()[0] + "T" + time + "Z"

# Baseline Ingestion Time across the dataset
BASE_TRANSACTION_TIME = "2026-01-10T10:00:00Z"

# ── 1. customer.csv ──────────────────────────────────────────────────────────
# Nodes: Customer valid_from is anchored to the earliest date they opened an account.
print("Processing customer.csv ...")
customer = pd.read_csv(f"{SRC}/customer.csv")
account_ref = pd.read_csv(f"{SRC}/account.csv")[["customer_id", "account_open_date"]]

# Sort to get the absolute earliest account opening date per customer
earliest_open = account_ref.sort_values("account_open_date").groupby("customer_id").first().reset_index()
customer = customer.merge(earliest_open, on="customer_id", how="left")

# Fallback to 'record_updated_at' if they don't have an account open date linked
customer["valid_from"] = customer["account_open_date"].fillna(customer["record_updated_at"]).apply(lambda d: ts(d))
customer["valid_to"]   = ""  # Currently active customer states
customer["created_at"] = BASE_TRANSACTION_TIME
customer["updated_at"] = BASE_TRANSACTION_TIME

customer = customer.drop(columns=["account_open_date"])
customer.to_csv(f"{DEST}/customer.csv", index=False)
print(f"  → {len(customer)} rows processed & timestamped.")

# ── 2. account.csv ───────────────────────────────────────────────────────────
# Nodes: valid_from matches the business 'account_open_date' field directly.
print("Processing account.csv ...")
account = pd.read_csv(f"{SRC}/account.csv")

account["valid_from"] = account["account_open_date"].apply(lambda d: ts(d))
account["valid_to"]   = ""  # Active accounts
account["created_at"] = BASE_TRANSACTION_TIME
account["updated_at"] = BASE_TRANSACTION_TIME

account.to_csv(f"{DEST}/account.csv", index=False)
print(f"  → {len(account)} rows processed & timestamped.")

# ── 3. loan_account.csv ──────────────────────────────────────────────────────
# Nodes: Since there is no distinct open date, valid_from defaults to system timeline base.
print("Processing loan_account.csv ...")
loan_account = pd.read_csv(f"{SRC}/loan_account.csv")

loan_account["valid_from"] = ts("2026-01-10")  # Baseline system initialization point
loan_account["valid_to"]   = ""  # Active timeline status
loan_account["created_at"] = BASE_TRANSACTION_TIME
loan_account["updated_at"] = BASE_TRANSACTION_TIME

loan_account.to_csv(f"{DEST}/loan_account.csv", index=False)
print(f"  → {len(loan_account)} rows processed & timestamped.")

# ── 4. client.csv ────────────────────────────────────────────────────────────
# Nodes: Wealth Management Clients. valid_from maps to 'client_since_date'.
print("Processing client.csv ...")
client = pd.read_csv(f"{SRC}/client.csv")

client["valid_from"] = client["client_since_date"].apply(lambda d: ts(d))
client["valid_to"]   = ""
client["created_at"] = BASE_TRANSACTION_TIME
client["updated_at"] = BASE_TRANSACTION_TIME

client.to_csv(f"{DEST}/client.csv", index=False)
print(f"  → {len(client)} rows processed & timestamped.")

# ── 5. portfolio.csv ─────────────────────────────────────────────────────────
# Edges/Nodes: Portfolios valid_from matches when their associated client joined the firm.
print("Processing portfolio.csv ...")
portfolio = pd.read_csv(f"{SRC}/portfolio.csv")
client_ref = pd.read_csv(f"{SRC}/client.csv")[["wm_client_id", "client_since_date"]]

portfolio = portfolio.merge(client_ref, on="wm_client_id", how="left")
portfolio["valid_from"] = portfolio["client_since_date"].apply(lambda d: ts(d))
portfolio["valid_to"]   = ""
portfolio["created_at"] = BASE_TRANSACTION_TIME
portfolio["updated_at"] = BASE_TRANSACTION_TIME

portfolio = portfolio.drop(columns=["client_since_date"])
portfolio.to_csv(f"{DEST}/portfolio.csv", index=False)
print(f"  → {len(portfolio)} rows processed & timestamped.")

# ── 6. employee.csv ──────────────────────────────────────────────────────────
# Nodes: valid_from maps to their business 'hire_date'.
print("Processing employee.csv ...")
employee = pd.read_csv(f"{SRC}/employee.csv")

employee["valid_from"] = employee["hire_date"].apply(lambda d: ts(d))
employee["valid_to"]   = ""
employee["created_at"] = BASE_TRANSACTION_TIME
employee["updated_at"] = BASE_TRANSACTION_TIME

employee.to_csv(f"{DEST}/employee.csv", index=False)
print(f"  → {len(employee)} rows processed & timestamped.")

# ── 7. advisor.csv ───────────────────────────────────────────────────────────
# Nodes: Advisors map their valid_from back to their underlying employee record 'hire_date'.
print("Processing advisor.csv ...")
advisor = pd.read_csv(f"{SRC}/advisor.csv")
emp_ref = pd.read_csv(f"{SRC}/employee.csv")[["employee_id", "hire_date"]]

advisor = advisor.merge(emp_ref, on="employee_id", how="left")
advisor["valid_from"] = advisor["hire_date"].apply(lambda d: ts(d))
advisor["valid_to"]   = ""
advisor["created_at"]  = BASE_TRANSACTION_TIME
advisor["updated_at"]  = BASE_TRANSACTION_TIME

advisor = advisor.drop(columns=["hire_date"])
advisor.to_csv(f"{DEST}/advisor.csv", index=False)
print(f"  → {len(advisor)} rows processed & timestamped.")

# ── 8. branch.csv ────────────────────────────────────────────────────────────
# Nodes: Static branch infrastructure.
print("Processing branch.csv ...")
branch = pd.read_csv(f"{SRC}/branch.csv")

branch["valid_from"] = ts("2011-03-12")  # Earliest active firm system date (matches original employee hire baseline)
branch["valid_to"]   = ""
branch["created_at"] = BASE_TRANSACTION_TIME
branch["updated_at"] = BASE_TRANSACTION_TIME

branch.to_csv(f"{DEST}/branch.csv", index=False)
print(f"  → {len(branch)} rows processed & timestamped.")

# ── RUNTIME SUMMARY ───────────────────────────────────────────────────────────
print("\n✓ Bi-temporal pipeline modification completed! Target location:", DEST)
print("Updated structural file maps:")
for file_name in sorted(os.listdir(DEST)):
    if file_name.endswith('.csv'):
        temp_df = pd.read_csv(f"{DEST}/{file_name}")
        print(f"  {file_name:25s} Columns: {list(temp_df.columns)}")
