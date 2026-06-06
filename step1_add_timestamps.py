"""
STEP 1 — Add bi-temporal timestamps to all FinSight CSVs
=========================================================
Two time axes on every row:
  valid_from  / valid_to   → when the fact was TRUE in the real world
  created_at  / updated_at → when this record was INGESTED (transaction time)

Rules applied:
  • Nodes  → valid_from = earliest business event; valid_to = NULL (still active) or closure date
  • Edges  → valid_from/valid_to already present where meaningful; created_at = ingestion run time
  • NULL valid_to means the record is CURRENTLY ACTIVE
  • created_at for bulk-load rows = the ingestion_run timestamp that first created them
"""

import pandas as pd
import os

SRC  = "C:/Users/vinpande11/Downloads/temporal_graphrag_dataset"
DEST = "C:/Users/vinpande11/Downloads/finsight_falkordb_pipeline/finsight_output/timestamped_csvs"
os.makedirs(DEST, exist_ok=True)

# ── helpers ──────────────────────────────────────────────────────────────────
def iso(val):
    """Return ISO datetime string or empty string."""
    return str(val) + "T00:00:00Z" if val and str(val).strip() else ""

def ts(date_str, time="00:00:00"):
    if not date_str or str(date_str).strip() == "":
        return ""
    return str(date_str).strip() + "T" + time + "Z"

# ── 1. customers.csv ─────────────────────────────────────────────────────────
# Nodes: customers were created when they first opened an account (opened_date).
# valid_to = NULL (all are currently active customers).
# created_at = first ingestion run (RUN001 / 2024-01-02) for all existing records.
print("Processing customers.csv ...")
customers = pd.read_csv(f"{SRC}/customers.csv")

# Map each customer to their account opened_date as the business valid_from
account_open = pd.read_csv(f"{SRC}/accounts.csv")[["customer_id", "opened_date"]]
customers = customers.merge(account_open, on="customer_id", how="left")

customers["valid_from"]  = customers["opened_date"].apply(lambda d: ts(d))
customers["valid_to"]    = ""          # all currently active
customers["created_at"]  = "2024-01-02T09:00:00Z"   # bulk ingestion RUN001
customers["updated_at"]  = "2024-01-02T09:00:00Z"

customers = customers.drop(columns=["opened_date"])
customers.to_csv(f"{DEST}/customers.csv", index=False)
print(f"  → {len(customers)} rows written")

# ── 2. accounts.csv ───────────────────────────────────────────────────────────
# Nodes: valid_from = opened_date; valid_to = NULL (all accounts still open).
# created_at = ingestion run matching the opened_date period.
print("Processing accounts.csv ...")
accounts = pd.read_csv(f"{SRC}/accounts.csv")

accounts["valid_from"]  = accounts["opened_date"].apply(lambda d: ts(d))
accounts["valid_to"]    = ""
accounts["created_at"]  = "2024-01-02T09:00:00Z"
accounts["updated_at"]  = "2024-01-02T09:00:00Z"

accounts.to_csv(f"{DEST}/accounts.csv", index=False)
print(f"  → {len(accounts)} rows written")

# ── 3. facts.csv ──────────────────────────────────────────────────────────────
# Already has valid_from / valid_to (business time).
# Add created_at = the ingestion run that first recorded each fact.
# Map: facts referencing D001/D003/D004/D008/D010 → RUN001 (2024-01-02)
#       facts referencing D005                     → RUN002 (2024-04-01)
#       facts referencing D002/D007/D009/D011      → RUN003 (2024-07-01) or nearest
print("Processing facts.csv ...")
facts = pd.read_csv(f"{SRC}/facts.csv")

doc_to_run = {
    "D001": "2024-01-02T09:00:00Z",
    "D002": "2024-07-02T09:00:00Z",
    "D003": "2024-01-05T09:00:00Z",
    "D004": "2024-01-03T09:00:00Z",
    "D005": "2024-04-01T09:00:00Z",
    "D006": "2024-02-01T09:00:00Z",
    "D007": "2024-06-01T09:00:00Z",
    "D008": "2024-01-10T09:00:00Z",
    "D009": "2024-09-01T09:00:00Z",
    "D010": "2024-01-01T09:00:00Z",
    "D011": "2024-02-16T09:00:00Z",
}

facts["valid_from"]  = facts["valid_from"].apply(lambda d: ts(d))
facts["valid_to"]    = facts["valid_to"].apply(lambda d: ts(d) if pd.notna(d) and str(d).strip() != "" else "")
facts["created_at"]  = facts["document_id"].map(doc_to_run).fillna("2024-01-02T09:00:00Z")
facts["updated_at"]  = facts["created_at"]

facts.to_csv(f"{DEST}/facts.csv", index=False)
print(f"  → {len(facts)} rows written")

# ── 4. policies.csv ───────────────────────────────────────────────────────────
# Already has valid_from / valid_to. Add created_at = when policy was enacted.
print("Processing policies.csv ...")
policies = pd.read_csv(f"{SRC}/policies.csv")

policies["valid_from"]  = policies["valid_from"].apply(lambda d: ts(d))
policies["valid_to"]    = policies["valid_to"].apply(lambda d: ts(d) if pd.notna(d) and str(d).strip() != "" else "")
policies["created_at"]  = policies["valid_from"]   # policy created when it became active
policies["updated_at"]  = policies["created_at"]

policies.to_csv(f"{DEST}/policies.csv", index=False)
print(f"  → {len(policies)} rows written")

# ── 5. decisions.csv ──────────────────────────────────────────────────────────
# decision_time is the point-in-time event (valid_from).
# Decisions are immutable events, valid_to = NULL.
# created_at = decision_time (recorded at time of decision).
print("Processing decisions.csv ...")
decisions = pd.read_csv(f"{SRC}/decisions.csv")

decisions["valid_from"]  = decisions["decision_time"].apply(lambda d: ts(d))
decisions["valid_to"]    = ""           # immutable event — no expiry
decisions["created_at"]  = decisions["valid_from"]
decisions["updated_at"]  = decisions["valid_from"]

decisions.to_csv(f"{DEST}/decisions.csv", index=False)
print(f"  → {len(decisions)} rows written")

# ── 6. documents.csv ─────────────────────────────────────────────────────────
# created_date is when the document was generated → valid_from.
# valid_to = NULL (documents don't expire; they can be superseded).
print("Processing documents.csv ...")
documents = pd.read_csv(f"{SRC}/documents.csv")

documents["valid_from"]  = documents["created_date"].apply(lambda d: ts(d))
documents["valid_to"]    = ""
documents["created_at"]  = documents["valid_from"]
documents["updated_at"]  = documents["valid_from"]

documents.to_csv(f"{DEST}/documents.csv", index=False)
print(f"  → {len(documents)} rows written")

# ── 7. decision_evidence.csv ─────────────────────────────────────────────────
# Junction / edge table: link between decision and fact.
# valid_from = decision_time of the referenced decision.
# created_at = same (evidence recorded when decision was made).
print("Processing decision_evidence.csv ...")
evidence = pd.read_csv(f"{SRC}/decision_evidence.csv")
dec_times = decisions[["decision_id", "valid_from"]].rename(columns={"valid_from": "decision_valid_from"})
evidence = evidence.merge(dec_times, on="decision_id", how="left")

evidence["valid_from"]  = evidence["decision_valid_from"]
evidence["valid_to"]    = ""
evidence["created_at"]  = evidence["valid_from"]
evidence["updated_at"]  = evidence["valid_from"]

evidence = evidence.drop(columns=["decision_valid_from"])
evidence.to_csv(f"{DEST}/decision_evidence.csv", index=False)
print(f"  → {len(evidence)} rows written")

# ── 8. ingestion_runs.csv ────────────────────────────────────────────────────
# Metadata table — run_time is its own timestamp.
print("Processing ingestion_runs.csv ...")
runs = pd.read_csv(f"{SRC}/ingestion_runs.csv")
runs["valid_from"]  = runs["run_time"].apply(lambda d: ts(d))
runs["valid_to"]    = ""
runs["created_at"]  = runs["valid_from"]
runs["updated_at"]  = runs["valid_from"]

runs.to_csv(f"{DEST}/ingestion_runs.csv", index=False)
print(f"  → {len(runs)} rows written")

# ── Summary ──────────────────────────────────────────────────────────────────
print("\n✓ All timestamped CSVs written to:", DEST)
print("\nColumn summary per file:")
for fname in sorted(os.listdir(DEST)):
    df = pd.read_csv(f"{DEST}/{fname}")
    print(f"  {fname:35s} {list(df.columns)}")
