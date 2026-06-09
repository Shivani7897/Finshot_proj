"""
FalkorDB Graph Ingestion Pipeline — Indian Banking Dataset
==========================================================
Nodes  : Branch, Employee, Advisor, Customer, Client, Account, LoanAccount, Portfolio
Edges  : HAS_EMPLOYEE, IS_ADVISOR, SERVES_CUSTOMER, UPGRADES_TO,
         MANAGES_CLIENT, OWNS_PORTFOLIO, OWNS_DEPOSIT, OWES_LIABILITY
"""

import os
import pandas as pd
from falkordb import FalkorDB

# ==========================================
# 1. CONFIGURATION
# ==========================================
BASE_DIR   = r"C:\Users\shisingh77\Downloads\temporal_graphrag_dataset\dataset with timestamp"   # 
FALKOR_PORT = 6379
GRAPH_NAME  = "IndianBankingGraph"

db    = FalkorDB(host="localhost", port=FALKOR_PORT)
graph = db.select_graph(GRAPH_NAME)
print(f"✅ Connected to FalkorDB  →  Graph: '{GRAPH_NAME}'")


# ==========================================
# 2. SCHEMA DEFINITION
# ==========================================
NODE_SCHEMA = {
    # file_name          label           id_column
    "branch.csv":       ("Branch",       "branch_id"),
    "employee.csv":     ("Employee",     "employee_id"),
    "advisor.csv":      ("Advisor",      "advisor_id"),
    "customer.csv":     ("Customer",     "customer_id"),
    "client.csv":       ("Client",       "wm_client_id"),
    "account.csv":      ("Account",      "account_id"),
    "loan_account.csv": ("LoanAccount",  "loan_id"),
    "portfolio.csv":    ("Portfolio",    "portfolio_id"),
}

EDGE_SCHEMA = [
    # Stream 1 — Branch Hierarchy
    {
        "file":     "employee.csv",
        "from_lbl": "Branch",    "from_id": "branch_id",
        "to_lbl":   "Employee",  "to_id":   "employee_id",
        "rel":      "HAS_EMPLOYEE",
    },
    {
        "file":     "advisor.csv",
        "from_lbl": "Employee",  "from_id": "employee_id",
        "to_lbl":   "Advisor",   "to_id":   "advisor_id",
        "rel":      "IS_ADVISOR",
    },
    {
        "file":     "customer.csv",
        "from_lbl": "Branch",    "from_id": "branch_id",
        "to_lbl":   "Customer",  "to_id":   "customer_id",
        "rel":      "SERVES_CUSTOMER",
    },

    # Stream 2 — Wealth Management Advisory
    {
        "file":     "client.csv",
        "from_lbl": "Customer",  "from_id": "retail_customer_id",
        "to_lbl":   "Client",    "to_id":   "wm_client_id",
        "rel":      "UPGRADES_TO",
    },
    {
        "file":     "client.csv",
        "from_lbl": "Advisor",   "from_id": "advisor_id",
        "to_lbl":   "Client",    "to_id":   "wm_client_id",
        "rel":      "MANAGES_CLIENT",
    },
    {
        "file":     "portfolio.csv",
        "from_lbl": "Client",    "from_id": "wm_client_id",
        "to_lbl":   "Portfolio", "to_id":   "portfolio_id",
        "rel":      "OWNS_PORTFOLIO",
    },

    # Stream 3 — Retail Banking Products
    {
        "file":     "account.csv",
        "from_lbl": "Customer",    "from_id": "customer_id",
        "to_lbl":   "Account",     "to_id":   "account_id",
        "rel":      "OWNS_DEPOSIT",
    },
    {
        "file":     "loan_account.csv",
        "from_lbl": "Customer",    "from_id": "customer_id",
        "to_lbl":   "LoanAccount", "to_id":   "loan_id",
        "rel":      "OWES_LIABILITY",
    },
]


# ==========================================
# 3. CREATE INDEXES FOR FAST LOOKUPS
# ==========================================
print("\n--- Creating Indexes ---")
for file_name, (label, id_col) in NODE_SCHEMA.items():
    try:
        graph.query(f"CREATE INDEX FOR (n:{label}) ON (n.{id_col})")
        print(f"  ✓ Index created → {label}.{id_col}")
    except Exception:
        print(f"  ↩ Index already exists → {label}.{id_col}  (skipped)")


# ==========================================
# 4. NODE INGESTION
# ==========================================
def ingest_nodes(base_dir, file_name, label, id_col):
    full_path = os.path.join(base_dir, file_name)
    if not os.path.exists(full_path):
        print(f"   File not found: {full_path}  (skipping)")
        return

    df = pd.read_csv(full_path)
    df = df.where(pd.notnull(df), None)   # NaN → None (clean nulls)

    query = f"""
    MERGE (n:{label} {{{id_col}: $id_val}})
    SET n += $props
    """

    count = 0
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        # Convert everything to safe string/number types
        props = {
            k: (str(v) if v is not None else None)
            for k, v in row_dict.items()
        }
        id_val = str(row_dict[id_col])
        graph.query(query, {"id_val": id_val, "props": props})
        count += 1

    print(f"  ✓ {label:15s} → {count} nodes upserted  ({file_name})")


# ==========================================
# 5. EDGE INGESTION
# ==========================================
def ingest_edges(base_dir, rule):
    full_path = os.path.join(base_dir, rule["file"])
    if not os.path.exists(full_path):
        print(f"   File not found: {full_path}  (skipping)")
        return

    df = pd.read_csv(full_path)

    query = f"""
    MATCH (src:{rule['from_lbl']} {{{rule['from_id']}: $from_key}})
    MATCH (dst:{rule['to_lbl']}  {{{rule['to_id']}:   $to_key}})
    MERGE (src)-[:{rule['rel']}]->(dst)
    """

    count = 0
    skipped = 0
    for _, row in df.iterrows():
        from_val = row.get(rule["from_id"])
        to_val   = row.get(rule["to_id"])

        # Skip rows where either key is missing
        if pd.isna(from_val) or pd.isna(to_val):
            skipped += 1
            continue

        graph.query(query, {
            "from_key": str(from_val),
            "to_key":   str(to_val),
        })
        count += 1

    rel = rule["rel"]
    print(f"  ✓ [:{rel:20s}] → {count} edges created  ({skipped} skipped)")


# ==========================================
# 6. ORCHESTRATION
# ==========================================
print("\n--- Phase 1: Ingesting Nodes ---")
for file_name, (label, id_col) in NODE_SCHEMA.items():
    ingest_nodes(BASE_DIR, file_name, label, id_col)

print("\n--- Phase 2: Ingesting Edges ---")
for rule in EDGE_SCHEMA:
    ingest_edges(BASE_DIR, rule)

print("\n Banking Knowledge Graph successfully loaded into FalkorDB!")
print(f"   Graph name : {GRAPH_NAME}")
print(f"   Port      : {FALKOR_PORT}")



# ==========================================
# 7. QUICK VERIFICATION QUERIES
# ==========================================
print("\n--- Verification Counts ---")
node_labels = [label for (label, _) in NODE_SCHEMA.values()]
for label in node_labels:
    result = graph.query(f"MATCH (n:{label}) RETURN count(n) AS cnt")
    cnt = result.result_set[0][0] if result.result_set else 0
    print(f"  {label:15s} : {cnt} nodes")
