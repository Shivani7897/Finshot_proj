"""
FINSIGHT — FalkorDB Temporal Graph Pipeline
============================================
Step 2: Create schema + ingest all timestamped CSVs into FalkorDB

Graph model:
  Nodes  → Customer, Account, Fact, Policy, Decision, Document, IngestionRun
  Edges  → OWNS_ACCOUNT, HAS_FACT, GOVERNED_BY, MADE_DECISION,
            SUPPORTED_BY (decision←fact), SOURCED_FROM (fact→document),
            RECORDED_IN (decision→ingestion_run)

Every edge carries: valid_from, valid_to, created_at

Usage:
  Make sure FalkorDB is running:
    docker run -p 6379:6379 falkordb/falkordb:latest

  Then run:
    python3 step2_falkordb_ingest.py
"""

import csv
import os
import sys
from datetime import datetime

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: falkordb not installed. Run: pip install falkordb")
    sys.exit(1)

# ─── Config ──────────────────────────────────────────────────────────────────
FALKOR_HOST  = "localhost"
FALKOR_PORT  = 6379
GRAPH_NAME   = "finsight"
DATA_DIR = r"C:\Users\vinpande11\Downloads\finsight_falkordb_pipeline\finsight_output\timestamped_csvs"

BULK_INGEST_TIMESTAMP = "2024-01-02T09:00:00Z"  # transaction time for initial load

# ─── Connection ──────────────────────────────────────────────────────────────
def connect():
    print(f"Connecting to FalkorDB at {FALKOR_HOST}:{FALKOR_PORT} ...")
    db    = FalkorDB(host=FALKOR_HOST, port=FALKOR_PORT)
    graph = db.select_graph(GRAPH_NAME)
    print(f"  Connected. Graph: '{GRAPH_NAME}'")
    return graph

# ─── Helpers ─────────────────────────────────────────────────────────────────
def read_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def safe(val):
    """Return None for empty strings, else the string."""
    if val is None:
        return None
    v = str(val).strip()
    return v if v != "" else None

def run(graph, query, params=None):
    """Execute a Cypher query with optional params dict."""
    try:
        if params:
            return graph.query(query, params)
        return graph.query(query)
    except Exception as e:
        print(f"  QUERY ERROR: {e}")
        print(f"  Query: {query[:200]}")
        raise

# ─── Step 2a: Drop existing graph + create indexes ───────────────────────────
def setup_schema(graph):
    print("\n── Schema setup ─────────────────────────────────────────────────")

    # Indexes for fast lookup on ID fields and temporal columns
    index_statements = [
        "CREATE INDEX FOR (n:Customer)     ON (n.customer_id)",
        "CREATE INDEX FOR (n:Account)      ON (n.account_id)",
        "CREATE INDEX FOR (n:Fact)         ON (n.fact_id)",
        "CREATE INDEX FOR (n:Policy)       ON (n.policy_id)",
        "CREATE INDEX FOR (n:Decision)     ON (n.decision_id)",
        "CREATE INDEX FOR (n:Document)     ON (n.document_id)",
        "CREATE INDEX FOR (n:IngestionRun) ON (n.run_id)",
        # Temporal indexes — critical for point-in-time queries
        "CREATE INDEX FOR (n:Fact)     ON (n.valid_from)",
        "CREATE INDEX FOR (n:Fact)     ON (n.valid_to)",
        "CREATE INDEX FOR (n:Policy)   ON (n.valid_from)",
        "CREATE INDEX FOR (n:Policy)   ON (n.valid_to)",
        "CREATE INDEX FOR (n:Decision) ON (n.valid_from)",
    ]

    for stmt in index_statements:
        try:
            run(graph, stmt)
        except Exception as e:
            # Index may already exist on re-runs — that's fine
            if "already indexed" not in str(e).lower():
                print(f"  Index warning: {e}")
    print("  ✓ Indexes created")

# ─── Step 2b: Ingest Nodes ───────────────────────────────────────────────────
def ingest_customers(graph):
    print("\n── Ingesting Customer nodes ─────────────────────────────────────")
    rows = read_csv("customers.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (c:Customer {customer_id: $customer_id})
        SET   c.name        = $name,
              c.dob         = $dob,
              c.city        = $city,
              c.valid_from  = $valid_from,
              c.valid_to    = $valid_to,
              c.created_at  = $created_at,
              c.updated_at  = $updated_at
        """
        params = {
            "customer_id": r["customer_id"],
            "name":        r["name"],
            "dob":         safe(r.get("dob")),
            "city":        safe(r.get("city")),
            "valid_from":  safe(r["valid_from"]),
            "valid_to":    safe(r["valid_to"]),
            "created_at":  safe(r["created_at"]),
            "updated_at":  safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} Customer nodes")

def ingest_accounts(graph):
    print("\n── Ingesting Account nodes ──────────────────────────────────────")
    rows = read_csv("accounts.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (a:Account {account_id: $account_id})
        SET   a.customer_id  = $customer_id,
              a.account_type = $account_type,
              a.opened_date  = $opened_date,
              a.valid_from   = $valid_from,
              a.valid_to     = $valid_to,
              a.created_at   = $created_at,
              a.updated_at   = $updated_at
        """
        params = {
            "account_id":   r["account_id"],
            "customer_id":  r["customer_id"],
            "account_type": r["account_type"],
            "opened_date":  safe(r.get("opened_date")),
            "valid_from":   safe(r["valid_from"]),
            "valid_to":     safe(r["valid_to"]),
            "created_at":   safe(r["created_at"]),
            "updated_at":   safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} Account nodes")

def ingest_facts(graph):
    print("\n── Ingesting Fact nodes ─────────────────────────────────────────")
    rows = read_csv("facts.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (f:Fact {fact_id: $fact_id})
        SET   f.entity_id   = $entity_id,
              f.predicate   = $predicate,
              f.value       = $value,
              f.confidence  = $confidence,
              f.document_id = $document_id,
              f.valid_from  = $valid_from,
              f.valid_to    = $valid_to,
              f.created_at  = $created_at,
              f.updated_at  = $updated_at
        """
        params = {
            "fact_id":     r["fact_id"],
            "entity_id":   r["entity_id"],
            "predicate":   r["predicate"],
            "value":       r["value"],
            "confidence":  float(r["confidence"]) if r["confidence"] else None,
            "document_id": safe(r.get("document_id")),
            "valid_from":  safe(r["valid_from"]),
            "valid_to":    safe(r["valid_to"]),
            "created_at":  safe(r["created_at"]),
            "updated_at":  safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} Fact nodes")

def ingest_policies(graph):
    print("\n── Ingesting Policy nodes ───────────────────────────────────────")
    rows = read_csv("policies.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (p:Policy {policy_id: $policy_id})
        SET   p.policy_name = $policy_name,
              p.threshold   = $threshold,
              p.valid_from  = $valid_from,
              p.valid_to    = $valid_to,
              p.created_at  = $created_at,
              p.updated_at  = $updated_at
        """
        params = {
            "policy_id":   r["policy_id"],
            "policy_name": r["policy_name"],
            "threshold":   float(r["threshold"]) if r["threshold"] else None,
            "valid_from":  safe(r["valid_from"]),
            "valid_to":    safe(r["valid_to"]),
            "created_at":  safe(r["created_at"]),
            "updated_at":  safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} Policy nodes")

def ingest_decisions(graph):
    print("\n── Ingesting Decision nodes ─────────────────────────────────────")
    rows = read_csv("decisions.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (d:Decision {decision_id: $decision_id})
        SET   d.customer_id    = $customer_id,
              d.decision_type  = $decision_type,
              d.result         = $result,
              d.decision_time  = $decision_time,
              d.model_version  = $model_version,
              d.valid_from     = $valid_from,
              d.valid_to       = $valid_to,
              d.created_at     = $created_at,
              d.updated_at     = $updated_at
        """
        params = {
            "decision_id":   r["decision_id"],
            "customer_id":   r["customer_id"],
            "decision_type": r["decision_type"],
            "result":        r["result"],
            "decision_time": safe(r.get("decision_time")),
            "model_version": safe(r.get("model_version")),
            "valid_from":    safe(r["valid_from"]),
            "valid_to":      safe(r["valid_to"]),
            "created_at":    safe(r["created_at"]),
            "updated_at":    safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} Decision nodes")

def ingest_documents(graph):
    print("\n── Ingesting Document nodes ─────────────────────────────────────")
    rows = read_csv("documents.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (doc:Document {document_id: $document_id})
        SET   doc.document_name = $document_name,
              doc.source_system = $source_system,
              doc.page_no       = $page_no,
              doc.created_date  = $created_date,
              doc.valid_from    = $valid_from,
              doc.valid_to      = $valid_to,
              doc.created_at    = $created_at,
              doc.updated_at    = $updated_at
        """
        params = {
            "document_id":   r["document_id"],
            "document_name": r["document_name"],
            "source_system": r["source_system"],
            "page_no":       int(r["page_no"]) if r["page_no"] else None,
            "created_date":  safe(r.get("created_date")),
            "valid_from":    safe(r["valid_from"]),
            "valid_to":      safe(r["valid_to"]),
            "created_at":    safe(r["created_at"]),
            "updated_at":    safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} Document nodes")

def ingest_ingestion_runs(graph):
    print("\n── Ingesting IngestionRun nodes ─────────────────────────────────")
    rows = read_csv("ingestion_runs.csv")
    count = 0
    for r in rows:
        query = """
        MERGE (ir:IngestionRun {run_id: $run_id})
        SET   ir.run_time      = $run_time,
              ir.source_system = $source_system,
              ir.valid_from    = $valid_from,
              ir.valid_to      = $valid_to,
              ir.created_at    = $created_at,
              ir.updated_at    = $updated_at
        """
        params = {
            "run_id":        r["run_id"],
            "run_time":      safe(r.get("run_time")),
            "source_system": r["source_system"],
            "valid_from":    safe(r["valid_from"]),
            "valid_to":      safe(r["valid_to"]),
            "created_at":    safe(r["created_at"]),
            "updated_at":    safe(r["updated_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} IngestionRun nodes")

# ─── Step 2c: Ingest Edges ───────────────────────────────────────────────────
def ingest_edges_customer_account(graph):
    """Customer -[OWNS_ACCOUNT]-> Account"""
    print("\n── Edges: Customer -[OWNS_ACCOUNT]-> Account ────────────────────")
    rows = read_csv("accounts.csv")
    count = 0
    for r in rows:
        query = """
        MATCH (c:Customer {customer_id: $customer_id})
        MATCH (a:Account  {account_id:  $account_id})
        MERGE (c)-[rel:OWNS_ACCOUNT {account_id: $account_id}]->(a)
        SET   rel.valid_from = $valid_from,
              rel.valid_to   = $valid_to,
              rel.created_at = $created_at
        """
        params = {
            "customer_id": r["customer_id"],
            "account_id":  r["account_id"],
            "valid_from":  safe(r["valid_from"]),
            "valid_to":    safe(r["valid_to"]),
            "created_at":  safe(r["created_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} OWNS_ACCOUNT edges")

def ingest_edges_customer_fact(graph):
    """Customer -[HAS_FACT]-> Fact  (via entity_id on Fact)"""
    print("\n── Edges: Customer -[HAS_FACT]-> Fact ───────────────────────────")
    rows = read_csv("facts.csv")
    count = 0
    for r in rows:
        query = """
        MATCH (c:Customer {customer_id: $entity_id})
        MATCH (f:Fact      {fact_id:     $fact_id})
        MERGE (c)-[rel:HAS_FACT {fact_id: $fact_id}]->(f)
        SET   rel.valid_from = $valid_from,
              rel.valid_to   = $valid_to,
              rel.created_at = $created_at
        """
        params = {
            "entity_id":  r["entity_id"],
            "fact_id":    r["fact_id"],
            "valid_from": safe(r["valid_from"]),
            "valid_to":   safe(r["valid_to"]),
            "created_at": safe(r["created_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} HAS_FACT edges")

def ingest_edges_fact_document(graph):
    """Fact -[SOURCED_FROM]-> Document"""
    print("\n── Edges: Fact -[SOURCED_FROM]-> Document ───────────────────────")
    rows = read_csv("facts.csv")
    count = 0
    for r in rows:
        if not safe(r.get("document_id")):
            continue
        query = """
        MATCH (f:Fact     {fact_id:     $fact_id})
        MATCH (d:Document {document_id: $document_id})
        MERGE (f)-[rel:SOURCED_FROM {fact_id: $fact_id}]->(d)
        SET   rel.valid_from = $valid_from,
              rel.valid_to   = $valid_to,
              rel.created_at = $created_at
        """
        params = {
            "fact_id":     r["fact_id"],
            "document_id": r["document_id"],
            "valid_from":  safe(r["valid_from"]),
            "valid_to":    safe(r["valid_to"]),
            "created_at":  safe(r["created_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} SOURCED_FROM edges")

def ingest_edges_customer_decision(graph):
    """Customer -[MADE_DECISION]-> Decision"""
    print("\n── Edges: Customer -[MADE_DECISION]-> Decision ──────────────────")
    rows = read_csv("decisions.csv")
    count = 0
    for r in rows:
        query = """
        MATCH (c:Customer {customer_id: $customer_id})
        MATCH (d:Decision {decision_id: $decision_id})
        MERGE (c)-[rel:MADE_DECISION {decision_id: $decision_id}]->(d)
        SET   rel.valid_from = $valid_from,
              rel.valid_to   = $valid_to,
              rel.created_at = $created_at
        """
        params = {
            "customer_id": r["customer_id"],
            "decision_id": r["decision_id"],
            "valid_from":  safe(r["valid_from"]),
            "valid_to":    safe(r["valid_to"]),
            "created_at":  safe(r["created_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} MADE_DECISION edges")

def ingest_edges_decision_fact(graph):
    """Decision -[SUPPORTED_BY]-> Fact  (from decision_evidence.csv)"""
    print("\n── Edges: Decision -[SUPPORTED_BY]-> Fact ───────────────────────")
    rows = read_csv("decision_evidence.csv")
    count = 0
    for r in rows:
        query = """
        MATCH (dec:Decision {decision_id: $decision_id})
        MATCH (f:Fact        {fact_id:     $fact_id})
        MERGE (dec)-[rel:SUPPORTED_BY {evidence_key: $evidence_key}]->(f)
        SET   rel.valid_from = $valid_from,
              rel.valid_to   = $valid_to,
              rel.created_at = $created_at
        """
        params = {
            "decision_id":  r["decision_id"],
            "fact_id":      r["fact_id"],
            "evidence_key": r["decision_id"] + "_" + r["fact_id"],
            "valid_from":   safe(r["valid_from"]),
            "valid_to":     safe(r["valid_to"]),
            "created_at":   safe(r["created_at"]),
        }
        run(graph, query, params)
        count += 1
    print(f"  ✓ {count} SUPPORTED_BY edges")

def ingest_edges_policy_governance(graph):
    """Decision -[GOVERNED_BY]-> Policy (match by decision_time within policy validity window)"""
    print("\n── Edges: Decision -[GOVERNED_BY]-> Policy ──────────────────────")
    decisions = read_csv("decisions.csv")
    policies  = read_csv("policies.csv")
    count = 0

    # Map loan/credit decision types to their policy names
    policy_type_map = {
        "LoanApproval":       "LoanRiskThreshold",
        "CreditCardApproval": "CreditCardIncomeLimit",
        "FraudReview":        "FraudAlertThreshold",
    }

    for dec in decisions:
        policy_name = policy_type_map.get(dec["decision_type"])
        if not policy_name:
            continue
        dec_time = safe(dec.get("decision_time")) or safe(dec["valid_from"])

        # Find policy valid at decision_time
        for pol in policies:
            if pol["policy_name"] != policy_name:
                continue
            p_from = safe(pol["valid_from"]) or ""
            p_to   = safe(pol["valid_to"])   or "9999-12-31T00:00:00Z"
            if p_from <= dec_time <= p_to:
                query = """
                MATCH (dec:Decision {decision_id: $decision_id})
                MATCH (pol:Policy   {policy_id:   $policy_id})
                MERGE (dec)-[rel:GOVERNED_BY {decision_id: $decision_id, policy_id: $policy_id}]->(pol)
                SET   rel.valid_from = $valid_from,
                      rel.valid_to   = $valid_to,
                      rel.created_at = $created_at
                """
                params = {
                    "decision_id": dec["decision_id"],
                    "policy_id":   pol["policy_id"],
                    "valid_from":  safe(dec["valid_from"]),
                    "valid_to":    safe(dec["valid_to"]),
                    "created_at":  safe(dec["created_at"]),
                }
                run(graph, query, params)
                count += 1
    print(f"  ✓ {count} GOVERNED_BY edges")

# ─── Step 2d: Verify ingestion ───────────────────────────────────────────────
def verify(graph):
    print("\n── Verification ─────────────────────────────────────────────────")

    node_labels = ["Customer", "Account", "Fact", "Policy", "Decision", "Document", "IngestionRun"]
    for label in node_labels:
        result = run(graph, f"MATCH (n:{label}) RETURN count(n) AS cnt")
        cnt = result.result_set[0][0] if result.result_set else 0
        print(f"  {label:15s}: {cnt} nodes")

    print()
    edge_types = ["OWNS_ACCOUNT", "HAS_FACT", "SOURCED_FROM", "MADE_DECISION", "SUPPORTED_BY", "GOVERNED_BY"]
    for etype in edge_types:
        result = run(graph, f"MATCH ()-[r:{etype}]->() RETURN count(r) AS cnt")
        cnt = result.result_set[0][0] if result.result_set else 0
        print(f"  {etype:20s}: {cnt} edges")

# ─── Step 2e: Sample temporal queries ────────────────────────────────────────
def demo_temporal_queries(graph):
    print("\n── Demo: Temporal queries ───────────────────────────────────────")

    # Q1: What was C001's risk score on 2024-03-01?
    q1 = """
    MATCH (c:Customer {customer_id: 'C001'})-[r:HAS_FACT]->(f:Fact)
    WHERE f.predicate = 'risk_score'
      AND f.valid_from <= '2024-03-01T00:00:00Z'
      AND (f.valid_to = '' OR f.valid_to IS NULL OR f.valid_to >= '2024-03-01T00:00:00Z')
    RETURN f.predicate, f.value, f.valid_from, f.valid_to
    """
    res = run(graph, q1)
    print(f"\n  Q1 — C001 risk_score on 2024-03-01:")
    for row in res.result_set:
        print(f"    predicate={row[0]}, value={row[1]}, from={row[2]}, to={row[3]}")

    # Q2: Which policy governed DEC001?
    q2 = """
    MATCH (dec:Decision {decision_id: 'DEC001'})-[r:GOVERNED_BY]->(pol:Policy)
    RETURN dec.decision_type, dec.result, pol.policy_name, pol.threshold, pol.valid_from, pol.valid_to
    """
    res = run(graph, q2)
    print(f"\n  Q2 — Policy that governed DEC001:")
    for row in res.result_set:
        print(f"    type={row[0]}, result={row[1]}, policy={row[2]}, threshold={row[3]}, policy_window=[{row[4]} → {row[5]}]")

    # Q3: What facts supported DEC002?
    q3 = """
    MATCH (dec:Decision {decision_id: 'DEC002'})-[:SUPPORTED_BY]->(f:Fact)-[:SOURCED_FROM]->(doc:Document)
    RETURN f.fact_id, f.predicate, f.value, f.valid_from, doc.document_name
    """
    res = run(graph, q3)
    print(f"\n  Q3 — Facts + documents behind DEC002 (Approved loan):")
    for row in res.result_set:
        print(f"    fact={row[0]}, {row[1]}={row[2]}, valid_from={row[3]}, doc={row[4]}")

    # Q4: Full timeline of C001's risk score
    q4 = """
    MATCH (c:Customer {customer_id: 'C001'})-[r:HAS_FACT]->(f:Fact)
    WHERE f.predicate = 'risk_score'
    RETURN f.value, f.valid_from, f.valid_to, f.confidence
    ORDER BY f.valid_from
    """
    res = run(graph, q4)
    print(f"\n  Q4 — Full risk_score timeline for C001:")
    for row in res.result_set:
        to_str = row[2] if row[2] else "NOW"
        print(f"    score={row[1]}  [{row[1]} → {to_str}]  confidence={row[3]}")

    # Q5: All currently active facts (valid_to empty = still active)
    q5 = """
    MATCH (c:Customer)-[:HAS_FACT]->(f:Fact)
    WHERE (f.valid_to = '' OR f.valid_to IS NULL)
    RETURN c.customer_id, c.name, f.predicate, f.value
    ORDER BY c.customer_id
    """
    res = run(graph, q5)
    print(f"\n  Q5 — All currently active facts:")
    for row in res.result_set:
        print(f"    {row[0]} ({row[1]}): {row[2]} = {row[3]}")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  FinSight — FalkorDB Temporal Graph Ingestion")
    print("=" * 60)

    graph = connect()

    # Nodes
    setup_schema(graph)
    ingest_customers(graph)
    ingest_accounts(graph)
    ingest_facts(graph)
    ingest_policies(graph)
    ingest_decisions(graph)
    ingest_documents(graph)
    ingest_ingestion_runs(graph)

    # Edges
    ingest_edges_customer_account(graph)
    ingest_edges_customer_fact(graph)
    ingest_edges_fact_document(graph)
    ingest_edges_customer_decision(graph)
    ingest_edges_decision_fact(graph)
    ingest_edges_policy_governance(graph)

    # Verify
    verify(graph)

    # Demo temporal queries
    demo_temporal_queries(graph)

    print("\n" + "=" * 60)
    print("  ✓ FinSight graph fully ingested and verified!")
    print("=" * 60)

if __name__ == "__main__":
    main()
