"""
FINSIGHT — Temporal GraphRAG Query Engine
==========================================
Step 3: Query the FalkorDB temporal graph using point-in-time Cypher.
        This is the retriever layer that plugs into any LLM pipeline.

Usage:
  python3 step3_temporal_rag.py

Temporal query patterns implemented:
  1. point_in_time_facts(customer_id, query_time)
     → What facts were true about a customer at a given date?

  2. decision_context(decision_id)
     → Full context of a decision: facts, policy, documents

  3. fact_timeline(customer_id, predicate)
     → Complete history of a fact changing over time

  4. policy_at_time(policy_name, query_time)
     → Which version of a policy was active at a date?

  5. currently_active_facts()
     → Snapshot of all currently active facts across all customers

  6. explain_decision(decision_id)
     → RAG-ready context string for "why was this decision made?"
"""

import sys
from datetime import datetime

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: falkordb not installed. Run: pip install falkordb")
    sys.exit(1)

FALKOR_HOST = "localhost"
FALKOR_PORT = 6379
GRAPH_NAME  = "finsight"


# ─── Connection ──────────────────────────────────────────────────────────────
def connect():
    db    = FalkorDB(host=FALKOR_HOST, port=FALKOR_PORT)
    graph = db.select_graph(GRAPH_NAME)
    return graph


def run(graph, query, params=None):
    try:
        if params:
            return graph.query(query, params)
        return graph.query(query)
    except Exception as e:
        print(f"  QUERY ERROR: {e}")
        raise


# ─── Temporal Query Functions ────────────────────────────────────────────────

def point_in_time_facts(graph, customer_id: str, query_time: str) -> list[dict]:
    """
    Return all facts that were TRUE about a customer at query_time.
    Bi-temporal filter: valid_from <= query_time AND (valid_to IS NULL/empty OR valid_to >= query_time)
    """
    query = """
    MATCH (c:Customer {customer_id: $cid})-[r:HAS_FACT]->(f:Fact)
    WHERE f.valid_from <= $qt
      AND (f.valid_to = '' OR f.valid_to IS NULL OR f.valid_to >= $qt)
    RETURN f.fact_id, f.predicate, f.value, f.confidence,
           f.valid_from, f.valid_to, f.document_id
    ORDER BY f.predicate, f.valid_from
    """
    result = run(graph, query, {"cid": customer_id, "qt": query_time})
    rows = []
    for row in result.result_set:
        rows.append({
            "fact_id":    row[0],
            "predicate":  row[1],
            "value":      row[2],
            "confidence": row[3],
            "valid_from": row[4],
            "valid_to":   row[5] or "CURRENT",
            "document_id": row[6],
        })
    return rows


def decision_context(graph, decision_id: str) -> dict:
    """
    Full subgraph context for a decision:
    - The decision itself
    - Supporting facts (and their documents)
    - Governing policy
    """
    # Decision + customer
    q_dec = """
    MATCH (c:Customer)-[:MADE_DECISION]->(d:Decision {decision_id: $did})
    RETURN c.customer_id, c.name, d.decision_type, d.result,
           d.decision_time, d.model_version, d.valid_from
    """
    dec_res = run(graph, q_dec, {"did": decision_id})
    if not dec_res.result_set:
        return {}

    row = dec_res.result_set[0]
    context = {
        "decision_id":   decision_id,
        "customer_id":   row[0],
        "customer_name": row[1],
        "decision_type": row[2],
        "result":        row[3],
        "decision_time": row[4],
        "model_version": row[5],
        "valid_from":    row[6],
        "supporting_facts": [],
        "governing_policy": None,
    }

    # Supporting facts + documents
    q_facts = """
    MATCH (d:Decision {decision_id: $did})-[:SUPPORTED_BY]->(f:Fact)
    OPTIONAL MATCH (f)-[:SOURCED_FROM]->(doc:Document)
    RETURN f.fact_id, f.predicate, f.value, f.confidence,
           f.valid_from, f.valid_to, doc.document_name, doc.source_system
    """
    facts_res = run(graph, q_facts, {"did": decision_id})
    for frow in facts_res.result_set:
        context["supporting_facts"].append({
            "fact_id":    frow[0],
            "predicate":  frow[1],
            "value":      frow[2],
            "confidence": frow[3],
            "valid_from": frow[4],
            "valid_to":   frow[5] or "CURRENT",
            "document":   frow[6],
            "source":     frow[7],
        })

    # Governing policy
    q_pol = """
    MATCH (d:Decision {decision_id: $did})-[:GOVERNED_BY]->(p:Policy)
    RETURN p.policy_id, p.policy_name, p.threshold, p.valid_from, p.valid_to
    """
    pol_res = run(graph, q_pol, {"did": decision_id})
    if pol_res.result_set:
        pr = pol_res.result_set[0]
        context["governing_policy"] = {
            "policy_id":   pr[0],
            "policy_name": pr[1],
            "threshold":   pr[2],
            "valid_from":  pr[3],
            "valid_to":    pr[4] or "CURRENT",
        }

    return context


def fact_timeline(graph, customer_id: str, predicate: str) -> list[dict]:
    """Return all versions of a fact for a customer, ordered by time."""
    query = """
    MATCH (c:Customer {customer_id: $cid})-[:HAS_FACT]->(f:Fact)
    WHERE f.predicate = $pred
    OPTIONAL MATCH (f)-[:SOURCED_FROM]->(doc:Document)
    RETURN f.fact_id, f.value, f.confidence, f.valid_from, f.valid_to,
           doc.document_name, doc.created_date
    ORDER BY f.valid_from
    """
    result = run(graph, query, {"cid": customer_id, "pred": predicate})
    rows = []
    for row in result.result_set:
        rows.append({
            "fact_id":       row[0],
            "value":         row[1],
            "confidence":    row[2],
            "valid_from":    row[3],
            "valid_to":      row[4] or "CURRENT",
            "document_name": row[5],
            "doc_date":      row[6],
        })
    return rows


def policy_at_time(graph, policy_name: str, query_time: str) -> dict | None:
    """Which version of a policy was active at query_time?"""
    query = """
    MATCH (p:Policy {policy_name: $pname})
    WHERE p.valid_from <= $qt
      AND (p.valid_to = '' OR p.valid_to IS NULL OR p.valid_to >= $qt)
    RETURN p.policy_id, p.policy_name, p.threshold, p.valid_from, p.valid_to
    """
    result = run(graph, query, {"pname": policy_name, "qt": query_time})
    if not result.result_set:
        return None
    row = result.result_set[0]
    return {
        "policy_id":   row[0],
        "policy_name": row[1],
        "threshold":   row[2],
        "valid_from":  row[3],
        "valid_to":    row[4] or "CURRENT",
    }


def currently_active_facts(graph) -> list[dict]:
    """Snapshot: all facts with no valid_to (currently active)."""
    query = """
    MATCH (c:Customer)-[:HAS_FACT]->(f:Fact)
    WHERE (f.valid_to = '' OR f.valid_to IS NULL)
    RETURN c.customer_id, c.name, f.predicate, f.value,
           f.confidence, f.valid_from
    ORDER BY c.customer_id, f.predicate
    """
    result = run(graph, query)
    rows = []
    for row in result.result_set:
        rows.append({
            "customer_id": row[0],
            "name":        row[1],
            "predicate":   row[2],
            "value":       row[3],
            "confidence":  row[4],
            "valid_from":  row[5],
        })
    return rows


def explain_decision(graph, decision_id: str) -> str:
    """
    Build a RAG-ready natural language context string for an LLM.
    This is the string you pass into your LLM prompt as retrieved context.
    """
    ctx = decision_context(graph, decision_id)
    if not ctx:
        return f"No context found for decision {decision_id}."

    lines = [
        f"Decision ID   : {ctx['decision_id']}",
        f"Customer      : {ctx['customer_name']} ({ctx['customer_id']})",
        f"Decision Type : {ctx['decision_type']}",
        f"Result        : {ctx['result']}",
        f"Decision Time : {ctx['decision_time']}",
        f"AI Model      : {ctx['model_version']}",
        "",
        "Supporting facts at time of decision:",
    ]
    for f in ctx["supporting_facts"]:
        to_str = f["valid_to"] if f["valid_to"] else "CURRENT"
        lines.append(
            f"  • [{f['predicate']}] = {f['value']}  "
            f"(confidence: {f['confidence']}, "
            f"window: {f['valid_from']} → {to_str}, "
            f"source: {f['document']} from {f['source']})"
        )

    if ctx["governing_policy"]:
        pol = ctx["governing_policy"]
        lines += [
            "",
            "Governing policy at time of decision:",
            f"  • {pol['policy_name']} — threshold: {pol['threshold']}",
            f"    Active: {pol['valid_from']} → {pol['valid_to']}",
        ]

    return "\n".join(lines)


# ─── Demo runner ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  FinSight — Temporal GraphRAG Query Engine")
    print("=" * 65)

    graph = connect()
    print("  Connected to FalkorDB ✓\n")

    # ── Query 1: Point-in-time facts for C001 at 2024-03-01 ──────────────
    print("━" * 65)
    print("Query 1 — Point-in-time: C001 facts on 2024-03-01")
    print("━" * 65)
    facts = point_in_time_facts(graph, "C001", "2024-03-01T00:00:00Z")
    if facts:
        for f in facts:
            print(f"  [{f['predicate']}] = {f['value']}")
            print(f"    valid: {f['valid_from']} → {f['valid_to']}")
            print(f"    confidence: {f['confidence']}  doc: {f['document_id']}")
    else:
        print("  (no facts found)")

    # ── Query 2: Same customer at 2024-09-01 (after risk score dropped) ──
    print("\n━" * 65)
    print("Query 2 — Point-in-time: C001 facts on 2024-09-01")
    print("━" * 65)
    facts = point_in_time_facts(graph, "C001", "2024-09-01T00:00:00Z")
    if facts:
        for f in facts:
            print(f"  [{f['predicate']}] = {f['value']}")
            print(f"    valid: {f['valid_from']} → {f['valid_to']}")
    else:
        print("  (no facts found)")

    # ── Query 3: Decision context (DEC001 — Rejected loan) ───────────────
    print("\n━" * 65)
    print("Query 3 — Decision context: DEC001 (Rejected LoanApproval)")
    print("━" * 65)
    print(explain_decision(graph, "DEC001"))

    # ── Query 4: Decision context (DEC002 — Approved loan) ───────────────
    print("\n━" * 65)
    print("Query 4 — Decision context: DEC002 (Approved LoanApproval)")
    print("━" * 65)
    print(explain_decision(graph, "DEC002"))

    # ── Query 5: Risk score timeline for C001 ────────────────────────────
    print("\n━" * 65)
    print("Query 5 — Fact timeline: C001 → risk_score")
    print("━" * 65)
    timeline = fact_timeline(graph, "C001", "risk_score")
    for t in timeline:
        print(f"  {t['valid_from']} → {t['valid_to']}")
        print(f"    value={t['value']}, confidence={t['confidence']}")
        print(f"    source: {t['document_name']}")

    # ── Query 6: Policy at decision time ─────────────────────────────────
    print("\n━" * 65)
    print("Query 6 — Policy at time: LoanRiskThreshold on 2024-03-10")
    print("━" * 65)
    pol = policy_at_time(graph, "LoanRiskThreshold", "2024-03-10T00:00:00Z")
    if pol:
        print(f"  Policy  : {pol['policy_name']} ({pol['policy_id']})")
        print(f"  Threshold: {pol['threshold']}")
        print(f"  Active   : {pol['valid_from']} → {pol['valid_to']}")
    else:
        print("  (no policy found)")

    print("\n━" * 65)
    print("Query 7 — Same policy on 2024-09-01 (after policy update)")
    print("━" * 65)
    pol = policy_at_time(graph, "LoanRiskThreshold", "2024-09-01T00:00:00Z")
    if pol:
        print(f"  Policy  : {pol['policy_name']} ({pol['policy_id']})")
        print(f"  Threshold: {pol['threshold']}")
        print(f"  Active   : {pol['valid_from']} → {pol['valid_to']}")

    # ── Query 8: Currently active facts snapshot ──────────────────────────
    print("\n━" * 65)
    print("Query 8 — Currently active facts (all customers)")
    print("━" * 65)
    active = currently_active_facts(graph)
    for a in active:
        print(f"  {a['customer_id']} ({a['name']}): "
              f"{a['predicate']} = {a['value']}  "
              f"[since {a['valid_from']}]  conf={a['confidence']}")

    print("\n" + "=" * 65)
    print("  ✓ All temporal RAG queries executed successfully!")
    print("=" * 65)


if __name__ == "__main__":
    main()
