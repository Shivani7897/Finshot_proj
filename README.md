# FinSight — FalkorDB Temporal GraphRAG Pipeline

## Project structure

```
finsight_output/
├── timestamped_csvs/          ← Step 1 output: CSVs with bi-temporal columns
│   ├── customers.csv
│   ├── accounts.csv
│   ├── facts.csv
│   ├── policies.csv
│   ├── decisions.csv
│   ├── documents.csv
│   ├── decision_evidence.csv
│   └── ingestion_runs.csv
│
├── step1_add_timestamps.py    ← Adds valid_from, valid_to, created_at to all CSVs
├── step2_falkordb_ingest.py   ← Creates graph schema + ingests all nodes and edges
├── step3_temporal_rag.py      ← Temporal query engine (point-in-time, timeline, etc.)
└── README.md                  ← This file
```

---

## Bi-temporal design

Every node and edge carries two time axes:

| Column       | Meaning                                      |
|-------------|----------------------------------------------|
| `valid_from` | When the fact became TRUE in the real world  |
| `valid_to`   | When the fact stopped being TRUE (NULL = now)|
| `created_at` | When this record was ingested into the graph |
| `updated_at` | Last modification timestamp                  |

A `NULL` or empty `valid_to` means the record is **currently active**.

---

## Setup

### 1. Start FalkorDB

```bash
docker run -p 6379:6379 falkordb/falkordb:latest
```

### 2. Install Python dependencies

```bash
pip install falkordb pandas
```

### 3. Run Step 1 — Add timestamps to CSVs

```bash
python3 step1_add_timestamps.py
```

Reads from `../dataset/` and writes to `timestamped_csvs/`.

### 4. Run Step 2 — Ingest into FalkorDB

```bash
python3 step2_falkordb_ingest.py
```

Creates all node labels, edge types, and indexes, then runs verification.

### 5. Run Step 3 — Temporal RAG queries

```bash
python3 step3_temporal_rag.py
```

Demonstrates 8 temporal query patterns.

---

## Graph schema

### Nodes

| Label        | Key field     | Temporal meaning                        |
|-------------|---------------|-----------------------------------------|
| Customer     | customer_id   | valid from account open date            |
| Account      | account_id    | valid from opened_date                  |
| Fact         | fact_id       | valid over the business validity window |
| Policy       | policy_id     | valid over the regulatory window        |
| Decision     | decision_id   | immutable event; valid_from = timestamp |
| Document     | document_id   | valid from creation date                |
| IngestionRun | run_id        | metadata; valid from run_time           |

### Edges

| Relationship   | From      | To       | Temporal meaning                    |
|----------------|-----------|----------|-------------------------------------|
| OWNS_ACCOUNT   | Customer  | Account  | account open date → close date      |
| HAS_FACT       | Customer  | Fact     | same as Fact validity window        |
| SOURCED_FROM   | Fact      | Document | inherits from fact                  |
| MADE_DECISION  | Customer  | Decision | decision timestamp                  |
| SUPPORTED_BY   | Decision  | Fact     | at decision time                    |
| GOVERNED_BY    | Decision  | Policy   | policy active at decision time      |

---

## Temporal Cypher patterns

### Point-in-time lookup

```cypher
MATCH (c:Customer {customer_id: 'C001'})-[:HAS_FACT]->(f:Fact)
WHERE f.predicate = 'risk_score'
  AND f.valid_from <= '2024-03-01T00:00:00Z'
  AND (f.valid_to = '' OR f.valid_to IS NULL OR f.valid_to >= '2024-03-01T00:00:00Z')
RETURN f.value, f.valid_from, f.valid_to
```

### Full history / timeline

```cypher
MATCH (c:Customer {customer_id: 'C001'})-[:HAS_FACT]->(f:Fact)
WHERE f.predicate = 'risk_score'
RETURN f.value, f.valid_from, f.valid_to
ORDER BY f.valid_from
```

### Currently active facts

```cypher
MATCH (c:Customer)-[:HAS_FACT]->(f:Fact)
WHERE (f.valid_to = '' OR f.valid_to IS NULL)
RETURN c.name, f.predicate, f.value
```

### Policy at a given date

```cypher
MATCH (p:Policy {policy_name: 'LoanRiskThreshold'})
WHERE p.valid_from <= '2024-08-01T00:00:00Z'
  AND (p.valid_to = '' OR p.valid_to IS NULL OR p.valid_to >= '2024-08-01T00:00:00Z')
RETURN p.threshold
```

---

## Adding future test data

When you add new rows for testing, set timestamps as follows:

```python
# Business event happened in the past → backfill
valid_from  = "2024-11-01T00:00:00Z"   # actual business date
valid_to    = ""                         # still active
created_at  = datetime.now(utc).isoformat()  # actual ingestion time (NOW)

# New event happening now
valid_from  = datetime.now(utc).isoformat()
valid_to    = ""
created_at  = datetime.now(utc).isoformat()
```

This is the Graphiti bi-temporal contract: `valid_from` is the business truth time,
`created_at` is the graph write time. They will differ for backfilled rows.
