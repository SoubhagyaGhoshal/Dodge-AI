# SAP O2C Graph Intelligence

A **context graph system with an LLM-powered query interface** for the SAP Order-to-Cash dataset.

## What It Does

- Ingests all 19 JSONL data tables into SQLite
- Constructs a graph of 674 nodes and 967 edges across 9 entity types
- Visualizes the graph interactively (vis.js) — click nodes, expand neighbors, filter by type
- Accepts natural language questions, translates them to SQL via **Google Gemini**, and returns data-backed answers
- Highlights referenced nodes in the graph when you get a chat response

## Architecture

```
JSONL Dataset (19 tables)
        ↓
  ingest.py → o2c.db (SQLite)
        ↓
graph_builder.py → NetworkX DiGraph (in-memory)
        ↓
  app.py (FastAPI) ←→ Gemini 1.5 Flash (NL→SQL, answers)
        ↓
static/ (HTML + vis.js frontend)
```

**Tech Stack:**
| Layer | Technology |
|---|---|
| Backend | Python 3, FastAPI |
| Database | SQLite (19 normalized tables) |
| Graph | NetworkX DiGraph |
| LLM | Google Gemini 1.5 Flash (free tier) |
| Frontend | Vanilla HTML/CSS/JS + vis.js |

## Graph Model

**Node Types (9):** SalesOrder, SalesOrderItem, BillingDocument, OutboundDelivery, BusinessPartner, Product, Plant, JournalEntry, Payment

**Edge Types (11):**
- `HAS_ITEM`: SalesOrder → SalesOrderItem
- `SOLD_TO`: SalesOrder → BusinessPartner
- `REFERENCES_PRODUCT`: SalesOrderItem → Product
- `PRODUCED_AT`: SalesOrderItem → Plant
- `BILLED_AS`: SalesOrder → BillingDocument
- `BILLED_TO`: BillingDocument → BusinessPartner
- `POSTED_TO`: BillingDocument → JournalEntry
- `CANCELS`: BillingDocument → BillingDocument
- `DELIVERS_FOR`: OutboundDelivery → SalesOrder
- `SHIPPED_FROM`: OutboundDelivery → Plant
- `ASSOCIATED_WITH`: Payment → JournalEntry

## LLM Prompting Strategy

1. **Schema + sample rows** are injected into the system prompt so Gemini knows the database structure
2. **Step 1 — SQL generation**: Gemini receives the user question and generates a SQLite SELECT query
3. **Step 2 — Natural language answer**: The SQL results are sent back to Gemini to produce a human-readable, data-backed answer
4. **Conversation history**: Last 4 turns are included for context continuity

## Guardrails

- Pre-filter regex matches off-topic patterns (general knowledge, creative writing, etc.)
- System prompt explicitly instructs the model to refuse non-domain queries
- Only SELECT queries are allowed (no INSERT/UPDATE/DELETE)
- All results are capped at 50 rows

Off-topic response: *"This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only."*

## Database Choice

**SQLite** was chosen because:
- Zero-configuration, no server needed
- Sufficient for the dataset size (~3,000 total rows across 19 tables)
- Perfect for single-user exploratory analytics
- Gemini can write valid SQLite SQL reliably

For production with multiple concurrent users, **PostgreSQL** would be the natural upgrade.

## Running Locally

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set your Gemini API key (get free at https://ai.google.dev)
echo "GEMINI_API_KEY=your_key_here" > .env

# 3. Build the database
python ingest.py

# 4. Start the server
python -m uvicorn app:app --host 0.0.0.0 --port 8000

# 5. Open browser
open http://localhost:8000
```

You can also paste the API key directly in the UI header without creating a .env file.

## Example Queries

| Query | What happens |
|---|---|
| *"Which products have the most billing documents?"* | SQL GROUP BY + COUNT on billing_document_items JOIN |
| *"Trace billing document 90504248"* | Joins billing → sales order → delivery → journal entry |
| *"Sales orders delivered but not billed"* | Finds orders with delivery status C but no billing document |
| *"Total revenue by customer"* | SUM(totalNetAmount) GROUP BY soldToParty |

## File Structure

```
sap-order-to-cash-dataset/
├── ingest.py          # Data loader: JSONL → SQLite
├── graph_builder.py   # Graph construction (NetworkX)
├── app.py             # FastAPI backend + Gemini integration
├── o2c.db             # Generated SQLite database
├── requirements.txt
├── .env               # Your GEMINI_API_KEY
└── static/
    ├── index.html     # Main UI
    ├── style.css      # Dark glassmorphism design
    └── app.js         # vis.js graph + chat logic
```
