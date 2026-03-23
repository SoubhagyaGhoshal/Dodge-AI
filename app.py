"""
Main FastAPI backend for the SAP O2C Graph System.
Serves the frontend and provides API endpoints for graph exploration and NL chat.
"""
import os
import re
import json
import sqlite3
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

import google.generativeai as genai
from graph_builder import get_graph, make_node_id

load_dotenv()

# Runtime API key store (can be overridden via /api/set-api-key)
_runtime_api_key: str = ""

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DB_PATH = os.path.join(os.path.dirname(__file__), "o2c.db")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

def get_gemini_model(api_key: str = ""):
    """Return a Gemini model, using provided key, runtime key, or env key."""
    key = api_key or _runtime_api_key or GEMINI_API_KEY
    if not key:
        return None
    genai.configure(api_key=key)
    return genai.GenerativeModel("gemini-2.5-flash")

# ── Lifespan: preload graph ───────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Loading graph from database...")
    get_graph()
    yield

app = FastAPI(title="SAP O2C Graph System", lifespan=lifespan)

# ── Static files ──────────────────────────────────────────────────────────────
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

class ApiKeyRequest(BaseModel):
    api_key: str

@app.post("/api/set-api-key")
async def set_api_key(req: ApiKeyRequest):
    global _runtime_api_key
    _runtime_api_key = req.api_key.strip()
    return {"status": "ok"}

# ── DB Helper ─────────────────────────────────────────────────────────────────
def get_db_schema() -> str:
    """Return the SQLite schema as a string for LLM context."""
    conn = sqlite3.connect(DB_PATH)
    schema_parts = []
    for row in conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"):
        name, sql = row
        if sql:
            schema_parts.append(sql)
    conn.close()
    return "\n\n".join(schema_parts)

def get_sample_rows(limit: int = 2) -> str:
    """Get sample rows from key tables for LLM context."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    tables = [
        "sales_order_headers",
        "sales_order_items",
        "billing_document_headers",
        "billing_document_items",
        "outbound_delivery_headers",
        "outbound_delivery_items",
        "business_partners",
        "payments_accounts_receivable",
    ]
    samples = []
    for t in tables:
        try:
            rows = conn.execute(f"SELECT * FROM {t} LIMIT {limit}").fetchall()
            if rows:
                samples.append(f"-- {t} sample:")
                for row in rows:
                    samples.append(json.dumps(dict(row)))
        except:
            pass
    conn.close()
    return "\n".join(samples)

def run_sql(sql: str) -> List[Dict]:
    """Execute a SELECT SQL query and return results as list of dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        raise ValueError(f"SQL error: {e}")
    finally:
        conn.close()

# ── Off-topic guardrail ───────────────────────────────────────────────────────
OFF_TOPIC_PATTERNS = [
    r"\b(who is|what is the capital|history of|write a|tell me a|recipe for|explain quantum|"
    r"weather|stock price|poem|story|joke|translate|movie|music|sport|celebrity|"
    r"president|prime minister|election|covid|covid-19|vaccine|python tutorial|"
    r"machine learning basics|define the word)\b",
]
DOMAIN_KEYWORDS = [
    "sales order", "billing", "invoice", "delivery", "payment", "product", "material",
    "customer", "business partner", "plant", "journal", "accounting", "shipment",
    "order", "document", "flow", "sap", "o2c", "outbound", "fiscal", "currency",
    "quantity", "amount", "status", "date", "billed", "delivered", "cancelled",
    "transaction", "reference", "schedule", "incoterms", "distribution"
]

def is_off_topic(query: str) -> bool:
    """Return True if the query is clearly not domain-related."""
    q_lower = query.lower()
    
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, q_lower, re.IGNORECASE):
            # Still allow if domain keywords present
            if not any(kw in q_lower for kw in DOMAIN_KEYWORDS):
                return True
    
    # If query is very long and has no domain keywords, likely off-topic
    if len(query) > 20 and not any(kw in q_lower for kw in DOMAIN_KEYWORDS):
        # But only flag if it mentions clearly non-domain topics
        nonsense_patterns = [r"\b(love|hate|feel|opinion|think about|dream|wish)\b"]
        for np in nonsense_patterns:
            if re.search(np, q_lower):
                return True
    
    return False

# ── System prompt for LLM ─────────────────────────────────────────────────────
SYSTEM_PROMPT_TEMPLATE = """You are an AI assistant for a SAP Order-to-Cash (O2C) data analysis system.
You ONLY answer questions about the following dataset. You MUST refuse any off-topic queries.

DATABASE SCHEMA:
{schema}

SAMPLE DATA:
{samples}

IMPORTANT RULES:
1. You can ONLY answer questions about the SAP O2C dataset (sales orders, billing documents, deliveries, payments, products, customers, plants, journal entries).
2. If asked anything unrelated, respond: "This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only."
3. Always generate valid SQLite SQL queries when needed.
4. The billingDocument ID is the primary key for billing_document_headers.
5. SalesOrder links to BillingDocument via billing_document_items.salesDocument.
6. OutboundDelivery links to SalesOrder via outbound_delivery_items.salesOrder.
7. Delivery status "C" = complete, "A" = in progress, "B" = partially delivered.
8. billingDocumentIsCancelled = 1 means the billing document is cancelled.
"""

def build_system_prompt() -> str:
    schema = get_db_schema()
    samples = get_sample_rows()
    return SYSTEM_PROMPT_TEMPLATE.format(schema=schema, samples=samples)

def extract_sql_from_response(text: str) -> Optional[str]:
    """Extract SQL query from LLM response."""
    patterns = [
        r"```sql\n(.*?)\n```",
        r"```\n(SELECT.*?)\n```",
        r"(SELECT\s+.+?;)",
        r"(SELECT\s+.+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            sql = match.group(1).strip()
            if sql.upper().startswith("SELECT"):
                return sql
    return None

def extract_node_references(text: str) -> List[str]:
    """Extract entity IDs referenced in responses for graph highlighting."""
    ids = set()
    
    # Sales order pattern (6-digit numbers)
    for m in re.finditer(r'\b(7[0-9]{5})\b', text):
        ids.add(f"SalesOrder:{m.group(1)}")
    
    # Billing document pattern (8-digit starting with 9)
    for m in re.finditer(r'\b(9[0-9]{7})\b', text):
        ids.add(f"BillingDocument:{m.group(1)}")
    
    return list(ids)

# ── API Endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/graph/overview")
async def graph_overview():
    """Get a sample of nodes and edges for initial visualization."""
    g = get_graph()
    return g.to_vis_data(max_nodes=150)

@app.get("/api/graph/stats")
async def graph_stats():
    """Get graph statistics."""
    g = get_graph()
    return g.get_stats()

@app.get("/api/graph/expand/{node_id:path}")
async def expand_node(node_id: str):
    """Get neighboring nodes and edges for a given node."""
    g = get_graph()
    data = g.get_neighbors(node_id)
    if not data["nodes"]:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
    return data

@app.get("/api/graph/node/{node_id:path}")
async def get_node(node_id: str):
    """Get full details for a node."""
    g = get_graph()
    data = g.get_node_data(node_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
    return data

@app.get("/api/graph/type/{node_type}")
async def nodes_by_type(node_type: str, limit: int = 50):
    """List all nodes of a given type."""
    g = get_graph()
    nodes = g.find_nodes_by_type(node_type, limit)
    return {"nodes": nodes}

# ── Chat ──────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    history: Optional[List[Dict[str, str]]] = []
    api_key: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    sql_used: Optional[str] = None
    results_count: Optional[int] = None
    referenced_nodes: List[str] = []

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Process a natural language query about the O2C data."""
    
    query = request.message.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Empty message")
    
    # ── Guardrail ──
    if is_off_topic(query):
        return ChatResponse(
            response="This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only.",
            referenced_nodes=[]
        )
    
    model = get_gemini_model(request.api_key or "")
    if not model:
        return ChatResponse(
            response="⚠️ LLM not configured. Please enter your Gemini API key in the header input field above, "
                     "or set GEMINI_API_KEY in a .env file. You can still explore the graph visually.",
            referenced_nodes=[]
        )
    
    try:
        system_prompt = build_system_prompt()
        
        # Build conversation history for context
        history_text = ""
        if request.history:
            for turn in request.history[-4:]:  # last 4 turns
                role = turn.get("role", "user")
                content = turn.get("content", "")
                history_text += f"\n{role.upper()}: {content}"
        
        # ── Step 1: Generate SQL ──
        sql_prompt = f"""{system_prompt}

{history_text}

USER QUESTION: {query}

Generate a SQLite SQL query to answer this question. Return ONLY the SQL query wrapped in ```sql ... ``` blocks.
If this question cannot be answered with SQL (e.g. it's asking about relationships or flows), 
return NO_SQL_NEEDED instead.
Make sure to LIMIT results to 50 rows maximum."""

        sql_response = model.generate_content(sql_prompt)
        sql_text = sql_response.text.strip()
        
        sql_query = None
        query_results = None
        results_count = None
        
        if "NO_SQL_NEEDED" not in sql_text:
            sql_query = extract_sql_from_response(sql_text)
            
            if sql_query:
                try:
                    # Safety: only allow SELECT
                    if not re.match(r"^\s*SELECT\s", sql_query, re.IGNORECASE):
                        sql_query = None
                    else:
                        # Ensure LIMIT
                        if "LIMIT" not in sql_query.upper():
                            sql_query = sql_query.rstrip(";") + " LIMIT 50"
                        query_results = run_sql(sql_query)
                        results_count = len(query_results)
                except ValueError as e:
                    sql_query = None
        
        # ── Step 2: Generate natural language answer ──
        results_context = ""
        if query_results is not None:
            if query_results:
                results_context = f"\nQUERY RESULTS ({len(query_results)} rows):\n{json.dumps(query_results[:20], indent=2)}"
            else:
                results_context = "\nQUERY RESULTS: No data found."
        
        answer_prompt = f"""{system_prompt}

USER QUESTION: {query}
{f"SQL EXECUTED: {sql_query}" if sql_query else ""}
{results_context}

Provide a clear, concise, data-backed answer to the user's question based on the results above.
- If the results are empty, say no data was found.
- Include specific IDs and numbers from the results.
- Format numbers nicely.
- If this is about a flow/trace, describe the full flow step by step.
- Do NOT make up data that's not in the results.
- Keep your answer focused and professional."""

        answer_response = model.generate_content(answer_prompt)
        answer_text = answer_response.text.strip()
        
        # ── Extract node references for highlighting ──
        referenced = extract_node_references(answer_text)
        if sql_query and query_results:
            # Also extract from raw results
            for row in query_results[:10]:
                for k, v in row.items():
                    if v and isinstance(v, str):
                        if re.match(r'^7\d{5}$', v):
                            referenced.append(f"SalesOrder:{v}")
                        elif re.match(r'^9\d{7}$', v):
                            referenced.append(f"BillingDocument:{v}")
        
        referenced = list(set(referenced))[:20]  # deduplicate, cap
        
        return ChatResponse(
            response=answer_text,
            sql_used=sql_query,
            results_count=results_count,
            referenced_nodes=referenced
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
