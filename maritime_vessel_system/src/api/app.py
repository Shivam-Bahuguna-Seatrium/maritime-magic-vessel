"""
FastAPI Backend – Maritime Vessel Identity Resolution System
============================================================

Endpoints:
    POST /api/upload         – Upload / load a CSV dataset
    POST /api/analyze        – Run EDA analysis
    POST /api/validate       – Run validation pipeline
    POST /api/build-graph    – Build Knowledge Graph in Neo4j (after validation)
    GET  /api/graph          – Fetch graph data for visualization
    GET  /api/graph/filter   – Hierarchical filter options
    GET  /api/ontology       – Get ontology tree
    POST /api/chat           – Chat (Cypher generation + NL answer)
    GET  /api/status         – System status & statistics
"""

from __future__ import annotations

import os
import io
import sys
import json
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Ensure src is importable
_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Import validation pipeline
try:
    from validation.validation_pipeline import DataValidationPipeline
except ImportError as e:
    print(f"⚠️  Could not import DataValidationPipeline: {e}")
    DataValidationPipeline = None

# Import knowledge graph components
try:
    from knowledge_graph.ontology import (
        build_ontology_tree,
        get_category_for_type,
        VESSEL_TYPE_HIERARCHY,
    )
except ImportError as e:
    print(f"⚠️  Could not import ontology: {e}")
    build_ontology_tree = None
    get_category_for_type = None
    VESSEL_TYPE_HIERARCHY = None

# Import Neo4j clients
try:
    from api.neo4j_client import Neo4jClient, InMemoryGraphDB
except ImportError as e:
    print(f"⚠️  Could not import Neo4j clients: {e}")
    # Create stub classes for fallback
    class Neo4jClient:
        def __init__(self, *args, **kwargs):
            raise RuntimeError(f"Neo4jClient not available: {e}")
    
    class InMemoryGraphDB:
        def initialize(self):
            pass
    
    InMemoryGraphDB = InMemoryGraphDB

# Import KG router
try:
    from api.kg_router import router as kg_router, set_graph_state
except ImportError as e:
    print(f"⚠️  Could not import kg_router: {e}")
    from fastapi import APIRouter
    kg_router = APIRouter()
    def set_graph_state(*args, **kwargs):
        pass

# Optional: OpenAI Agent SDK chat
try:
    from agents.multi_agent_system import run_agent_query_async, AGENTS_SDK_AVAILABLE
except ImportError:
    AGENTS_SDK_AVAILABLE = False

    async def run_agent_query_async(msg: str) -> str:
        return "Agent SDK not available."

# Optional: openai for direct Cypher generation fallback
try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
    _openai_client = None  # Lazy init to avoid requiring API key at import time

    def _get_openai_client():
        global _openai_client
        if _openai_client is None:
            _openai_client = AsyncOpenAI()
        return _openai_client
except ImportError:
    OPENAI_AVAILABLE = False
    _openai_client = None

    def _get_openai_client():
        return None


# ============================================================================
# Application state (simple in-memory store for the loaded dataset)
# ============================================================================

class AppState:
    df: Optional[pd.DataFrame] = None
    validation_results: Optional[pd.DataFrame] = None
    validation_summary: Optional[Dict] = None
    eda_report: Optional[Dict] = None
    graph_built: bool = False
    neo4j: Optional[Any] = None  # Neo4jClient | InMemoryGraphDB


state = AppState()

# ============================================================================
# FastAPI app
# ============================================================================

app = FastAPI(
    title="Maritime Vessel Identity Resolution API",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include dedicated Knowledge Graph router
app.include_router(kg_router)


def _get_neo4j():
    """Lazy-init Neo4j client. Falls back to in-memory graph if Neo4j unavailable."""
    if state.neo4j is None:
        try:
            state.neo4j = Neo4jClient()
            state.neo4j.initialize()
            print("✅ Using Neo4j database at", state.neo4j.uri)
        except Exception as e:
            print(f"⚠️  Neo4j unavailable ({e}), using in-memory graph")
            state.neo4j = InMemoryGraphDB()
            state.neo4j.initialize()
    return state.neo4j


# ============================================================================
# Pydantic models
# ============================================================================

class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    answer: str
    cypher: Optional[str] = None
    data: Optional[Any] = None


class StatusResponse(BaseModel):
    dataset_loaded: bool
    record_count: int
    validated: bool
    graph_built: bool
    statistics: Optional[Dict] = None


# ============================================================================
# Routes
# ============================================================================

@app.post("/api/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """Upload a CSV file as the working dataset."""
    try:
        content = await file.read()
        state.df = pd.read_csv(io.BytesIO(content), low_memory=False)
        state.validation_results = None
        state.validation_summary = None
        state.graph_built = False
        return {
            "status": "ok",
            "records": len(state.df),
            "columns": list(state.df.columns),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/load")
async def load_dataset(path: str):
    """Load a CSV from a server-side file path."""
    try:
        state.df = pd.read_csv(path, low_memory=False)
        state.validation_results = None
        state.validation_summary = None
        state.graph_built = False
        return {
            "status": "ok",
            "records": len(state.df),
            "columns": list(state.df.columns),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/load-default")
async def load_default_dataset():
    """Auto-load the default case study dataset."""
    try:
        # Try to find the CSV file in various possible locations
        # Works in both local and Vercel deployments
        possible_paths = [
            # Local development
            Path(__file__).resolve().parent.parent.parent / "case_study_dataset_202509152039.csv",
            Path(__file__).resolve().parent.parent.parent.parent / "case_study_dataset_202509152039.csv",
            # Vercel deployment (in api folder)
            Path(__file__).resolve().parent.parent.parent.parent / "api" / "case_study_dataset_202509152039.csv",
        ]
        
        csv_path = None
        for p in possible_paths:
            if p.exists():
                csv_path = p
                break
        
        if not csv_path:
            raise HTTPException(status_code=404, detail="Default dataset not found")
        
        state.df = pd.read_csv(csv_path, low_memory=False)
        state.validation_results = None
        state.validation_summary = None
        state.graph_built = False
        return {
            "status": "ok",
            "records": len(state.df),
            "columns": list(state.df.columns),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/analyze")
async def run_analysis():
    """Run EDA analysis on the loaded dataset."""
    if state.df is None:
        raise HTTPException(400, "No dataset loaded")

    df = state.df
    report = {
        "total_records": len(df),
        "total_columns": len(df.columns),
        "vessel_types": df["vessel_type"].value_counts().to_dict() if "vessel_type" in df.columns else {},
        "flags": df["flag"].value_counts().head(20).to_dict() if "flag" in df.columns else {},
        "missing_values": {
            col: int(df[col].isna().sum()) for col in df.columns
        },
        "unique_imos": int(df["imo"].nunique()) if "imo" in df.columns else 0,
        "unique_mmsis": int(df["mmsi"].nunique()) if "mmsi" in df.columns else 0,
        "zero_imo_count": int((df["imo"] == 0).sum()) if "imo" in df.columns else 0,
        "imo_duplicates": int(
            (df[df["imo"] > 0]["imo"].value_counts() > 1).sum()
        ) if "imo" in df.columns else 0,
    }
    state.eda_report = report
    return report


@app.post("/api/validate")
async def run_validation():
    """Run validation pipeline on the dataset."""
    if state.df is None:
        raise HTTPException(400, "No dataset loaded")

    pipeline = DataValidationPipeline()
    results_df = pipeline.validate_dataframe(state.df)
    summary = pipeline.generate_validation_summary(results_df)
    state.validation_results = results_df
    state.validation_summary = summary

    # Attach per-row validation status to the dataframe for KG ingestion
    failed_indices = set(
        results_df[~results_df["is_valid"]]["record_index"].unique()
    )
    state.df["_validation_status"] = state.df.index.map(
        lambda i: "invalid" if i in failed_indices else "valid"
    )

    # Collect errors per record index
    error_map: Dict[int, List[str]] = {}
    for _, row in results_df[~results_df["is_valid"]].iterrows():
        idx = row["record_index"]
        error_map.setdefault(idx, []).append(row.get("error_message", ""))
    state.df["_validation_errors"] = state.df.index.map(
        lambda i: json.dumps(error_map.get(i, []))
    )

    return summary


@app.post("/api/build-graph")
async def build_knowledge_graph():
    """
    Build the knowledge graph in Neo4j.

    **Important**: Validation MUST be run first.
    Vessels that failed validation are marked red (`validation_status='invalid'`).
    """
    if state.df is None:
        raise HTTPException(400, "No dataset loaded")
    if state.validation_results is None:
        raise HTTPException(400, "Run validation first before building the graph")

    neo = _get_neo4j()
    neo.clear_all()
    neo.create_constraints()
    neo.seed_ontology()

    total_nodes = 0
    total_rels = 0

    for _, row in state.df.iterrows():
        record = row.to_dict()
        vstatus = record.pop("_validation_status", "valid")
        verrors_str = record.pop("_validation_errors", "[]")
        try:
            verrors = json.loads(verrors_str)
        except Exception:
            verrors = []

        stats = neo.ingest_vessel(record, vstatus, verrors)
        total_nodes += stats.get("nodes", 0)
        total_rels += stats.get("relationships", 0)

    state.graph_built = True
    # Initialize the Knowledge Graph router with the built graph
    set_graph_state(state.neo4j, state.graph_built)
    return {
        "status": "ok",
        "nodes_created": total_nodes,
        "relationships_created": total_rels,
    }


@app.get("/api/graph")
async def get_graph(
    category: Optional[str] = Query(None),
    vessel_type: Optional[str] = Query(None),
    flag: Optional[str] = Query(None),
    validation_status: Optional[str] = Query(None),
):
    """
    Get graph data for frontend visualization with optional hierarchical
    filters (category → type → flag → validation_status).
    
    Returns empty graph if not built yet.
    """
    if not state.graph_built:
        return {"nodes": [], "relationships": []}
    neo = _get_neo4j()
    return neo.get_graph_data(category, vessel_type, flag, validation_status)


@app.get("/api/graph/filters")
async def get_filter_options():
    """Return available values for each hierarchical filter dropdown."""
    if not state.graph_built:
        raise HTTPException(400, "Knowledge graph not built yet")
    neo = _get_neo4j()
    return neo.get_filter_options()


@app.get("/api/ontology")
async def get_ontology():
    """Return the vessel-type ontology tree."""
    tree = build_ontology_tree()
    return tree.to_dict()


@app.get("/api/ontology/neo4j")
async def get_ontology_neo4j():
    """Return the ontology tree from Neo4j with vessel counts."""
    if not state.graph_built:
        return {"categories": list(VESSEL_TYPE_HIERARCHY.keys())}
    neo = _get_neo4j()
    return neo.get_ontology_tree()


# --------------------------------------------------------------------------
# Chat endpoint: generates Cypher → runs query → answers in natural language
# --------------------------------------------------------------------------

CYPHER_SYSTEM_PROMPT = """You are a maritime vessel data assistant with access to a Neo4j knowledge graph.

The graph schema:
- (:Vessel) -[:IS_TYPE]-> (:VesselType) -[:BELONGS_TO_CATEGORY]-> (:VesselCategory)
- (:Vessel) -[:HAS_IMO]-> (:IMO {value})
- (:Vessel) -[:USES_MMSI]-> (:MMSI {value})
- (:Vessel) -[:REGISTERED_UNDER]-> (:Flag {code})
- (:Vessel) -[:BUILT_BY]-> (:ShipBuilder {name})
- (:Vessel) -[:VISITED]-> (:Port {unlocode, name})

Vessel properties: vessel_id, imo, mmsi, name, vessel_type, category,
length, width, gross_tonnage, built_year, draught, deadweight, callsign,
flag, validation_status, validation_errors, last_lat, last_lon, destination.

When the user asks a question:
1. Generate a valid Cypher query to answer it.
2. Return ONLY a JSON object: {"cypher": "<query>"}
Do NOT include explanations, only the JSON."""


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Chat endpoint.

    Flow:
    1. Use LLM to generate Cypher from the user's question.
    2. Execute the Cypher against Neo4j.
    3. Use LLM to formulate a natural-language answer from the results.
    """
    message = request.message

    # --- Strategy 1: Use OpenAI Agent SDK if available ---
    if AGENTS_SDK_AVAILABLE and state.graph_built:
        try:
            answer = await run_agent_query_async(message)
            return ChatResponse(answer=answer)
        except Exception:
            pass  # fall through to direct OpenAI call

    # --- Strategy 2: Direct OpenAI call for Cypher generation ---
    if not OPENAI_AVAILABLE:
        return ChatResponse(
            answer="OpenAI is not configured. Please set OPENAI_API_KEY.",
        )

    if not state.graph_built:
        return ChatResponse(
            answer="Please build the knowledge graph first (run validation then build graph).",
        )

    neo = _get_neo4j()

    try:
        # Step 1: Generate Cypher
        client = _get_openai_client()
        cypher_resp = await client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            messages=[
                {"role": "system", "content": CYPHER_SYSTEM_PROMPT},
                {"role": "user", "content": message},
            ],
        )
        raw = cypher_resp.choices[0].message.content.strip()

        # Extract JSON from the response
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        cypher_obj = json.loads(raw)
        cypher = cypher_obj.get("cypher", "")

        if not cypher:
            return ChatResponse(answer="Could not generate a query for that question.")

        # Step 2: Execute Cypher
        rows = neo.run_cypher(cypher)

        # Serialize for the LLM
        def _ser(o):
            if hasattr(o, "items"):
                return dict(o)
            if hasattr(o, "labels"):
                return {"labels": list(o.labels), **dict(o)}
            return str(o)

        data_json = json.dumps(rows[:50], default=_ser, indent=2)

        # Step 3: Natural language answer
        answer_resp = await client.chat.completions.create(
            model="gpt-4o",
            temperature=0.2,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a maritime data analyst. The user asked a question "
                        "and we ran the following Cypher query against our knowledge "
                        "graph. Summarise the results in clear natural language. "
                        "Include specific numbers and vessel names."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Question: {message}\n\n"
                        f"Cypher: {cypher}\n\n"
                        f"Results:\n{data_json}"
                    ),
                },
            ],
        )
        answer = answer_resp.choices[0].message.content.strip()

        return ChatResponse(answer=answer, cypher=cypher, data=rows[:50])

    except Exception as e:
        traceback.print_exc()
        return ChatResponse(answer=f"Error: {e}")


@app.post("/api/chat/predefined")
async def chat_predefined(request: ChatRequest):
    """
    Execute a predefined Cypher query without needing OpenAI.
    Automatically matches user query to a predefined query template.
    """
    if not state.graph_built:
        return ChatResponse(
            answer="Please build the knowledge graph first (run validation then build graph).",
        )

    message = request.message.lower()
    neo = _get_neo4j()

    # 5 Creative predefined queries for maritime intelligence
    queries = {
        "energy|tanker|oil|strategic": {
            "cypher": '''MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType) 
WHERE vt.name CONTAINS 'Tanker' AND v.properties.gross_tonnage > 20000
RETURN v.properties.name AS name, v.properties.flag AS flag, v.properties.gross_tonnage AS tonnage, vt.name AS type
ORDER BY v.properties.gross_tonnage DESC LIMIT 50''',
            "answer_template": "⚡ Energy Sector Giants: Top 50 large tanker vessels (>20,000 GT) - Strategic assets moving global energy reserves. Critical infrastructure in the global economy.",
        },
        "jurisdiction|flag|distribution|global|map": {
            "cypher": '''MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType)
RETURN v.properties.flag AS flag, vt.name AS type, COUNT(*) AS count
ORDER BY count DESC LIMIT 50''',
            "answer_template": "🌍 Global Maritime Jurisdiction Heatmap: Top 50 flag countries & vessel type combinations - Shows which countries dominate shipping. Reveals geopolitical maritime trade patterns and regulatory control points.",
        },
        "aging|old|risk|maintenance|compliance|lifecycle": {
            "cypher": '''MATCH (v:Vessel)
WHERE v.properties.built_year > 0 AND v.properties.built_year < 2010
RETURN v.properties.name AS name, v.properties.built_year AS year, v.properties.gross_tonnage AS tonnage
ORDER BY v.properties.built_year ASC LIMIT 50''',
            "answer_template": "⚠️ Aging Asset Lifecycle Analysis: Top 50 oldest vessels (built before 2010) - Potential maintenance costs, regulatory compliance risks, and obsolescence considerations. Critical for fleet modernization planning.",
        },
        "premium|mega|ultra|large|high|value|capacity|flagship": {
            "cypher": '''MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType)
WHERE v.properties.gross_tonnage > 50000
RETURN v.properties.name AS name, v.properties.gross_tonnage AS tonnage, v.properties.flag AS flag, vt.name AS type
ORDER BY v.properties.gross_tonnage DESC LIMIT 50''',
            "answer_template": "💎 Ultra-Premium Fleet Leaders: Top 50 high-capacity vessels (>50,000 GT) - The maritime industry's most valuable assets. Dominate global trade in containers, LNG, and bulk commodities.",
        },
        "fleet|statistics|market|intelligence|analytics|overview": {
            "cypher": '''MATCH (v:Vessel)
RETURN COUNT(*) AS total_vessels, AVG(v.properties.gross_tonnage) AS avg_tonnage, AVG(v.properties.built_year) AS avg_year LIMIT 1''',
            "answer_template": "📊 Fleet Statistics & Market Intelligence: Complete aggregate metrics - Revealing total global fleet composition, average capacity, and age profile. Strategic market analysis data.",
        },
    }

    # Match message to a predefined query
    matching_query = None
    for keywords, query_info in queries.items():
        if any(kw in message for kw in keywords.split("|")):
            matching_query = query_info
            break

    if not matching_query:
        return ChatResponse(
            answer="""🔍 **Query Assistant Mode (Predefined Queries)**

No direct match found. Try asking about:
• ⚡ **Energy Sector Giants** - Strategic tanker fleet analysis
• 🌍 **Global Maritime Jurisdiction** - Flag country distribution
• ⚠️ **Aging Asset Lifecycle** - Vessels & maintenance risk assessment  
• 💎 **Ultra-Premium Fleet** - High-capacity vessels >50K GT
• 📊 **Fleet Statistics** - Market intelligence & analytics

---
📌 *Note: Full natural language AI interaction requires a paid API key (OpenAI/Claude). Currently using intelligent predefined query matching based on maritime topics.*"""
        )

    try:
        cypher = matching_query["cypher"]
        rows = neo.run_cypher(cypher)

        # Serialize for response
        def _ser(o):
            if hasattr(o, "items"):
                return dict(o)
            if hasattr(o, "labels"):
                return {"labels": list(o.labels), **dict(o)}
            return str(o)

        answer = matching_query["answer_template"]
        return ChatResponse(answer=answer, cypher=cypher, data=rows)

    except Exception as e:
        traceback.print_exc()
        return ChatResponse(answer=f"Error executing query: {e}")



@app.get("/api/status", response_model=StatusResponse)
async def get_status():
    stats = None
    if state.graph_built:
        try:
            neo = _get_neo4j()
            stats = neo.get_statistics()
        except Exception:
            pass

    return StatusResponse(
        dataset_loaded=state.df is not None,
        record_count=len(state.df) if state.df is not None else 0,
        validated=state.validation_results is not None,
        graph_built=state.graph_built,
        statistics=stats,
    )


# ============================================================================
# Entrypoint
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
