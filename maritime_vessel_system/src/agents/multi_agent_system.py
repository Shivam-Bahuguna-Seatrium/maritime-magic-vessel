"""
Multi-Agent System – OpenAI Agent SDK
======================================

Replaces the previous rule-based orchestrator with LLM-powered agents
using the **OpenAI Agent SDK** (``openai-agents``).

Reference: https://openai.github.io/openai-agents-python/

Architecture
------------
┌────────────────────────────────────────────────────────┐
│              TRIAGE  AGENT  (Router)                   │
│  Interprets user intent and hands off to specialists   │
└──────┬──────────┬──────────┬──────────┬───────────────┘
       │          │          │          │
  ┌────▼───┐ ┌───▼────┐ ┌───▼────┐ ┌───▼─────┐
  │Validate│ │Similar │ │Anomaly │ │Knowledge│
  │ Agent  │ │ Agent  │ │ Agent  │ │ Graph   │
  └────────┘ └────────┘ └────────┘ └─────────┘

Each specialist agent has access to **function tools** that perform
the actual deterministic computation (IMO checksum, similarity scoring,
Neo4j queries, etc.).
"""

from __future__ import annotations

import os
import json
import hashlib
from typing import Dict, List, Any, Optional
from datetime import datetime

# ---------------------------------------------------------------------------
# OpenAI Agent SDK imports
# ---------------------------------------------------------------------------
try:
    # The openai-agents package installs as "agents" which collides with
    # this local package.  Temporarily manipulate sys.path and sys.modules
    # to load the installed SDK from site-packages.
    import sys as _sys
    import site as _site
    import os as _os

    # Save current state
    _saved_path = _sys.path[:]
    _saved_agents_mod = _sys.modules.pop("agents", None)

    # Remove paths that point to local src/agents
    _local_agents_dir = _os.path.dirname(_os.path.abspath(__file__))
    _local_src_dir = _os.path.dirname(_local_agents_dir)
    _sys.path = [
        p for p in _sys.path
        if _os.path.abspath(p) not in (_local_agents_dir, _local_src_dir)
    ]
    # Prepend site-packages so the installed SDK is found first
    for _sp in _site.getsitepackages():
        if _os.path.isdir(_os.path.join(_sp, "agents")):
            _sys.path.insert(0, _sp)
            break

    import importlib
    _agents_sdk = importlib.import_module("agents")
    Agent = _agents_sdk.Agent
    Runner = _agents_sdk.Runner
    function_tool = _agents_sdk.function_tool
    handoff = _agents_sdk.handoff
    RunResult = _agents_sdk.RunResult
    AGENTS_SDK_AVAILABLE = True

    # Restore sys.path; keep the loaded SDK in sys.modules
    _sys.path = _saved_path
    if _saved_agents_mod is not None:
        # Store the SDK under a safe alias so it stays accessible
        _sys.modules["openai_agents_sdk"] = _sys.modules["agents"]
        _sys.modules["agents"] = _saved_agents_mod

except (ImportError, Exception) as _e:
    AGENTS_SDK_AVAILABLE = False
    # Restore state on failure
    import sys as _sys
    _sys.path = globals().get("_saved_path", _sys.path)
    if globals().get("_saved_agents_mod") is not None:
        _sys.modules["agents"] = _saved_agents_mod

    # Provide stubs so the module can still be imported
    def function_tool(fn=None, **kw):  # type: ignore
        if fn is None:
            return lambda f: f
        return fn

    class Agent:  # type: ignore
        def __init__(self, **kw):
            self.name = kw.get("name", "stub")

    class Runner:  # type: ignore
        @staticmethod
        async def run(agent, message, **kw):
            return type("R", (), {"final_output": "Agent SDK not available"})()
        @staticmethod
        def run_sync(agent, message, **kw):
            return type("R", (), {"final_output": "Agent SDK not available"})()

    class RunResult:  # type: ignore
        final_output: str = ""

    def handoff(agent):  # type: ignore
        return agent


# ============================================================================
# FUNCTION TOOLS  (deterministic helpers exposed to the LLM agents)
# ============================================================================

@function_tool
def validate_imo(imo: int) -> str:
    """
    Validate an IMO number.

    Checks:
    - 7-digit format (1000000–9999999)
    - Weighted checksum (digits × [7,6,5,4,3,2], mod 10 == last digit)

    Returns a JSON string with ``valid`` (bool) and ``errors`` (list).
    """
    errors: List[str] = []
    if imo == 0:
        errors.append("IMO cannot be 0")
    elif imo < 1000000 or imo > 9999999:
        errors.append(f"IMO must be 7 digits (1000000-9999999), got {imo}")
    else:
        s = str(imo)
        weights = [7, 6, 5, 4, 3, 2]
        checksum = sum(int(s[i]) * weights[i] for i in range(6))
        if checksum % 10 != int(s[6]):
            errors.append(f"IMO checksum failed for {imo}")
    return json.dumps({"valid": len(errors) == 0, "errors": errors})


@function_tool
def validate_mmsi(mmsi: int) -> str:
    """
    Validate an MMSI number.

    Checks:
    - Must be 9 digits
    - MID (first 3 digits) between 200-775 for ship stations

    Returns a JSON string with ``valid`` (bool) and ``errors`` (list).
    """
    errors: List[str] = []
    s = str(mmsi)
    if len(s) != 9:
        errors.append(f"MMSI must be 9 digits, got {len(s)}")
    else:
        mid = int(s[:3])
        if mid < 200 or mid > 775:
            if not s.startswith("00"):
                errors.append(f"Invalid MID: {mid}")
    return json.dumps({"valid": len(errors) == 0, "errors": errors})


@function_tool
def validate_coordinates(latitude: float, longitude: float) -> str:
    """
    Validate geographic coordinates.

    Returns JSON with ``valid`` and ``warnings``.
    """
    warnings: List[str] = []
    valid = True
    if latitude < -90 or latitude > 90:
        warnings.append(f"Latitude out of range: {latitude}")
        valid = False
    if longitude < -180 or longitude > 180:
        warnings.append(f"Longitude out of range: {longitude}")
        valid = False
    if latitude == 0 and longitude == 0:
        warnings.append("Null Island (0,0) – likely data quality issue")
    return json.dumps({"valid": valid, "warnings": warnings})


@function_tool
def calculate_vessel_similarity(record_a_json: str, record_b_json: str) -> str:
    """
    Calculate similarity between two vessel records.

    Each record is a JSON string with keys: imo, name, mmsi, length, width,
    builtYear, grossTonnage, shipBuilder.

    Returns JSON with ``overall_similarity`` and per-attribute scores.
    """
    a = json.loads(record_a_json)
    b = json.loads(record_b_json)

    weights = {
        "imo": 0.30, "name": 0.20, "mmsi": 0.10,
        "length": 0.10, "width": 0.08, "builtYear": 0.08,
        "grossTonnage": 0.07, "shipBuilder": 0.07,
    }
    scores: Dict[str, float] = {}

    # IMO
    if a.get("imo") and b.get("imo"):
        scores["imo"] = 1.0 if a["imo"] == b["imo"] else 0.0

    # Name (Jaccard on chars)
    na, nb = (a.get("name") or "").upper(), (b.get("name") or "").upper()
    if na and nb:
        sa, sb = set(na.replace(" ", "")), set(nb.replace(" ", ""))
        scores["name"] = len(sa & sb) / len(sa | sb) if sa | sb else 0.0

    # MMSI
    if a.get("mmsi") and b.get("mmsi"):
        scores["mmsi"] = 1.0 if a["mmsi"] == b["mmsi"] else 0.0

    # Numeric dimensions
    for dim in ["length", "width", "grossTonnage"]:
        va, vb = a.get(dim), b.get(dim)
        if va and vb and va > 0 and vb > 0:
            diff = abs(va - vb) / max(va, vb)
            scores[dim] = max(0, 1 - diff / 0.1)

    # Build year
    if a.get("builtYear") and b.get("builtYear"):
        scores["builtYear"] = 1.0 if a["builtYear"] == b["builtYear"] else 0.0

    # Ship builder
    ba_name = (a.get("shipBuilder") or "").upper()
    bb_name = (b.get("shipBuilder") or "").upper()
    if ba_name and bb_name:
        sa2, sb2 = set(ba_name), set(bb_name)
        scores["shipBuilder"] = len(sa2 & sb2) / len(sa2 | sb2) if sa2 | sb2 else 0.0

    total_w, w_sum = 0.0, 0.0
    for attr, w in weights.items():
        if attr in scores:
            w_sum += scores[attr] * w
            total_w += w

    overall = round(w_sum / total_w, 4) if total_w else 0.0
    return json.dumps({
        "overall_similarity": overall,
        "attribute_scores": {k: round(v, 4) for k, v in scores.items()},
        "matching": [k for k, v in scores.items() if v > 0.9],
        "conflicting": [k for k, v in scores.items() if v < 0.3],
    })


@function_tool
def detect_anomalies(record_json: str) -> str:
    """
    Score a vessel record for anomalies.

    Returns JSON with ``anomaly_score`` (0-1) and ``flags``.
    """
    rec = json.loads(record_json)
    score = 0.0
    flags: List[str] = []

    imo = rec.get("imo")
    if imo is not None:
        if imo == 0:
            flags.append("zero_imo"); score += 0.3
        elif imo < 1000000:
            flags.append("invalid_imo_format"); score += 0.4

    name = rec.get("name", "")
    if name:
        bad = sum(1 for c in name if not c.isalnum() and c != " ")
        if bad > len(name) * 0.3:
            flags.append("garbled_name"); score += 0.3

    lat = rec.get("last_position_latitude")
    lon = rec.get("last_position_longitude")
    if lat == 0 and lon == 0:
        flags.append("null_island"); score += 0.2
    if lat is not None and (lat < -90 or lat > 90):
        flags.append("invalid_latitude"); score += 0.3
    if lon is not None and (lon < -180 or lon > 180):
        flags.append("invalid_longitude"); score += 0.3

    return json.dumps({"anomaly_score": min(1.0, score), "flags": flags})


@function_tool
def detect_identifier_conflicts(records_json: str) -> str:
    """
    Detect IMO↔MMSI conflicts across a list of vessel records.

    Input: JSON array of records with ``imo`` and ``mmsi`` keys.
    Returns JSON with conflict details.
    """
    records = json.loads(records_json)
    imo_to_mmsi: Dict[int, set] = {}
    mmsi_to_imo: Dict[int, set] = {}

    for r in records:
        imo, mmsi = r.get("imo"), r.get("mmsi")
        if imo and int(imo) > 0 and mmsi:
            imo_to_mmsi.setdefault(int(imo), set()).add(int(mmsi))
            mmsi_to_imo.setdefault(int(mmsi), set()).add(int(imo))

    conflicts = {
        "imo_with_multiple_mmsi": [
            {"imo": k, "mmsi_values": list(v)}
            for k, v in imo_to_mmsi.items() if len(v) > 1
        ],
        "mmsi_with_multiple_imo": [
            {"mmsi": k, "imo_values": list(v)}
            for k, v in mmsi_to_imo.items() if len(v) > 1
        ],
    }
    conflicts["total"] = (
        len(conflicts["imo_with_multiple_mmsi"])
        + len(conflicts["mmsi_with_multiple_imo"])
    )
    return json.dumps(conflicts)


@function_tool
def run_cypher_query(cypher: str) -> str:
    """
    Execute a Cypher query against the Neo4j knowledge graph and return the results.

    Returns the query results as a JSON array of records.
    Use this tool when you need to answer questions about vessels,
    relationships, or graph structure.
    """
    try:
        # Import here to avoid circular dependencies at module load time
        import sys, os as _os
        _src = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        if _src not in sys.path:
            sys.path.insert(0, _src)
        from api.neo4j_client import Neo4jClient

        client = Neo4jClient()
        client.connect()
        rows = client.run_cypher(cypher)
        client.close()

        def _ser(obj):
            if hasattr(obj, "items"):
                return dict(obj)
            if hasattr(obj, "labels"):
                return {"labels": list(obj.labels), **dict(obj)}
            return str(obj)

        return json.dumps(rows, default=_ser)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ============================================================================
# SPECIALIST AGENTS
# ============================================================================

def _build_validation_agent() -> Agent:
    return Agent(
        name="Validation Agent",
        instructions="""You are a maritime data-validation specialist.
Your job is to validate vessel identifiers and data quality.
Always use the provided tools instead of guessing. Return structured results.
If the user provides an IMO, call validate_imo. For MMSI, call validate_mmsi.
For coordinates, call validate_coordinates.
Summarise findings clearly.""",
        tools=[validate_imo, validate_mmsi, validate_coordinates],
    )


def _build_similarity_agent() -> Agent:
    return Agent(
        name="Similarity Agent",
        instructions="""You are a maritime entity-resolution specialist.
Use calculate_vessel_similarity to compare two vessel records and determine
if they represent the same physical vessel.
Explain the evidence behind your conclusion.""",
        tools=[calculate_vessel_similarity],
    )


def _build_anomaly_agent() -> Agent:
    return Agent(
        name="Anomaly Detection Agent",
        instructions="""You are an anomaly-detection specialist for maritime data.
Use detect_anomalies to score a vessel record and detect_identifier_conflicts
to find cross-record conflicts. Explain each flag you raise.""",
        tools=[detect_anomalies, detect_identifier_conflicts],
    )


def _build_knowledge_graph_agent() -> Agent:
    return Agent(
        name="Knowledge Graph Agent",
        instructions="""You are a Neo4j knowledge-graph specialist for maritime data.
Use the run_cypher_query tool to query the graph database.
You can write Cypher to answer questions about vessels, their types,
flags, ports, builders, and relationships.
Always return the Cypher you used so the user can verify.

The graph schema:
- (:Vessel) -[:IS_TYPE]-> (:VesselType) -[:BELONGS_TO_CATEGORY]-> (:VesselCategory)
- (:Vessel) -[:HAS_IMO]-> (:IMO)
- (:Vessel) -[:USES_MMSI]-> (:MMSI)
- (:Vessel) -[:REGISTERED_UNDER]-> (:Flag)
- (:Vessel) -[:BUILT_BY]-> (:ShipBuilder)
- (:Vessel) -[:VISITED]-> (:Port)

Vessel properties: vessel_id, imo, mmsi, name, vessel_type, category,
length, width, gross_tonnage, built_year, draught, deadweight, callsign,
flag, validation_status, validation_errors, last_lat, last_lon, destination.
""",
        tools=[run_cypher_query],
    )


# ============================================================================
# TRIAGE (ORCHESTRATOR) AGENT
# ============================================================================

def build_triage_agent() -> Agent:
    """
    Build the top-level triage agent that routes user requests
    to the appropriate specialist via handoffs.
    """
    validation_agent = _build_validation_agent()
    similarity_agent = _build_similarity_agent()
    anomaly_agent = _build_anomaly_agent()
    kg_agent = _build_knowledge_graph_agent()

    triage = Agent(
        name="Maritime Triage Agent",
        instructions="""You are the main maritime vessel data assistant.
Route user requests to the appropriate specialist:
- Validation questions → Validation Agent
- Record similarity / entity resolution → Similarity Agent
- Anomaly or conflict detection → Anomaly Detection Agent
- Knowledge graph queries / Cypher → Knowledge Graph Agent

If the request spans multiple areas, hand off to each in turn and
synthesise the results.""",
        handoffs=[
            handoff(validation_agent),
            handoff(similarity_agent),
            handoff(anomaly_agent),
            handoff(kg_agent),
        ],
    )
    return triage


# ============================================================================
# CONVENIENCE RUNNER
# ============================================================================

def run_agent_query(message: str) -> str:
    """
    Convenience function: send *message* to the triage agent and return
    the final text output.
    """
    if not AGENTS_SDK_AVAILABLE:
        return (
            "OpenAI Agent SDK not installed. "
            "Run: pip install openai-agents"
        )
    agent = build_triage_agent()
    result: RunResult = Runner.run_sync(agent, message)
    return result.final_output


async def run_agent_query_async(message: str) -> str:
    """Async variant of :func:`run_agent_query`."""
    if not AGENTS_SDK_AVAILABLE:
        return "OpenAI Agent SDK not installed."
    agent = build_triage_agent()
    result: RunResult = await Runner.run(agent, message)
    return result.final_output


# ============================================================================
# BACKWARD-COMPATIBILITY SHIMS
# ============================================================================
# These keep old imports in main.py from breaking.

class ToolRegistry:
    """Legacy shim – returns tool list for introspection."""
    TOOLS = {
        "validate_imo": validate_imo,
        "validate_mmsi": validate_mmsi,
        "validate_coordinates": validate_coordinates,
        "calculate_vessel_similarity": calculate_vessel_similarity,
        "detect_anomalies": detect_anomalies,
        "detect_identifier_conflicts": detect_identifier_conflicts,
        "run_cypher_query": run_cypher_query,
    }

    @classmethod
    def list_tools(cls) -> List[str]:
        return list(cls.TOOLS.keys())


class ValidationTool:
    @staticmethod
    def validate_vessel_identifiers(imo=None, mmsi=None):
        result = {"imo_valid": None, "mmsi_valid": None, "errors": []}
        if imo is not None:
            r = json.loads(validate_imo(imo))
            result["imo_valid"] = r["valid"]
            result["errors"].extend(r["errors"])
        if mmsi is not None:
            r = json.loads(validate_mmsi(mmsi))
            result["mmsi_valid"] = r["valid"]
            result["errors"].extend(r["errors"])
        return result


class SimilarityTool:
    @staticmethod
    def calculate_record_similarity(r1, r2):
        return json.loads(
            calculate_vessel_similarity(json.dumps(r1), json.dumps(r2))
        )


class ConflictDetectionTool:
    @staticmethod
    def detect_imo_mmsi_conflict(recs):
        return json.loads(detect_identifier_conflicts(json.dumps(recs)))


class AnomalyDetectionTool:
    @staticmethod
    def score_vessel_record_anomaly(rec):
        return json.loads(detect_anomalies(json.dumps(rec)))


class KnowledgeGraphTool:
    pass


class QueryTool:
    pass


class LLMOrchestrator:
    def __init__(self, **kw):
        self.api_key = kw.get("api_key")
