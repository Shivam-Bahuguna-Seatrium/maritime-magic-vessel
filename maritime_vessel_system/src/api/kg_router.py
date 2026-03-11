"""
Knowledge Graph Router
======================

Dedicated API routes for knowledge graph visualization and querying.
Separate from core data processing routes to improve performance and maintainability.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, Dict, Any

router = APIRouter(prefix="/api/kg", tags=["Knowledge Graph"])

# Global state for graph operations (will be injected by main app)
_graph_state = {"neo4j": None, "graph_built": False}


def set_graph_state(neo4j_client, graph_built: bool):
    """Set the graph state (called by main app)."""
    _graph_state["neo4j"] = neo4j_client
    _graph_state["graph_built"] = graph_built


@router.get("/data")
async def get_graph_data(
    category: Optional[str] = Query(None),
    vessel_type: Optional[str] = Query(None),
    flag: Optional[str] = Query(None),
    validation_status: Optional[str] = Query(None),
    vessel_name: Optional[str] = Query(None),
):
    """
    Get graph data for visualization.
    
    Query Parameters:
    - category: Filter by vessel category
    - vessel_type: Filter by vessel type
    - flag: Filter by flag country
    - validation_status: Filter by validation status (valid/invalid/warning)
    - vessel_name: Filter by specific vessel name
    
    Returns empty graph if not built yet.
    """
    if not _graph_state["graph_built"]:
        return {"nodes": [], "relationships": []}
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    neo = _graph_state["neo4j"]
    return neo.get_graph_data(category, vessel_type, flag, validation_status, vessel_name)


@router.get("/filters")
async def get_filter_options():
    """
    Get available filter options for the knowledge graph.
    
    Returns:
    - categories: List of vessel categories
    - vessel_types: List of vessel types
    - flags: List of flag countries
    - statuses: List of validation statuses
    """
    if not _graph_state["graph_built"]:
        return {"categories": [], "vessel_types": [], "flags": [], "statuses": []}
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    neo = _graph_state["neo4j"]
    return neo.get_filter_options()


@router.get("/filter-options")
async def get_filtered_options(
    category: Optional[str] = Query(None),
    vessel_type: Optional[str] = Query(None),
    vessel_name: Optional[str] = Query(None),
    flag: Optional[str] = Query(None),
):
    """
    Get filtered options based on current filter selections.
    Returns available options for the next level in the cascade.
    
    This endpoint returns which vessel_types, vessel_names, flags, and validation_statuses
    are available given the current selections.
    """
    if not _graph_state["graph_built"]:
        return {
            "categories": [],
            "vessel_types": [],
            "vessel_names": [],
            "flags": [],
            "validation_statuses": [],
            "vessel_count": 0,
        }
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    neo = _graph_state["neo4j"]
    return neo.get_filtered_options(category, vessel_type, vessel_name, flag)


@router.get("/ontology")
async def get_ontology_tree():
    """
    Get the ontology hierarchy with vessel counts.
    
    Returns hierarchical structure:
    - categories with their types and vessel counts
    """
    if not _graph_state["graph_built"]:
        return []
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    neo = _graph_state["neo4j"]
    return neo.get_ontology_tree()


@router.get("/statistics")
async def get_graph_statistics():
    """
    Get statistics about the knowledge graph.
    
    Returns:
    - total_vessels: Total number of vessels in graph
    - total_nodes: Total number of all nodes
    - total_relationships: Total number of relationships
    - categories: Number of vessel categories
    - vessel_types: Number of vessel types
    - flags: Number of flag countries
    """
    if not _graph_state["graph_built"]:
        return {
            "total_vessels": 0,
            "total_nodes": 0,
            "total_relationships": 0,
            "categories": 0,
            "vessel_types": 0,
            "flags": 0,
        }
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    neo = _graph_state["neo4j"]
    return neo.get_statistics()


@router.post("/search")
async def search_graph(query: str):
    """
    Search the knowledge graph for vessels matching a query.
    
    Query can be:
    - Vessel name
    - IMO number
    - MMSI number
    - Flag country
    - Vessel type
    
    Returns matching vessels with details.
    """
    if not _graph_state["graph_built"]:
        return {"results": []}
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    if not query or len(query) < 2:
        raise HTTPException(400, "Query must be at least 2 characters")
    
    neo = _graph_state["neo4j"]
    # Use Cypher-like search (mocked for in-memory)
    try:
        results = neo.run_cypher(f"""
        MATCH (v:Vessel)
        WHERE v.name CONTAINS $query OR 
              v.callsign CONTAINS $query OR
              toString(v.imo) = $query OR
              toString(v.mmsi) = $query
        RETURN v
        LIMIT 20
        """, query=query)
        return {"results": results if results else []}
    except Exception as e:
        return {"results": [], "error": str(e)}


@router.get("/status")
async def get_graph_status():
    """
    Get the status of the knowledge graph.
    
    Returns:
    - graph_built: Whether graph has been built
    - type: Graph database type (Neo4j or InMemory)
    - stats: Current statistics
    """
    if not _graph_state["graph_built"]:
        return {
            "graph_built": False,
            "type": None,
            "stats": None,
        }
    
    if not _graph_state["neo4j"]:
        raise HTTPException(500, "Knowledge graph not initialized")
    
    neo = _graph_state["neo4j"]
    graph_type = "Neo4j" if "Neo4jClient" in str(type(neo)) else "InMemory"
    
    return {
        "graph_built": True,
        "type": graph_type,
        "stats": neo.get_statistics(),
    }
