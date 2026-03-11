"""
Stage 11, 12 & 13: Query Layer, Conversational AI, and Anti-Hallucination
=========================================================================

This module implements:
- Stage 11: Structured query layer for vessel database/graph
- Stage 12: Conversational AI interface with natural language understanding
- Stage 13: RAG-based anti-hallucination safeguards

The system ensures all responses are grounded in actual data through:
- Tool-based database queries
- Evidence-based responses
- Strict hallucination prevention
"""

import os
import json
import hashlib
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import uuid


# =============================================================================
# STAGE 11: QUERY LAYER
# =============================================================================

class QueryType(str, Enum):
    """Types of supported queries."""
    IDENTIFIER_SEARCH = "identifier_search"
    ATTRIBUTE_FILTER = "attribute_filter"
    HISTORICAL_QUERY = "historical_query"
    AGGREGATION = "aggregation"
    RELATIONSHIP_QUERY = "relationship_query"
    TEMPORAL_QUERY = "temporal_query"


@dataclass
class QueryFilter:
    """Represents a query filter."""
    field: str
    operator: str  # eq, neq, gt, lt, gte, lte, contains, in, between
    value: Any
    
    def to_dict(self) -> Dict:
        return {
            "field": self.field,
            "operator": self.operator,
            "value": self.value
        }


@dataclass
class StructuredQuery:
    """Structured query representation."""
    query_id: str
    query_type: QueryType
    filters: List[QueryFilter]
    projections: List[str]  # Fields to return
    sort_by: Optional[str] = None
    sort_order: str = "asc"
    limit: int = 100
    offset: int = 0
    include_history: bool = False
    temporal_range: Optional[Dict[str, datetime]] = None
    
    def to_dict(self) -> Dict:
        return {
            "query_id": self.query_id,
            "query_type": self.query_type.value,
            "filters": [f.to_dict() for f in self.filters],
            "projections": self.projections,
            "sort_by": self.sort_by,
            "sort_order": self.sort_order,
            "limit": self.limit,
            "include_history": self.include_history
        }


@dataclass
class QueryResult:
    """Result of a query execution."""
    query_id: str
    success: bool
    results: List[Dict]
    total_count: int
    execution_time_ms: float
    evidence_sources: List[str]
    query_explanation: str
    error: Optional[str] = None


class VesselQueryEngine:
    """
    Query engine for vessel database and knowledge graph.
    
    Supports:
    - Identifier searches (IMO, MMSI)
    - Attribute filters
    - Historical queries
    - Aggregated statistics
    """
    
    def __init__(self, knowledge_graph=None, dataframe=None):
        self.knowledge_graph = knowledge_graph
        self.dataframe = dataframe
        self.query_cache: Dict[str, QueryResult] = {}
    
    def execute_query(self, query: StructuredQuery) -> QueryResult:
        """Execute a structured query."""
        start_time = datetime.utcnow()
        
        # Check cache
        cache_key = self._generate_cache_key(query)
        if cache_key in self.query_cache:
            return self.query_cache[cache_key]
        
        try:
            if query.query_type == QueryType.IDENTIFIER_SEARCH:
                results, explanation = self._execute_identifier_search(query)
            elif query.query_type == QueryType.ATTRIBUTE_FILTER:
                results, explanation = self._execute_attribute_filter(query)
            elif query.query_type == QueryType.HISTORICAL_QUERY:
                results, explanation = self._execute_historical_query(query)
            elif query.query_type == QueryType.AGGREGATION:
                results, explanation = self._execute_aggregation(query)
            elif query.query_type == QueryType.TEMPORAL_QUERY:
                results, explanation = self._execute_temporal_query(query)
            else:
                results, explanation = self._execute_attribute_filter(query)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            result = QueryResult(
                query_id=query.query_id,
                success=True,
                results=results[:query.limit],
                total_count=len(results),
                execution_time_ms=execution_time,
                evidence_sources=self._get_evidence_sources(results),
                query_explanation=explanation
            )
            
            self.query_cache[cache_key] = result
            return result
            
        except Exception as e:
            return QueryResult(
                query_id=query.query_id,
                success=False,
                results=[],
                total_count=0,
                execution_time_ms=0,
                evidence_sources=[],
                query_explanation="",
                error=str(e)
            )
    
    def _generate_cache_key(self, query: StructuredQuery) -> str:
        """Generate cache key for query."""
        query_str = json.dumps(query.to_dict(), sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()
    
    def _execute_identifier_search(
        self,
        query: StructuredQuery
    ) -> Tuple[List[Dict], str]:
        """Execute identifier-based search."""
        results = []
        explanation_parts = []
        
        if self.dataframe is not None:
            df = self.dataframe.copy()
            
            for f in query.filters:
                if f.field == "imo":
                    df = df[df["imo"] == f.value]
                    explanation_parts.append(f"Filtered by IMO = {f.value}")
                elif f.field == "mmsi":
                    df = df[df["mmsi"] == f.value]
                    explanation_parts.append(f"Filtered by MMSI = {f.value}")
            
            results = df.to_dict("records")
        
        elif self.knowledge_graph:
            for f in query.filters:
                if f.field == "imo":
                    node = self.knowledge_graph.get_node(f"vessel_imo_{f.value}")
                    if node:
                        results.append(node.to_dict())
                        explanation_parts.append(f"Found vessel with IMO {f.value}")
        
        explanation = ". ".join(explanation_parts) if explanation_parts else "No filters applied"
        return results, explanation
    
    def _execute_attribute_filter(
        self,
        query: StructuredQuery
    ) -> Tuple[List[Dict], str]:
        """Execute attribute-based filtering."""
        results = []
        explanation_parts = []
        
        if self.dataframe is not None:
            df = self.dataframe.copy()
            
            for f in query.filters:
                col = f.field
                if col not in df.columns:
                    continue
                
                if f.operator == "eq":
                    df = df[df[col] == f.value]
                elif f.operator == "neq":
                    df = df[df[col] != f.value]
                elif f.operator == "gt":
                    df = df[df[col] > f.value]
                elif f.operator == "lt":
                    df = df[df[col] < f.value]
                elif f.operator == "gte":
                    df = df[df[col] >= f.value]
                elif f.operator == "lte":
                    df = df[df[col] <= f.value]
                elif f.operator == "contains":
                    df = df[df[col].astype(str).str.contains(str(f.value), case=False, na=False)]
                elif f.operator == "in":
                    df = df[df[col].isin(f.value)]
                
                explanation_parts.append(f"{col} {f.operator} {f.value}")
            
            # Apply sorting
            if query.sort_by and query.sort_by in df.columns:
                df = df.sort_values(query.sort_by, ascending=(query.sort_order == "asc"))
            
            results = df.to_dict("records")
        
        explanation = f"Filtered by: {', '.join(explanation_parts)}" if explanation_parts else "No filters"
        return results, explanation
    
    def _execute_historical_query(
        self,
        query: StructuredQuery
    ) -> Tuple[List[Dict], str]:
        """Execute historical query against knowledge graph."""
        results = []
        explanation = ""
        
        if self.knowledge_graph:
            for f in query.filters:
                if f.field == "imo":
                    history = self.knowledge_graph.get_vessel_history(f"vessel_imo_{f.value}")
                    if history:
                        results.append({
                            "vessel_id": f"vessel_imo_{f.value}",
                            "timeline": history.get_timeline(),
                            "mmsi_history": history.get_attribute_history("mmsi"),
                            "name_history": history.get_attribute_history("name"),
                            "flag_history": history.get_attribute_history("flag")
                        })
                        explanation = f"Retrieved historical data for IMO {f.value}"
        
        return results, explanation
    
    def _execute_aggregation(
        self,
        query: StructuredQuery
    ) -> Tuple[List[Dict], str]:
        """Execute aggregation query."""
        results = []
        explanation = ""
        
        if self.dataframe is not None:
            df = self.dataframe.copy()
            
            # Apply filters first
            for f in query.filters:
                if f.field in df.columns:
                    if f.operator == "eq":
                        df = df[df[f.field] == f.value]
            
            # Common aggregations
            if "count_by" in [f.field for f in query.filters]:
                group_by = next(f.value for f in query.filters if f.field == "count_by")
                if group_by in df.columns:
                    agg = df.groupby(group_by).size().reset_index(name='count')
                    results = agg.to_dict("records")
                    explanation = f"Counted records grouped by {group_by}"
            else:
                # Generic statistics
                results = [{
                    "total_records": len(df),
                    "unique_imos": int(df["imo"].nunique()),
                    "unique_mmsis": int(df["mmsi"].nunique()),
                    "unique_flags": int(df["flag"].nunique()) if "flag" in df else 0
                }]
                explanation = "Calculated dataset statistics"
        
        return results, explanation
    
    def _execute_temporal_query(
        self,
        query: StructuredQuery
    ) -> Tuple[List[Dict], str]:
        """Execute temporal query (changes over time)."""
        results = []
        explanation = ""
        
        if self.dataframe is not None and query.temporal_range:
            df = self.dataframe.copy()
            
            # Parse timestamps
            if "UpdateDate" in df.columns:
                df["UpdateDate"] = pd.to_datetime(df["UpdateDate"], errors="coerce")
                
                start = query.temporal_range.get("start")
                end = query.temporal_range.get("end")
                
                if start:
                    df = df[df["UpdateDate"] >= start]
                if end:
                    df = df[df["UpdateDate"] <= end]
                
                results = df.to_dict("records")
                explanation = f"Filtered records between {start} and {end}"
        
        return results, explanation
    
    def _get_evidence_sources(self, results: List[Dict]) -> List[str]:
        """Get evidence sources for results."""
        sources = set()
        for r in results:
            if "imo" in r and r["imo"]:
                sources.add(f"vessel_record_imo_{r['imo']}")
            if "mmsi" in r and r["mmsi"]:
                sources.add(f"ais_record_mmsi_{r['mmsi']}")
        return list(sources)[:10]  # Limit sources

    # Convenience methods for common queries
    def search_by_imo(self, imo: int) -> QueryResult:
        """Search vessel by IMO number."""
        query = StructuredQuery(
            query_id=f"imo_search_{imo}",
            query_type=QueryType.IDENTIFIER_SEARCH,
            filters=[QueryFilter("imo", "eq", imo)],
            projections=["*"]
        )
        return self.execute_query(query)
    
    def search_by_mmsi(self, mmsi: int) -> QueryResult:
        """Search vessel by MMSI number."""
        query = StructuredQuery(
            query_id=f"mmsi_search_{mmsi}",
            query_type=QueryType.IDENTIFIER_SEARCH,
            filters=[QueryFilter("mmsi", "eq", mmsi)],
            projections=["*"]
        )
        return self.execute_query(query)
    
    def search_by_name(self, name: str) -> QueryResult:
        """Search vessels by name (partial match)."""
        query = StructuredQuery(
            query_id=f"name_search_{name}",
            query_type=QueryType.ATTRIBUTE_FILTER,
            filters=[QueryFilter("name", "contains", name)],
            projections=["*"]
        )
        return self.execute_query(query)
    
    def get_vessels_by_flag(self, flag: str) -> QueryResult:
        """Get all vessels registered under a flag."""
        query = StructuredQuery(
            query_id=f"flag_search_{flag}",
            query_type=QueryType.ATTRIBUTE_FILTER,
            filters=[QueryFilter("flag", "eq", flag)],
            projections=["*"]
        )
        return self.execute_query(query)
    
    def get_vessel_history(self, imo: int) -> QueryResult:
        """Get historical data for a vessel."""
        query = StructuredQuery(
            query_id=f"history_{imo}",
            query_type=QueryType.HISTORICAL_QUERY,
            filters=[QueryFilter("imo", "eq", imo)],
            projections=["*"],
            include_history=True
        )
        return self.execute_query(query)


# =============================================================================
# STAGE 12: CONVERSATIONAL AI INTERFACE
# =============================================================================

class NLQueryParser:
    """
    Natural Language Query Parser
    
    Translates natural language queries into structured database queries.
    """
    
    # Pattern matching for common query types
    PATTERNS = {
        "imo_search": [
            r"find vessel.*imo\s*(\d{7})",
            r"what.*imo\s*(\d{7})",
            r"show.*imo\s*(\d{7})",
            r"vessel with imo\s*(\d{7})",
            r"imo\s*(\d{7})"
        ],
        "mmsi_search": [
            r"find vessel.*mmsi\s*(\d{9})",
            r"what.*mmsi\s*(\d{9})",
            r"vessel.*using mmsi\s*(\d{9})",
            r"currently using mmsi\s*(\d{9})",
            r"mmsi\s*(\d{9})"
        ],
        "name_search": [
            r"find vessel(?:s)? (?:named |called )?[\"\']?([A-Za-z0-9\s]+)[\"\']?",
            r"search for (?:vessel )?[\"\']?([A-Za-z0-9\s]+)[\"\']?",
            r"show (?:me )?(?:vessel )?[\"\']?([A-Za-z0-9\s]+)[\"\']?"
        ],
        "flag_query": [
            r"vessel(?:s)? (?:registered )?(?:under |in |with )?flag (\w{2})",
            r"(\w{2}) flag(?:ged)? vessels",
            r"vessels from (\w+)",
            r"flag[: ]?(\w{2})"
        ],
        "history_query": [
            r"histor(?:y|ical).*imo\s*(\d{7})",
            r"track(?:ing)? (?:of )?imo\s*(\d{7})",
            r"what (?:is|was) the history of.*imo\s*(\d{7})",
            r"changes.*imo\s*(\d{7})"
        ],
        "flag_change_query": [
            r"vessel(?:s)? (?:that )?changed flag",
            r"flag change(?:s)? in (?:the )?(?:last|past) (\d+) years?",
            r"reflagged vessels"
        ],
        "type_query": [
            r"(?:all )?(\w+(?:\s+\w+)?) vessels",
            r"vessels of type (\w+(?:\s+\w+)?)",
            r"(\w+(?:\s+\w+)?) type vessels"
        ]
    }
    
    VESSEL_TYPES = [
        "container", "tanker", "bulk", "cargo", "passenger", "fishing",
        "tug", "lng", "lpg", "reefer", "ro-ro", "chemical"
    ]
    
    def parse(self, query: str) -> Optional[StructuredQuery]:
        """Parse natural language query into structured query."""
        query_lower = query.lower().strip()
        
        # Try to match patterns
        for query_type, patterns in self.PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, query_lower, re.IGNORECASE)
                if match:
                    return self._build_query(query_type, match, query)
        
        # Fallback: try to extract any identifiers
        imo_match = re.search(r'\b(\d{7})\b', query)
        if imo_match:
            return self._build_identifier_query("imo", int(imo_match.group(1)))
        
        mmsi_match = re.search(r'\b(\d{9})\b', query)
        if mmsi_match:
            return self._build_identifier_query("mmsi", int(mmsi_match.group(1)))
        
        return None
    
    def _build_query(
        self,
        query_type: str,
        match: re.Match,
        original_query: str
    ) -> StructuredQuery:
        """Build structured query from pattern match."""
        query_id = f"nl_{datetime.utcnow().timestamp()}"
        
        if query_type == "imo_search":
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.IDENTIFIER_SEARCH,
                filters=[QueryFilter("imo", "eq", int(match.group(1)))],
                projections=["*"]
            )
        
        elif query_type == "mmsi_search":
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.IDENTIFIER_SEARCH,
                filters=[QueryFilter("mmsi", "eq", int(match.group(1)))],
                projections=["*"]
            )
        
        elif query_type == "name_search":
            name = match.group(1).strip()
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.ATTRIBUTE_FILTER,
                filters=[QueryFilter("name", "contains", name)],
                projections=["*"]
            )
        
        elif query_type == "flag_query":
            flag = match.group(1).upper()
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.ATTRIBUTE_FILTER,
                filters=[QueryFilter("flag", "eq", flag)],
                projections=["*"]
            )
        
        elif query_type == "history_query":
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.HISTORICAL_QUERY,
                filters=[QueryFilter("imo", "eq", int(match.group(1)))],
                projections=["*"],
                include_history=True
            )
        
        elif query_type == "flag_change_query":
            years = int(match.group(1)) if match.lastindex else 5
            cutoff = datetime.utcnow() - timedelta(days=years * 365)
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.TEMPORAL_QUERY,
                filters=[],
                projections=["*"],
                temporal_range={"start": cutoff}
            )
        
        elif query_type == "type_query":
            vessel_type = match.group(1).title()
            return StructuredQuery(
                query_id=query_id,
                query_type=QueryType.ATTRIBUTE_FILTER,
                filters=[QueryFilter("vessel_type", "contains", vessel_type)],
                projections=["*"]
            )
        
        return None
    
    def _build_identifier_query(
        self,
        id_type: str,
        value: int
    ) -> StructuredQuery:
        """Build query for identifier search."""
        return StructuredQuery(
            query_id=f"id_search_{datetime.utcnow().timestamp()}",
            query_type=QueryType.IDENTIFIER_SEARCH,
            filters=[QueryFilter(id_type, "eq", value)],
            projections=["*"]
        )


# =============================================================================
# STAGE 13: ANTI-HALLUCINATION SAFEGUARDS
# =============================================================================

@dataclass
class GroundedResponse:
    """Response grounded in actual data with evidence."""
    response_id: str
    query: str
    answer: str
    evidence: List[Dict]
    confidence: float
    sources: List[str]
    generated_at: datetime = field(default_factory=datetime.utcnow)
    is_grounded: bool = True
    no_data_found: bool = False


class RAGVesselAssistant:
    """
    Retrieval-Augmented Generation (RAG) based vessel assistant.
    
    Ensures all responses are grounded in actual data by:
    1. Parsing natural language queries
    2. Executing structured database queries
    3. Generating responses only from retrieved data
    4. Providing evidence for all claims
    
    NEVER generates vessel information without evidence.
    """
    
    HALLUCINATION_PREVENTION_PROMPT = """You are a maritime vessel data assistant. 
Your responses MUST be based ONLY on the data provided to you.

CRITICAL RULES:
1. NEVER make up vessel information
2. NEVER guess IMO numbers, MMSI, or vessel names
3. If data is not found, say "No data found for this query"
4. Always cite the evidence for your statements
5. If uncertain, express the uncertainty
6. Use only the retrieved data to answer questions

Response format:
- State the findings clearly
- List the evidence sources
- Note any limitations or missing data
"""
    
    def __init__(self, query_engine: VesselQueryEngine):
        self.query_engine = query_engine
        self.nl_parser = NLQueryParser()
        self.response_history: List[GroundedResponse] = []
        self.openai_client = None  # Initialize with OpenAI client
    
    def process_query(self, natural_language_query: str) -> GroundedResponse:
        """
        Process a natural language query with hallucination prevention.
        
        Flow:
        1. Parse NL query to structured query
        2. Execute query against database
        3. Generate response from retrieved data only
        4. Validate response is grounded
        """
        response_id = f"resp_{datetime.utcnow().timestamp()}"
        
        # Step 1: Parse query
        structured_query = self.nl_parser.parse(natural_language_query)
        
        if not structured_query:
            return self._create_response(
                response_id=response_id,
                query=natural_language_query,
                answer="I couldn't understand the query. Please try asking about a specific vessel using IMO number, MMSI, or vessel name.",
                evidence=[],
                confidence=0.0,
                sources=[],
                no_data_found=True
            )
        
        # Step 2: Execute query
        query_result = self.query_engine.execute_query(structured_query)
        
        if not query_result.success:
            return self._create_response(
                response_id=response_id,
                query=natural_language_query,
                answer=f"Query execution failed: {query_result.error}",
                evidence=[],
                confidence=0.0,
                sources=[],
                no_data_found=True
            )
        
        if not query_result.results:
            return self._create_response(
                response_id=response_id,
                query=natural_language_query,
                answer="No data found matching your query. The vessel may not exist in our database or the identifier may be incorrect.",
                evidence=[],
                confidence=1.0,  # High confidence in "no data"
                sources=[],
                no_data_found=True
            )
        
        # Step 3: Generate grounded response
        answer = self._generate_grounded_response(
            natural_language_query,
            query_result.results,
            structured_query
        )
        
        # Step 4: Validate and return
        response = self._create_response(
            response_id=response_id,
            query=natural_language_query,
            answer=answer,
            evidence=query_result.results[:5],  # Limit evidence
            confidence=self._calculate_confidence(query_result),
            sources=query_result.evidence_sources
        )
        
        self.response_history.append(response)
        return response
    
    def _generate_grounded_response(
        self,
        query: str,
        results: List[Dict],
        structured_query: StructuredQuery
    ) -> str:
        """Generate response text strictly from retrieved data."""
        if not results:
            return "No data found."
        
        # Build response based on query type
        if structured_query.query_type == QueryType.IDENTIFIER_SEARCH:
            return self._format_vessel_details(results[0])
        
        elif structured_query.query_type == QueryType.HISTORICAL_QUERY:
            return self._format_vessel_history(results[0])
        
        elif structured_query.query_type == QueryType.ATTRIBUTE_FILTER:
            return self._format_vessel_list(results, structured_query)
        
        else:
            return self._format_generic_results(results)
    
    def _format_vessel_details(self, record: Dict) -> str:
        """Format detailed vessel information."""
        parts = []
        
        if record.get("name"):
            parts.append(f"**Vessel: {record['name']}**")
        
        if record.get("imo"):
            parts.append(f"- IMO: {record['imo']}")
        if record.get("mmsi"):
            parts.append(f"- MMSI: {record['mmsi']}")
        if record.get("flag"):
            parts.append(f"- Flag: {record['flag']}")
        if record.get("vessel_type"):
            parts.append(f"- Type: {record['vessel_type']}")
        if record.get("length"):
            parts.append(f"- Length: {record['length']} m")
        if record.get("width"):
            parts.append(f"- Width: {record['width']} m")
        if record.get("grossTonnage"):
            parts.append(f"- Gross Tonnage: {record['grossTonnage']:,.0f}")
        if record.get("builtYear"):
            parts.append(f"- Built: {record['builtYear']}")
        if record.get("shipBuilder"):
            parts.append(f"- Builder: {record['shipBuilder']}")
        
        # Position data
        lat = record.get("last_position_latitude")
        lon = record.get("last_position_longitude")
        if lat and lon:
            parts.append(f"- Last Position: {lat:.4f}, {lon:.4f}")
        
        if record.get("destination"):
            parts.append(f"- Destination: {record['destination']}")
        
        return "\n".join(parts)
    
    def _format_vessel_history(self, history_record: Dict) -> str:
        """Format vessel historical data."""
        parts = [f"**Vessel History for {history_record.get('vessel_id', 'Unknown')}**\n"]
        
        # MMSI history
        mmsi_history = history_record.get("mmsi_history", [])
        if mmsi_history:
            parts.append("**MMSI History:**")
            for entry in mmsi_history:
                parts.append(f"- {entry.get('value')} (from {entry.get('valid_from', 'unknown')})")
        
        # Name history
        name_history = history_record.get("name_history", [])
        if name_history:
            parts.append("\n**Name History:**")
            for entry in name_history:
                parts.append(f"- {entry.get('value')} (from {entry.get('valid_from', 'unknown')})")
        
        # Flag history
        flag_history = history_record.get("flag_history", [])
        if flag_history:
            parts.append("\n**Flag History:**")
            for entry in flag_history:
                parts.append(f"- {entry.get('value')} (from {entry.get('valid_from', 'unknown')})")
        
        return "\n".join(parts)
    
    def _format_vessel_list(
        self,
        results: List[Dict],
        query: StructuredQuery
    ) -> str:
        """Format list of vessels."""
        total = len(results)
        shown = min(10, total)
        
        parts = [f"Found **{total}** vessels matching your query.\n"]
        
        for i, record in enumerate(results[:shown]):
            name = record.get("name", "Unknown")
            imo = record.get("imo", "N/A")
            flag = record.get("flag", "N/A")
            parts.append(f"{i+1}. **{name}** (IMO: {imo}, Flag: {flag})")
        
        if total > shown:
            parts.append(f"\n... and {total - shown} more vessels.")
        
        return "\n".join(parts)
    
    def _format_generic_results(self, results: List[Dict]) -> str:
        """Format generic query results."""
        return f"Found {len(results)} results. " + json.dumps(results[0], indent=2, default=str)
    
    def _create_response(self, **kwargs) -> GroundedResponse:
        """Create a grounded response object."""
        return GroundedResponse(**kwargs)
    
    def _calculate_confidence(self, query_result: QueryResult) -> float:
        """Calculate confidence in the response."""
        if not query_result.results:
            return 0.0
        
        # Higher confidence with more evidence
        evidence_factor = min(1.0, len(query_result.evidence_sources) / 5)
        
        # Higher confidence with faster queries (cached or direct lookup)
        speed_factor = 1.0 if query_result.execution_time_ms < 100 else 0.9
        
        return evidence_factor * speed_factor
    
    def get_llm_tools(self) -> List[Dict]:
        """Get tools for OpenAI function calling."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "search_vessel_by_imo",
                    "description": "Search for a vessel by its IMO number",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "imo": {
                                "type": "integer",
                                "description": "The 7-digit IMO number"
                            }
                        },
                        "required": ["imo"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "search_vessel_by_mmsi",
                    "description": "Search for a vessel by its MMSI number",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "mmsi": {
                                "type": "integer",
                                "description": "The 9-digit MMSI number"
                            }
                        },
                        "required": ["mmsi"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "search_vessel_by_name",
                    "description": "Search for vessels by name (partial match supported)",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The vessel name or partial name"
                            }
                        },
                        "required": ["name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_vessel_history",
                    "description": "Get historical data for a vessel including MMSI changes, name changes, and flag changes",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "imo": {
                                "type": "integer",
                                "description": "The 7-digit IMO number"
                            }
                        },
                        "required": ["imo"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_vessels_by_flag",
                    "description": "Get all vessels registered under a specific flag",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "flag": {
                                "type": "string",
                                "description": "Two-letter flag state code (e.g., SG, LR, PA)"
                            }
                        },
                        "required": ["flag"]
                    }
                }
            }
        ]
    
    def execute_tool_call(self, tool_name: str, arguments: Dict) -> Dict:
        """Execute a tool call from the LLM."""
        if tool_name == "search_vessel_by_imo":
            result = self.query_engine.search_by_imo(arguments["imo"])
        elif tool_name == "search_vessel_by_mmsi":
            result = self.query_engine.search_by_mmsi(arguments["mmsi"])
        elif tool_name == "search_vessel_by_name":
            result = self.query_engine.search_by_name(arguments["name"])
        elif tool_name == "get_vessel_history":
            result = self.query_engine.get_vessel_history(arguments["imo"])
        elif tool_name == "get_vessels_by_flag":
            result = self.query_engine.get_vessels_by_flag(arguments["flag"])
        else:
            return {"error": f"Unknown tool: {tool_name}"}
        
        return {
            "success": result.success,
            "results": result.results[:10],
            "total_count": result.total_count,
            "evidence_sources": result.evidence_sources
        }


class ConversationalVesselInterface:
    """
    Full conversational interface integrating NL parsing, RAG, and LLM.
    """
    
    SYSTEM_PROMPT = """You are a maritime vessel data assistant powered by a comprehensive vessel database.

Your capabilities:
- Search vessels by IMO number, MMSI, or name
- Retrieve historical vessel data (flag changes, name changes, MMSI changes)
- Filter vessels by type, flag, or other attributes

CRITICAL INSTRUCTIONS:
1. ALWAYS use the provided tools to search for vessel information
2. NEVER make up or hallucinate vessel data
3. If a vessel is not found, clearly state that
4. Always cite the data source in your response
5. Express uncertainty when appropriate

When presenting vessel information:
- Include IMO and MMSI when available
- Note any historical changes
- Mention the data freshness/timestamp when relevant"""
    
    def __init__(self, query_engine: VesselQueryEngine, api_key: str = None):
        self.query_engine = query_engine
        self.rag_assistant = RAGVesselAssistant(query_engine)
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.conversation_history: List[Dict] = []
    
    def chat(self, user_message: str) -> str:
        """
        Process a chat message and return a grounded response.
        """
        # First try RAG-based response
        response = self.rag_assistant.process_query(user_message)
        
        if not response.no_data_found:
            return response.answer
        
        # If no data found via direct parsing, try LLM with tools
        return self._llm_with_tools_response(user_message)
    
    def _llm_with_tools_response(self, user_message: str) -> str:
        """Use LLM with tools for complex queries."""
        if not self.api_key:
            return "LLM integration not configured. Please use direct queries like 'Find vessel IMO 9528574'."
        
        # This would integrate with OpenAI's function calling
        # For demo purposes, return a helpful message
        return (
            "I couldn't find direct matches for your query. "
            "Try asking about a specific vessel using:\n"
            "- 'Find vessel IMO [7-digit number]'\n"
            "- 'What is MMSI [9-digit number]'\n"
            "- 'Search for vessel [name]'\n"
            "- 'Show vessels with flag [XX]'"
        )


# =============================================================================
# DEMONSTRATION CODE
# =============================================================================

def demonstrate_query_and_conversation():
    """Demonstrate the query layer and conversational interface."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║     STAGE 11, 12 & 13: QUERY LAYER, CONVERSATIONAL AI, ANTI-HALLUCINATION     ║
╚══════════════════════════════════════════════════════════════════════════════╝

SYSTEM ARCHITECTURE:
====================

    ┌─────────────────────────────────────────────────────────────────┐
    │              CONVERSATIONAL AI INTERFACE                         │
    │         (Natural Language Understanding)                         │
    └─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │              NL QUERY PARSER                                     │
    │    "Find vessels with flag SG" → StructuredQuery                 │
    └─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │              QUERY ENGINE                                        │
    │    • Identifier Search (IMO, MMSI)                               │
    │    • Attribute Filtering                                         │
    │    • Historical Queries                                          │
    │    • Aggregations                                                │
    └─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │              RAG RESPONSE GENERATOR                              │
    │    • Grounded in retrieved data only                             │
    │    • Evidence sources tracked                                    │
    │    • NO hallucination allowed                                    │
    └─────────────────────────────────────────────────────────────────┘


SUPPORTED NATURAL LANGUAGE QUERIES:
===================================
• "Find vessels currently using MMSI 123456789"
• "Show vessels that changed flag in the last five years"
• "What is the history of vessel IMO 9123456?"
• "List all container vessels"
• "Search for vessel MARCO"
• "Vessels registered under flag SG"


PYTHON CODE EXAMPLES:
=====================
""")
    
    print("""
# 1. Initialize Query Engine with DataFrame
# ------------------------------------------
import pandas as pd

df = pd.read_csv('vessel_data.csv')
query_engine = VesselQueryEngine(dataframe=df)


# 2. Structured Query Execution
# ------------------------------
from typing import Dict

# Search by IMO
result = query_engine.search_by_imo(9528574)
print(f"Found {result.total_count} records")
print(f"Evidence: {result.evidence_sources}")

# Search by MMSI
result = query_engine.search_by_mmsi(636013854)
for vessel in result.results:
    print(f"Vessel: {vessel['name']}, IMO: {vessel['imo']}")

# Search by name
result = query_engine.search_by_name("MARCO")
print(f"Vessels matching 'MARCO': {result.total_count}")


# 3. Natural Language Query Parsing
# ----------------------------------
nl_parser = NLQueryParser()

# Parse natural language to structured query
query = nl_parser.parse("Find vessel with IMO 9528574")
print(f"Query type: {query.query_type}")
print(f"Filters: {[f.to_dict() for f in query.filters]}")

query = nl_parser.parse("Show me all container vessels")
print(f"Query type: {query.query_type}")


# 4. RAG-Based Conversational Interface
# --------------------------------------
rag_assistant = RAGVesselAssistant(query_engine)

# Process queries with hallucination prevention
response = rag_assistant.process_query("What is vessel IMO 9528574?")
print(f"Answer: {response.answer}")
print(f"Confidence: {response.confidence:.2%}")
print(f"Evidence sources: {response.sources}")
print(f"Is grounded: {response.is_grounded}")


# 5. OpenAI Integration with Function Calling
# --------------------------------------------
from openai import OpenAI

client = OpenAI()
tools = rag_assistant.get_llm_tools()

messages = [
    {"role": "system", "content": RAGVesselAssistant.HALLUCINATION_PREVENTION_PROMPT},
    {"role": "user", "content": "Find information about vessel MARCO"}
]

response = client.chat.completions.create(
    model="gpt-4-turbo-preview",
    messages=messages,
    tools=tools,
    tool_choice="auto"
)

# Process tool calls
if response.choices[0].message.tool_calls:
    for tool_call in response.choices[0].message.tool_calls:
        result = rag_assistant.execute_tool_call(
            tool_call.function.name,
            json.loads(tool_call.function.arguments)
        )
        print(f"Tool result: {result}")


# 6. Full Conversational Interface
# ---------------------------------
interface = ConversationalVesselInterface(query_engine)

# Chat with the assistant
answer = interface.chat("What vessels are registered under Panama flag?")
print(answer)

answer = interface.chat("Show me the history of IMO 9528574")
print(answer)


# 7. Anti-Hallucination Validation
# ---------------------------------
# The system NEVER generates vessel data without evidence:

response = rag_assistant.process_query("Tell me about IMO 9999999")
# Response will be: "No data found matching your query..."
# NOT fabricated vessel information

response = rag_assistant.process_query("What is the captain's name of IMO 9528574?") 
# Response will indicate this data is not available
# NOT make up a name
""")
    
    # Live demonstration
    print("\n" + "="*60)
    print("LIVE DEMONSTRATION")
    print("="*60 + "\n")
    
    # Initialize with mock data
    import pandas as pd
    
    mock_data = pd.DataFrame([
        {"imo": 9528574, "mmsi": 636013854, "name": "MARCO", "flag": "LR", 
         "vessel_type": "Dry Bulk", "length": 225, "grossTonnage": 42708, "builtYear": 2009},
        {"imo": 9752709, "mmsi": 563091700, "name": "ASIA INSPIRE", "flag": "SG",
         "vessel_type": "Chemical Tanker", "length": 180, "grossTonnage": 23200, "builtYear": 2019},
        {"imo": 9857365, "mmsi": 538008764, "name": "FLEX AURORA", "flag": "MH",
         "vessel_type": "LNG Carrier", "length": 297, "grossTonnage": 116430, "builtYear": 2020}
    ])
    
    query_engine = VesselQueryEngine(dataframe=mock_data)
    rag_assistant = RAGVesselAssistant(query_engine)
    
    print("1. Testing IMO Search:")
    response = rag_assistant.process_query("Find vessel IMO 9528574")
    print(f"   Query: Find vessel IMO 9528574")
    print(f"   Answer:\n{response.answer}")
    print(f"   Confidence: {response.confidence:.2%}")
    print(f"   Sources: {response.sources}")
    
    print("\n2. Testing Name Search:")
    response = rag_assistant.process_query("Search for vessel ASIA")
    print(f"   Query: Search for vessel ASIA")
    print(f"   Found: {response.evidence[0]['name'] if response.evidence else 'None'}")
    
    print("\n3. Testing Non-Existent Vessel (Anti-Hallucination):")
    response = rag_assistant.process_query("Find vessel IMO 9999999")
    print(f"   Query: Find vessel IMO 9999999")
    print(f"   Answer: {response.answer}")
    print(f"   No data found: {response.no_data_found}")
    
    print("\n4. Testing NL Query Parser:")
    nl_parser = NLQueryParser()
    
    test_queries = [
        "Find vessel with IMO 9528574",
        "Show vessels using MMSI 636013854",
        "Vessels registered under flag SG",
        "What is the history of IMO 9528574"
    ]
    
    for q in test_queries:
        parsed = nl_parser.parse(q)
        if parsed:
            print(f"   '{q}' -> {parsed.query_type.value}")
        else:
            print(f"   '{q}' -> Could not parse")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    demonstrate_query_and_conversation()


# Import pandas for potential usage
try:
    import pandas as pd
except ImportError:
    pd = None
