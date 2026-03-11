"""
Stage 3 & 4: Multi-Agent Architecture with Specialized Tools
============================================================

This module implements a multi-agent architecture powered by OpenAI SDK where:
- An LLM acts as the orchestrator calling specialized tools/agents
- Specialized agents handle specific tasks (validation, similarity, conflict detection, etc.)
- The system produces deterministic, traceable outputs

Architecture:
- Orchestrator Agent: Interprets tasks, selects tools, aggregates results
- Validation Agent: Dataset validation and quality checks
- Similarity Agent: Vessel record similarity detection
- Conflict Detection Agent: Identifier conflict analysis
- Anomaly Agent: AI-assisted anomaly detection
- Knowledge Graph Agent: Graph construction and querying
- Query Agent: Database/graph query handling
"""

import os
import json
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib


# =============================================================================
# AGENT FRAMEWORK
# =============================================================================

class AgentRole(str, Enum):
    """Roles of specialized agents."""
    ORCHESTRATOR = "orchestrator"
    VALIDATOR = "validator"
    SIMILARITY = "similarity"
    CONFLICT_DETECTOR = "conflict_detector"
    ANOMALY_DETECTOR = "anomaly_detector"
    KNOWLEDGE_GRAPH = "knowledge_graph"
    QUERY_HANDLER = "query_handler"
    ENTITY_RESOLVER = "entity_resolver"


@dataclass
class AgentContext:
    """Context passed to agents for execution."""
    task_id: str
    input_data: Dict[str, Any]
    previous_results: List[Dict] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentResult:
    """Result from an agent execution."""
    agent_role: AgentRole
    task_id: str
    success: bool
    output: Dict[str, Any]
    evidence: List[str]
    execution_time_ms: float
    error: Optional[str] = None


@dataclass
class Tool:
    """Represents a tool callable by the LLM orchestrator."""
    name: str
    description: str
    parameters: Dict[str, Any]
    handler: Callable
    
    def to_openai_function(self) -> Dict:
        """Convert to OpenAI function calling format."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters
            }
        }


# =============================================================================
# SPECIALIZED TOOL IMPLEMENTATIONS
# =============================================================================

class ValidationTool:
    """Tool for dataset validation operations."""
    
    @staticmethod
    def validate_vessel_identifiers(imo: Optional[int], mmsi: Optional[int]) -> Dict:
        """
        Validate vessel identifiers (IMO and MMSI).
        
        Args:
            imo: IMO number to validate
            mmsi: MMSI number to validate
            
        Returns:
            Validation results with details
        """
        results = {"imo_valid": None, "mmsi_valid": None, "errors": []}
        
        # IMO validation
        if imo is not None:
            if imo == 0:
                results["imo_valid"] = False
                results["errors"].append("IMO cannot be 0")
            elif imo < 1000000 or imo > 9999999:
                results["imo_valid"] = False
                results["errors"].append(f"IMO must be 7 digits: {imo}")
            else:
                # Checksum validation
                imo_str = str(imo)
                weights = [7, 6, 5, 4, 3, 2]
                checksum = sum(int(imo_str[i]) * weights[i] for i in range(6))
                if checksum % 10 == int(imo_str[6]):
                    results["imo_valid"] = True
                else:
                    results["imo_valid"] = False
                    results["errors"].append(f"IMO checksum failed: {imo}")
        
        # MMSI validation
        if mmsi is not None:
            mmsi_str = str(mmsi)
            if len(mmsi_str) != 9:
                results["mmsi_valid"] = False
                results["errors"].append(f"MMSI must be 9 digits: {mmsi}")
            else:
                mid = int(mmsi_str[:3])
                if 200 <= mid <= 775:
                    results["mmsi_valid"] = True
                else:
                    results["mmsi_valid"] = False
                    results["errors"].append(f"Invalid MMSI MID: {mid}")
        
        return results


class SimilarityTool:
    """Tool for vessel record similarity detection."""
    
    @staticmethod
    def calculate_name_similarity(name1: str, name2: str) -> float:
        """
        Calculate similarity between two vessel names.
        Uses character-level Jaccard similarity.
        """
        if not name1 or not name2:
            return 0.0
        
        n1 = set(name1.upper().strip())
        n2 = set(name2.upper().strip())
        
        intersection = len(n1.intersection(n2))
        union = len(n1.union(n2))
        
        return intersection / union if union > 0 else 0.0
    
    @staticmethod
    def calculate_record_similarity(record1: Dict, record2: Dict) -> Dict:
        """
        Calculate comprehensive similarity between two vessel records.
        
        Returns:
            Dictionary with overall similarity and per-attribute scores
        """
        scores = {}
        weights = {
            "name": 0.3,
            "imo": 0.25,
            "mmsi": 0.15,
            "length": 0.1,
            "width": 0.1,
            "built_year": 0.1
        }
        
        # Name similarity
        if record1.get("name") and record2.get("name"):
            scores["name"] = SimilarityTool.calculate_name_similarity(
                record1["name"], record2["name"]
            )
        
        # IMO match
        if record1.get("imo") and record2.get("imo"):
            scores["imo"] = 1.0 if record1["imo"] == record2["imo"] else 0.0
        
        # MMSI match
        if record1.get("mmsi") and record2.get("mmsi"):
            scores["mmsi"] = 1.0 if record1["mmsi"] == record2["mmsi"] else 0.0
        
        # Dimension similarity (with 10% tolerance)
        for dim in ["length", "width"]:
            v1 = record1.get(dim)
            v2 = record2.get(dim)
            if v1 and v2 and v1 > 0 and v2 > 0:
                diff = abs(v1 - v2) / max(v1, v2)
                scores[dim] = max(0, 1 - diff / 0.1)  # 10% tolerance
        
        # Build year match
        if record1.get("builtYear") and record2.get("builtYear"):
            scores["built_year"] = 1.0 if record1["builtYear"] == record2["builtYear"] else 0.0
        
        # Calculate weighted overall score
        total_weight = 0
        weighted_sum = 0
        for attr, weight in weights.items():
            if attr in scores:
                weighted_sum += scores[attr] * weight
                total_weight += weight
        
        overall = weighted_sum / total_weight if total_weight > 0 else 0.0
        
        return {
            "overall_similarity": round(overall, 4),
            "attribute_scores": scores,
            "matching_attributes": [k for k, v in scores.items() if v > 0.9],
            "conflicting_attributes": [k for k, v in scores.items() if v < 0.5]
        }


class ConflictDetectionTool:
    """Tool for detecting identifier conflicts."""
    
    @staticmethod
    def detect_imo_mmsi_conflict(records: List[Dict]) -> Dict:
        """
        Detect conflicts between IMO and MMSI across records.
        
        Args:
            records: List of vessel records
            
        Returns:
            Dictionary with conflict analysis
        """
        imo_to_mmsi = {}
        mmsi_to_imo = {}
        conflicts = {
            "imo_with_multiple_mmsi": [],
            "mmsi_with_multiple_imo": [],
            "total_conflicts": 0
        }
        
        for record in records:
            imo = record.get("imo")
            mmsi = record.get("mmsi")
            
            if imo and imo > 0 and mmsi:
                # Track IMO -> MMSI mappings
                if imo not in imo_to_mmsi:
                    imo_to_mmsi[imo] = set()
                imo_to_mmsi[imo].add(mmsi)
                
                # Track MMSI -> IMO mappings
                if mmsi not in mmsi_to_imo:
                    mmsi_to_imo[mmsi] = set()
                mmsi_to_imo[mmsi].add(imo)
        
        # Find conflicts
        for imo, mmsi_set in imo_to_mmsi.items():
            if len(mmsi_set) > 1:
                conflicts["imo_with_multiple_mmsi"].append({
                    "imo": imo,
                    "mmsi_values": list(mmsi_set)
                })
        
        for mmsi, imo_set in mmsi_to_imo.items():
            if len(imo_set) > 1:
                conflicts["mmsi_with_multiple_imo"].append({
                    "mmsi": mmsi,
                    "imo_values": list(imo_set)
                })
        
        conflicts["total_conflicts"] = (
            len(conflicts["imo_with_multiple_mmsi"]) +
            len(conflicts["mmsi_with_multiple_imo"])
        )
        
        return conflicts


class AnomalyDetectionTool:
    """Tool for AI-assisted anomaly detection."""
    
    @staticmethod
    def score_vessel_record_anomaly(record: Dict) -> Dict:
        """
        Calculate anomaly score for a vessel record.
        
        Returns:
            Dictionary with anomaly scores and flags
        """
        anomalies = {
            "total_score": 0.0,
            "flags": [],
            "details": {}
        }
        
        # Check for invalid IMO
        imo = record.get("imo")
        if imo is not None:
            if imo == 0:
                anomalies["flags"].append("zero_imo")
                anomalies["total_score"] += 0.3
            elif imo < 1000000:
                anomalies["flags"].append("invalid_imo_format")
                anomalies["total_score"] += 0.4
        
        # Check for garbled name
        name = record.get("name", "")
        if name:
            # Check for unusual characters
            unusual_chars = sum(1 for c in name if not c.isalnum() and c != ' ')
            if unusual_chars > len(name) * 0.3:
                anomalies["flags"].append("garbled_name")
                anomalies["total_score"] += 0.3
        
        # Check for null coordinates
        lat = record.get("last_position_latitude")
        lon = record.get("last_position_longitude")
        if lat == 0 and lon == 0:
            anomalies["flags"].append("null_island_coordinates")
            anomalies["total_score"] += 0.2
        
        # Check for invalid coordinates
        if lat is not None and (lat < -90 or lat > 90):
            anomalies["flags"].append("invalid_latitude")
            anomalies["total_score"] += 0.3
        if lon is not None and (lon < -180 or lon > 180):
            anomalies["flags"].append("invalid_longitude")
            anomalies["total_score"] += 0.3
        
        anomalies["total_score"] = min(1.0, anomalies["total_score"])
        
        return anomalies


class KnowledgeGraphTool:
    """Tool for knowledge graph operations."""
    
    @staticmethod
    def extract_entities_from_record(record: Dict) -> Dict:
        """
        Extract entities and relationships from a vessel record.
        
        Returns:
            Dictionary with extracted entities and relationships
        """
        entities = []
        relationships = []
        
        # Vessel entity
        if record.get("imo") or record.get("mmsi"):
            vessel_id = f"vessel_{record.get('imo', record.get('mmsi'))}"
            entities.append({
                "id": vessel_id,
                "type": "Vessel",
                "properties": {
                    "imo": record.get("imo"),
                    "name": record.get("name"),
                    "vessel_type": record.get("vessel_type"),
                    "length": record.get("length"),
                    "width": record.get("width"),
                    "gross_tonnage": record.get("grossTonnage"),
                    "built_year": record.get("builtYear")
                }
            })
            
            # MMSI entity
            if record.get("mmsi"):
                mmsi_id = f"mmsi_{record['mmsi']}"
                entities.append({
                    "id": mmsi_id,
                    "type": "MMSI",
                    "properties": {"value": record["mmsi"]}
                })
                relationships.append({
                    "source": vessel_id,
                    "target": mmsi_id,
                    "type": "USES_MMSI"
                })
            
            # Flag entity
            if record.get("flag"):
                flag_id = f"flag_{record['flag']}"
                entities.append({
                    "id": flag_id,
                    "type": "Flag",
                    "properties": {"code": record["flag"]}
                })
                relationships.append({
                    "source": vessel_id,
                    "target": flag_id,
                    "type": "REGISTERED_UNDER"
                })
            
            # Builder entity
            if record.get("shipBuilder"):
                builder_id = f"builder_{hashlib.md5(record['shipBuilder'].encode()).hexdigest()[:8]}"
                entities.append({
                    "id": builder_id,
                    "type": "ShipBuilder",
                    "properties": {"name": record["shipBuilder"]}
                })
                relationships.append({
                    "source": vessel_id,
                    "target": builder_id,
                    "type": "BUILT_BY"
                })
            
            # Port entity
            if record.get("matchedPort_name"):
                port_id = f"port_{record.get('matchedPort_unlocode', hashlib.md5(record['matchedPort_name'].encode()).hexdigest()[:8])}"
                entities.append({
                    "id": port_id,
                    "type": "Port",
                    "properties": {
                        "name": record["matchedPort_name"],
                        "unlocode": record.get("matchedPort_unlocode"),
                        "latitude": record.get("matchedPort_latitude"),
                        "longitude": record.get("matchedPort_longitude")
                    }
                })
                relationships.append({
                    "source": vessel_id,
                    "target": port_id,
                    "type": "VISITED"
                })
        
        return {
            "entities": entities,
            "relationships": relationships,
            "entity_count": len(entities),
            "relationship_count": len(relationships)
        }


class QueryTool:
    """Tool for structured query handling."""
    
    @staticmethod
    def build_vessel_search_query(
        imo: Optional[int] = None,
        mmsi: Optional[int] = None,
        name: Optional[str] = None,
        vessel_type: Optional[str] = None,
        flag: Optional[str] = None,
        min_length: Optional[float] = None,
        max_length: Optional[float] = None
    ) -> Dict:
        """
        Build a structured query for vessel search.
        
        Returns:
            Query specification that can be executed against database
        """
        query = {
            "type": "vessel_search",
            "filters": [],
            "sort": "name",
            "limit": 100
        }
        
        if imo:
            query["filters"].append({"field": "imo", "operator": "eq", "value": imo})
        if mmsi:
            query["filters"].append({"field": "mmsi", "operator": "eq", "value": mmsi})
        if name:
            query["filters"].append({"field": "name", "operator": "contains", "value": name})
        if vessel_type:
            query["filters"].append({"field": "vessel_type", "operator": "eq", "value": vessel_type})
        if flag:
            query["filters"].append({"field": "flag", "operator": "eq", "value": flag})
        if min_length:
            query["filters"].append({"field": "length", "operator": "gte", "value": min_length})
        if max_length:
            query["filters"].append({"field": "length", "operator": "lte", "value": max_length})
        
        return query


# =============================================================================
# LLM ORCHESTRATOR
# =============================================================================

class LLMOrchestrator:
    """
    LLM-powered orchestrator that interprets tasks and calls specialized tools.
    
    This is the central coordinator that:
    1. Receives natural language or structured requests
    2. Determines which tools to call
    3. Executes tools in appropriate sequence
    4. Aggregates and returns results
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.tools = self._register_tools()
        self.execution_history = []
    
    def _register_tools(self) -> Dict[str, Tool]:
        """Register all available tools."""
        return {
            "validate_identifiers": Tool(
                name="validate_identifiers",
                description="Validate vessel identifiers (IMO and MMSI numbers)",
                parameters={
                    "type": "object",
                    "properties": {
                        "imo": {"type": "integer", "description": "IMO number to validate"},
                        "mmsi": {"type": "integer", "description": "MMSI number to validate"}
                    }
                },
                handler=ValidationTool.validate_vessel_identifiers
            ),
            "calculate_similarity": Tool(
                name="calculate_similarity",
                description="Calculate similarity between two vessel records",
                parameters={
                    "type": "object",
                    "properties": {
                        "record1": {"type": "object", "description": "First vessel record"},
                        "record2": {"type": "object", "description": "Second vessel record"}
                    },
                    "required": ["record1", "record2"]
                },
                handler=SimilarityTool.calculate_record_similarity
            ),
            "detect_conflicts": Tool(
                name="detect_conflicts",
                description="Detect identifier conflicts across vessel records",
                parameters={
                    "type": "object",
                    "properties": {
                        "records": {"type": "array", "description": "List of vessel records"}
                    },
                    "required": ["records"]
                },
                handler=ConflictDetectionTool.detect_imo_mmsi_conflict
            ),
            "score_anomaly": Tool(
                name="score_anomaly",
                description="Calculate anomaly score for a vessel record",
                parameters={
                    "type": "object",
                    "properties": {
                        "record": {"type": "object", "description": "Vessel record to analyze"}
                    },
                    "required": ["record"]
                },
                handler=AnomalyDetectionTool.score_vessel_record_anomaly
            ),
            "extract_entities": Tool(
                name="extract_entities",
                description="Extract entities and relationships from a vessel record for knowledge graph",
                parameters={
                    "type": "object",
                    "properties": {
                        "record": {"type": "object", "description": "Vessel record"}
                    },
                    "required": ["record"]
                },
                handler=KnowledgeGraphTool.extract_entities_from_record
            ),
            "build_search_query": Tool(
                name="build_search_query",
                description="Build a structured search query for vessels",
                parameters={
                    "type": "object",
                    "properties": {
                        "imo": {"type": "integer", "description": "IMO number"},
                        "mmsi": {"type": "integer", "description": "MMSI number"},
                        "name": {"type": "string", "description": "Vessel name (partial match)"},
                        "vessel_type": {"type": "string", "description": "Type of vessel"},
                        "flag": {"type": "string", "description": "Flag state code"}
                    }
                },
                handler=QueryTool.build_vessel_search_query
            )
        }
    
    def get_tools_for_openai(self) -> List[Dict]:
        """Get tools in OpenAI function calling format."""
        return [tool.to_openai_function() for tool in self.tools.values()]
    
    def execute_tool(self, tool_name: str, arguments: Dict) -> AgentResult:
        """Execute a specific tool with given arguments."""
        start_time = datetime.utcnow()
        
        if tool_name not in self.tools:
            return AgentResult(
                agent_role=AgentRole.ORCHESTRATOR,
                task_id=f"tool_{datetime.utcnow().timestamp()}",
                success=False,
                output={},
                evidence=[],
                execution_time_ms=0,
                error=f"Unknown tool: {tool_name}"
            )
        
        tool = self.tools[tool_name]
        
        try:
            result = tool.handler(**arguments)
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            agent_result = AgentResult(
                agent_role=AgentRole.ORCHESTRATOR,
                task_id=f"tool_{datetime.utcnow().timestamp()}",
                success=True,
                output=result,
                evidence=[f"Executed {tool_name} with {len(arguments)} arguments"],
                execution_time_ms=execution_time
            )
            
            self.execution_history.append({
                "tool": tool_name,
                "arguments": arguments,
                "result": result,
                "timestamp": datetime.utcnow().isoformat()
            })
            
            return agent_result
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            return AgentResult(
                agent_role=AgentRole.ORCHESTRATOR,
                task_id=f"tool_{datetime.utcnow().timestamp()}",
                success=False,
                output={},
                evidence=[],
                execution_time_ms=execution_time,
                error=str(e)
            )
    
    def process_vessel_record(self, record: Dict) -> Dict:
        """
        Process a single vessel record through the full pipeline.
        
        This demonstrates the orchestrator calling multiple tools sequentially.
        """
        results = {
            "record_id": record.get("imo") or record.get("mmsi"),
            "validation": None,
            "anomaly_score": None,
            "entities": None,
            "processing_time_ms": 0
        }
        
        start_time = datetime.utcnow()
        
        # Step 1: Validate identifiers
        validation_result = self.execute_tool("validate_identifiers", {
            "imo": record.get("imo"),
            "mmsi": record.get("mmsi")
        })
        results["validation"] = validation_result.output
        
        # Step 2: Score anomalies
        anomaly_result = self.execute_tool("score_anomaly", {"record": record})
        results["anomaly_score"] = anomaly_result.output
        
        # Step 3: Extract entities for knowledge graph
        entity_result = self.execute_tool("extract_entities", {"record": record})
        results["entities"] = entity_result.output
        
        results["processing_time_ms"] = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        return results


# =============================================================================
# DEMONSTRATION CODE
# =============================================================================

def demonstrate_multi_agent_architecture():
    """Demonstrate the multi-agent architecture with examples."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║         STAGE 3 & 4: MULTI-AGENT ARCHITECTURE & SPECIALIZED TOOLS             ║
╚══════════════════════════════════════════════════════════════════════════════╝

ARCHITECTURE OVERVIEW:
======================
The system uses an LLM as orchestrator that calls specialized tools/agents:

    ┌─────────────────────────────────────────────────────────────────┐
    │                      LLM ORCHESTRATOR                            │
    │   (Interprets tasks, selects tools, aggregates results)          │
    └─────────────────────────────────────────────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │                          │                          │
         ▼                          ▼                          ▼
    ┌─────────────┐         ┌─────────────┐          ┌─────────────┐
    │ Validation  │         │ Similarity  │          │  Conflict   │
    │    Tool     │         │    Tool     │          │  Detection  │
    └─────────────┘         └─────────────┘          └─────────────┘
         │                          │                          │
         ▼                          ▼                          ▼
    ┌─────────────┐         ┌─────────────┐          ┌─────────────┐
    │  Anomaly    │         │  Knowledge  │          │   Query     │
    │  Detection  │         │    Graph    │          │   Handler   │
    └─────────────┘         └─────────────┘          └─────────────┘


PYTHON CODE EXAMPLES:
=====================
""")
    
    print("""
# 1. Initialize the LLM Orchestrator
# -----------------------------------
from openai import OpenAI

orchestrator = LLMOrchestrator()

# Get tools in OpenAI function calling format
tools = orchestrator.get_tools_for_openai()
print(f"Available tools: {[t['function']['name'] for t in tools]}")


# 2. OpenAI Integration Example
# ------------------------------
client = OpenAI()

messages = [
    {"role": "system", "content": '''You are a maritime vessel data analysis assistant.
    Use the provided tools to validate vessel data, detect anomalies, and build queries.
    NEVER make up vessel information - always use tools to get real data.'''},
    {"role": "user", "content": "Validate the vessel with IMO 9528574 and MMSI 636013854"}
]

# Let LLM decide which tool to call
response = client.chat.completions.create(
    model="gpt-4-turbo-preview",
    messages=messages,
    tools=tools,
    tool_choice="auto"
)

# Process tool calls
if response.choices[0].message.tool_calls:
    for tool_call in response.choices[0].message.tool_calls:
        tool_name = tool_call.function.name
        arguments = json.loads(tool_call.function.arguments)
        
        # Execute the tool
        result = orchestrator.execute_tool(tool_name, arguments)
        print(f"Tool: {tool_name}")
        print(f"Result: {result.output}")


# 3. Validation Tool Example
# ---------------------------
result = ValidationTool.validate_vessel_identifiers(
    imo=9528574,
    mmsi=636013854
)
print(f"Validation result: {result}")
# Output: {'imo_valid': True, 'mmsi_valid': True, 'errors': []}


# 4. Similarity Tool Example
# ---------------------------
record1 = {
    "imo": 9528574,
    "name": "MARCO",
    "length": 225,
    "width": 32,
    "builtYear": 2009
}
record2 = {
    "imo": 9528574,
    "name": "MARCO POLO",
    "length": 225,
    "width": 32,
    "builtYear": 2009
}

similarity = SimilarityTool.calculate_record_similarity(record1, record2)
print(f"Overall similarity: {similarity['overall_similarity']}")
print(f"Matching attributes: {similarity['matching_attributes']}")


# 5. Conflict Detection Example
# ------------------------------
records = [
    {"imo": 9528574, "mmsi": 636013854, "name": "MARCO"},
    {"imo": 9528574, "mmsi": 636013855, "name": "MARCO"},  # Same IMO, different MMSI
    {"imo": 9710749, "mmsi": 55637776, "name": "HOEGH TROTTER"},
    {"imo": 9710749, "mmsi": 259870736, "name": "HOEGH TROTTER"}  # Same IMO, different MMSI
]

conflicts = ConflictDetectionTool.detect_imo_mmsi_conflict(records)
print(f"Total conflicts: {conflicts['total_conflicts']}")
print(f"IMOs with multiple MMSI: {conflicts['imo_with_multiple_mmsi']}")


# 6. Anomaly Detection Example
# -----------------------------
suspicious_record = {
    "imo": 0,
    "mmsi": 123456789,
    "name": "!@#$%^&*()",
    "last_position_latitude": 0,
    "last_position_longitude": 0
}

anomaly = AnomalyDetectionTool.score_vessel_record_anomaly(suspicious_record)
print(f"Anomaly score: {anomaly['total_score']}")
print(f"Flags: {anomaly['flags']}")


# 7. Knowledge Graph Entity Extraction
# -------------------------------------
record = {
    "imo": 9528574,
    "mmsi": 636013854,
    "name": "MARCO",
    "flag": "LR",
    "shipBuilder": "Universal Maizuru",
    "matchedPort_name": "Singapore",
    "matchedPort_unlocode": "SGSIN"
}

entities = KnowledgeGraphTool.extract_entities_from_record(record)
print(f"Extracted {entities['entity_count']} entities")
print(f"Extracted {entities['relationship_count']} relationships")

for entity in entities['entities']:
    print(f"  - {entity['type']}: {entity['id']}")

for rel in entities['relationships']:
    print(f"  - {rel['source']} --[{rel['type']}]--> {rel['target']}")


# 8. Query Building Example
# --------------------------
query = QueryTool.build_vessel_search_query(
    vessel_type="Container",
    flag="SG",
    min_length=200
)
print(f"Generated query: {json.dumps(query, indent=2)}")
""")
    
    # Run actual demonstrations
    print("\n" + "="*60)
    print("LIVE DEMONSTRATION")
    print("="*60 + "\n")
    
    # Demo validation
    print("1. Testing Validation Tool:")
    result = ValidationTool.validate_vessel_identifiers(9528574, 636013854)
    print(f"   IMO 9528574, MMSI 636013854: {result}")
    
    result = ValidationTool.validate_vessel_identifiers(0, 12345)
    print(f"   IMO 0, MMSI 12345: {result}")
    
    # Demo similarity
    print("\n2. Testing Similarity Tool:")
    r1 = {"imo": 9528574, "name": "MARCO", "length": 225}
    r2 = {"imo": 9528574, "name": "MARCO POLO", "length": 225}
    sim = SimilarityTool.calculate_record_similarity(r1, r2)
    print(f"   Similarity between MARCO and MARCO POLO: {sim['overall_similarity']:.2%}")
    
    # Demo anomaly detection
    print("\n3. Testing Anomaly Detection:")
    bad_record = {"imo": 0, "name": "!@#$", "last_position_latitude": 0, "last_position_longitude": 0}
    anomaly = AnomalyDetectionTool.score_vessel_record_anomaly(bad_record)
    print(f"   Anomaly score for suspicious record: {anomaly['total_score']:.2f}")
    print(f"   Flags: {anomaly['flags']}")
    
    # Demo entity extraction
    print("\n4. Testing Knowledge Graph Entity Extraction:")
    record = {
        "imo": 9528574, "mmsi": 636013854, "name": "MARCO",
        "flag": "LR", "shipBuilder": "Universal Maizuru"
    }
    entities = KnowledgeGraphTool.extract_entities_from_record(record)
    print(f"   Extracted: {entities['entity_count']} entities, {entities['relationship_count']} relationships")
    
    print("\n" + "="*60)


# ToolRegistry - Collection of all available tools for the orchestrator
class ToolRegistry:
    """Registry of all available tools for the LLM orchestrator."""
    
    TOOLS = {
        "validation": ValidationTool,
        "similarity": SimilarityTool,
        "conflict_detection": ConflictDetectionTool,
        "anomaly_detection": AnomalyDetectionTool,
        "knowledge_graph": KnowledgeGraphTool,
        "query": QueryTool,
    }
    
    @classmethod
    def get_tool(cls, name: str):
        """Get a tool by name."""
        return cls.TOOLS.get(name)
    
    @classmethod
    def list_tools(cls) -> List[str]:
        """List all available tools."""
        return list(cls.TOOLS.keys())


if __name__ == "__main__":
    demonstrate_multi_agent_architecture()
