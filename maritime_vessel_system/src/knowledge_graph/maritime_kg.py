"""
Stage 8, 9 & 10: Knowledge Graph with Temporal History and Canonical Ground Truth
================================================================================

This module implements:
- Stage 8: Dataset-derived maritime knowledge graph
- Stage 9: Temporal vessel state tracking
- Stage 10: Canonical ground truth representation

The knowledge graph captures entities, relationships, and temporal changes
while serving as the authoritative source of vessel truth.
"""

import uuid
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import json


# =============================================================================
# KNOWLEDGE GRAPH NODE AND EDGE DEFINITIONS
# =============================================================================

class NodeType(str, Enum):
    """Types of nodes in the maritime knowledge graph."""
    VESSEL = "Vessel"
    MMSI = "MMSI"
    IMO = "IMO"
    PORT = "Port"
    FLAG = "Flag"
    SHIP_BUILDER = "ShipBuilder"
    OWNER = "Owner"
    LOCATION = "Location"
    VESSEL_TYPE = "VesselType"


class RelationshipType(str, Enum):
    """Types of relationships in the knowledge graph."""
    USES_MMSI = "USES_MMSI"
    HAS_IMO = "HAS_IMO"
    REGISTERED_UNDER = "REGISTERED_UNDER"
    BUILT_BY = "BUILT_BY"
    OWNED_BY = "OWNED_BY"
    VISITED = "VISITED"
    DEPARTED_FROM = "DEPARTED_FROM"
    LOCATED_AT = "LOCATED_AT"
    IS_TYPE = "IS_TYPE"
    FORMERLY_KNOWN_AS = "FORMERLY_KNOWN_AS"
    PREVIOUSLY_FLAGGED = "PREVIOUSLY_FLAGGED"


@dataclass
class KnowledgeGraphNode:
    """Represents a node in the knowledge graph."""
    node_id: str
    node_type: NodeType
    properties: Dict[str, Any]
    confidence: float = 1.0
    provenance: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "properties": self.properties,
            "confidence": self.confidence,
            "provenance": self.provenance,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class KnowledgeGraphEdge:
    """Represents an edge (relationship) in the knowledge graph."""
    edge_id: str
    source_node_id: str
    target_node_id: str
    relationship_type: RelationshipType
    properties: Dict[str, Any] = field(default_factory=dict)
    temporal_validity: Optional[Dict[str, datetime]] = None  # start_date, end_date
    confidence: float = 1.0
    provenance: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            "edge_id": self.edge_id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "relationship_type": self.relationship_type.value,
            "properties": self.properties,
            "temporal_validity": {
                k: v.isoformat() for k, v in (self.temporal_validity or {}).items()
            },
            "confidence": self.confidence,
            "provenance": self.provenance,
            "created_at": self.created_at.isoformat()
        }


# =============================================================================
# STAGE 9: TEMPORAL VESSEL STATE TRACKING
# =============================================================================

@dataclass
class TemporalState:
    """Represents a state of an attribute at a point in time."""
    attribute_name: str
    value: Any
    valid_from: datetime
    valid_to: Optional[datetime] = None
    source_record: Optional[str] = None
    confidence: float = 1.0


class VesselTemporalHistory:
    """
    Tracks temporal changes for a vessel entity.
    
    Captures:
    - MMSI changes over time
    - Vessel name updates
    - Flag changes
    - AIS position history
    - Ownership changes
    """
    
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.state_history: Dict[str, List[TemporalState]] = defaultdict(list)
        self.current_state: Dict[str, Any] = {}
        self.timeline: List[Dict] = []
    
    def add_state_change(
        self,
        attribute: str,
        new_value: Any,
        timestamp: datetime,
        source: Optional[str] = None,
        confidence: float = 1.0
    ):
        """Record a state change for an attribute."""
        # Close previous state
        if self.state_history[attribute]:
            prev_state = self.state_history[attribute][-1]
            if prev_state.valid_to is None:
                prev_state.valid_to = timestamp
        
        # Add new state
        new_state = TemporalState(
            attribute_name=attribute,
            value=new_value,
            valid_from=timestamp,
            source_record=source,
            confidence=confidence
        )
        self.state_history[attribute].append(new_state)
        self.current_state[attribute] = new_value
        
        # Add to timeline
        self.timeline.append({
            "timestamp": timestamp.isoformat(),
            "attribute": attribute,
            "old_value": self.state_history[attribute][-2].value if len(self.state_history[attribute]) > 1 else None,
            "new_value": new_value,
            "source": source
        })
    
    def get_state_at_time(self, attribute: str, target_time: datetime) -> Optional[Any]:
        """Get the value of an attribute at a specific point in time."""
        for state in self.state_history[attribute]:
            if state.valid_from <= target_time:
                if state.valid_to is None or state.valid_to > target_time:
                    return state.value
        return None
    
    def get_attribute_history(self, attribute: str) -> List[Dict]:
        """Get the complete history of an attribute."""
        history = []
        for state in self.state_history[attribute]:
            history.append({
                "value": state.value,
                "valid_from": state.valid_from.isoformat(),
                "valid_to": state.valid_to.isoformat() if state.valid_to else None,
                "confidence": state.confidence,
                "source": state.source_record
            })
        return history
    
    def get_timeline(self) -> List[Dict]:
        """Get chronological timeline of all changes."""
        return sorted(self.timeline, key=lambda x: x["timestamp"])
    
    def reconstruct_vessel_at_time(self, target_time: datetime) -> Dict:
        """Reconstruct the complete vessel state at a given time."""
        state = {"entity_id": self.entity_id, "as_of": target_time.isoformat()}
        for attribute in self.state_history.keys():
            state[attribute] = self.get_state_at_time(attribute, target_time)
        return state


# =============================================================================
# STAGE 8 & 10: KNOWLEDGE GRAPH WITH CANONICAL GROUND TRUTH
# =============================================================================

class MaritimeKnowledgeGraph:
    """
    Maritime Knowledge Graph - Canonical Ground Truth Representation
    
    This graph serves as the authoritative source of truth for vessel data:
    - Dynamically derived from the dataset
    - Captures entities and relationships
    - Tracks temporal state changes
    - Stores confidence scores and provenance
    """
    
    def __init__(self):
        self.nodes: Dict[str, KnowledgeGraphNode] = {}
        self.edges: Dict[str, KnowledgeGraphEdge] = {}
        
        # Index structures for efficient querying
        self.nodes_by_type: Dict[NodeType, Set[str]] = defaultdict(set)
        self.edges_by_source: Dict[str, Set[str]] = defaultdict(set)
        self.edges_by_target: Dict[str, Set[str]] = defaultdict(set)
        self.edges_by_type: Dict[RelationshipType, Set[str]] = defaultdict(set)
        
        # Vessel temporal histories
        self.vessel_histories: Dict[str, VesselTemporalHistory] = {}
        
        # Entity to canonical representation mapping
        self.canonical_entities: Dict[str, 'CanonicalVesselEntity'] = {}
    
    def add_node(self, node: KnowledgeGraphNode) -> str:
        """Add a node to the graph."""
        self.nodes[node.node_id] = node
        self.nodes_by_type[node.node_type].add(node.node_id)
        return node.node_id
    
    def add_edge(self, edge: KnowledgeGraphEdge) -> str:
        """Add an edge to the graph."""
        self.edges[edge.edge_id] = edge
        self.edges_by_source[edge.source_node_id].add(edge.edge_id)
        self.edges_by_target[edge.target_node_id].add(edge.edge_id)
        self.edges_by_type[edge.relationship_type].add(edge.edge_id)
        return edge.edge_id
    
    def get_node(self, node_id: str) -> Optional[KnowledgeGraphNode]:
        """Get a node by ID."""
        return self.nodes.get(node_id)
    
    def get_nodes_by_type(self, node_type: NodeType) -> List[KnowledgeGraphNode]:
        """Get all nodes of a specific type."""
        return [self.nodes[nid] for nid in self.nodes_by_type[node_type]]
    
    def get_outgoing_edges(self, node_id: str) -> List[KnowledgeGraphEdge]:
        """Get all edges originating from a node."""
        return [self.edges[eid] for eid in self.edges_by_source.get(node_id, set())]
    
    def get_incoming_edges(self, node_id: str) -> List[KnowledgeGraphEdge]:
        """Get all edges pointing to a node."""
        return [self.edges[eid] for eid in self.edges_by_target.get(node_id, set())]
    
    def ingest_vessel_record(self, record: Dict, source: str = "dataset") -> Dict[str, Any]:
        """
        Ingest a vessel record and extract entities/relationships.
        
        Dynamically discovers entities and relationships from the data.
        """
        ingestion_result = {
            "nodes_created": [],
            "edges_created": [],
            "temporal_updates": []
        }
        
        # Determine vessel entity ID
        imo = record.get("imo")
        mmsi = record.get("mmsi")
        
        if imo and imo > 0:
            vessel_id = f"vessel_imo_{imo}"
        elif mmsi:
            vessel_id = f"vessel_mmsi_{mmsi}"
        else:
            return ingestion_result  # Cannot create vessel without identifier
        
        # Create or update vessel node
        vessel_node = self._create_or_update_vessel_node(record, vessel_id)
        ingestion_result["nodes_created"].append(vessel_node.node_id)
        
        # Initialize temporal history if needed
        if vessel_id not in self.vessel_histories:
            self.vessel_histories[vessel_id] = VesselTemporalHistory(vessel_id)
        
        timestamp = self._parse_timestamp(record.get("UpdateDate")) or datetime.utcnow()
        
        # Extract and link MMSI
        if mmsi:
            mmsi_node, mmsi_edge = self._create_mmsi_relationship(
                vessel_id, mmsi, timestamp, source
            )
            if mmsi_node:
                ingestion_result["nodes_created"].append(mmsi_node.node_id)
            if mmsi_edge:
                ingestion_result["edges_created"].append(mmsi_edge.edge_id)
                
            # Track MMSI change in temporal history
            self.vessel_histories[vessel_id].add_state_change(
                "mmsi", mmsi, timestamp, source
            )
            ingestion_result["temporal_updates"].append({"attribute": "mmsi", "value": mmsi})
        
        # Extract and link Flag
        flag = record.get("flag")
        if flag:
            flag_node, flag_edge = self._create_flag_relationship(
                vessel_id, flag, timestamp, source
            )
            if flag_node:
                ingestion_result["nodes_created"].append(flag_node.node_id)
            if flag_edge:
                ingestion_result["edges_created"].append(flag_edge.edge_id)
            
            self.vessel_histories[vessel_id].add_state_change(
                "flag", flag, timestamp, source
            )
            ingestion_result["temporal_updates"].append({"attribute": "flag", "value": flag})
        
        # Extract and link Ship Builder
        builder = record.get("shipBuilder")
        if builder:
            builder_node, builder_edge = self._create_builder_relationship(
                vessel_id, builder, source
            )
            if builder_node:
                ingestion_result["nodes_created"].append(builder_node.node_id)
            if builder_edge:
                ingestion_result["edges_created"].append(builder_edge.edge_id)
        
        # Extract and link Port
        port_name = record.get("matchedPort_name")
        if port_name:
            port_node, port_edge = self._create_port_relationship(
                vessel_id, record, timestamp, source
            )
            if port_node:
                ingestion_result["nodes_created"].append(port_node.node_id)
            if port_edge:
                ingestion_result["edges_created"].append(port_edge.edge_id)
        
        # Extract and link Vessel Type
        vessel_type = record.get("vessel_type")
        if vessel_type:
            type_node, type_edge = self._create_vessel_type_relationship(
                vessel_id, vessel_type, source
            )
            if type_node:
                ingestion_result["nodes_created"].append(type_node.node_id)
            if type_edge:
                ingestion_result["edges_created"].append(type_edge.edge_id)
        
        # Track name changes
        name = record.get("name")
        if name:
            self.vessel_histories[vessel_id].add_state_change(
                "name", name, timestamp, source
            )
        
        # Track position updates
        lat = record.get("last_position_latitude")
        lon = record.get("last_position_longitude")
        if lat is not None and lon is not None:
            self.vessel_histories[vessel_id].add_state_change(
                "position", {"lat": lat, "lon": lon}, timestamp, source
            )
        
        # Update canonical entity
        self._update_canonical_entity(vessel_id, record, timestamp)
        
        return ingestion_result
    
    def _create_or_update_vessel_node(
        self,
        record: Dict,
        vessel_id: str
    ) -> KnowledgeGraphNode:
        """Create or update a vessel node."""
        if vessel_id in self.nodes:
            # Update existing node
            node = self.nodes[vessel_id]
            node.properties.update({
                "name": record.get("name") or node.properties.get("name"),
                "length": record.get("length") or node.properties.get("length"),
                "width": record.get("width") or node.properties.get("width"),
                "gross_tonnage": record.get("grossTonnage") or node.properties.get("gross_tonnage"),
                "built_year": record.get("builtYear") or node.properties.get("built_year")
            })
            node.updated_at = datetime.utcnow()
        else:
            # Create new node
            node = KnowledgeGraphNode(
                node_id=vessel_id,
                node_type=NodeType.VESSEL,
                properties={
                    "imo": record.get("imo"),
                    "name": record.get("name"),
                    "length": record.get("length"),
                    "width": record.get("width"),
                    "gross_tonnage": record.get("grossTonnage"),
                    "built_year": record.get("builtYear"),
                    "draught": record.get("draught")
                },
                provenance={"source": "dataset_ingestion"}
            )
            self.add_node(node)
        
        return node
    
    def _create_mmsi_relationship(
        self,
        vessel_id: str,
        mmsi: int,
        timestamp: datetime,
        source: str
    ) -> Tuple[Optional[KnowledgeGraphNode], Optional[KnowledgeGraphEdge]]:
        """Create MMSI node and relationship."""
        mmsi_id = f"mmsi_{mmsi}"
        
        # Create MMSI node if not exists
        mmsi_node = None
        if mmsi_id not in self.nodes:
            mmsi_node = KnowledgeGraphNode(
                node_id=mmsi_id,
                node_type=NodeType.MMSI,
                properties={"value": mmsi},
                provenance={"source": source}
            )
            self.add_node(mmsi_node)
        
        # Create edge with temporal validity
        edge_id = f"edge_{vessel_id}_uses_{mmsi_id}_{timestamp.timestamp()}"
        edge = KnowledgeGraphEdge(
            edge_id=edge_id,
            source_node_id=vessel_id,
            target_node_id=mmsi_id,
            relationship_type=RelationshipType.USES_MMSI,
            temporal_validity={"start_date": timestamp},
            provenance={"source": source}
        )
        self.add_edge(edge)
        
        return mmsi_node, edge
    
    def _create_flag_relationship(
        self,
        vessel_id: str,
        flag: str,
        timestamp: datetime,
        source: str
    ) -> Tuple[Optional[KnowledgeGraphNode], Optional[KnowledgeGraphEdge]]:
        """Create Flag node and relationship."""
        flag_id = f"flag_{flag}"
        
        flag_node = None
        if flag_id not in self.nodes:
            flag_node = KnowledgeGraphNode(
                node_id=flag_id,
                node_type=NodeType.FLAG,
                properties={"code": flag},
                provenance={"source": source}
            )
            self.add_node(flag_node)
        
        edge_id = f"edge_{vessel_id}_flag_{flag_id}_{timestamp.timestamp()}"
        edge = KnowledgeGraphEdge(
            edge_id=edge_id,
            source_node_id=vessel_id,
            target_node_id=flag_id,
            relationship_type=RelationshipType.REGISTERED_UNDER,
            temporal_validity={"start_date": timestamp},
            provenance={"source": source}
        )
        self.add_edge(edge)
        
        return flag_node, edge
    
    def _create_builder_relationship(
        self,
        vessel_id: str,
        builder: str,
        source: str
    ) -> Tuple[Optional[KnowledgeGraphNode], Optional[KnowledgeGraphEdge]]:
        """Create Builder node and relationship."""
        builder_id = f"builder_{hashlib.md5(builder.encode()).hexdigest()[:12]}"
        
        builder_node = None
        if builder_id not in self.nodes:
            builder_node = KnowledgeGraphNode(
                node_id=builder_id,
                node_type=NodeType.SHIP_BUILDER,
                properties={"name": builder},
                provenance={"source": source}
            )
            self.add_node(builder_node)
        
        edge_id = f"edge_{vessel_id}_built_by_{builder_id}"
        existing_edges = [e for e in self.edges_by_source.get(vessel_id, set())
                        if self.edges[e].relationship_type == RelationshipType.BUILT_BY]
        
        if not existing_edges:
            edge = KnowledgeGraphEdge(
                edge_id=edge_id,
                source_node_id=vessel_id,
                target_node_id=builder_id,
                relationship_type=RelationshipType.BUILT_BY,
                provenance={"source": source}
            )
            self.add_edge(edge)
            return builder_node, edge
        
        return builder_node, None
    
    def _create_port_relationship(
        self,
        vessel_id: str,
        record: Dict,
        timestamp: datetime,
        source: str
    ) -> Tuple[Optional[KnowledgeGraphNode], Optional[KnowledgeGraphEdge]]:
        """Create Port node and visited relationship."""
        port_name = record.get("matchedPort_name")
        unlocode = record.get("matchedPort_unlocode", "")
        
        port_id = f"port_{unlocode}" if unlocode else f"port_{hashlib.md5(port_name.encode()).hexdigest()[:12]}"
        
        port_node = None
        if port_id not in self.nodes:
            port_node = KnowledgeGraphNode(
                node_id=port_id,
                node_type=NodeType.PORT,
                properties={
                    "name": port_name,
                    "unlocode": unlocode,
                    "latitude": record.get("matchedPort_latitude"),
                    "longitude": record.get("matchedPort_longitude")
                },
                provenance={"source": source}
            )
            self.add_node(port_node)
        
        edge_id = f"edge_{vessel_id}_visited_{port_id}_{timestamp.timestamp()}"
        edge = KnowledgeGraphEdge(
            edge_id=edge_id,
            source_node_id=vessel_id,
            target_node_id=port_id,
            relationship_type=RelationshipType.VISITED,
            temporal_validity={"start_date": timestamp},
            properties={"destination": record.get("destination")},
            provenance={"source": source}
        )
        self.add_edge(edge)
        
        return port_node, edge
    
    def _create_vessel_type_relationship(
        self,
        vessel_id: str,
        vessel_type: str,
        source: str
    ) -> Tuple[Optional[KnowledgeGraphNode], Optional[KnowledgeGraphEdge]]:
        """Create Vessel Type node and relationship."""
        type_id = f"type_{hashlib.md5(vessel_type.encode()).hexdigest()[:12]}"
        
        type_node = None
        if type_id not in self.nodes:
            type_node = KnowledgeGraphNode(
                node_id=type_id,
                node_type=NodeType.VESSEL_TYPE,
                properties={"name": vessel_type},
                provenance={"source": source}
            )
            self.add_node(type_node)
        
        edge_id = f"edge_{vessel_id}_is_type_{type_id}"
        existing = [e for e in self.edges_by_source.get(vessel_id, set())
                   if self.edges[e].relationship_type == RelationshipType.IS_TYPE]
        
        if not existing:
            edge = KnowledgeGraphEdge(
                edge_id=edge_id,
                source_node_id=vessel_id,
                target_node_id=type_id,
                relationship_type=RelationshipType.IS_TYPE,
                provenance={"source": source}
            )
            self.add_edge(edge)
            return type_node, edge
        
        return type_node, None
    
    def _parse_timestamp(self, ts_value: Any) -> Optional[datetime]:
        """Parse timestamp from various formats."""
        if ts_value is None:
            return None
        if isinstance(ts_value, datetime):
            return ts_value
        try:
            import pandas as pd
            return pd.to_datetime(ts_value).to_pydatetime()
        except:
            return None
    
    def _update_canonical_entity(
        self,
        vessel_id: str,
        record: Dict,
        timestamp: datetime
    ):
        """Update the canonical entity representation."""
        if vessel_id not in self.canonical_entities:
            self.canonical_entities[vessel_id] = CanonicalVesselEntity(vessel_id)
        
        entity = self.canonical_entities[vessel_id]
        entity.update_from_record(record, timestamp)
    
    def get_vessel_history(self, vessel_id: str) -> Optional[VesselTemporalHistory]:
        """Get temporal history for a vessel."""
        return self.vessel_histories.get(vessel_id)
    
    def get_canonical_entity(self, vessel_id: str) -> Optional['CanonicalVesselEntity']:
        """Get canonical ground truth entity for a vessel."""
        return self.canonical_entities.get(vessel_id)
    
    def query_vessels_by_flag(self, flag_code: str) -> List[KnowledgeGraphNode]:
        """Query all vessels registered under a flag."""
        flag_id = f"flag_{flag_code}"
        if flag_id not in self.nodes:
            return []
        
        # Find all vessels with this flag
        vessels = []
        for edge_id in self.edges_by_target.get(flag_id, set()):
            edge = self.edges[edge_id]
            if edge.relationship_type == RelationshipType.REGISTERED_UNDER:
                vessel = self.nodes.get(edge.source_node_id)
                if vessel:
                    vessels.append(vessel)
        
        return vessels
    
    def query_vessels_by_port(self, port_unlocode: str) -> List[KnowledgeGraphNode]:
        """Query all vessels that visited a port."""
        port_id = f"port_{port_unlocode}"
        if port_id not in self.nodes:
            return []
        
        vessels = []
        for edge_id in self.edges_by_target.get(port_id, set()):
            edge = self.edges[edge_id]
            if edge.relationship_type == RelationshipType.VISITED:
                vessel = self.nodes.get(edge.source_node_id)
                if vessel:
                    vessels.append(vessel)
        
        return vessels
    
    def get_graph_statistics(self) -> Dict:
        """Get statistics about the knowledge graph."""
        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "nodes_by_type": {
                nt.value: len(nodes) for nt, nodes in self.nodes_by_type.items()
            },
            "edges_by_type": {
                rt.value: len(edges) for rt, edges in self.edges_by_type.items()
            },
            "vessels_with_history": len(self.vessel_histories),
            "canonical_entities": len(self.canonical_entities)
        }


# =============================================================================
# STAGE 10: CANONICAL VESSEL ENTITY
# =============================================================================

@dataclass
class AttributeWithProvenance:
    """Attribute value with confidence and provenance."""
    value: Any
    confidence: float
    source: str
    last_updated: datetime


class CanonicalVesselEntity:
    """
    Canonical Ground Truth Representation for a Vessel
    
    Aggregates:
    - Registry attributes
    - AIS-derived operational data
    - Historical identifier changes
    - Confidence scores for each attribute
    - Full provenance metadata
    """
    
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.attributes: Dict[str, AttributeWithProvenance] = {}
        self.mmsi_history: List[Dict] = []
        self.name_history: List[Dict] = []
        self.flag_history: List[Dict] = []
        self.source_records: List[str] = []
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def update_from_record(self, record: Dict, timestamp: datetime, source: str = "dataset"):
        """Update entity from a new record."""
        self.source_records.append(f"{source}_{timestamp.isoformat()}")
        self.updated_at = timestamp
        
        # Update attributes with provenance
        attribute_mappings = {
            "imo": "imo",
            "name": "name",
            "length": "length",
            "width": "width",
            "gross_tonnage": "grossTonnage",
            "built_year": "builtYear",
            "ship_builder": "shipBuilder",
            "hull_number": "hullNumber"
        }
        
        for attr_name, record_key in attribute_mappings.items():
            value = record.get(record_key)
            if value is not None:
                self._update_attribute(attr_name, value, timestamp, source)
        
        # Track historical changes
        if record.get("mmsi"):
            self._track_mmsi(record["mmsi"], timestamp)
        if record.get("name"):
            self._track_name(record["name"], timestamp)
        if record.get("flag"):
            self._track_flag(record["flag"], timestamp)
    
    def _update_attribute(
        self,
        attr_name: str,
        value: Any,
        timestamp: datetime,
        source: str
    ):
        """Update an attribute with provenance."""
        existing = self.attributes.get(attr_name)
        
        # Calculate confidence based on recency and source
        confidence = 0.9 if source == "registry" else 0.8
        
        if existing:
            # Higher confidence for consistent values
            if existing.value == value:
                confidence = min(1.0, existing.confidence + 0.05)
        
        self.attributes[attr_name] = AttributeWithProvenance(
            value=value,
            confidence=confidence,
            source=source,
            last_updated=timestamp
        )
    
    def _track_mmsi(self, mmsi: int, timestamp: datetime):
        """Track MMSI history."""
        if not self.mmsi_history or self.mmsi_history[-1]["mmsi"] != mmsi:
            # Close previous MMSI
            if self.mmsi_history:
                self.mmsi_history[-1]["end_date"] = timestamp.isoformat()
            
            self.mmsi_history.append({
                "mmsi": mmsi,
                "start_date": timestamp.isoformat(),
                "end_date": None
            })
    
    def _track_name(self, name: str, timestamp: datetime):
        """Track name history."""
        if not self.name_history or self.name_history[-1]["name"] != name:
            if self.name_history:
                self.name_history[-1]["end_date"] = timestamp.isoformat()
            
            self.name_history.append({
                "name": name,
                "start_date": timestamp.isoformat(),
                "end_date": None
            })
    
    def _track_flag(self, flag: str, timestamp: datetime):
        """Track flag history."""
        if not self.flag_history or self.flag_history[-1]["flag"] != flag:
            if self.flag_history:
                self.flag_history[-1]["end_date"] = timestamp.isoformat()
            
            self.flag_history.append({
                "flag": flag,
                "start_date": timestamp.isoformat(),
                "end_date": None
            })
    
    def get_canonical_representation(self) -> Dict:
        """Get the canonical ground truth representation."""
        return {
            "entity_id": self.entity_id,
            "attributes": {
                k: {
                    "value": v.value,
                    "confidence": v.confidence,
                    "source": v.source,
                    "last_updated": v.last_updated.isoformat()
                }
                for k, v in self.attributes.items()
            },
            "mmsi_history": self.mmsi_history,
            "name_history": self.name_history,
            "flag_history": self.flag_history,
            "source_records_count": len(self.source_records),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
    
    def get_current_name(self) -> Optional[str]:
        """Get current vessel name."""
        return self.attributes.get("name", AttributeWithProvenance(None, 0, "", datetime.utcnow())).value
    
    def get_attribute_confidence(self, attr_name: str) -> float:
        """Get confidence score for an attribute."""
        attr = self.attributes.get(attr_name)
        return attr.confidence if attr else 0.0


# =============================================================================
# DEMONSTRATION CODE
# =============================================================================

def demonstrate_knowledge_graph():
    """Demonstrate the knowledge graph system."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║       STAGE 8, 9 & 10: KNOWLEDGE GRAPH WITH TEMPORAL HISTORY                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

KNOWLEDGE GRAPH SCHEMA:
=======================

    Nodes (Entities):
    ┌────────────────┐     ┌────────────────┐     ┌────────────────┐
    │    VESSEL      │     │     MMSI       │     │     FLAG       │
    │  • imo         │     │  • value       │     │  • code        │
    │  • name        │     └────────────────┘     └────────────────┘
    │  • length      │
    │  • built_year  │     ┌────────────────┐     ┌────────────────┐
    └────────────────┘     │     PORT       │     │  SHIP_BUILDER  │
                           │  • name        │     │  • name        │
                           │  • unlocode    │     └────────────────┘
                           │  • lat/lon     │
                           └────────────────┘

    Relationships (Edges):
    ┌───────────────────────────────────────────────────────────────┐
    │  VESSEL --[USES_MMSI]--------> MMSI                           │
    │  VESSEL --[REGISTERED_UNDER]-> FLAG                           │
    │  VESSEL --[BUILT_BY]---------> SHIP_BUILDER                   │
    │  VESSEL --[VISITED]----------> PORT                           │
    │  VESSEL --[IS_TYPE]----------> VESSEL_TYPE                    │
    │                                                               │
    │  All edges include:                                           │
    │  • temporal_validity (start_date, end_date)                   │
    │  • confidence score                                           │
    │  • provenance metadata                                        │
    └───────────────────────────────────────────────────────────────┘


PYTHON CODE EXAMPLES:
=====================
""")
    
    print("""
# 1. Initialize Knowledge Graph
# ------------------------------
kg = MaritimeKnowledgeGraph()


# 2. Ingest Vessel Records
# -------------------------
record1 = {
    "imo": 9528574,
    "mmsi": 636013854,
    "name": "MARCO",
    "flag": "LR",
    "vessel_type": "Dry Bulk",
    "length": 225,
    "grossTonnage": 42708,
    "builtYear": 2009,
    "shipBuilder": "Universal Maizuru",
    "matchedPort_name": "Singapore",
    "matchedPort_unlocode": "SGSIN",
    "UpdateDate": "2025-09-16 03:15:32"
}

result = kg.ingest_vessel_record(record1, source="ais_feed")
print(f"Nodes created: {len(result['nodes_created'])}")
print(f"Edges created: {len(result['edges_created'])}")


# 3. Query Vessel by IMO
# -----------------------
vessel = kg.get_node("vessel_imo_9528574")
print(f"Vessel: {vessel.properties['name']}")
print(f"Built: {vessel.properties['built_year']}")


# 4. Get Temporal History
# ------------------------
history = kg.get_vessel_history("vessel_imo_9528574")

# Get name history
name_history = history.get_attribute_history("name")
print(f"Name changes: {len(name_history)}")

# Get MMSI history
mmsi_history = history.get_attribute_history("mmsi")
print(f"MMSI changes: {len(mmsi_history)}")

# Reconstruct vessel state at a specific time
from datetime import datetime
past_state = history.reconstruct_vessel_at_time(
    datetime(2024, 1, 1)
)
print(f"Vessel state on 2024-01-01: {past_state}")


# 5. Get Canonical Ground Truth Entity
# -------------------------------------
entity = kg.get_canonical_entity("vessel_imo_9528574")
canonical = entity.get_canonical_representation()

print(f"Canonical name: {canonical['attributes']['name']['value']}")
print(f"Confidence: {canonical['attributes']['name']['confidence']:.2%}")
print(f"MMSI history: {len(canonical['mmsi_history'])} entries")


# 6. Query Vessels by Flag
# -------------------------
lr_vessels = kg.query_vessels_by_flag("LR")
print(f"Vessels registered in Liberia: {len(lr_vessels)}")


# 7. Query Vessels by Port
# -------------------------
singapore_visitors = kg.query_vessels_by_port("SGSIN")
print(f"Vessels that visited Singapore: {len(singapore_visitors)}")


# 8. Get Graph Statistics
# ------------------------
stats = kg.get_graph_statistics()
print(f"Total nodes: {stats['total_nodes']}")
print(f"Total edges: {stats['total_edges']}")
print(f"Nodes by type: {stats['nodes_by_type']}")


# 9. Neo4j Cypher Query Example (for integration)
# ------------------------------------------------
# The graph can be exported to Neo4j for advanced querying:

cypher_queries = '''
// Find all vessels built by a specific shipyard
MATCH (v:Vessel)-[:BUILT_BY]->(b:ShipBuilder {name: 'Universal Maizuru'})
RETURN v.name, v.imo, v.built_year

// Find vessels that changed MMSI
MATCH (v:Vessel)-[r1:USES_MMSI]->(m1:MMSI),
      (v)-[r2:USES_MMSI]->(m2:MMSI)
WHERE m1 <> m2
RETURN v.name, m1.value, m2.value

// Track flag changes over time
MATCH (v:Vessel)-[r:REGISTERED_UNDER]->(f:Flag)
WHERE v.imo = 9528574
RETURN f.code, r.temporal_validity.start_date
ORDER BY r.temporal_validity.start_date

// Find vessels visiting multiple ports
MATCH (v:Vessel)-[:VISITED]->(p:Port)
WITH v, collect(p) as ports
WHERE size(ports) > 5
RETURN v.name, [p in ports | p.name]
'''
print(cypher_queries)
""")
    
    # Live demonstration
    print("\n" + "="*60)
    print("LIVE DEMONSTRATION")
    print("="*60 + "\n")
    
    # Initialize
    kg = MaritimeKnowledgeGraph()
    
    # Ingest sample records
    records = [
        {
            "imo": 9528574, "mmsi": 636013854, "name": "MARCO",
            "flag": "LR", "vessel_type": "Dry Bulk", "length": 225,
            "grossTonnage": 42708, "builtYear": 2009,
            "shipBuilder": "Universal Maizuru",
            "matchedPort_name": "Singapore", "matchedPort_unlocode": "SGSIN",
            "UpdateDate": "2025-01-15"
        },
        {
            "imo": 9528574, "mmsi": 636013855, "name": "MARCO",
            "flag": "LR", "vessel_type": "Dry Bulk", "length": 225,
            "matchedPort_name": "Rotterdam", "matchedPort_unlocode": "NLRTM",
            "UpdateDate": "2025-06-20"
        },
        {
            "imo": 9752709, "mmsi": 563091700, "name": "ASIA INSPIRE",
            "flag": "SG", "vessel_type": "Chemical Tanker", "length": 180,
            "grossTonnage": 23200, "builtYear": 2019,
            "shipBuilder": "Taizhou Sanfu",
            "matchedPort_name": "Bintulu", "matchedPort_unlocode": "MYBTU",
            "UpdateDate": "2025-09-16"
        }
    ]
    
    print("1. Ingesting Vessel Records:")
    for i, record in enumerate(records):
        result = kg.ingest_vessel_record(record, source=f"batch_{i}")
        print(f"   Record {i+1}: {len(result['nodes_created'])} nodes, {len(result['edges_created'])} edges")
    
    print(f"\n2. Knowledge Graph Statistics:")
    stats = kg.get_graph_statistics()
    print(f"   Total nodes: {stats['total_nodes']}")
    print(f"   Total edges: {stats['total_edges']}")
    for node_type, count in stats['nodes_by_type'].items():
        print(f"   - {node_type}: {count}")
    
    print(f"\n3. Temporal History for IMO 9528574:")
    history = kg.get_vessel_history("vessel_imo_9528574")
    if history:
        timeline = history.get_timeline()
        print(f"   Total events: {len(timeline)}")
        for event in timeline[:5]:
            print(f"   - {event['attribute']}: {event['old_value']} -> {event['new_value']}")
    
    print(f"\n4. Canonical Entity for IMO 9528574:")
    entity = kg.get_canonical_entity("vessel_imo_9528574")
    if entity:
        canonical = entity.get_canonical_representation()
        print(f"   Name: {canonical['attributes'].get('name', {}).get('value')}")
        print(f"   MMSI history: {len(canonical['mmsi_history'])} entries")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    demonstrate_knowledge_graph()
