"""
Maritime Vessel Data Models
===========================
Pydantic models for representing vessel data, alerts, and validation results.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, field_validator
import re


class VesselType(str, Enum):
    """Standard vessel types."""
    GENERAL_CARGO = "General Cargo"
    DRY_BULK = "Dry Bulk"
    CONTAINER = "Container"
    CRUDE_TANKER = "Crude Tanker"
    CHEMICAL_TANKER = "Chemical Tanker"
    PRODUCT_TANKER = "Product Tanker"
    LNG_CARRIER = "LNG Carrier"
    LPG_CARRIER = "LPG Carrier"
    PASSENGER_SHIP = "Passenger Ship"
    FISHING_VESSEL = "Fishing Vessel"
    TUG = "Tug"
    REEFER = "Reefer"
    RO_RO = "Ro-Ro/Vehicle Carrier"
    SUPPORT_VESSEL = "Support Vessel"
    UNSPECIFIED = "Unspecified"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Types of data quality alerts."""
    INVALID_IMO = "invalid_imo"
    INVALID_MMSI = "invalid_mmsi"
    DUPLICATE_RECORD = "duplicate_record"
    CONFLICTING_IDENTIFIERS = "conflicting_identifiers"
    SUSPICIOUS_NAME_CHANGE = "suspicious_name_change"
    MMSI_REUSE = "mmsi_reuse"
    TEMPORAL_INCONSISTENCY = "temporal_inconsistency"
    GEOGRAPHIC_ANOMALY = "geographic_anomaly"
    ATTRIBUTE_INCONSISTENCY = "attribute_inconsistency"
    DATA_QUALITY_ISSUE = "data_quality_issue"


class ReviewDecision(str, Enum):
    """Human reviewer decisions."""
    CONFIRMED_ISSUE = "confirmed_issue"
    MERGE_RECORDS = "merge_records"
    MARK_VALID = "mark_valid"
    FLAG_INVESTIGATION = "flag_investigation"
    REJECTED = "rejected"


class VesselRecord(BaseModel):
    """Represents a single vessel record from the dataset."""
    imo: Optional[int] = None
    mmsi: Optional[int] = None
    name: Optional[str] = None
    ais_class: Optional[str] = Field(None, alias="aisClass")
    callsign: Optional[str] = None
    length: Optional[float] = None
    width: Optional[float] = None
    vessel_type: Optional[str] = Field(None, alias="vessel_type")
    flag: Optional[str] = None
    deadweight: Optional[float] = None
    gross_tonnage: Optional[float] = Field(None, alias="grossTonnage")
    built_year: Optional[int] = Field(None, alias="builtYear")
    net_tonnage: Optional[float] = Field(None, alias="netTonnage")
    draught: Optional[float] = None
    length_overall: Optional[float] = Field(None, alias="lengthOverall")
    ship_builder: Optional[str] = Field(None, alias="shipBuilder")
    hull_number: Optional[str] = Field(None, alias="hullNumber")
    launch_year: Optional[int] = Field(None, alias="launchYear")
    propulsion_type: Optional[str] = Field(None, alias="propulsionType")
    engine_designation: Optional[str] = Field(None, alias="engineDesignation")
    
    # AIS Position Data
    last_position_latitude: Optional[float] = Field(None, alias="last_position_latitude")
    last_position_longitude: Optional[float] = Field(None, alias="last_position_longitude")
    last_position_speed: Optional[float] = Field(None, alias="last_position_speed")
    last_position_course: Optional[float] = Field(None, alias="last_position_course")
    last_position_heading: Optional[float] = Field(None, alias="last_position_heading")
    last_position_timestamp: Optional[datetime] = Field(None, alias="last_position_updateTimestamp")
    
    # Port Information
    destination: Optional[str] = None
    eta: Optional[datetime] = None
    matched_port_name: Optional[str] = Field(None, alias="matchedPort_name")
    matched_port_unlocode: Optional[str] = Field(None, alias="matchedPort_unlocode")
    
    # Metadata
    insert_date: Optional[datetime] = Field(None, alias="InsertDate")
    update_date: Optional[datetime] = Field(None, alias="UpdateDate")
    
    class Config:
        populate_by_name = True


class DataQualityAlert(BaseModel):
    """Represents a data quality alert."""
    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    affected_records: List[Dict[str, Any]]
    description: str
    rule_or_model: str
    confidence_score: float = Field(ge=0, le=1)
    suggested_actions: List[str]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolution_details: Optional[str] = None


class ValidationResult(BaseModel):
    """Result of a validation check."""
    record_id: str
    field_name: str
    validation_rule: str
    is_valid: bool
    error_message: Optional[str] = None
    original_value: Any
    suggested_correction: Optional[Any] = None


class EntityMatch(BaseModel):
    """Represents a potential entity match between records."""
    record_a_id: str
    record_b_id: str
    match_type: str
    confidence_score: float = Field(ge=0, le=1)
    matching_attributes: Dict[str, float]
    conflicting_attributes: Dict[str, Any]
    evidence: List[str]
    requires_review: bool = False


class HumanReviewItem(BaseModel):
    """Item for human-in-the-loop review."""
    review_id: str
    entity_match: EntityMatch
    alert: Optional[DataQualityAlert] = None
    vessel_records: List[VesselRecord]
    similarity_scores: Dict[str, float]
    historical_data: Dict[str, Any]
    supporting_evidence: List[str]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    assigned_to: Optional[str] = None
    status: str = "pending"


class ReviewFeedback(BaseModel):
    """Feedback from human reviewer."""
    review_id: str
    reviewer_id: str
    decision: ReviewDecision
    confidence: float = Field(ge=0, le=1)
    notes: Optional[str] = None
    reviewed_at: datetime = Field(default_factory=datetime.utcnow)
    time_spent_seconds: Optional[int] = None


class VesselEntity(BaseModel):
    """Unified vessel entity after resolution."""
    entity_id: str
    canonical_imo: Optional[int] = None
    canonical_name: str
    mmsi_history: List[Dict[str, Any]] = []
    name_history: List[Dict[str, Any]] = []
    flag_history: List[Dict[str, Any]] = []
    source_records: List[str] = []
    attributes: Dict[str, Any] = {}
    confidence_scores: Dict[str, float] = {}
    provenance: Dict[str, Any] = {}
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class KnowledgeGraphNode(BaseModel):
    """Node in the knowledge graph."""
    node_id: str
    node_type: str
    properties: Dict[str, Any]
    confidence: float = 1.0
    provenance: Dict[str, Any] = {}


class KnowledgeGraphEdge(BaseModel):
    """Edge in the knowledge graph."""
    edge_id: str
    source_node_id: str
    target_node_id: str
    relationship_type: str
    properties: Dict[str, Any] = {}
    temporal_validity: Optional[Dict[str, datetime]] = None
    confidence: float = 1.0


class QueryRequest(BaseModel):
    """Natural language query request."""
    query_text: str
    query_type: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    include_history: bool = False
    max_results: int = 100


class QueryResponse(BaseModel):
    """Response to a query."""
    query_id: str
    original_query: str
    structured_query: str
    results: List[Dict[str, Any]]
    evidence_sources: List[str]
    confidence: float
    execution_time_ms: float
