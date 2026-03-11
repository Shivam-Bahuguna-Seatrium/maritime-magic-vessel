"""
Stage 14: Modular Architecture Design - Main Integration Module
================================================================

This is the main entry point for the Maritime Vessel Identity Resolution System.
It integrates all components into a cohesive, production-ready system.

System Architecture:
                    ┌──────────────────────────────────────┐
                    │     CONVERSATIONAL INTERFACE         │
                    │        (Natural Language)            │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │       LLM ORCHESTRATOR               │
                    │    (Multi-Agent Coordination)        │
                    └──────────────────┬───────────────────┘
                                       │
         ┌─────────────┬───────────────┼───────────────┬─────────────┐
         │             │               │               │             │
    ┌────▼────┐  ┌─────▼────┐  ┌───────▼─────┐  ┌─────▼────┐  ┌─────▼────┐
    │Validation│  │ Entity   │  │  Knowledge  │  │  Query   │  │   RAG    │
    │ Pipeline │  │Resolution│  │   Graph     │  │  Engine  │  │ Guard    │
    └────┬────┘  └─────┬────┘  └───────┬─────┘  └─────┬────┘  └─────┬────┘
         │             │               │               │             │
         └─────────────┴───────────────┼───────────────┴─────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │       VESSEL DATA STORE              │
                    │   (CSV/Database/Knowledge Graph)     │
                    └──────────────────────────────────────┘
"""

import os
import sys
import json
from datetime import datetime
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import system components
try:
    from models.data_models import (
        VesselRecord, ValidationResult, DataQualityAlert,
        EntityMatch, ReviewDecision, QueryRequest, QueryResponse
    )
    from core.eda_analysis import (
        load_vessel_data, generate_eda_report
    )
    from validation.validation_pipeline import (
        DataValidator, ValidationPipeline, AIAnomalyDetector,
        AlertGenerationSystem
    )
    from agents.multi_agent_system import (
        LLMOrchestrator, ToolRegistry, ValidationTool,
        SimilarityTool, ConflictDetectionTool, AnomalyDetectionTool,
        KnowledgeGraphTool, QueryTool
    )
    from entity_resolution.entity_resolver import (
        VesselEntityResolutionEngine, HumanReviewInterface,
        FeedbackStore
    )
    from knowledge_graph.maritime_kg import (
        MaritimeKnowledgeGraph, VesselTemporalHistory,
        CanonicalVesselEntity
    )
    from query.conversational_ai import (
        VesselQueryEngine, NLQueryParser, RAGVesselAssistant,
        ConversationalVesselInterface, StructuredQuery, QueryType
    )
    IMPORTS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Some imports not available: {e}")
    IMPORTS_AVAILABLE = False


@dataclass
class SystemConfiguration:
    """System-wide configuration settings."""
    
    # Data paths
    data_file_path: str = ""
    output_directory: str = "output"
    
    # Validation settings
    enable_imo_validation: bool = True
    enable_mmsi_validation: bool = True
    enable_coordinate_validation: bool = True
    enable_timestamp_validation: bool = True
    
    # Entity resolution thresholds
    high_confidence_threshold: float = 0.9
    review_required_threshold: float = 0.6
    low_confidence_threshold: float = 0.3
    
    # AI settings
    openai_api_key: Optional[str] = None
    llm_model: str = "gpt-4-turbo-preview"
    enable_ai_anomaly_detection: bool = True
    
    # Knowledge graph settings
    enable_knowledge_graph: bool = True
    enable_temporal_history: bool = True
    
    # HITL settings
    require_human_review: bool = True
    auto_approve_high_confidence: bool = True
    
    # Query settings
    max_query_results: int = 100
    enable_query_cache: bool = True
    
    # Hallucination prevention
    strict_grounding: bool = True
    evidence_required: bool = True
    
    def to_dict(self) -> Dict:
        return {
            "data_file_path": self.data_file_path,
            "output_directory": self.output_directory,
            "validation": {
                "imo": self.enable_imo_validation,
                "mmsi": self.enable_mmsi_validation,
                "coordinates": self.enable_coordinate_validation,
                "timestamps": self.enable_timestamp_validation
            },
            "entity_resolution": {
                "high_confidence": self.high_confidence_threshold,
                "review_required": self.review_required_threshold,
                "low_confidence": self.low_confidence_threshold
            },
            "knowledge_graph": self.enable_knowledge_graph,
            "temporal_history": self.enable_temporal_history,
            "hitl": {
                "require_review": self.require_human_review,
                "auto_approve": self.auto_approve_high_confidence
            },
            "hallucination_prevention": {
                "strict_grounding": self.strict_grounding,
                "evidence_required": self.evidence_required
            }
        }
    
    @classmethod
    def from_file(cls, config_path: str) -> "SystemConfiguration":
        """Load configuration from JSON file."""
        with open(config_path, "r") as f:
            data = json.load(f)
        return cls(**data)
    
    def save(self, config_path: str):
        """Save configuration to JSON file."""
        with open(config_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


class MaritimeVesselIdentitySystem:
    """
    Main system class integrating all components.
    
    This is the production-grade Maritime Vessel Identity Resolution System
    with the following capabilities:
    
    1. Data Ingestion & EDA
    2. Multi-Rule Validation Pipeline
    3. AI-Assisted Anomaly Detection
    4. Entity Resolution with Similarity Scoring
    5. Human-in-the-Loop Verification
    6. RLHF-based Continuous Learning
    7. Knowledge Graph Construction
    8. Temporal Vessel History Tracking
    9. Canonical Ground Truth Management
    10. Natural Language Query Interface
    11. RAG-based Anti-Hallucination
    """
    
    def __init__(self, config: SystemConfiguration):
        self.config = config
        self.initialized = False
        
        # Core components (initialized lazily)
        self._dataframe = None
        self._validation_pipeline = None
        self._alert_system = None
        self._anomaly_detector = None
        self._llm_orchestrator = None
        self._entity_resolver = None
        self._review_interface = None
        self._feedback_store = None
        self._knowledge_graph = None
        self._query_engine = None
        self._rag_assistant = None
        self._conversational_interface = None
        
        # System state
        self.eda_report = None
        self.validation_results = []
        self.entity_matches = []
        self.alerts = []
    
    def initialize(self, data_path: Optional[str] = None) -> bool:
        """Initialize the system with vessel data."""
        try:
            # Load data
            path = data_path or self.config.data_file_path
            if path and os.path.exists(path):
                print(f"Loading vessel data from: {path}")
                self._dataframe = load_vessel_data(path)
                print(f"Loaded {len(self._dataframe)} records")
            else:
                print("Warning: No data file specified or found")
                self._dataframe = None
            
            # Initialize components
            self._initialize_validation_pipeline()
            self._initialize_entity_resolution()
            self._initialize_knowledge_graph()
            self._initialize_query_system()
            
            self.initialized = True
            return True
            
        except Exception as e:
            print(f"Initialization failed: {e}")
            return False
    
    def _initialize_validation_pipeline(self):
        """Initialize the validation pipeline."""
        if IMPORTS_AVAILABLE:
            self._validation_pipeline = ValidationPipeline()
            self._alert_system = AlertGenerationSystem()
            if self.config.enable_ai_anomaly_detection:
                self._anomaly_detector = AIAnomalyDetector()
    
    def _initialize_entity_resolution(self):
        """Initialize entity resolution components."""
        if IMPORTS_AVAILABLE:
            self._entity_resolver = VesselEntityResolutionEngine()
            self._review_interface = HumanReviewInterface()
            self._feedback_store = FeedbackStore()
    
    def _initialize_knowledge_graph(self):
        """Initialize knowledge graph."""
        if IMPORTS_AVAILABLE and self.config.enable_knowledge_graph:
            self._knowledge_graph = MaritimeKnowledgeGraph()
    
    def _initialize_query_system(self):
        """Initialize query and conversational interface."""
        if IMPORTS_AVAILABLE:
            self._query_engine = VesselQueryEngine(
                knowledge_graph=self._knowledge_graph,
                dataframe=self._dataframe
            )
            self._rag_assistant = RAGVesselAssistant(self._query_engine)
            self._conversational_interface = ConversationalVesselInterface(
                self._query_engine,
                api_key=self.config.openai_api_key
            )
    
    # =========================================================================
    # STAGE 1: Exploratory Data Analysis
    # =========================================================================
    
    def run_eda(self) -> Dict:
        """Run exploratory data analysis on the dataset."""
        if self._dataframe is None:
            return {"error": "No data loaded"}
        
        self.eda_report = generate_eda_report(self._dataframe)
        return self.eda_report
    
    # =========================================================================
    # STAGE 2: Data Validation
    # =========================================================================
    
    def validate_data(self) -> List[Dict]:
        """Run validation pipeline on all records."""
        if self._dataframe is None or self._validation_pipeline is None:
            return []
        
        results = []
        for _, row in self._dataframe.iterrows():
            record = row.to_dict()
            validation_result = self._validation_pipeline.validate(record)
            results.append(validation_result)
            
            # Generate alerts for issues
            if validation_result.get("issues"):
                alerts = self._alert_system.generate_alerts(
                    record, validation_result["issues"]
                )
                self.alerts.extend(alerts)
        
        self.validation_results = results
        return results
    
    # =========================================================================
    # STAGE 3-4: Multi-Agent System
    # =========================================================================
    
    def get_orchestrator(self) -> Optional["LLMOrchestrator"]:
        """Get or create the LLM orchestrator."""
        if self._llm_orchestrator is None and IMPORTS_AVAILABLE:
            self._llm_orchestrator = LLMOrchestrator(
                api_key=self.config.openai_api_key
            )
        return self._llm_orchestrator
    
    # =========================================================================
    # STAGE 5-7: Entity Resolution with HITL
    # =========================================================================
    
    def resolve_entities(self) -> List[Dict]:
        """Run entity resolution on the dataset."""
        if self._dataframe is None or self._entity_resolver is None:
            return []
        
        records = self._dataframe.to_dict("records")
        matches = self._entity_resolver.find_all_matches(records)
        
        for match in matches:
            if match["confidence"] < self.config.review_required_threshold:
                if self.config.require_human_review:
                    self._review_interface.queue_for_review(match)
        
        self.entity_matches = matches
        return matches
    
    def get_pending_reviews(self) -> List[Dict]:
        """Get items pending human review."""
        if self._review_interface is None:
            return []
        return self._review_interface.get_pending_reviews()
    
    def submit_review(self, item_id: str, decision: str, notes: str = "") -> bool:
        """Submit a human review decision."""
        if self._review_interface is None:
            return False
        return self._review_interface.submit_decision(item_id, decision, notes)
    
    # =========================================================================
    # STAGE 8-10: Knowledge Graph
    # =========================================================================
    
    def build_knowledge_graph(self) -> Dict:
        """Build knowledge graph from the dataset."""
        if self._knowledge_graph is None or self._dataframe is None:
            return {"error": "Knowledge graph not initialized"}
        
        # Ingest all records
        for _, row in self._dataframe.iterrows():
            self._knowledge_graph.ingest_record(row.to_dict())
        
        return {
            "nodes": len(self._knowledge_graph.nodes),
            "edges": len(self._knowledge_graph.edges),
            "status": "built"
        }
    
    def query_knowledge_graph(self, node_id: str) -> Optional[Dict]:
        """Query the knowledge graph for a specific node."""
        if self._knowledge_graph is None:
            return None
        return self._knowledge_graph.get_node(node_id)
    
    # =========================================================================
    # STAGE 11-13: Query & Conversational Interface
    # =========================================================================
    
    def query(self, query_string: str) -> Dict:
        """Execute a natural language query."""
        if self._rag_assistant is None:
            return {"error": "Query system not initialized"}
        
        response = self._rag_assistant.process_query(query_string)
        return {
            "query": query_string,
            "answer": response.answer,
            "confidence": response.confidence,
            "evidence": response.evidence,
            "sources": response.sources,
            "grounded": response.is_grounded
        }
    
    def chat(self, message: str) -> str:
        """Chat with the vessel assistant."""
        if self._conversational_interface is None:
            return "Chat interface not initialized"
        return self._conversational_interface.chat(message)
    
    # =========================================================================
    # System Status & Reporting
    # =========================================================================
    
    def get_system_status(self) -> Dict:
        """Get current system status."""
        return {
            "initialized": self.initialized,
            "components": {
                "dataframe": self._dataframe is not None,
                "validation_pipeline": self._validation_pipeline is not None,
                "entity_resolver": self._entity_resolver is not None,
                "knowledge_graph": self._knowledge_graph is not None,
                "query_engine": self._query_engine is not None,
                "rag_assistant": self._rag_assistant is not None
            },
            "data": {
                "records_loaded": len(self._dataframe) if self._dataframe is not None else 0,
                "validation_results": len(self.validation_results),
                "entity_matches": len(self.entity_matches),
                "alerts": len(self.alerts)
            },
            "config": self.config.to_dict()
        }
    
    def generate_report(self) -> Dict:
        """Generate comprehensive system report."""
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "system_status": self.get_system_status(),
            "eda_summary": self.eda_report if self.eda_report else "Not run",
            "validation_summary": {
                "total_validated": len(self.validation_results),
                "with_issues": sum(1 for r in self.validation_results if r.get("issues"))
            },
            "entity_resolution_summary": {
                "total_matches": len(self.entity_matches),
                "high_confidence": sum(
                    1 for m in self.entity_matches 
                    if m.get("confidence", 0) >= self.config.high_confidence_threshold
                ),
                "pending_review": len(self.get_pending_reviews())
            },
            "alerts": {
                "total": len(self.alerts),
                "critical": sum(1 for a in self.alerts if a.get("severity") == "critical"),
                "high": sum(1 for a in self.alerts if a.get("severity") == "high")
            }
        }


def create_default_system(data_path: Optional[str] = None) -> MaritimeVesselIdentitySystem:
    """Create a system instance with default configuration."""
    config = SystemConfiguration(
        data_file_path=data_path or "",
        enable_ai_anomaly_detection=True,
        enable_knowledge_graph=True,
        enable_temporal_history=True,
        require_human_review=True,
        strict_grounding=True
    )
    
    system = MaritimeVesselIdentitySystem(config)
    if data_path:
        system.initialize(data_path)
    
    return system


# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Main entry point for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Maritime Vessel Identity Resolution System"
    )
    parser.add_argument(
        "--data", "-d",
        help="Path to vessel data CSV file"
    )
    parser.add_argument(
        "--eda", action="store_true",
        help="Run exploratory data analysis"
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Run validation pipeline"
    )
    parser.add_argument(
        "--resolve", action="store_true",
        help="Run entity resolution"
    )
    parser.add_argument(
        "--build-kg", action="store_true",
        help="Build knowledge graph"
    )
    parser.add_argument(
        "--query", "-q",
        help="Natural language query"
    )
    parser.add_argument(
        "--interactive", "-i", action="store_true",
        help="Start interactive chat mode"
    )
    parser.add_argument(
        "--report", action="store_true",
        help="Generate system report"
    )
    
    args = parser.parse_args()
    
    # Create and initialize system
    system = create_default_system(args.data)
    
    if args.data and not system.initialized:
        system.initialize(args.data)
    
    # Execute requested operations
    if args.eda:
        print("\nRunning EDA...")
        report = system.run_eda()
        print(json.dumps(report, indent=2, default=str))
    
    if args.validate:
        print("\nRunning validation...")
        results = system.validate_data()
        print(f"Validated {len(results)} records")
        issues = sum(1 for r in results if r.get("issues"))
        print(f"Records with issues: {issues}")
    
    if args.resolve:
        print("\nRunning entity resolution...")
        matches = system.resolve_entities()
        print(f"Found {len(matches)} potential matches")
    
    if args.build_kg:
        print("\nBuilding knowledge graph...")
        result = system.build_knowledge_graph()
        print(json.dumps(result, indent=2))
    
    if args.query:
        print(f"\nQuery: {args.query}")
        result = system.query(args.query)
        print(f"Answer: {result['answer']}")
        print(f"Confidence: {result['confidence']:.2%}")
    
    if args.interactive:
        print("\nInteractive Chat Mode (type 'exit' to quit)")
        print("-" * 50)
        while True:
            try:
                user_input = input("\nYou: ").strip()
                if user_input.lower() in ["exit", "quit", "q"]:
                    break
                if user_input:
                    response = system.chat(user_input)
                    print(f"\nAssistant: {response}")
            except KeyboardInterrupt:
                break
        print("\nGoodbye!")
    
    if args.report:
        print("\nGenerating report...")
        report = system.generate_report()
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
