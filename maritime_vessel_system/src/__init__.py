# Maritime Vessel Identity Resolution System
# src package initialization
"""
Maritime Vessel Identity Resolution System

A comprehensive AI system for vessel identity resolution using:
- LLM Orchestration with function calling
- Knowledge Graph with temporal history
- Entity Resolution with fuzzy matching
- Human-in-the-Loop validation
- RAG-based conversational AI

Usage:
    from src.core.eda_analysis import load_vessel_data, generate_eda_report
    from src.validation.validation_pipeline import ValidationPipeline, DataValidator
    from src.entity_resolution.entity_resolver import VesselEntityResolutionEngine
    from src.knowledge_graph.maritime_kg import MaritimeKnowledgeGraph
    from src.query.conversational_ai import VesselQueryEngine, ConversationalVesselInterface
    from src.agents.multi_agent_system import LLMOrchestrator
"""

__version__ = "1.0.0"
__author__ = "Maritime AI Team"

# Lazy imports to avoid circular dependencies
def __getattr__(name):
    """Lazy import system components."""
    if name == "load_vessel_data":
        from src.core.eda_analysis import load_vessel_data
        return load_vessel_data
    elif name == "ValidationPipeline":
        from src.validation.validation_pipeline import ValidationPipeline
        return ValidationPipeline
    elif name == "DataValidationPipeline":
        from src.validation.validation_pipeline import DataValidationPipeline
        return DataValidationPipeline
    elif name == "VesselEntityResolutionEngine":
        from src.entity_resolution.entity_resolver import VesselEntityResolutionEngine
        return VesselEntityResolutionEngine
    elif name == "MaritimeKnowledgeGraph":
        from src.knowledge_graph.maritime_kg import MaritimeKnowledgeGraph
        return MaritimeKnowledgeGraph
    elif name == "VesselQueryEngine":
        from src.query.conversational_ai import VesselQueryEngine
        return VesselQueryEngine
    elif name == "LLMOrchestrator":
        from src.agents.multi_agent_system import LLMOrchestrator
        return LLMOrchestrator
    elif name == "ToolRegistry":
        from src.agents.multi_agent_system import ToolRegistry
        return ToolRegistry
    raise AttributeError(f"module 'src' has no attribute '{name}'")

__all__ = [
    "load_vessel_data",
    "ValidationPipeline",
    "VesselEntityResolutionEngine", 
    "MaritimeKnowledgeGraph",
    "VesselQueryEngine",
    "LLMOrchestrator",
]