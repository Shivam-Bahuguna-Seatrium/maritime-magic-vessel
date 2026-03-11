# Maritime Vessel Identity Resolution System

A production-grade AI system for maritime vessel identity resolution using LLM orchestration, multi-agent architecture, knowledge graphs, entity resolution, and human-in-the-loop validation.

## 🎯 System Overview

This system addresses the critical challenge of maintaining accurate vessel identity information in the maritime domain. It processes AIS (Automatic Identification System) data and vessel registry information to:

- **Detect data quality issues** (invalid IMO/MMSI, suspicious coordinates)
- **Identify duplicate or conflicting records**
- **Resolve entity ambiguities** using similarity scoring
- **Build a knowledge graph** representing vessel relationships
- **Enable natural language queries** with anti-hallucination safeguards

## 📁 System Architecture

```
maritime_vessel_system/
├── src/
│   ├── models/
│   │   └── data_models.py       # Pydantic data models
│   ├── core/
│   │   └── eda_analysis.py      # Exploratory data analysis
│   ├── validation/
│   │   └── validation_pipeline.py # Rule-based validation
│   ├── agents/
│   │   └── multi_agent_system.py  # LLM orchestrator & tools
│   ├── entity_resolution/
│   │   └── entity_resolver.py   # Entity resolution + HITL
│   ├── knowledge_graph/
│   │   └── maritime_kg.py       # Knowledge graph
│   ├── query/
│   │   └── conversational_ai.py # NL queries + RAG
│   └── main.py                  # Main integration module
├── config.json                  # System configuration
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🔧 Installation

```bash
# Clone or create the project
cd maritime_vessel_system

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Set OpenAI API key (optional, for LLM features)
set OPENAI_API_KEY=your-api-key
```

## 🚀 Quick Start

### Command Line Usage

```bash
# Run EDA analysis
python src/main.py --data ../case_study_dataset_202509152039.csv --eda

# Run validation pipeline
python src/main.py --data ../case_study_dataset_202509152039.csv --validate

# Run entity resolution
python src/main.py --data ../case_study_dataset_202509152039.csv --resolve

# Build knowledge graph
python src/main.py --data ../case_study_dataset_202509152039.csv --build-kg

# Natural language query
python src/main.py --data ../case_study_dataset_202509152039.csv --query "Find vessel IMO 9528574"

# Interactive chat mode
python src/main.py --data ../case_study_dataset_202509152039.csv --interactive

# Generate system report
python src/main.py --data ../case_study_dataset_202509152039.csv --report
```

### Python API Usage

```python
from src.main import create_default_system

# Initialize system
system = create_default_system("../case_study_dataset_202509152039.csv")

# Run EDA
eda_report = system.run_eda()
print(f"Total records: {eda_report['total_records']}")

# Validate data
validations = system.validate_data()
print(f"Records with issues: {len([v for v in validations if v.get('issues')])}")

# Entity resolution
matches = system.resolve_entities()
print(f"Potential duplicates: {len(matches)}")

# Build knowledge graph
kg_stats = system.build_knowledge_graph()
print(f"Knowledge graph: {kg_stats['nodes']} nodes, {kg_stats['edges']} edges")

# Natural language query
result = system.query("Find vessels with flag SG")
print(result['answer'])

# Chat interface
response = system.chat("What is the history of vessel IMO 9528574?")
print(response)
```

## 📊 System Stages

### Stage 1: Exploratory Data Analysis
- Schema analysis
- Missing value detection
- Duplicate identification
- Identifier conflict detection
- Vessel name variation analysis

### Stage 2: Data Validation Pipeline
- **IMO Validation**: 7-digit format with checksum verification
- **MMSI Validation**: 9-digit format with MID validation
- **Geographic Validation**: Coordinate range checks, null island detection
- **Timestamp Validation**: Future date and data freshness checks

### Stage 3-4: Multi-Agent Architecture
- LLM orchestrator with function calling
- Specialized tools: Validation, Similarity, Conflict Detection, Anomaly Detection
- OpenAI GPT-4 integration

### Stage 5-7: Entity Resolution with HITL
- Deterministic rules for exact matches
- Probabilistic similarity scoring
- Human-in-the-loop review for uncertain matches
- RLHF feedback collection for continuous improvement

### Stage 8-10: Knowledge Graph
- Vessel entities with relationships
- Temporal history tracking
- Canonical ground truth representation
- Neo4j-compatible structure

### Stage 11-13: Query & Conversational AI
- Structured query layer
- Natural language parsing
- RAG-based response generation
- Anti-hallucination safeguards

### Stage 14: Modular Architecture
- Unified main module
- Configuration management
- CLI interface
- Python API

## 🛡️ Anti-Hallucination Safeguards

The system implements strict measures to prevent AI hallucination:

1. **Tool-based Retrieval**: All LLM responses use function calling to query actual data
2. **Evidence Requirements**: Every answer must cite data sources
3. **No Fabrication**: The system never generates vessel information without evidence
4. **Grounded Responses**: All responses are validated against retrieved data

## 🔑 Key Features

| Feature | Description |
|---------|-------------|
| **IMO Checksum** | Weighted sum validation (7,6,5,4,3,2) mod 10 |
| **MMSI MID** | Maritime Identification Digit validation (200-775) |
| **Similarity Scoring** | Multi-method: Jaccard, token overlap, containment |
| **Confidence Thresholds** | HIGH: 0.9, REVIEW: 0.6, LOW: 0.3 |
| **Temporal Tracking** | Full state history with timestamps |
| **NL Queries** | "Find vessel IMO X", "Show flag changes" |

## 📝 Configuration

Edit `config.json` to customize:

```json
{
    "entity_resolution": {
        "high_confidence": 0.9,
        "review_required": 0.6,
        "attribute_weights": {
            "imo": 0.30,
            "name": 0.20,
            "mmsi": 0.10
        }
    },
    "hitl": {
        "require_review": true,
        "auto_approve_high_confidence": true
    }
}
```

## 📈 Dataset Information

The system is designed for the maritime vessel dataset with columns:

| Column | Description |
|--------|-------------|
| `imo` | International Maritime Organization number |
| `mmsi` | Maritime Mobile Service Identity |
| `name` | Vessel name |
| `flag` | Flag state code |
| `vessel_type` | Type classification |
| `length`, `width` | Physical dimensions |
| `grossTonnage` | Vessel tonnage |
| `builtYear` | Year of construction |
| `shipBuilder` | Builder name |
| `last_position_latitude/longitude` | AIS position |
| `UpdateDate` | Data timestamp |

## 🧪 Testing

```bash
# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

## 📜 License

Internal use only. All rights reserved.

## 👥 Authors

Maritime Vessel Identity Resolution System - Production AI Implementation
