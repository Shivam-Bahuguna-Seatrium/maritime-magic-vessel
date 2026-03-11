# Maritime Vessel Identity Resolution System – Quick Start

## Prerequisites
- Python 3.9+
- Node.js 18+
- OpenAI API key
- **Optional:** Neo4j (for persistent graph storage)

## 🎯 No Neo4j Needed!

**The system automatically detects if Neo4j is unavailable and falls back to an in-memory graph database.** All features work the same way!

| Feature | With Neo4j | Without Neo4j |
|---------|-----------|---------------|
| Upload & Validate | ✅ | ✅ |
| Build Graph | ✅ Persistent | ✅ In-Memory |
| Filter & Explore | ✅ | ✅ |
| Chat with Cypher | ✅ | ⚠️ Basic (no real Cypher) |

**If you want to use Neo4j:**

### Option 1: Docker (Easiest)
```bash
docker run -d --name neo4j -p 7687:7687 -p 7474:7474 \
  -e NEO4J_AUTH=neo4j/password neo4j:latest
```

### Option 2: Local Installation
- Download from https://neo4j.com/download/
- Run `bin/neo4j console` (or `bin\neo4j.bat console` on Windows)
- Default: `neo4j://localhost:7687`, user: `neo4j`, password: `neo4j`

### Option 3: Neo4j Desktop
- Download: https://neo4j.com/download/other-releases/
- Create a project and start a database with bolt port 7687

## Setup

### 1. Install Python Dependencies
```bash
cd maritime_vessel_system
pip install -r requirements.txt
```

### 2. Install Frontend Dependencies
```bash
cd frontend
npm install
cd ..
```

## Environment Variables

```bash
# Linux/Mac
export OPENAI_API_KEY="sk-..."
export NEO4J_PASSWORD="your-neo4j-password"

# Windows PowerShell
$env:OPENAI_API_KEY = "sk-..."
$env:NEO4J_PASSWORD = "your-neo4j-password"
```

## Running the System

### Backend (FastAPI)
```bash
cd maritime_vessel_system
python -m uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```
✅ Backend: **http://localhost:8000**
- API docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Frontend (React + Vite)
```bash
cd maritime_vessel_system/frontend
npm run dev
```
✅ Frontend: **http://localhost:3000** (or next available port)

## Workflow

1. **Upload CSV** → Upload `case_study_dataset_202509152039.csv`
2. **Analyze** → Run EDA analysis
3. **Validate** → Validate vessel data (IMO, MMSI, coordinates)
4. **Build Graph** → Create Neo4j knowledge graph (requires Neo4j running)
5. **Explore** → View graph with filters & hierarchical ontology
6. **Chat** → Query with natural language (generates Cypher)

## Key Features

| Component | Location |
|-----------|----------|
| Ontology (11 categories, 59 types) | `src/knowledge_graph/ontology.py` |
| Neo4j Client | `src/api/neo4j_client.py` |
| OpenAI Agents (LLM) | `src/agents/multi_agent_system.py` |
| FastAPI Backend | `src/api/app.py` |
| React Dashboard | `frontend/src/components/Dashboard.jsx` |
| Graph Visualization | `frontend/src/components/GraphViewer.jsx` |
| Chat/Cypher | `frontend/src/components/ChatPanel.jsx` |

## Validation + Graph

- **Validation** marks entities as RED (invalid) or GREEN (valid)
- **Graph Build** works with or without Neo4j:
  - **With Neo4j:** Data persists in external database
  - **Without Neo4j:** Data stored in-memory (lost on restart)
- **Invalid entities** appear as red nodes in visualization
- **Fallback Detection:** Backend automatically uses in-memory DB if Neo4j unavailable

## Test Backend

```bash
curl http://localhost:8000/api/status
```

Expected response:
```json
{
  "status": "running",
  "neo4j_connected": true,
  "agents_available": true
}
```

## Common Issues

| Issue | Solution |
|-------|----------|
| `ServiceUnavailable: Couldn't connect to localhost:7687` | **Expected!** Backend automatically uses in-memory graph—all features work |
| Port 3000 in use | Vite automatically uses next available port (3001, 3002, etc.) |
| `OPENAI_API_KEY` not set | Set environment variable with your OpenAI API key |
| Neo4j auth fails | Check password matches `NEO4J_PASSWORD` env var |
| Chat queries not working | In-memory DB has limited Cypher support; use Neo4j for full functionality |

---

**URLs:**
- Backend: http://localhost:8000
- Frontend: http://localhost:3000
- Neo4j Browser: http://localhost:7474 (if using Neo4j)
- Dataset: `case_study_dataset_202509152039.csv` (37 vessels)

---

## 💡 In-Memory Fallback Details

When Neo4j is unavailable, the backend automatically switches to `InMemoryGraphDB`:

- **Automatically detected:** No manual configuration needed
- **All visualizations work:** Nodes, edges, filters, colors
- **Limited Cypher:** The chat feature returns basic results (no complex query engine)
- **Data loss on restart:** Graph data is cleared when backend restarts
- **Performance:** Suitable for up to ~500 vessels

To get full Cypher support and data persistence, **start Neo4j** (see install options above).
