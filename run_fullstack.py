"""
Full-stack development server
Serves both React frontend (static) and FastAPI backend on the same port
This simulates how Vercel will run
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "maritime_vessel_system"))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

# Import the actual app
from src.api.app import app as api_app

# Get paths
FRONTEND_BUILD = Path(__file__).parent / "maritime_vessel_system" / "frontend" / "dist"

# Mount static files
api_app.mount("/assets", StaticFiles(directory=FRONTEND_BUILD / "assets"), name="assets")

# SPA fallback - serve index.html for non-API routes
@api_app.get("/{path_name:path}")
async def spa_fallback(path_name: str):
    """Serve index.html for all non-API routes (SPA routing)"""
    if path_name.startswith("api/"):
        return {"error": "Not found"}, 404
    return FileResponse(FRONTEND_BUILD / "index.html")

@api_app.get("/")
async def root():
    """Root path - serve index.html"""
    return FileResponse(FRONTEND_BUILD / "index.html")

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Full-Stack Development Server (Single Port)")
    print("=" * 70)
    print(f"Frontend: {FRONTEND_BUILD}")
    print(f"Frontend exists: {FRONTEND_BUILD.exists()}")
    print(f"API: FastAPI")
    print("\nStarting on http://localhost:8000")
    print("=" * 70)
    
    uvicorn.run(
        api_app,
        host="0.0.0.0",
        port=8000,
        reload=False,  # Disable reload since we're serving static files
    )
