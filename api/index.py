"""
Vercel Python serverless handler - Simple FastAPI wrapper
This works directly without Mangum - Vercel handles ASGI->WSGI conversion
"""
import sys
import json
import traceback
from pathlib import Path

print("=" * 70)
print("🚀 Starting API handler...")
print("=" * 70)
print(f"Working directory: {Path.cwd()}")
print(f"Python version: {sys.version}")

# Add maritime_vessel_system/src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "maritime_vessel_system"))
print(f"📁 Added to sys.path: {project_root / 'maritime_vessel_system'}")

# Try to import the real app
app = None
import_error = None

try:
    from src.api.app import app as imported_app
    app = imported_app
    print("✅ Successfully imported FastAPI app from src.api.app")
except Exception as e:
    import_error = e
    print(f"❌ Failed to import app from src.api.app: {e}")
    print(f"📋 Error type: {type(e).__name__}")

# Create fallback app if import failed
if app is None:
    print("⚠️  Creating minimal fallback FastAPI app")
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    
    app = FastAPI(title="Maritime API - Fallback Mode")
    
    error_msg = f"App import failed: {str(import_error)}" if import_error else "Unknown error"
    
    @app.get("/api/status")
    async def fallback_status():
        return {
            "ok": False,
            "dataset_loaded": False,
            "record_count": 0,
            "validated": False,
            "graph_built": False,
            "error": error_msg,
            "message": "Application in fallback mode"
        }
    
    @app.get("/api/health")
    async def fallback_health():
        return {"status": "fallback", "message": error_msg}
    
    @app.get("/health")
    async def root_health():
        return {"status": "running"}

# Add global exception handler
from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    """Catch all exceptions and return JSON"""
    print(f"❌ Exception: {type(exc).__name__}: {exc}")
    print(traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": str(exc),
            "type": type(exc).__name__,
        }
    )

# Export app for Vercel
# Vercel will call the app directly as ASGI application
__all__ = ['app']

print("=" * 70)
print("✅ API handler loaded successfully")
print("=" * 70)




