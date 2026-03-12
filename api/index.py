"""
Vercel Python serverless function entry point for FastAPI
Routes all /api/* requests to the FastAPI application
"""
import sys
import traceback
from pathlib import Path

# Try to import Mangum for serverless adaptation
try:
    from mangum import Mangum
    MANGUM_AVAILABLE = True
except ImportError:
    MANGUM_AVAILABLE = False
    print("⚠️  Mangum not installed - serverless runtime may fail")

from fastapi import FastAPI
from fastapi.responses import JSONResponse

# Add maritime_vessel_system/src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "maritime_vessel_system"))

# Try to import the real app, fall back to a simple one if it fails
try:
    from src.api.app import app
    print("✅ Successfully imported app from src.api.app")
except Exception as e:
    print(f"❌ Failed to import app: {e}")
    print(f"Traceback: {traceback.format_exc()}")
    
    # Create a minimal fallback app
    app = FastAPI(title="Maritime API Fallback")
    
    @app.get("/api/status")
    async def status():
        return {
            "error": f"Failed to initialize app: {str(e)}",
            "traceback": traceback.format_exc()
        }
    
    @app.get("/api/health")
    async def health():
        return {"status": "fallback"}

# Health check endpoint
@app.get("/api/health")
async def health():
    """Health check endpoint for Vercel"""
    try:
        # Try to access the state to verify initialization
        from src.api.app import state
        return {
            "status": "ok",
            "dataset_loaded": state.df is not None if state.df is not None else False,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# Wrap with Mangum for Vercel serverless runtime
if MANGUM_AVAILABLE:
    handler = Mangum(app, lifespan="off")
    print("✅ Wrapped app with Mangum for serverless runtime")
else:
    # Fallback - just use the app directly (less likely to work on Vercel)
    handler = app
    print("⚠️  Using app directly without Mangum wrapper")

# Export for Vercel
__all__ = ['app', 'handler']


