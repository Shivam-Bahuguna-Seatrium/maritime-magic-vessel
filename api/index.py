"""
Vercel Python serverless function entry point for FastAPI with Mangum
"""
import sys
import traceback
from pathlib import Path

print("=" * 70)
print("🚀 Starting API handler initialization...")
print("=" * 70)

# Try to import Mangum for serverless adaptation
try:
    from mangum import Mangum
    MANGUM_AVAILABLE = True
    print("✅ Mangum imported successfully")
except ImportError as e:
    MANGUM_AVAILABLE = False
    print(f"❌ Mangum import failed: {e}")

from fastapi import FastAPI
from fastapi.responses import JSONResponse

# Add maritime_vessel_system/src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "maritime_vessel_system"))
print(f"📁 Added to sys.path: {project_root / 'maritime_vessel_system'}")

# Try to import the real app, fall back to a simple one if it fails
app = None
import_error = None

try:
    from src.api.app import app as imported_app
    app = imported_app
    print("✅ Successfully imported FastAPI app from src.api.app")
except Exception as e:
    import_error = e
    print(f"❌ Failed to import app: {e}")
    print(f"📋 Traceback: {traceback.format_exc()}")

# Create fallback app if import failed
if app is None:
    print("⚠️  Creating minimal fallback FastAPI app")
    app = FastAPI(title="Maritime API - Fallback Mode")
    
    @app.get("/api/health")
    async def fallback_health():
        return {
            "status": "fallback",
            "error": f"Failed to import main app: {str(import_error)}",
        }
    
    @app.get("/api/status")
    async def fallback_status():
        return {
            "status": "fallback",
            "error": f"Failed to import main app: {str(import_error)}",
        }

# Add health check endpoint to all apps
@app.get("/api/health-check")
async def health_check():
    """Simple health check - should always work"""
    return {
        "status": "ok",
        "message": "API handler is running",
        "mangum_available": MANGUM_AVAILABLE
    }

# Create the Mangum handler
if MANGUM_AVAILABLE:
    handler = Mangum(app)
    print("✅ Wrapped app with Mangum handler")
else:
    # Direct app as handler won't work on Vercel but allows local testing
    handler = app
    print("⚠️  Using app directly as handler (may fail on Vercel)")

print("=" * 70)
print("🎯 API module loaded successfully")
print("=" * 70)

# Export for Vercel
__all__ = ['app', 'handler']



