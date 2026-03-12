"""
Vercel Python serverless function entry point for FastAPI with Mangum
"""
import sys
import json
import traceback
from pathlib import Path

print("=" * 70)
print("🚀 Starting API handler initialization...")
print("=" * 70)
print(f"Working directory: {Path.cwd()}")
print(f"Python version: {sys.version}")
print(f"Project root: {Path(__file__).parent.parent}")

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
    print(f"❌ Failed to import app from src.api.app: {e}")
    print(f"📋 Error type: {type(e).__name__}")
    # Don't fail completely - we'll create a fallback app below

# Create fallback app if import failed
if app is None:
    print("⚠️  Creating minimal fallback FastAPI app")
    import json as json_module
    app = FastAPI(title="Maritime API - Fallback Mode")
    
    error_msg = f"App import failed: {str(import_error)}" if import_error else "Unknown error"
    error_details = {
        "status": "error",
        "message": error_msg,
        "type": type(import_error).__name__ if import_error else "Unknown",
    }
    
    # Import sample data if possible
    sample_data = None
    try:
        import csv
        csv_path = project_root / "case_study_dataset_202509152039.csv"
        if csv_path.exists():
            import pandas as pd_test
            sample_data = pd_test.read_csv(str(csv_path)).to_dict('records')[:5]
            print(f"✅ Loaded sample CSV data ({len(sample_data)} rows)")
    except Exception as csv_err:
        print(f"⚠️  Could not load sample CSV: {csv_err}")
    
    # Create fallback endpoints
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
    
    @app.post("/api/upload")
    async def fallback_upload():
        return {"error": "App not fully initialized", "status": "fallback"}
    
    @app.get("/api/load-default")
    async def fallback_load_default():
        return {
            "success": False,
            "message": "Fallback mode - unable to load data",
            "data": sample_data,
            "error": error_msg
        }
    
    @app.get("/")
    async def fallback_root():
        return {"status": "fallback", "message": "Application not initialized. Check logs."}

# Add health check endpoint to all apps
@app.get("/api/health-check")
async def health_check():
    """Simple health check - should always work"""
    return {
        "status": "ok",
        "message": "API handler is running",
        "mangum_available": MANGUM_AVAILABLE
    }

# Add exception handlers to ensure JSON responses
@app.exception_handler(Exception)
async def exception_handler(request, exc):
    """Catch all exceptions and return JSON"""
    print(f"❌ Exception in app: {exc}")
    print(f"📋 Type: {type(exc).__name__}")
    print(f"📋 Traceback: {traceback.format_exc()}")
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": str(exc),
            "type": type(exc).__name__,
        }
    )

# Create the Mangum handler
if MANGUM_AVAILABLE:
    # Mangum can be used directly as the handler
    # It will be called by Vercel with (event, context) arguments
    handler = Mangum(app, lifespan="off")
    print("✅ Using Mangum handler directly")
else:
    # Fallback handler without Mangum
    async def handler(event, context):
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Mangum not available"})
        }
    print("⚠️  Mangum not available - using fallback handler")

print("=" * 70)
print("🎯 API module loaded successfully") 
print("=" * 70)

# Export for Vercel
__all__ = ['app', 'handler']



