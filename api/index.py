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
    import json as json_module
    app = FastAPI(title="Maritime API - Fallback Mode")
    
    error_msg = f"App import failed: {str(import_error)}" if import_error else "Unknown error"
    error_details = {
        "status": "error",
        "message": error_msg,
        "type": type(import_error).__name__ if import_error else "Unknown",
    }
    
    # Create fallback endpoints that handle errors gracefully
    @app.get("/api/status")
    async def fallback_status():
        return {"status": "fallback", "error": error_msg}
    
    @app.get("/api/health")
    async def fallback_health():
        return {"status": "fallback", "error": error_msg}
    
    @app.get("/health")
    async def root_health():
        return {"status": "fallback"}
    
    @app.post("/api/upload")
    async def fallback_upload():
        return {"error": "App not initialized", "status": "fallback"}
    
    @app.get("/api/load-default")
    async def fallback_load_default():
        return {"error": "App not initialized", "status": "fallback"}
    
    @app.get("/")
    async def fallback_root():
        return {"error": "Application not initialized. " + error_msg}

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

# Create the Mangum handler with error wrapper
if MANGUM_AVAILABLE:
    # Create Mangum handler with lifespan disabled for serverless
    _mangum_handler = Mangum(app, lifespan="off")
    
    # Wrap the handler to catch and log errors
    def handler(event, context):
        """Wrapped handler with error logging"""
        try:
            path = event.get('rawPath', event.get('path', 'UNKNOWN'))
            method = event.get('requestContext', {}).get('http', {}).get('method', 'UNKNOWN')
            print(f"📨 Request: {method} {path}")
            result = _mangum_handler(event, context)
            status = result.get('statusCode', 'UNKNOWN') if isinstance(result, dict) else '200'
            print(f"📤 Response: {status}")
            return result
        except Exception as e:
            print(f"❌ Handler error: {e}")
            print(f"📋 Traceback: {traceback.format_exc()}")
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "error": "Internal Server Error",
                    "message": str(e),
                    "type": type(e).__name__
                })
            }
    
    print("✅ Wrapped app with Mangum handler (with error wrapper)")
else:
    # Fallback handler without Mangum
    def handler(event, context):
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



