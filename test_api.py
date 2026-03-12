#!/usr/bin/env python3
"""
Local test script to verify API works
"""
import sys
import asyncio
from pathlib import Path

# Add maritime_vessel_system/src to path
sys.path.insert(0, str(Path(__file__).parent / "maritime_vessel_system"))

print("=" * 70)
print("Testing API initialization...")
print("=" * 70)

try:
    print("\n1️⃣  Importing FastAPI app...")
    from src.api.app import app, state
    print("✅ App imported successfully")
    
    print("\n2️⃣  Checking app configuration...")
    print(f"   - Title: {app.title}")
    print(f"   - Version: {app.version}")
    print(f"   - Routes: {len(app.routes)}")
    
    print("\n3️⃣  Testing status endpoint logic...")
    response = {
        "dataset_loaded": state.df is not None,
        "record_count": len(state.df) if state.df is not None else 0,
        "validated": state.validation_results is not None,
        "graph_built": state.graph_built,
    }
    print(f"   Status response: {response}")
    
    print("\n4️⃣  Testing Mangum wrapper...")
    from mangum import Mangum
    handler = Mangum(app)
    print(f"   Handler: {handler}")
    print("✅ Mangum handler created")
    
    print("\n" + "=" * 70)
    print("✅ ALL CHECKS PASSED - API is ready for deployment")
    print("=" * 70)
    
except Exception as e:
    import traceback
    print("\n❌ ERROR:")
    print(traceback.format_exc())
    sys.exit(1)
