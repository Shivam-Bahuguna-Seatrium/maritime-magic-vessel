"""
Test script to verify API endpoints locally
"""
import sys
from pathlib import Path
import asyncio
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "maritime_vessel_system"))

from src.api.app import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_load_default():
    """Test if default dataset can load"""
    print("\n🧪 Testing /api/load-default...")
    response = client.get("/api/load-default")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    return response

def test_status():
    """Test basic status endpoint"""
    print("\n🧪 Testing /api/status...")
    response = client.get("/api/status")
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print(f"Response: {response.json()}")
    else:
        print(f"Error: {response.text}")
    return response

if __name__ == "__main__":
    print("=" * 60)
    print("API Local Test Suite")
    print("=" * 60)
    
    try:
        test_status()
    except Exception as e:
        print(f"❌ Status test failed: {e}")
    
    try:
        test_load_default()
    except Exception as e:
        print(f"❌ Load default test failed: {e}")
    
    print("\n" + "=" * 60)
