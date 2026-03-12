"""
Vercel Python serverless function entry point for FastAPI
Routes all /api/* requests to the FastAPI application
"""
import sys
from pathlib import Path

# Add maritime_vessel_system/src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "maritime_vessel_system"))

# Import and expose the FastAPI app
from src.api.app import app

# Export for Vercel
__all__ = ['app']
