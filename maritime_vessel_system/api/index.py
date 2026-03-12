"""
Vercel Python serverless function entry point for FastAPI
"""
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import and expose the FastAPI app
from src.api.app import app

# This is what Vercel will call
__all__ = ['app']
