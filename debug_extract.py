#!/usr/bin/env python3
"""Debug extraction issue"""
import sys
from pathlib import Path

_SRC = Path(__file__).parent / "maritime_vessel_system" / "src"
sys.path.insert(0, str(_SRC.parent))
sys.path.insert(0, str(_SRC))

from api.neo4j_client import InMemoryGraphDB

# Create simple test KG
kg = InMemoryGraphDB()

# Add test data manually
v_id = kg._create_node("Vessel", {
    "name": "Test Vessel",
    "flag": "PA",
    "gross_tonnage": 50000,
    "built_year": 2010,
})

print("Created vessel node:")
vessel = kg.nodes[v_id]
print(f"  Full node: {vessel}")
print(f"  Properties: {vessel.get('properties', {})}")

# Test _extract_value directly
row = {"v": vessel.get("properties", {})}
print(f"\nRow dict: {row}")

# Test extraction
test_cases = [
    "v.properties.name",
    "v.name",
    "v.properties.flag",
]

for spec in test_cases:
    val = kg._extract_value(spec, row)
    print(f"  Extract '{spec}' -> {val}")

# Test full query
print("\n=== Full Query Test ===")
cypher = "MATCH (v:Vessel) RETURN v.properties.name AS name, v.properties.flag AS flag"
print(f"Query: {cypher}")
results = kg.run_cypher(cypher)
print(f"Results: {results}")
if results:
    print(f"First row: {results[0]}")
    print(f"Keys: {list(results[0].keys())}")
