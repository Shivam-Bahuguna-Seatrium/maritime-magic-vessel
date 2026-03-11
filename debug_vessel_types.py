#!/usr/bin/env python3
"""Debug vessel types in the dataset"""
import sys
from pathlib import Path
import csv
from collections import Counter

_SRC = Path(__file__).parent / "maritime_vessel_system" / "src"
sys.path.insert(0, str(_SRC.parent))
sys.path.insert(0, str(_SRC))

from api.neo4j_client import InMemoryGraphDB

kg = InMemoryGraphDB()
kg.initialize()

csv_file = Path(__file__).parent / "case_study_dataset_202509152039.csv"
vessel_types = Counter()

print("Analyzing vessel types...")
with open(csv_file) as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if i >= 1000:
            break
        vt = row.get("vessel_type", "")
        vessel_types[vt] += 1
        
        # Create node
        v_id = kg._create_node("Vessel", {
            "name": row.get("name", f"Vessel_{i}"),
            "vessel_type": vt,
            "gross_tonnage": float(row.get("grossTonnage", 0) or 0),
        })
        
        # Create type
        vt_id = kg._find_node("VesselType", name=vt)
        if not vt_id:
            vt_id = kg._create_node("VesselType", {"name": vt})
        kg._create_relationship(v_id, vt_id, "IS_TYPE", {})

print(f"\nTop 20 vessel types:")
for vt, count in vessel_types.most_common(20):
    print(f"  '{vt}': {count} vessels")
    if "tank" in vt.lower() or "tanker" in vt.lower():
        print(f"    ^^^ CONTAINS TANKER")

# Test CONTAINS query
print("\n=== Testing VesselType names with CONTAINS ===")
cypher = "MATCH (vt:VesselType) WHERE vt.name CONTAINS 'Tanker' RETURN vt.name AS name"
results = kg.run_cypher(cypher)
print(f"Query: WHERE vt.name CONTAINS 'Tanker'")
print(f"Results: {len(results)} types")
for r in results[:5]:
    print(f"  {r}")

# Test case-insensitive CONTAINS
print("\n=== Testing case variations ===")
for pattern in ["Tanker", "tanker", "Tank", "tank"]:
    cypher = f"MATCH (vt:VesselType) WHERE vt.name CONTAINS '{pattern}' RETURN COUNT(*) AS count"
    results = kg.run_cypher(cypher)
    print(f"  CONTAINS '{pattern}': {results}")
