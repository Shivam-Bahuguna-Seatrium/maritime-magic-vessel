#!/usr/bin/env python3
"""Inspect the Knowledge Graph structure and properties"""

from src.api.neo4j_client import InMemoryGraphDB
import json

neo = InMemoryGraphDB()
neo.load_from_file()

print('=== SAMPLE VESSEL PROPERTIES ===')
# Get sample of all vessel nodes
cypher = 'MATCH (v:Vessel) RETURN v.properties AS props LIMIT 3'
results = neo.run_cypher(cypher)
if results:
    for row in results:
        if 'props' in row:
            props = row['props']
            print(json.dumps(props, indent=2, default=str))
            print('---')

print('\n=== VALIDATION STATUS DISTRIBUTION ===')
# Get all validation stat uses
all_vessels = neo.run_cypher('MATCH (v:Vessel) RETURN v.properties AS props')
if all_vessels:
    status_counts = {}
    for row in all_vessels:
        props = row.get('props', {})
        status = props.get('validation_status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    total = sum(status_counts.values())
    print(f'Total vessels: {total}')
    for status, count in sorted(status_counts.items()):
        pct = (count / total * 100) if total > 0 else 0
        print(f'{status}: {count} vessels ({pct:.1f}%)')

print('\n=== ALL NODE TYPES ===')
types = neo.run_cypher('MATCH (n) RETURN DISTINCT labels(n) AS type, COUNT(*) AS count ORDER BY count DESC')
if types:
    for row in types:
        print(f'{row}')

print('\n=== RELATIONSHIP TYPES ===')
rels = neo.run_cypher('MATCH ()-[r]->() RETURN DISTINCT type(r) AS rel_type, COUNT(*) AS count ORDER BY count DESC')
if rels:
    for row in rels:
        print(f'{row}')

print('\n=== CHECKING VESSEL WITH VALIDATION ERRORS ===')
invalid = neo.run_cypher('MATCH (v:Vessel) WHERE v.properties.validation_status = "invalid" RETURN v.properties.name AS name, v.properties.validation_errors AS errors LIMIT 3')
if invalid:
    for row in invalid:
        print(json.dumps(row, indent=2, default=str))
else:
    print('No invalid vessels found')

print('\n=== AVAILABLE VESSEL PROPERTIES (from first vessel) ===')
single = neo.run_cypher('MATCH (v:Vessel) RETURN v.properties LIMIT 1')
if single:
    props = single[0].get('properties', {})
    if props:
        print('Property names:', list(props.keys()))
    else:
        print('No properties found')
