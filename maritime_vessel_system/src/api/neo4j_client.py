"""
Neo4j Graph Database Client
============================

Manages connection to a local Neo4j instance and provides methods
to persist / query the maritime knowledge graph.

Requires a running Neo4j instance (default bolt://localhost:7687).
"""

import os
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    from neo4j import GraphDatabase, Driver
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    Driver = None

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from knowledge_graph.ontology import (
    VESSEL_TYPE_HIERARCHY,
    VESSEL_TYPE_TO_CATEGORY,
    get_category_for_type,
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def sanitize_value(value: Any) -> Any:
    """
    Sanitize a value for JSON serialization.
    Converts NaN, Inf, and other non-serializable values to None.
    """
    if value is None:
        return None
    
    # Handle NaN and Infinity
    if isinstance(value, float):
        if str(value).lower() == 'nan' or value != value:  # NaN check
            return None
        if value == float('inf') or value == float('-inf'):
            return None
    
    return value


def sanitize_properties(props: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize all property values in a dictionary."""
    return {k: sanitize_value(v) for k, v in props.items()}


# ============================================================================
# IN-MEMORY FALLBACK (When Neo4j is not available)
# ============================================================================

class InMemoryGraphDB:
    """
    Fallback in-memory graph database when Neo4j is not available.
    Provides the same interface as Neo4jClient for seamless fallback.
    """

    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.relationships: List[Dict[str, Any]] = []
        self._node_id_counter = 0
        self._rel_id_counter = 0
        self._initialize_ontology()

    def _initialize_ontology(self):
        """Pre-populate with ontology nodes."""
        for category, types in VESSEL_TYPE_HIERARCHY.items():
            cat_id = self._create_node("VesselCategory", {"name": category})
            for vtype in types:
                type_id = self._create_node("VesselType", {"name": vtype})
                self._create_relationship(type_id, cat_id, "BELONGS_TO_CATEGORY", {})

    def _create_node(
        self, label: str, props: Dict[str, Any]
    ) -> str:
        """Create a node and return its ID. Sanitizes properties for JSON serialization."""
        node_id = f"node_{self._node_id_counter}"
        self._node_id_counter += 1
        self.nodes[node_id] = {
            "id": node_id,
            "labels": [label],
            "properties": sanitize_properties(props),
            "color": "#3498db",
        }
        return node_id

    def _create_relationship(
        self,
        start_id: str,
        end_id: str,
        rel_type: str,
        props: Dict[str, Any],
    ) -> str:
        """Create a relationship and return its ID."""
        rel_id = f"rel_{self._rel_id_counter}"
        self._rel_id_counter += 1
        self.relationships.append({
            "id": rel_id,
            "type": rel_type,
            "startNode": start_id,
            "endNode": end_id,
            "properties": props.copy(),
        })
        return rel_id

    def _find_node(self, label: str, **props) -> Optional[str]:
        """Find a node by label and properties."""
        for node_id, node in self.nodes.items():
            if label in node["labels"]:
                if all(node["properties"].get(k) == v for k, v in props.items()):
                    return node_id
        return None

    def connect(self):
        """No-op for fallback."""
        pass

    def close(self):
        """No-op for fallback."""
        pass

    def initialize(self):
        """Already initialized in __init__."""
        pass

    def create_constraints(self):
        """No-op for fallback."""
        pass

    def seed_ontology(self):
        """Already seeded in __init__."""
        pass

    def clear_all(self):
        """Clear all data."""
        self.nodes.clear()
        self.relationships.clear()
        self._node_id_counter = 0
        self._rel_id_counter = 0
        self._initialize_ontology()

    def ingest_vessel(
        self,
        record: Dict[str, Any],
        validation_status: str = "valid",
        validation_errors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Ingest a vessel record into the in-memory graph."""
        imo = record.get("imo")
        mmsi = record.get("mmsi")
        name = record.get("name") or ""
        
        # Handle vessel_type: convert NaN/None to empty string (treated as missing value)
        vessel_type = record.get("vessel_type")
        if vessel_type is None or (isinstance(vessel_type, float) and str(vessel_type).lower() == 'nan'):
            vessel_type = ""
        else:
            vessel_type = str(vessel_type).strip() if vessel_type else ""
        
        category = get_category_for_type(vessel_type)
        flag = record.get("flag")
        builder = record.get("shipBuilder")
        port_name = record.get("matchedPort_name")
        port_unlocode = record.get("matchedPort_unlocode")

        if imo and int(imo) > 0:
            vessel_id = f"vessel_imo_{int(imo)}"
        elif mmsi:
            vessel_id = f"vessel_mmsi_{int(mmsi)}"
        else:
            return {"status": "skipped", "reason": "no identifier"}

        stats = {"nodes": 0, "relationships": 0}
        errors_json = json.dumps(validation_errors or [])

        # Create Vessel node
        color = "#e74c3c" if validation_status == "invalid" else "#2ecc71"
        v_id = self._create_node(
            "Vessel",
            {
                "vessel_id": vessel_id,
                "imo": int(imo) if imo else None,
                "mmsi": int(mmsi) if mmsi else None,
                "name": name,
                "vessel_type": vessel_type,
                "category": category,
                "length": record.get("length"),
                "width": record.get("width"),
                "gross_tonnage": record.get("grossTonnage"),
                "built_year": record.get("builtYear"),
                "draught": record.get("draught"),
                "deadweight": record.get("deadweight"),
                "callsign": record.get("callsign"),
                "flag": flag,
                "validation_status": validation_status,
                "validation_errors": errors_json,
                "last_lat": record.get("last_position_latitude"),
                "last_lon": record.get("last_position_longitude"),
                "last_speed": record.get("last_position_speed"),
                "destination": record.get("destination"),
            },
        )
        self.nodes[v_id]["color"] = color
        stats["nodes"] += 1
        
        # Debug: log first 5 vessels ingested
        if len([n for n in self.nodes.values() if "Vessel" in n["labels"]]) <= 5:
            print(f"[InMemoryGraphDB.ingest_vessel] Ingested vessel: {vessel_id}, name='{name}', flag={flag}")

        # IMO node
        if imo and int(imo) > 0:
            imo_node = self._find_node("IMO", value=int(imo))
            if not imo_node:
                imo_node = self._create_node("IMO", {"value": int(imo)})
                stats["nodes"] += 1
            self._create_relationship(v_id, imo_node, "HAS_IMO", {})
            stats["relationships"] += 1

        # MMSI node
        if mmsi:
            mmsi_node = self._find_node("MMSI", value=int(mmsi))
            if not mmsi_node:
                mmsi_node = self._create_node("MMSI", {"value": int(mmsi)})
                stats["nodes"] += 1
            self._create_relationship(v_id, mmsi_node, "USES_MMSI", {})
            stats["relationships"] += 1

        # Vessel Type node (only if vessel_type is not empty)
        if vessel_type:
            vt_node = self._find_node("VesselType", name=vessel_type)
            if not vt_node:
                vt_node = self._create_node("VesselType", {"name": vessel_type})
                stats["nodes"] += 1
            self._create_relationship(v_id, vt_node, "IS_TYPE", {})
            stats["relationships"] += 1

        # Flag node
        if flag:
            flag_node = self._find_node("Flag", code=flag)
            if not flag_node:
                flag_node = self._create_node("Flag", {"code": flag})
                stats["nodes"] += 1
            self._create_relationship(v_id, flag_node, "REGISTERED_UNDER", {})
            stats["relationships"] += 1

        # Ship Builder node
        if builder:
            builder_node = self._find_node("ShipBuilder", name=builder)
            if not builder_node:
                builder_node = self._create_node("ShipBuilder", {"name": builder})
                stats["nodes"] += 1
            self._create_relationship(v_id, builder_node, "BUILT_BY", {})
            stats["relationships"] += 1

        # Port node
        if port_name and port_unlocode:
            port_node = self._find_node("Port", unlocode=port_unlocode)
            if not port_node:
                port_node = self._create_node(
                    "Port",
                    {
                        "unlocode": port_unlocode,
                        "name": port_name,
                        "latitude": record.get("matchedPort_latitude"),
                        "longitude": record.get("matchedPort_longitude"),
                    },
                )
                stats["nodes"] += 1
            self._create_relationship(v_id, port_node, "VISITED", {})
            stats["relationships"] += 1

        return stats

    def get_graph_data(
        self,
        category: Optional[str] = None,
        vessel_type: Optional[str] = None,
        flag: Optional[str] = None,
        validation_status: Optional[str] = None,
        vessel_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return filtered graph data with sanitized properties."""
        filtered_nodes = {}
        filtered_rels = []

        # Filter vessels
        vessel_ids = set()
        vessel_count_debug = 0
        for node_id, node in self.nodes.items():
            if "Vessel" not in node["labels"]:
                continue
            vessel_count_debug += 1
            props = node["properties"]
            if category and props.get("category") != category:
                continue
            if vessel_type and props.get("vessel_type") != vessel_type:
                continue
            if flag and props.get("flag") != flag:
                continue
            if validation_status and props.get("validation_status") != validation_status:
                continue
            if vessel_name and props.get("name") != vessel_name:
                continue
            vessel_ids.add(node_id)
            filtered_nodes[node_id] = node

        print(f"[InMemoryGraphDB.get_graph_data] Total vessels in graph: {vessel_count_debug}, Filtered: {len(vessel_ids)}")

        # Get 1-hop neighbourhood
        connected_ids = set(vessel_ids)
        for rel in self.relationships:
            if rel["startNode"] in vessel_ids:
                connected_ids.add(rel["endNode"])
                if rel["endNode"] not in filtered_nodes:
                    filtered_nodes[rel["endNode"]] = self.nodes[rel["endNode"]]
                filtered_rels.append(rel)

        # Sanitize all node properties before returning (ensure JSON-serializable)
        sanitized_nodes = []
        for node in filtered_nodes.values():
            sanitized_node = {
                "id": node["id"],
                "labels": node["labels"],
                "properties": sanitize_properties(node["properties"]),
                "color": node.get("color", "#3498db"),
            }
            sanitized_nodes.append(sanitized_node)

        return {
            "nodes": sanitized_nodes,
            "relationships": filtered_rels,
        }

    def get_ontology_tree(self) -> List[Dict]:
        """Return ontology hierarchy with vessel counts."""
        tree: Dict[str, Dict] = {}
        for category in VESSEL_TYPE_HIERARCHY.keys():
            tree[category] = {"name": category, "types": [], "total": 0}
            for vtype in VESSEL_TYPE_HIERARCHY[category]:
                count = sum(
                    1
                    for node in self.nodes.values()
                    if "Vessel" in node["labels"]
                    and node["properties"].get("vessel_type") == vtype
                )
                tree[category]["types"].append({"name": vtype, "count": count})
                tree[category]["total"] += count
        return list(tree.values())

    def get_filter_options(self) -> Dict[str, List[str]]:
        """Return available filter values."""
        categories = set()
        vessel_types = set()
        flags = set()
        statuses = set()
        vessel_names = []
        name_count = {}
        
        # Debug: count vessel nodes
        vessel_nodes = [node for node in self.nodes.values() if "Vessel" in node["labels"]]
        vessel_node_count = len(vessel_nodes)
        print(f"[InMemoryGraphDB.get_filter_options] Total vessel nodes found: {vessel_node_count} out of {len(self.nodes)} total nodes")
        
        # Debug: show first vessel details
        if vessel_nodes:
            first_vessel = vessel_nodes[0]
            print(f"[InMemoryGraphDB.get_filter_options] First vessel properties: {first_vessel['properties']}")

        for node in vessel_nodes:
            props = node["properties"]
            if props.get("category"):
                categories.add(props["category"])
            if props.get("vessel_type"):
                vessel_types.add(props["vessel_type"])
            if props.get("flag"):
                flags.add(props["flag"])
            if props.get("validation_status"):
                statuses.add(props["validation_status"])
            # Collect vessel names - include names that may be empty strings
            name = props.get("name", "")
            if name:  # Only if name is not empty
                name_count[name] = name_count.get(name, 0) + 1

        # Sort vessel names alphabetically
        vessel_names = sorted(list(name_count.keys()))
        
        print(f"[InMemoryGraphDB.get_filter_options] Found {len(vessel_names)} unique vessel names, {sum(name_count.values())} total vessels with names")
        print(f"[InMemoryGraphDB.get_filter_options] Categories: {len(categories)}, Types: {len(vessel_types)}, Flags: {len(flags)}, Statuses: {len(statuses)}")

        return {
            "categories": sorted(list(categories)),
            "vessel_types": sorted(list(vessel_types)),
            "flags": sorted(list(flags)),
            "validation_statuses": sorted(list(statuses)),
            "vessel_names": vessel_names,
            "vessel_count": len(vessel_names),
        }

    def get_filtered_options(
        self,
        category: Optional[str] = None,
        vessel_type: Optional[str] = None,
        vessel_name: Optional[str] = None,
        flag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return filtered options based on current filter selections.
        This enables cascading dropdowns where selecting a category shows only
        vessel types available for that category, etc.
        """
        vessel_nodes = [node for node in self.nodes.values() if "Vessel" in node["labels"]]
        
        # Filter vessel nodes based on provided parameters
        filtered_vessels = vessel_nodes
        if category:
            filtered_vessels = [v for v in filtered_vessels if v["properties"].get("category") == category]
        if vessel_type:
            filtered_vessels = [v for v in filtered_vessels if v["properties"].get("vessel_type") == vessel_type]
        if vessel_name:
            filtered_vessels = [v for v in filtered_vessels if v["properties"].get("name") == vessel_name]
        if flag:
            filtered_vessels = [v for v in filtered_vessels if v["properties"].get("flag") == flag]
        
        # Extract unique values from filtered vessels
        filtered_vessel_types = set()
        filtered_vessel_names = set()
        filtered_flags = set()
        filtered_statuses = set()
        
        for vessel in filtered_vessels:
            props = vessel["properties"]
            if props.get("vessel_type"):
                filtered_vessel_types.add(props["vessel_type"])
            if props.get("name"):
                filtered_vessel_names.add(props["name"])
            if props.get("flag"):
                filtered_flags.add(props["flag"])
            if props.get("validation_status"):
                filtered_statuses.add(props["validation_status"])
        
        print(f"[InMemoryGraphDB.get_filtered_options] Filters: category={category}, vessel_type={vessel_type}, vessel_name={vessel_name}, flag={flag}")
        print(f"[InMemoryGraphDB.get_filtered_options] Filtered {len(vessel_nodes)} → {len(filtered_vessels)} vessels")
        print(f"[InMemoryGraphDB.get_filtered_options] Returning {len(filtered_vessel_types)} types, {len(filtered_vessel_names)} names, {len(filtered_flags)} flags, {len(filtered_statuses)} statuses")
        
        return {
            "vessel_types": sorted(list(filtered_vessel_types)),
            "vessel_names": sorted(list(filtered_vessel_names)),
            "flags": sorted(list(filtered_flags)),
            "validation_statuses": sorted(list(filtered_statuses)),
        }

    def run_cypher(self, cypher: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Execute Cypher queries on the in-memory graph.
        Supports: MATCH, WHERE, RETURN with projections + aggregations, ORDER BY, GROUP BY, LIMIT.
        """
        import re
        from collections import defaultdict
        
        # Use case-insensitive patterns to find clauses, but extract from original cypher
        where_pattern = re.search(r'WHERE\s+(.+?)(?:\s+RETURN|\s+ORDER|\s*$)', cypher, re.IGNORECASE)
        return_pattern = re.search(r'RETURN\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)', cypher, re.IGNORECASE)
        order_pattern = re.search(r'ORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s*$)', cypher, re.IGNORECASE)
        limit_pattern = re.search(r'LIMIT\s+(\d+)', cypher, re.IGNORECASE)
        
        # Get vessel nodes
        vessel_nodes = [(nid, n) for nid, n in self.nodes.items() if "Vessel" in n["labels"]]
        if not vessel_nodes:
            return []
        
        # Enrich each vessel with related data FIRST
        enriched_rows = []
        for nid, vessel_node in vessel_nodes:
            row = {"v": vessel_node.get("properties", {}), "v_id": nid}
            
            # Get VesselType
            for rel in self.relationships:
                if rel["startNode"] == nid and rel["type"] == "IS_TYPE":
                    vt_node = self.nodes.get(rel["endNode"], {})
                    row["vt"] = vt_node.get("properties", {})
                    break
            
            enriched_rows.append(row)
        
        # Apply WHERE filters on enriched rows
        filtered_enriched = enriched_rows
        if where_pattern:
            where_clause = where_pattern.group(1).strip()
            filtered_enriched = [
                row for row in enriched_rows
                if self._eval_where_condition_enriched(where_clause, row)
            ]
        
        # Parse RETURN clause
        aggregations = {}
        projections = {}
        
        if return_pattern:
            return_clause = return_pattern.group(1).strip()
            for item in [i.strip() for i in return_clause.split(",")]:
                if " AS " in item:
                    spec, alias = item.split(" AS ", 1)
                    spec = spec.strip()
                    alias = alias.strip()
                else:
                    spec = item
                    alias = item
                
                # Detect aggregations
                if any(agg in spec.upper() for agg in ["COUNT(", "AVG(", "SUM(", "MAX(", "MIN("]):
                    aggregations[alias] = spec
                else:
                    projections[alias] = spec
        
        # Apply GROUP BY if we have aggregations
        if aggregations:
            grouped = defaultdict(list)
            
            for row in filtered_enriched:
                # Create group key from projections
                group_key = []
                for alias, spec in projections.items():
                    val = self._extract_value(spec, row)
                    group_key.append((alias, val))
                
                grouped[tuple(group_key)].append(row)
            
            # Build result rows
            projected_results = []
            for group_key, group_rows in grouped.items():
                result_row = dict(group_key)
                
                # Apply aggregations
                for alias, agg_spec in aggregations.items():
                    agg_upper = agg_spec.upper()
                    
                    if "COUNT(*)" in agg_upper:
                        result_row[alias] = len(group_rows)
                    elif "COUNT(" in agg_upper:
                        field_match = re.search(r'COUNT\(([^)]+)\)', agg_spec, re.IGNORECASE)
                        if field_match:
                            field = field_match.group(1)
                            count = sum(1 for r in group_rows if self._extract_value(field, r) is not None)
                            result_row[alias] = count
                    elif "AVG(" in agg_upper:
                        field_match = re.search(r'AVG\(([^)]+)\)', agg_spec, re.IGNORECASE)
                        if field_match:
                            field = field_match.group(1)
                            values = [v for v in [self._extract_value(field, r) for r in group_rows] 
                                     if v is not None and isinstance(v, (int, float))]
                            result_row[alias] = round(sum(values) / len(values), 2) if values else 0
                
                projected_results.append(result_row)
        else:
            # No aggregations - just project fields
            projected_results = []
            for row in filtered_enriched:
                projected_row = {}
                for alias, spec in projections.items():
                    projected_row[alias] = self._extract_value(spec, row)
                projected_results.append(projected_row)
        
        # Apply ORDER BY
        if order_pattern and projected_results:
            order_clause = order_pattern.group(1).strip()
            
            # Handle multiple order columns
            for order_item in reversed(order_clause.split(",")):
                order_item = order_item.strip()
                reverse = False
                order_key = order_item
                
                if "DESC" in order_item.upper():
                    reverse = True
                    order_key = re.sub(r'\s+DESC', '', order_item, flags=re.IGNORECASE).strip()
                elif "ASC" in order_item.upper():
                    order_key = re.sub(r'\s+ASC', '', order_item, flags=re.IGNORECASE).strip()
                
                try:
                    projected_results.sort(
                        key=lambda x: (x.get(order_key) is None, x.get(order_key) or 0),
                        reverse=reverse
                    )
                except:
                    pass
        
        # Apply LIMIT
        if limit_pattern:
            limit = int(limit_pattern.group(1))
            projected_results = projected_results[:limit]
        
        return projected_results
    
    def _extract_value(self, spec: str, row: Dict):
        """Extract value from field spec like 'v.properties.name' or 'vt.name'."""
        import re
        spec = spec.strip()
        
        # Parse v.properties.fieldname or v.fieldname or vt.fieldname
        if "v.properties." in spec:
            # Extract: v.properties.NAME
            parts = spec.split("v.properties.")
            if len(parts) > 1:
                field = parts[1].strip()
                return row.get("v", {}).get(field)
        elif "v." in spec and "vt." not in spec:
            # Extract: v.NAME
            parts = spec.split("v.")
            if len(parts) > 1:
                field = parts[1].strip()
                return row.get("v", {}).get(field)
        elif "vt." in spec:
            # Extract: vt.NAME
            parts = spec.split("vt.")
            if len(parts) > 1:
                field = parts[1].strip()
                return row.get("vt", {}).get(field)
        
        return None
    
    def _eval_where_condition_enriched(self, where_clause: str, enriched_row: Dict) -> bool:
        """Evaluate WHERE clause on an enriched row with both v and vt properties."""
        import re
        
        # Handle multiple conditions with AND/OR
        conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            
            # Get the source (v. or vt.) and property name
            if "vt." in condition:
                # VesselType property
                props = enriched_row.get("vt", {})
                condition_to_eval = condition.replace("vt.", "")
            elif "v.properties." in condition:
                # Vessel property (long form)
                props = enriched_row.get("v", {})
                condition_to_eval = condition.replace("v.properties.", "")
            elif "v." in condition:
                # Vessel property (short form)
                props = enriched_row.get("v", {})
                condition_to_eval = condition.replace("v.", "")
            else:
                # No prefix, assume vessel  
                props = enriched_row.get("v", {})
                condition_to_eval = condition
            
            # Evaluate the condition on the extracted properties
            if not self._eval_where_condition(condition_to_eval, props):
                return False
        
        return True
    
    def _eval_where_condition(self, where_clause: str, props: Dict) -> bool:
        """Evaluate WHERE clause conditions."""
        import re
        
        # Handle multiple conditions with AND/OR
        conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            
            # IS NULL / IS NOT NULL
            if "IS NULL" in condition.upper():
                prop_name = condition.replace("IS NULL", "").replace("is null", "").strip()
                prop_name = prop_name.split(".")[-1]
                if props.get(prop_name) is not None:
                    return False
                continue
            
            if "IS NOT NULL" in condition.upper():
                prop_name = condition.replace("IS NOT NULL", "").replace("is not null", "").strip()
                prop_name = prop_name.split(".")[-1]
                if props.get(prop_name) is None:
                    return False
                continue
            
            # CONTAINS
            if "CONTAINS" in condition.upper():
                match = re.search(r"(\w+)\s+CONTAINS\s+'([^']+)'", condition, re.IGNORECASE)
                if match:
                    prop_name = match.group(1).split(".")[-1]
                    search_str = match.group(2)
                    val = props.get(prop_name)
                    if val is None or str(val).upper().find(search_str.upper()) == -1:
                        return False
                continue
            
            # Comparison operators: >, <, >=, <=, =, !=
            for op in [">=", "<=", ">", "<", "!=", "="]:
                if f" {op} " in condition:
                    parts = condition.split(f" {op} ", 1)
                    if len(parts) == 2:
                        prop_name = parts[0].strip().split(".")[-1]
                        compare_str = parts[1].strip()
                        prop_val = props.get(prop_name)
                        
                        # Try numeric comparison first
                        try:
                            compare_val = float(compare_str)
                            prop_num = float(prop_val or 0)
                            
                            if op == ">" and not (prop_num > compare_val):
                                return False
                            elif op == "<" and not (prop_num < compare_val):
                                return False
                            elif op == ">=" and not (prop_num >= compare_val):
                                return False
                            elif op == "<=" and not (prop_num <= compare_val):
                                return False
                            elif op == "=" and not (prop_num == compare_val):
                                return False
                            elif op == "!=" and not (prop_num != compare_val):
                                return False
                        except (ValueError, TypeError):
                            # String comparison
                            compare_val = compare_str.strip("'\"")
                            if op == "=" and str(prop_val) != compare_val:
                                return False
                            elif op == "!=" and str(prop_val) == compare_val:
                                return False
                    break
        
        return True

    def get_statistics(self) -> Dict[str, Any]:
        """Return graph statistics."""
        total_vessels = sum(
            1 for n in self.nodes.values() if "Vessel" in n["labels"]
        )
        invalid_vessels = sum(
            1
            for n in self.nodes.values()
            if "Vessel" in n["labels"]
            and n["properties"].get("validation_status") == "invalid"
        )
        valid_vessels = sum(
            1
            for n in self.nodes.values()
            if "Vessel" in n["labels"]
            and n["properties"].get("validation_status") == "valid"
        )

        type_dist = {}
        for node in self.nodes.values():
            if "Vessel" in node["labels"]:
                vtype = node["properties"].get("vessel_type", "Unknown")
                type_dist[vtype] = type_dist.get(vtype, 0) + 1

        return {
            "total_vessels": total_vessels,
            "valid_vessels": valid_vessels,
            "invalid_vessels": invalid_vessels,
            "total_relationships": len(self.relationships),
            "total_nodes": len(self.nodes),
            "type_distribution": type_dist,
        }





class Neo4jClient:
    """
    Client for interacting with a local Neo4j graph database.
    
    Environment variables:
        NEO4J_URI       – bolt://localhost:7687
        NEO4J_USER      – neo4j
        NEO4J_PASSWORD  – password
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        if not NEO4J_AVAILABLE:
            raise ImportError(
                "neo4j driver not installed. Run: pip install neo4j"
            )

        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password")
        self._driver: Optional[Driver] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self):
        """Establish connection to Neo4j."""
        self._driver = GraphDatabase.driver(
            self.uri, auth=(self.user, self.password)
        )
        # Verify connectivity
        self._driver.verify_connectivity()

    def close(self):
        if self._driver:
            self._driver.close()

    @property
    def driver(self) -> Driver:
        if self._driver is None:
            self.connect()
        return self._driver

    # ------------------------------------------------------------------
    # Schema / Ontology bootstrap
    # ------------------------------------------------------------------

    def create_constraints(self):
        """Create uniqueness constraints and indexes."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Vessel) REQUIRE v.vessel_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:MMSI) REQUIRE m.value IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:IMO) REQUIRE i.value IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (f:Flag) REQUIRE f.code IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Port) REQUIRE p.unlocode IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (b:ShipBuilder) REQUIRE b.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (vt:VesselType) REQUIRE vt.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (vc:VesselCategory) REQUIRE vc.name IS UNIQUE",
        ]
        with self.driver.session() as session:
            for cypher in constraints:
                try:
                    session.run(cypher)
                except Exception:
                    pass  # constraint may already exist

    def seed_ontology(self):
        """Create the vessel-type ontology nodes and relationships."""
        with self.driver.session() as session:
            for category, types in VESSEL_TYPE_HIERARCHY.items():
                session.run(
                    "MERGE (vc:VesselCategory {name: $cat})",
                    cat=category,
                )
                for vtype in types:
                    session.run(
                        """
                        MERGE (vt:VesselType {name: $vtype})
                        WITH vt
                        MATCH (vc:VesselCategory {name: $cat})
                        MERGE (vt)-[:BELONGS_TO_CATEGORY]->(vc)
                        """,
                        vtype=vtype,
                        cat=category,
                    )

    def initialize(self):
        """Full initialization: connect, create constraints, seed ontology."""
        self.connect()
        self.create_constraints()
        self.seed_ontology()

    # ------------------------------------------------------------------
    # Clear database
    # ------------------------------------------------------------------

    def clear_all(self):
        """Delete everything in the database. USE WITH CAUTION."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    # ------------------------------------------------------------------
    # Vessel ingestion (from validated records)
    # ------------------------------------------------------------------

    def ingest_vessel(
        self,
        record: Dict[str, Any],
        validation_status: str = "valid",
        validation_errors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Ingest a single vessel record into Neo4j.

        Parameters
        ----------
        record : dict
            A row from the CSV dataset (as a dict).
        validation_status : str
            "valid" | "invalid" | "warning"
        validation_errors : list
            List of error messages from the validation pipeline.

        Returns
        -------
        dict with created node/relationship counts.
        """
        imo = record.get("imo")
        mmsi = record.get("mmsi")
        name = record.get("name") or ""
        
        # Handle vessel_type: convert NaN/None to empty string (treated as missing value)
        vessel_type = record.get("vessel_type")
        if vessel_type is None or (isinstance(vessel_type, float) and str(vessel_type).lower() == 'nan'):
            vessel_type = ""
        else:
            vessel_type = str(vessel_type).strip() if vessel_type else ""
        
        category = get_category_for_type(vessel_type)
        flag = record.get("flag")
        builder = record.get("shipBuilder")
        port_name = record.get("matchedPort_name")
        port_unlocode = record.get("matchedPort_unlocode")

        # Build a deterministic vessel_id
        if imo and int(imo) > 0:
            vessel_id = f"vessel_imo_{int(imo)}"
        elif mmsi:
            vessel_id = f"vessel_mmsi_{int(mmsi)}"
        else:
            return {"status": "skipped", "reason": "no identifier"}

        stats = {"nodes": 0, "relationships": 0}
        errors_json = json.dumps(validation_errors or [])

        with self.driver.session() as session:
            # ---- Vessel node ----
            session.run(
                """
                MERGE (v:Vessel {vessel_id: $vid})
                SET v.imo = $imo,
                    v.mmsi = $mmsi,
                    v.name = $name,
                    v.vessel_type = $vtype,
                    v.category = $cat,
                    v.length = $length,
                    v.width = $width,
                    v.gross_tonnage = $gt,
                    v.built_year = $by,
                    v.draught = $dr,
                    v.deadweight = $dw,
                    v.callsign = $cs,
                    v.flag = $flag,
                    v.validation_status = $vs,
                    v.validation_errors = $verr,
                    v.last_lat = $lat,
                    v.last_lon = $lon,
                    v.last_speed = $spd,
                    v.destination = $dest,
                    v.updated_at = datetime()
                """,
                vid=vessel_id,
                imo=int(imo) if imo else None,
                mmsi=int(mmsi) if mmsi else None,
                name=name,
                vtype=vessel_type,
                cat=category,
                length=record.get("length"),
                width=record.get("width"),
                gt=record.get("grossTonnage"),
                by=record.get("builtYear"),
                dr=record.get("draught"),
                dw=record.get("deadweight"),
                cs=record.get("callsign"),
                flag=flag,
                vs=validation_status,
                verr=errors_json,
                lat=record.get("last_position_latitude"),
                lon=record.get("last_position_longitude"),
                spd=record.get("last_position_speed"),
                dest=record.get("destination"),
            )
            stats["nodes"] += 1

            # ---- IMO node ----
            if imo and int(imo) > 0:
                session.run(
                    """
                    MERGE (i:IMO {value: $imo})
                    WITH i
                    MATCH (v:Vessel {vessel_id: $vid})
                    MERGE (v)-[:HAS_IMO]->(i)
                    """,
                    imo=int(imo),
                    vid=vessel_id,
                )
                stats["nodes"] += 1
                stats["relationships"] += 1

            # ---- MMSI node ----
            if mmsi:
                session.run(
                    """
                    MERGE (m:MMSI {value: $mmsi})
                    WITH m
                    MATCH (v:Vessel {vessel_id: $vid})
                    MERGE (v)-[:USES_MMSI]->(m)
                    """,
                    mmsi=int(mmsi),
                    vid=vessel_id,
                )
                stats["nodes"] += 1
                stats["relationships"] += 1

            # ---- Vessel Type + Category (only if vessel_type is not empty) ----
            if vessel_type:
                session.run(
                    """
                    MERGE (vt:VesselType {name: $vtype})
                    WITH vt
                    MATCH (v:Vessel {vessel_id: $vid})
                    MERGE (v)-[:IS_TYPE]->(vt)
                    """,
                    vtype=vessel_type,
                    vid=vessel_id,
                )
                # Ensure category link exists
                if category:
                    session.run(
                        """
                        MERGE (vc:VesselCategory {name: $cat})
                        WITH vc
                        MATCH (vt:VesselType {name: $vtype})
                        MERGE (vt)-[:BELONGS_TO_CATEGORY]->(vc)
                        """,
                        cat=category,
                        vtype=vessel_type,
                    )
                stats["relationships"] += 1

            # ---- Flag ----
            if flag:
                session.run(
                    """
                    MERGE (f:Flag {code: $flag})
                    WITH f
                    MATCH (v:Vessel {vessel_id: $vid})
                    MERGE (v)-[:REGISTERED_UNDER]->(f)
                    """,
                    flag=flag,
                    vid=vessel_id,
                )
                stats["nodes"] += 1
                stats["relationships"] += 1

            # ---- Ship Builder ----
            if builder:
                session.run(
                    """
                    MERGE (b:ShipBuilder {name: $builder})
                    WITH b
                    MATCH (v:Vessel {vessel_id: $vid})
                    MERGE (v)-[:BUILT_BY]->(b)
                    """,
                    builder=builder,
                    vid=vessel_id,
                )
                stats["nodes"] += 1
                stats["relationships"] += 1

            # ---- Port ----
            if port_name and port_unlocode:
                session.run(
                    """
                    MERGE (p:Port {unlocode: $unlo})
                    SET p.name = $pname,
                        p.latitude = $plat,
                        p.longitude = $plon
                    WITH p
                    MATCH (v:Vessel {vessel_id: $vid})
                    MERGE (v)-[:VISITED]->(p)
                    """,
                    unlo=port_unlocode,
                    pname=port_name,
                    plat=record.get("matchedPort_latitude"),
                    plon=record.get("matchedPort_longitude"),
                    vid=vessel_id,
                )
                stats["nodes"] += 1
                stats["relationships"] += 1

        return stats

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_graph_data(
        self,
        category: Optional[str] = None,
        vessel_type: Optional[str] = None,
        flag: Optional[str] = None,
        validation_status: Optional[str] = None,
        vessel_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return nodes + relationships for frontend visualization.

        Supports hierarchical filtering:
            category -> vessel_type -> flag -> validation_status -> vessel_name
        """
        where_clauses = []
        params: Dict[str, Any] = {}

        if category:
            where_clauses.append("v.category = $category")
            params["category"] = category
        if vessel_type:
            where_clauses.append("v.vessel_type = $vessel_type")
            params["vessel_type"] = vessel_type
        if flag:
            where_clauses.append("v.flag = $flag")
            params["flag"] = flag
        if validation_status:
            where_clauses.append("v.validation_status = $validation_status")
            params["validation_status"] = validation_status
        if vessel_name:
            where_clauses.append("v.name = $vessel_name")
            params["vessel_name"] = vessel_name

        where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Fetch vessels and their 1-hop neighbourhood
        cypher = f"""
        MATCH (v:Vessel)
        {where_str}
        OPTIONAL MATCH (v)-[r]->(target)
        OPTIONAL MATCH (v)-[:IS_TYPE]->(vt:VesselType)-[:BELONGS_TO_CATEGORY]->(vc:VesselCategory)
        RETURN v, r, target, vt, vc
        LIMIT 2000
        """
        nodes = {}
        relationships = []

        with self.driver.session() as session:
            result = session.run(cypher, **params)
            for record in result:
                v = record["v"]
                if v:
                    nid = v.element_id
                    if nid not in nodes:
                        props = sanitize_properties(dict(v))
                        # Determine color based on validation
                        color = "#e74c3c" if props.get("validation_status") == "invalid" else "#2ecc71"
                        nodes[nid] = {
                            "id": nid,
                            "labels": list(v.labels),
                            "properties": props,
                            "color": color,
                        }

                target = record["target"]
                if target:
                    tid = target.element_id
                    if tid not in nodes:
                        props = sanitize_properties(dict(target))
                        nodes[tid] = {
                            "id": tid,
                            "labels": list(target.labels),
                            "properties": props,
                            "color": "#3498db",
                        }

                r = record["r"]
                if r:
                    relationships.append({
                        "id": r.element_id,
                        "type": r.type,
                        "startNode": r.start_node.element_id,
                        "endNode": r.end_node.element_id,
                        "properties": sanitize_properties(dict(r)),
                    })

                # Category / type nodes
                for extra in [record.get("vt"), record.get("vc")]:
                    if extra:
                        eid = extra.element_id
                        if eid not in nodes:
                            nodes[eid] = {
                                "id": eid,
                                "labels": list(extra.labels),
                                "properties": sanitize_properties(dict(extra)),
                                "color": "#9b59b6",
                            }

        return {
            "nodes": list(nodes.values()),
            "relationships": relationships,
        }

    def get_ontology_tree(self) -> List[Dict]:
        """Return the ontology hierarchy with vessel counts."""
        cypher = """
        MATCH (vc:VesselCategory)<-[:BELONGS_TO_CATEGORY]-(vt:VesselType)
        OPTIONAL MATCH (v:Vessel)-[:IS_TYPE]->(vt)
        RETURN vc.name AS category, vt.name AS type, count(v) AS vessel_count
        ORDER BY category, type
        """
        tree: Dict[str, Dict] = {}
        with self.driver.session() as session:
            result = session.run(cypher)
            for rec in result:
                cat = rec["category"]
                if cat not in tree:
                    tree[cat] = {"name": cat, "types": [], "total": 0}
                tree[cat]["types"].append({
                    "name": rec["type"],
                    "count": rec["vessel_count"],
                })
                tree[cat]["total"] += rec["vessel_count"]

        return list(tree.values())

    def get_filter_options(self) -> Dict[str, List[str]]:
        """Return available filter values for the frontend dropdowns."""
        options: Dict[str, Any] = {}
        with self.driver.session() as session:
            # Categories
            res = session.run("MATCH (vc:VesselCategory) RETURN vc.name AS name ORDER BY name")
            options["categories"] = [r["name"] for r in res]

            # Vessel Types
            res = session.run("MATCH (vt:VesselType) RETURN vt.name AS name ORDER BY name")
            options["vessel_types"] = [r["name"] for r in res]

            # Flags
            res = session.run("MATCH (f:Flag) RETURN f.code AS code ORDER BY code")
            options["flags"] = [r["code"] for r in res]

            # Validation statuses
            res = session.run(
                "MATCH (v:Vessel) RETURN DISTINCT v.validation_status AS status"
            )
            options["validation_statuses"] = [r["status"] for r in res if r["status"]]

            # Vessel names (sorted alphabetically)
            res = session.run("MATCH (v:Vessel) RETURN DISTINCT v.name AS name ORDER BY name")
            options["vessel_names"] = [r["name"] for r in res if r["name"]]
            
            # Get total vessel count
            counts_res = session.run("MATCH (v:Vessel) RETURN count(DISTINCT v.name) AS cnt").single()
            options["vessel_count"] = counts_res["cnt"] if counts_res else 0

        return options

    def get_filtered_options(
        self,
        category: Optional[str] = None,
        vessel_type: Optional[str] = None,
        vessel_name: Optional[str] = None,
        flag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return filtered options based on current filter selections.
        This enables cascading dropdowns where selecting a category shows only
        vessel types available for that category, etc.
        """
        options: Dict[str, Any] = {}
        
        with self.driver.session() as session:
            # Build WHERE clause based on provided filters
            where_clauses = []
            params = {}
            
            if category:
                where_clauses.append("(v)-[:BELONGS_TO_CATEGORY]->(vc:VesselCategory {name: $category})")
                params["category"] = category
            if vessel_type:
                where_clauses.append("(v)-[:IS_TYPE]->(vt:VesselType {name: $vessel_type})")
                params["vessel_type"] = vessel_type
            if vessel_name:
                where_clauses.append("v.name = $vessel_name")
                params["vessel_name"] = vessel_name
            if flag:
                where_clauses.append("(v)-[:FLAGGED_AS]->(f:Flag {code: $flag})")
                params["flag"] = flag
            
            where_clause = " AND ".join(where_clauses) if where_clauses else ""
            where_prefix = "WHERE " + where_clause if where_clause else ""
            
            # Filtered Vessel Types
            cypher = f"MATCH (v:Vessel) {where_prefix} OPTIONAL MATCH (v)-[:IS_TYPE]->(vt:VesselType) RETURN DISTINCT vt.name AS name WHERE name IS NOT NULL ORDER BY name"
            res = session.run(cypher, **params)
            options["vessel_types"] = [r["name"] for r in res]
            
            # Filtered Vessel Names
            cypher = f"MATCH (v:Vessel) {where_prefix} RETURN DISTINCT v.name AS name WHERE name IS NOT NULL ORDER BY name"
            res = session.run(cypher, **params)
            options["vessel_names"] = [r["name"] for r in res]
            
            # Filtered Flags
            cypher = f"MATCH (v:Vessel) {where_prefix} OPTIONAL MATCH (v)-[:FLAGGED_AS]->(f:Flag) RETURN DISTINCT f.code AS code WHERE code IS NOT NULL ORDER BY code"
            res = session.run(cypher, **params)
            options["flags"] = [r["code"] for r in res]
            
            # Filtered Validation Statuses
            cypher = f"MATCH (v:Vessel) {where_prefix} RETURN DISTINCT v.validation_status AS status WHERE status IS NOT NULL ORDER BY status"
            res = session.run(cypher, **params)
            options["validation_statuses"] = [r["status"] for r in res]
        
        print(f"[Neo4jClient.get_filtered_options] Filters: category={category}, vessel_type={vessel_type}, vessel_name={vessel_name}, flag={flag}")
        print(f"[Neo4jClient.get_filtered_options] Returning {len(options.get('vessel_types', []))} types, {len(options.get('vessel_names', []))} names, {len(options.get('flags', []))} flags, {len(options.get('validation_statuses', []))} statuses")
        
        return options

    def run_cypher(self, cypher: str, params: Optional[Dict] = None) -> List[Dict]:
        """Run an arbitrary Cypher query and return results as list of dicts."""
        with self.driver.session() as session:
            result = session.run(cypher, **(params or {}))
            return [dict(record) for record in result]

    def get_statistics(self) -> Dict[str, Any]:
        """Return graph-level statistics."""
        with self.driver.session() as session:
            stats: Dict[str, Any] = {}

            r = session.run("MATCH (v:Vessel) RETURN count(v) AS c").single()
            stats["total_vessels"] = r["c"] if r else 0

            r = session.run(
                "MATCH (v:Vessel) WHERE v.validation_status = 'invalid' RETURN count(v) AS c"
            ).single()
            stats["invalid_vessels"] = r["c"] if r else 0

            r = session.run(
                "MATCH (v:Vessel) WHERE v.validation_status = 'valid' RETURN count(v) AS c"
            ).single()
            stats["valid_vessels"] = r["c"] if r else 0

            r = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()
            stats["total_relationships"] = r["c"] if r else 0

            r = session.run("MATCH (n) RETURN count(n) AS c").single()
            stats["total_nodes"] = r["c"] if r else 0

            # Vessel type distribution
            res = session.run(
                """
                MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType)
                RETURN vt.name AS type, count(v) AS count
                ORDER BY count DESC
                """
            )
            stats["type_distribution"] = {r["type"]: r["count"] for r in res}

            return stats
