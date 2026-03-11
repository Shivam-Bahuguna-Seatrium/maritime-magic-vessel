"""
Maritime Vessel Ontology
========================

Defines the hierarchical vessel classification ontology:

    VESSELS (Center Hub)
            |
    --------+--------+--------+--------+
    |       |        |        |        |
  Cargo  Tanker  Container  Service  Other
  /|\\    /|\\     /|\\      /|\\
  V V V   V V V   V V V    V V V
  | | |   | | |   | | |    | | |
 IMO MMSI Flag  ... properties

Each vessel type belongs to a category. Vessels are classified by type
and linked to their identifiers and attributes.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field


# =============================================================================
# VESSEL TYPE ONTOLOGY
# =============================================================================

VESSEL_TYPE_HIERARCHY: Dict[str, List[str]] = {
    "Cargo Vessels": [
        "General Cargo",
        "Dry Bulk",
        "Ro-Ro/Vehicle Carrier",
        "Reefer",
        "Miscellaneous Cargo",
        "Multi Purpose Carrier",
        "Log Carrier",
        "Aggregates Carrier",
        "Cement Carrier",
        "Heavy Lift",
        "Tween Decker",
        "Self Discharging",
        "Deck Cargo",
        "Limestone Carrier",
    ],
    "Tanker Vessels": [
        "Crude Tanker",
        "Chemical Tanker",
        "Product Tanker",
        "General Tanker",
        "Bitumen Carrier",
        "Veg Oil Carrier",
        "Water Tanker",
        "Bunkering",
    ],
    "Gas Carriers": [
        "LNG Carrier",
        "LPG Carrier",
    ],
    "Container Vessels": [
        "Container",
    ],
    "Passenger Vessels": [
        "Passenger Ship",
        "Ro-Ro/Passenger",
        "High Speed Craft",
    ],
    "Service Vessels": [
        "Tug",
        "Tug/Supply Vessel",
        "Pusher Tug",
        "Support Vessel",
        "Supply Vessel",
        "Port Tender",
        "Pilot Vessel",
        "Dredger",
        "Icebreaker",
        "Anchor Hoy",
        "Utility Vessel",
        "Research Vessel",
        "Salvage Vessel",
        "Dive Vessel",
        "Drilling Vessel",
        "Work/Repair Vessel",
        "Anti Pollution",
        "Search And Rescue",
        "Law Enforcement",
        "Patrol Vessel",
        "Medical",
        "Power Station Vessel",
    ],
    "Fishing Vessels": [
        "Fishing Vessel",
        "Fish Carrier",
    ],
    "Pleasure Craft": [
        "Other Pleasure Craft",
        "Sailing",
        "Yacht",
    ],
    "Naval Vessels": [
        "Naval Vessel",
    ],
    "Offshore Vessels": [
        "Offshore Processing Vessel",
    ],
    "Other": [
        "Unspecified",
        "Other Special Craft",
    ],
}

# Reverse lookup: vessel_type -> category
VESSEL_TYPE_TO_CATEGORY: Dict[str, str] = {}
for category, types in VESSEL_TYPE_HIERARCHY.items():
    for vtype in types:
        VESSEL_TYPE_TO_CATEGORY[vtype] = category


def get_category_for_type(vessel_type) -> str:
    """Return the parent category for a given vessel type string.
    
    Gracefully handles None, NaN, and non-string types.
    Returns empty string for None/NaN, "Other" for unknown types.
    """
    # Check for None first
    if vessel_type is None:
        return ""
    
    # Handle NaN, floats, and other non-string types
    try:
        vessel_type = str(vessel_type).strip()
    except Exception:
        return ""
    
    # Check for empty or NaN values - return empty string, not "Other"
    if not vessel_type or vessel_type.lower() == 'nan':
        return ""
    
    # Exact match first
    if vessel_type in VESSEL_TYPE_TO_CATEGORY:
        return VESSEL_TYPE_TO_CATEGORY[vessel_type]
    
    # Fuzzy / substring match
    vt_lower = vessel_type.lower()
    for known_type, cat in VESSEL_TYPE_TO_CATEGORY.items():
        if known_type.lower() in vt_lower or vt_lower in known_type.lower():
            return cat
    
    return "Other"


def get_all_categories() -> List[str]:
    return list(VESSEL_TYPE_HIERARCHY.keys())


def get_types_for_category(category: str) -> List[str]:
    return VESSEL_TYPE_HIERARCHY.get(category, [])


@dataclass
class OntologyNode:
    """Represents a node in the ontology tree."""
    name: str
    level: str  # "root" | "category" | "type"
    children: List["OntologyNode"] = field(default_factory=list)
    vessel_count: int = 0

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "level": self.level,
            "vessel_count": self.vessel_count,
            "children": [c.to_dict() for c in self.children],
        }


def build_ontology_tree() -> OntologyNode:
    """Build the full ontology tree for visualization."""
    root = OntologyNode(name="VESSELS", level="root")
    for category, types in VESSEL_TYPE_HIERARCHY.items():
        cat_node = OntologyNode(name=category, level="category")
        for vtype in types:
            cat_node.children.append(OntologyNode(name=vtype, level="type"))
        root.children.append(cat_node)
    return root
