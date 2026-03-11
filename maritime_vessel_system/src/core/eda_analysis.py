"""
Stage 1: Maritime Vessel Data - Exploratory Data Analysis (EDA)
================================================================

This module performs comprehensive exploratory data analysis on the maritime
vessel dataset containing AIS and vessel registry information.

Key Analysis Areas:
- Dataset schema and column types
- Missing value analysis
- Duplicate record detection
- Identifier inconsistencies (IMO/MMSI conflicts)
- Vessel name variations
- Flag/ownership changes over time
"""

import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Any
import json


def load_vessel_data(filepath: str) -> pd.DataFrame:
    """Load the vessel dataset from CSV."""
    df = pd.read_csv(filepath, low_memory=False)
    print(f"✅ Loaded {len(df)} records with {len(df.columns)} columns")
    return df


def analyze_schema(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze dataset schema and column types."""
    schema_analysis = {
        "total_records": len(df),
        "total_columns": len(df.columns),
        "columns": {}
    }
    
    # Categorize columns by data domain
    identifier_cols = ['imo', 'mmsi', 'callsign']
    static_vessel_cols = ['name', 'length', 'width', 'vessel_type', 'flag', 
                          'deadweight', 'grossTonnage', 'builtYear', 'netTonnage',
                          'draught', 'lengthOverall', 'airDraught', 'depth',
                          'beamMoulded', 'berthCount', 'deadYear']
    builder_cols = ['shipBuilder', 'hullNumber', 'launchYear']
    engine_cols = ['mainEngineCount', 'mainEngineDesigner', 'propulsionType',
                   'engineDesignation', 'propellerCount', 'propellerType']
    ais_position_cols = ['last_position_accuracy', 'last_position_course',
                         'last_position_heading', 'last_position_latitude',
                         'last_position_longitude', 'last_position_maneuver',
                         'last_position_rot', 'last_position_speed',
                         'last_position_updateTimestamp']
    voyage_cols = ['destination', 'draught', 'eta', 'matchedPort_latitude',
                   'matchedPort_longitude', 'matchedPort_name', 'matchedPort_unlocode']
    timestamp_cols = ['staticData_updateTimestamp', 'InsertDate', 'UpdateDate']
    
    for col in df.columns:
        col_info = {
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].count()),
            "null_count": int(df[col].isna().sum()),
            "null_percentage": round(df[col].isna().sum() / len(df) * 100, 2),
            "unique_values": int(df[col].nunique())
        }
        
        # Determine column category
        if col in identifier_cols:
            col_info["category"] = "identifier"
        elif col in static_vessel_cols:
            col_info["category"] = "static_vessel_data"
        elif col in builder_cols:
            col_info["category"] = "builder_info"
        elif col in engine_cols:
            col_info["category"] = "engine_propulsion"
        elif col in ais_position_cols:
            col_info["category"] = "ais_position"
        elif col in voyage_cols:
            col_info["category"] = "voyage_data"
        elif col in timestamp_cols:
            col_info["category"] = "timestamp"
        else:
            col_info["category"] = "other"
            
        schema_analysis["columns"][col] = col_info
    
    return schema_analysis


def analyze_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze missing values across all columns."""
    missing_analysis = []
    
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / len(df)) * 100
        
        missing_analysis.append({
            "column": col,
            "missing_count": missing_count,
            "missing_percentage": round(missing_pct, 2),
            "present_count": len(df) - missing_count,
            "data_type": str(df[col].dtype)
        })
    
    result_df = pd.DataFrame(missing_analysis)
    result_df = result_df.sort_values("missing_percentage", ascending=False)
    return result_df


def detect_duplicate_records(df: pd.DataFrame) -> Dict[str, Any]:
    """Detect duplicate records based on various criteria."""
    duplicates = {
        "exact_duplicates": 0,
        "imo_duplicates": {},
        "mmsi_duplicates": {},
        "name_duplicates": {},
        "composite_key_duplicates": []
    }
    
    # Exact duplicates (all columns)
    duplicates["exact_duplicates"] = int(df.duplicated().sum())
    
    # IMO duplicates (same IMO, different records)
    valid_imo_df = df[df['imo'].notna() & (df['imo'] > 0)]
    imo_counts = valid_imo_df['imo'].value_counts()
    duplicate_imos = imo_counts[imo_counts > 1]
    duplicates["imo_duplicates"] = {
        "count": len(duplicate_imos),
        "total_affected_records": int(duplicate_imos.sum()),
        "top_duplicates": duplicate_imos.head(10).to_dict()
    }
    
    # MMSI duplicates
    valid_mmsi_df = df[df['mmsi'].notna()]
    mmsi_counts = valid_mmsi_df['mmsi'].value_counts()
    duplicate_mmsis = mmsi_counts[mmsi_counts > 1]
    duplicates["mmsi_duplicates"] = {
        "count": len(duplicate_mmsis),
        "total_affected_records": int(duplicate_mmsis.sum()),
        "top_duplicates": duplicate_mmsis.head(10).to_dict()
    }
    
    # Name duplicates (after normalization)
    df_temp = df.copy()
    df_temp['normalized_name'] = df_temp['name'].fillna('').str.upper().str.strip()
    name_counts = df_temp[df_temp['normalized_name'] != '']['normalized_name'].value_counts()
    duplicate_names = name_counts[name_counts > 1]
    duplicates["name_duplicates"] = {
        "count": len(duplicate_names),
        "total_affected_records": int(duplicate_names.sum()),
        "top_duplicates": duplicate_names.head(10).to_dict()
    }
    
    return duplicates


def detect_identifier_conflicts(df: pd.DataFrame) -> Dict[str, Any]:
    """Detect conflicts between identifiers (IMO/MMSI)."""
    conflicts = {
        "single_imo_multiple_mmsi": [],
        "single_mmsi_multiple_imo": [],
        "invalid_imo_records": 0,
        "invalid_mmsi_records": 0,
        "zero_imo_records": 0
    }
    
    # Filter valid records
    valid_df = df[(df['imo'].notna()) & (df['mmsi'].notna())]
    
    # Single IMO with multiple MMSI values
    imo_mmsi_groups = valid_df[valid_df['imo'] > 0].groupby('imo')['mmsi'].nunique()
    multiple_mmsi_imo = imo_mmsi_groups[imo_mmsi_groups > 1]
    
    for imo in multiple_mmsi_imo.index[:20]:  # Top 20
        imo_records = valid_df[valid_df['imo'] == imo]
        conflicts["single_imo_multiple_mmsi"].append({
            "imo": int(imo),
            "mmsi_values": imo_records['mmsi'].unique().tolist(),
            "vessel_names": imo_records['name'].unique().tolist(),
            "record_count": len(imo_records)
        })
    
    # Single MMSI with multiple IMO values
    mmsi_imo_groups = valid_df[valid_df['imo'] > 0].groupby('mmsi')['imo'].nunique()
    multiple_imo_mmsi = mmsi_imo_groups[mmsi_imo_groups > 1]
    
    for mmsi in multiple_imo_mmsi.index[:20]:  # Top 20
        mmsi_records = valid_df[valid_df['mmsi'] == mmsi]
        conflicts["single_mmsi_multiple_imo"].append({
            "mmsi": int(mmsi),
            "imo_values": mmsi_records['imo'].unique().tolist(),
            "vessel_names": mmsi_records['name'].unique().tolist(),
            "record_count": len(mmsi_records)
        })
    
    # Invalid/Zero IMO records
    conflicts["zero_imo_records"] = int((df['imo'] == 0).sum())
    conflicts["invalid_imo_records"] = int(df['imo'].isna().sum())
    conflicts["invalid_mmsi_records"] = int(df['mmsi'].isna().sum())
    
    return conflicts


def analyze_vessel_name_variations(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze vessel name variations and patterns."""
    name_analysis = {
        "total_unique_names": 0,
        "names_with_special_chars": 0,
        "garbled_names": [],
        "name_changes_by_imo": [],
        "similar_name_clusters": []
    }
    
    # Unique names
    valid_names = df['name'].dropna()
    name_analysis["total_unique_names"] = int(valid_names.nunique())
    
    # Names with special characters / potential data quality issues
    special_char_pattern = r'[^\w\s\-\.\']'
    garbled_pattern = r'^[\W\d\s]+$|[^\x00-\x7F]|[<>{}|\\`]'
    
    for name in valid_names.unique():
        if pd.notna(name):
            import re
            if re.search(garbled_pattern, str(name)):
                name_analysis["garbled_names"].append(str(name))
    
    name_analysis["names_with_special_chars"] = len(name_analysis["garbled_names"])
    name_analysis["garbled_names"] = name_analysis["garbled_names"][:20]  # Limit output
    
    # Name changes by IMO
    valid_imo_df = df[(df['imo'].notna()) & (df['imo'] > 0) & (df['name'].notna())]
    imo_names = valid_imo_df.groupby('imo')['name'].nunique()
    multi_name_imos = imo_names[imo_names > 1]
    
    for imo in multi_name_imos.index[:20]:
        imo_records = valid_imo_df[valid_imo_df['imo'] == imo]
        name_analysis["name_changes_by_imo"].append({
            "imo": int(imo),
            "names": imo_records['name'].unique().tolist(),
            "name_count": int(multi_name_imos[imo])
        })
    
    return name_analysis


def analyze_flag_ownership_changes(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze flag and ownership changes over time."""
    flag_analysis = {
        "total_unique_flags": 0,
        "flag_distribution": {},
        "vessels_with_flag_changes": [],
        "flag_changes_count": 0
    }
    
    # Flag distribution
    flag_counts = df['flag'].value_counts()
    flag_analysis["total_unique_flags"] = int(df['flag'].nunique())
    flag_analysis["flag_distribution"] = flag_counts.head(20).to_dict()
    
    # Vessels with flag changes (same IMO, different flags)
    valid_df = df[(df['imo'].notna()) & (df['imo'] > 0) & (df['flag'].notna())]
    imo_flags = valid_df.groupby('imo')['flag'].nunique()
    multi_flag_imos = imo_flags[imo_flags > 1]
    flag_analysis["flag_changes_count"] = len(multi_flag_imos)
    
    for imo in multi_flag_imos.index[:20]:
        imo_records = valid_df[valid_df['imo'] == imo]
        flag_analysis["vessels_with_flag_changes"].append({
            "imo": int(imo),
            "vessel_name": imo_records['name'].iloc[0] if len(imo_records) > 0 else None,
            "flags": imo_records['flag'].unique().tolist(),
            "flag_count": int(multi_flag_imos[imo])
        })
    
    return flag_analysis


def analyze_vessel_types(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze vessel type distribution and inconsistencies."""
    type_analysis = {
        "total_vessel_types": 0,
        "type_distribution": {},
        "vessels_with_type_changes": []
    }
    
    type_counts = df['vessel_type'].value_counts()
    type_analysis["total_vessel_types"] = int(df['vessel_type'].nunique())
    type_analysis["type_distribution"] = type_counts.to_dict()
    
    # Vessels with type changes
    valid_df = df[(df['imo'].notna()) & (df['imo'] > 0) & (df['vessel_type'].notna())]
    imo_types = valid_df.groupby('imo')['vessel_type'].nunique()
    multi_type_imos = imo_types[imo_types > 1]
    
    for imo in multi_type_imos.index[:10]:
        imo_records = valid_df[valid_df['imo'] == imo]
        type_analysis["vessels_with_type_changes"].append({
            "imo": int(imo),
            "vessel_name": imo_records['name'].iloc[0],
            "vessel_types": imo_records['vessel_type'].unique().tolist()
        })
    
    return type_analysis


def analyze_dimensions_consistency(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze dimension consistency across records."""
    dim_analysis = {
        "dimension_variations": [],
        "outliers": {
            "extreme_lengths": [],
            "extreme_widths": []
        }
    }
    
    # Check dimension consistency by IMO
    valid_df = df[(df['imo'].notna()) & (df['imo'] > 0)]
    
    for col in ['length', 'width', 'grossTonnage']:
        if col in df.columns:
            col_variations = valid_df.groupby('imo')[col].nunique()
            varying_imos = col_variations[col_variations > 1]
            
            if len(varying_imos) > 0:
                dim_analysis["dimension_variations"].append({
                    "attribute": col,
                    "imo_count_with_variations": len(varying_imos),
                    "examples": []
                })
                
                for imo in varying_imos.index[:5]:
                    imo_records = valid_df[valid_df['imo'] == imo]
                    dim_analysis["dimension_variations"][-1]["examples"].append({
                        "imo": int(imo),
                        "values": imo_records[col].unique().tolist()
                    })
    
    # Detect outliers
    if 'length' in df.columns:
        valid_lengths = df[df['length'].notna()]['length']
        q99 = valid_lengths.quantile(0.99)
        extreme_lengths = df[df['length'] > q99][['imo', 'name', 'length']].head(10)
        dim_analysis["outliers"]["extreme_lengths"] = extreme_lengths.to_dict('records')
    
    return dim_analysis


def analyze_ais_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze AIS position data quality."""
    ais_analysis = {
        "position_coverage": 0,
        "invalid_coordinates": 0,
        "future_timestamps": 0,
        "stale_positions": 0
    }
    
    # Position coverage
    has_lat = df['last_position_latitude'].notna()
    has_lon = df['last_position_longitude'].notna()
    ais_analysis["position_coverage"] = round((has_lat & has_lon).sum() / len(df) * 100, 2)
    
    # Invalid coordinates
    invalid_lat = (df['last_position_latitude'] < -90) | (df['last_position_latitude'] > 90)
    invalid_lon = (df['last_position_longitude'] < -180) | (df['last_position_longitude'] > 180)
    ais_analysis["invalid_coordinates"] = int((invalid_lat | invalid_lon).sum())
    
    return ais_analysis


def generate_eda_report(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate comprehensive EDA report."""
    print("\n" + "="*80)
    print("MARITIME VESSEL DATA - EXPLORATORY DATA ANALYSIS REPORT")
    print("="*80 + "\n")
    
    report = {}
    
    # 1. Schema Analysis
    print("📊 Analyzing dataset schema...")
    report["schema"] = analyze_schema(df)
    print(f"   Total records: {report['schema']['total_records']:,}")
    print(f"   Total columns: {report['schema']['total_columns']}")
    
    # 2. Missing Values
    print("\n📉 Analyzing missing values...")
    missing_df = analyze_missing_values(df)
    report["missing_values"] = missing_df.to_dict('records')
    high_missing = missing_df[missing_df['missing_percentage'] > 50]
    print(f"   Columns with >50% missing: {len(high_missing)}")
    
    # 3. Duplicates
    print("\n🔍 Detecting duplicate records...")
    report["duplicates"] = detect_duplicate_records(df)
    print(f"   Exact duplicates: {report['duplicates']['exact_duplicates']}")
    print(f"   IMO duplicates: {report['duplicates']['imo_duplicates']['count']}")
    print(f"   MMSI duplicates: {report['duplicates']['mmsi_duplicates']['count']}")
    
    # 4. Identifier Conflicts
    print("\n⚠️ Detecting identifier conflicts...")
    report["identifier_conflicts"] = detect_identifier_conflicts(df)
    print(f"   Single IMO with multiple MMSI: {len(report['identifier_conflicts']['single_imo_multiple_mmsi'])}")
    print(f"   Single MMSI with multiple IMO: {len(report['identifier_conflicts']['single_mmsi_multiple_imo'])}")
    print(f"   Zero IMO records: {report['identifier_conflicts']['zero_imo_records']}")
    
    # 5. Name Variations
    print("\n📝 Analyzing vessel name variations...")
    report["name_analysis"] = analyze_vessel_name_variations(df)
    print(f"   Unique names: {report['name_analysis']['total_unique_names']:,}")
    print(f"   Garbled/suspicious names: {report['name_analysis']['names_with_special_chars']}")
    print(f"   IMOs with name changes: {len(report['name_analysis']['name_changes_by_imo'])}")
    
    # 6. Flag Changes
    print("\n🚩 Analyzing flag/ownership changes...")
    report["flag_analysis"] = analyze_flag_ownership_changes(df)
    print(f"   Unique flags: {report['flag_analysis']['total_unique_flags']}")
    print(f"   Vessels with flag changes: {report['flag_analysis']['flag_changes_count']}")
    
    # 7. Vessel Types
    print("\n🚢 Analyzing vessel types...")
    report["vessel_types"] = analyze_vessel_types(df)
    print(f"   Unique vessel types: {report['vessel_types']['total_vessel_types']}")
    
    # 8. Dimensions
    print("\n📏 Analyzing dimension consistency...")
    report["dimensions"] = analyze_dimensions_consistency(df)
    
    # 9. AIS Data Quality
    print("\n📡 Analyzing AIS data quality...")
    report["ais_quality"] = analyze_ais_data_quality(df)
    print(f"   Position coverage: {report['ais_quality']['position_coverage']}%")
    print(f"   Invalid coordinates: {report['ais_quality']['invalid_coordinates']}")
    
    print("\n" + "="*80)
    print("EDA ANALYSIS COMPLETE")
    print("="*80)
    
    return report


# Example usage and demonstration code
def run_eda_demo():
    """Run EDA demonstration with sample code snippets."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║               STAGE 1: EXPLORATORY DATA ANALYSIS (EDA)                        ║
║                     Maritime Vessel Dataset Analysis                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

This analysis examines vessel data containing AIS and registry information.

PYTHON CODE SNIPPETS FOR ANALYSIS:
==================================
""")
    
    print("""
# 1. Load and inspect the dataset
# --------------------------------
import pandas as pd

df = pd.read_csv('case_study_dataset_202509152039.csv', low_memory=False)
print(f"Dataset shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# 2. Identify missing values
# ---------------------------
missing = df.isnull().sum().sort_values(ascending=False)
missing_pct = (missing / len(df) * 100).round(2)
print(missing_pct[missing_pct > 0])

# 3. Detect duplicate IMOs
# -------------------------
imo_counts = df[df['imo'] > 0]['imo'].value_counts()
duplicate_imos = imo_counts[imo_counts > 1]
print(f"IMOs appearing multiple times: {len(duplicate_imos)}")

# 4. Find conflicting identifiers (same IMO, different MMSI)
# ----------------------------------------------------------
conflicts = df.groupby('imo')['mmsi'].nunique()
multi_mmsi = conflicts[conflicts > 1]
print(f"IMOs with multiple MMSI: {len(multi_mmsi)}")

for imo in multi_mmsi.index[:5]:
    records = df[df['imo'] == imo][['imo', 'mmsi', 'name', 'flag']]
    print(f"\\nIMO {imo}:")
    print(records)

# 5. Analyze vessel name variations for same IMO
# -----------------------------------------------
name_variations = df.groupby('imo')['name'].nunique()
vessels_renamed = name_variations[name_variations > 1]
print(f"Vessels with name changes: {len(vessels_renamed)}")

# 6. Track flag changes over time
# --------------------------------
flag_changes = df.groupby('imo')['flag'].apply(lambda x: x.unique().tolist())
multi_flag = flag_changes[flag_changes.apply(len) > 1]
print(f"Vessels with flag changes: {len(multi_flag)}")

# 7. Validate geographic coordinates
# -----------------------------------
invalid_lat = (df['last_position_latitude'] < -90) | (df['last_position_latitude'] > 90)
invalid_lon = (df['last_position_longitude'] < -180) | (df['last_position_longitude'] > 180)
print(f"Invalid coordinates: {(invalid_lat | invalid_lon).sum()}")

# 8. Check for data quality issues in vessel names
# -------------------------------------------------
import re
garbled_pattern = r'^[\\W\\d\\s]+$|[^\\x00-\\x7F]'
garbled_names = df[df['name'].str.contains(garbled_pattern, regex=True, na=False)]
print(f"Garbled/suspicious names: {len(garbled_names)}")
""")
    
    return None


if __name__ == "__main__":
    run_eda_demo()
