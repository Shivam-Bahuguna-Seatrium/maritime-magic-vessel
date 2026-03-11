"""
Stage 2: Data Validation & Quality Monitoring Pipeline
======================================================

This module implements a comprehensive data validation pipeline combining:
- Rule-based deterministic validation
- AI-assisted anomaly detection
- Alert generation system
- Human-in-the-loop validation routing

The pipeline monitors both static vessel registry data and dynamic AIS data.
"""

import re
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import uuid

import pandas as pd
import numpy as np
from collections import defaultdict


# =============================================================================
# SECTION 1: RULE-BASED VALIDATION
# =============================================================================

class ValidationRuleType(str, Enum):
    """Types of validation rules."""
    FORMAT = "format"
    RANGE = "range"
    CHECKSUM = "checksum"
    CONSISTENCY = "consistency"
    TEMPORAL = "temporal"
    GEOGRAPHIC = "geographic"


@dataclass
class ValidationRule:
    """Represents a validation rule."""
    rule_id: str
    rule_type: ValidationRuleType
    field_name: str
    description: str
    severity: str = "medium"
    
    def validate(self, value: Any, record: Dict = None) -> Tuple[bool, Optional[str]]:
        """Validate a value. Returns (is_valid, error_message)."""
        raise NotImplementedError


class IMOValidationRule(ValidationRule):
    """
    IMO Number Validation
    
    IMO numbers follow a specific format:
    - 7 digits
    - Checksum validation (weighted sum mod 10)
    
    Example: IMO 9074729
    Checksum: (9×7 + 0×6 + 7×5 + 4×4 + 7×3 + 2×2) mod 10 = 9
    """
    
    def __init__(self):
        super().__init__(
            rule_id="RULE_IMO_001",
            rule_type=ValidationRuleType.CHECKSUM,
            field_name="imo",
            description="Validate IMO number format and checksum",
            severity="high"
        )
    
    def validate(self, value: Any, record: Dict = None) -> Tuple[bool, Optional[str]]:
        if value is None or pd.isna(value):
            return False, "IMO number is missing"
        
        try:
            imo = int(value)
        except (ValueError, TypeError):
            return False, f"IMO must be numeric, got: {value}"
        
        # IMO 0 is invalid
        if imo == 0:
            return False, "IMO number cannot be 0"
        
        # Check 7-digit format (IMO numbers are typically 7 digits, 1000000-9999999)
        if imo < 1000000 or imo > 9999999:
            return False, f"IMO must be 7 digits (1000000-9999999), got: {imo}"
        
        # Checksum validation
        imo_str = str(imo)
        if len(imo_str) == 7:
            weights = [7, 6, 5, 4, 3, 2]
            checksum = sum(int(imo_str[i]) * weights[i] for i in range(6))
            expected_check_digit = checksum % 10
            actual_check_digit = int(imo_str[6])
            
            if expected_check_digit != actual_check_digit:
                return False, f"IMO checksum failed. Expected check digit: {expected_check_digit}, got: {actual_check_digit}"
        
        return True, None


class MMSIValidationRule(ValidationRule):
    """
    MMSI Validation
    
    MMSI (Maritime Mobile Service Identity) rules:
    - 9 digits
    - First 3 digits (MID) identify the country
    - Valid MID range: 201-775
    """
    
    def __init__(self):
        super().__init__(
            rule_id="RULE_MMSI_001",
            rule_type=ValidationRuleType.FORMAT,
            field_name="mmsi",
            description="Validate MMSI format and country code",
            severity="high"
        )
    
    def validate(self, value: Any, record: Dict = None) -> Tuple[bool, Optional[str]]:
        if value is None or pd.isna(value):
            return False, "MMSI is missing"
        
        try:
            mmsi = int(value)
        except (ValueError, TypeError):
            return False, f"MMSI must be numeric, got: {value}"
        
        mmsi_str = str(mmsi)
        
        # Must be 9 digits
        if len(mmsi_str) != 9:
            return False, f"MMSI must be 9 digits, got {len(mmsi_str)} digits"
        
        # Check MID (first 3 digits)
        mid = int(mmsi_str[:3])
        
        # Standard ship stations: MID between 201-775
        # Coast stations: MID 00X where X is digit
        # Group stations: MID 0XX
        if mid < 200 or mid > 800:
            # Check for special formats
            if not (mmsi_str.startswith('00') or mmsi_str.startswith('0')):
                return False, f"Invalid MID (Maritime Identification Digits): {mid}"
        
        return True, None


class GeographicValidationRule(ValidationRule):
    """
    Geographic Coordinate Validation
    
    - Latitude: -90 to 90
    - Longitude: -180 to 180
    - Additional checks for null island (0,0)
    """
    
    def __init__(self, field_name: str):
        super().__init__(
            rule_id=f"RULE_GEO_{field_name.upper()}",
            rule_type=ValidationRuleType.GEOGRAPHIC,
            field_name=field_name,
            description=f"Validate {field_name} coordinate",
            severity="medium"
        )
        self.is_latitude = "lat" in field_name.lower()
    
    def validate(self, value: Any, record: Dict = None) -> Tuple[bool, Optional[str]]:
        if value is None or pd.isna(value):
            return True, None  # Missing coordinates are handled separately
        
        try:
            coord = float(value)
        except (ValueError, TypeError):
            return False, f"Coordinate must be numeric, got: {value}"
        
        if self.is_latitude:
            if coord < -90 or coord > 90:
                return False, f"Latitude must be between -90 and 90, got: {coord}"
        else:
            if coord < -180 or coord > 180:
                return False, f"Longitude must be between -180 and 180, got: {coord}"
        
        # Check for null island (suspicious 0,0 coordinates)
        if record:
            lat = record.get('last_position_latitude')
            lon = record.get('last_position_longitude')
            if lat == 0 and lon == 0:
                return False, "Coordinates (0,0) detected - possible data quality issue"
        
        return True, None


class TimestampValidationRule(ValidationRule):
    """
    Timestamp Validation
    
    - No future timestamps
    - Not too old (configurable)
    - Chronological ordering
    """
    
    def __init__(self, field_name: str, max_age_days: int = 365):
        super().__init__(
            rule_id=f"RULE_TS_{field_name.upper()}",
            rule_type=ValidationRuleType.TEMPORAL,
            field_name=field_name,
            description=f"Validate timestamp {field_name}",
            severity="medium"
        )
        self.max_age_days = max_age_days
    
    def validate(self, value: Any, record: Dict = None) -> Tuple[bool, Optional[str]]:
        if value is None or pd.isna(value):
            return True, None
        
        try:
            if isinstance(value, str):
                ts = pd.to_datetime(value)
            else:
                ts = value
        except Exception as e:
            return False, f"Invalid timestamp format: {value}"
        
        now = datetime.utcnow()
        
        # No future timestamps
        if ts > now + timedelta(hours=24):  # Allow 24h tolerance
            return False, f"Future timestamp detected: {ts}"
        
        # Not too old
        min_date = now - timedelta(days=self.max_age_days)
        if ts < min_date:
            return False, f"Timestamp too old: {ts} (older than {self.max_age_days} days)"
        
        return True, None


class AttributeConsistencyRule(ValidationRule):
    """
    Attribute Consistency Validation
    
    Validates that certain attributes remain stable for a vessel:
    - Build year should not change
    - Gross tonnage should remain stable
    - Length/width should be consistent
    """
    
    def __init__(self, field_name: str, tolerance_pct: float = 0.05):
        super().__init__(
            rule_id=f"RULE_CONSIST_{field_name.upper()}",
            rule_type=ValidationRuleType.CONSISTENCY,
            field_name=field_name,
            description=f"Check consistency of {field_name}",
            severity="medium"
        )
        self.tolerance_pct = tolerance_pct
    
    def validate(self, value: Any, record: Dict = None) -> Tuple[bool, Optional[str]]:
        # Single record validation - always passes
        # Real consistency checks happen at aggregate level
        if value is None or pd.isna(value):
            return True, None
        
        return True, None


# =============================================================================
# SECTION 2: VALIDATION PIPELINE
# =============================================================================

class DataValidationPipeline:
    """
    Main data validation pipeline that orchestrates all validation rules.
    """
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.alerts: List[Dict] = []
        self._initialize_rules()
    
    def _initialize_rules(self):
        """Initialize all validation rules."""
        # Identifier validation
        self.rules.append(IMOValidationRule())
        self.rules.append(MMSIValidationRule())
        
        # Geographic validation
        self.rules.append(GeographicValidationRule("last_position_latitude"))
        self.rules.append(GeographicValidationRule("last_position_longitude"))
        
        # Timestamp validation
        self.rules.append(TimestampValidationRule("last_position_updateTimestamp"))
        self.rules.append(TimestampValidationRule("staticData_updateTimestamp"))
        
        # Attribute consistency
        self.rules.append(AttributeConsistencyRule("grossTonnage"))
        self.rules.append(AttributeConsistencyRule("length"))
        self.rules.append(AttributeConsistencyRule("builtYear"))
    
    def validate_record(self, record: Dict) -> List[Dict]:
        """Validate a single record against all rules."""
        validation_results = []
        
        for rule in self.rules:
            value = record.get(rule.field_name)
            is_valid, error_message = rule.validate(value, record)
            
            validation_results.append({
                "rule_id": rule.rule_id,
                "field_name": rule.field_name,
                "is_valid": is_valid,
                "error_message": error_message,
                "severity": rule.severity,
                "value": value
            })
        
        return validation_results
    
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate entire dataframe and return results."""
        all_results = []
        
        for idx, row in df.iterrows():
            record = row.to_dict()
            results = self.validate_record(record)
            
            for result in results:
                result["record_index"] = idx
                result["imo"] = record.get("imo")
                result["mmsi"] = record.get("mmsi")
                all_results.append(result)
        
        return pd.DataFrame(all_results)
    
    def generate_validation_summary(self, validation_df: pd.DataFrame) -> Dict:
        """Generate summary of validation results."""
        summary = {
            "total_validations": len(validation_df),
            "passed": int(validation_df["is_valid"].sum()),
            "failed": int((~validation_df["is_valid"]).sum()),
            "by_rule": {},
            "by_severity": {}
        }
        
        # Group by rule
        for rule_id in validation_df["rule_id"].unique():
            rule_results = validation_df[validation_df["rule_id"] == rule_id]
            summary["by_rule"][rule_id] = {
                "passed": int(rule_results["is_valid"].sum()),
                "failed": int((~rule_results["is_valid"]).sum())
            }
        
        # Group by severity
        for severity in validation_df["severity"].unique():
            sev_results = validation_df[validation_df["severity"] == severity]
            failed = sev_results[~sev_results["is_valid"]]
            summary["by_severity"][severity] = int(len(failed))
        
        return summary


# =============================================================================
# SECTION 3: AI-ASSISTED ANOMALY DETECTION
# =============================================================================

class AIAnomalyDetector:
    """
    AI-assisted anomaly detection for patterns that cannot be captured
    by deterministic rules.
    """
    
    def __init__(self, similarity_threshold: float = 0.8):
        self.similarity_threshold = similarity_threshold
        self.anomaly_scores = {}
    
    def detect_suspicious_name_changes(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect suspicious vessel name changes using string similarity.
        
        Uses Levenshtein distance / fuzzy matching to find:
        - Names that changed too drastically
        - Names with unusual patterns
        """
        suspicious_changes = []
        
        # Group by IMO and analyze name changes
        valid_df = df[(df['imo'].notna()) & (df['imo'] > 0)]
        imo_groups = valid_df.groupby('imo')
        
        for imo, group in imo_groups:
            names = group['name'].dropna().unique()
            if len(names) > 1:
                # Compare all name pairs
                for i in range(len(names)):
                    for j in range(i + 1, len(names)):
                        similarity = self._calculate_name_similarity(names[i], names[j])
                        
                        # Flag if names are very different (low similarity)
                        if similarity < 0.3:
                            suspicious_changes.append({
                                "imo": int(imo),
                                "name_1": names[i],
                                "name_2": names[j],
                                "similarity_score": similarity,
                                "anomaly_type": "drastic_name_change",
                                "confidence": 1 - similarity
                            })
        
        return suspicious_changes
    
    def detect_mmsi_switching_patterns(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect abnormal MMSI switching patterns.
        
        Flags:
        - Vessels with frequent MMSI changes
        - MMSI being used by multiple vessels
        """
        anomalies = []
        
        # Find vessels with multiple MMSI
        valid_df = df[(df['imo'].notna()) & (df['imo'] > 0) & (df['mmsi'].notna())]
        
        imo_mmsi = valid_df.groupby('imo')['mmsi'].apply(list).to_dict()
        
        for imo, mmsi_list in imo_mmsi.items():
            unique_mmsi = list(set(mmsi_list))
            if len(unique_mmsi) > 2:  # More than 2 MMSI is suspicious
                anomalies.append({
                    "imo": int(imo),
                    "mmsi_count": len(unique_mmsi),
                    "mmsi_list": [int(m) for m in unique_mmsi],
                    "anomaly_type": "frequent_mmsi_switching",
                    "confidence": min(1.0, len(unique_mmsi) / 5)  # Scale confidence
                })
        
        # Find MMSI used by multiple vessels
        mmsi_imo = valid_df.groupby('mmsi')['imo'].apply(lambda x: list(set(x))).to_dict()
        
        for mmsi, imo_list in mmsi_imo.items():
            if len(imo_list) > 1:
                anomalies.append({
                    "mmsi": int(mmsi),
                    "imo_list": [int(i) for i in imo_list if i > 0],
                    "anomaly_type": "mmsi_reuse_across_vessels",
                    "confidence": min(1.0, len(imo_list) / 3)
                })
        
        return anomalies
    
    def detect_attribute_inconsistencies(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect attribute inconsistencies across records for same vessel.
        """
        inconsistencies = []
        
        # Check for attribute variations that shouldn't change
        stable_attributes = ['builtYear', 'grossTonnage', 'length', 'width', 'shipBuilder']
        
        valid_df = df[(df['imo'].notna()) & (df['imo'] > 0)]
        
        for attr in stable_attributes:
            if attr not in df.columns:
                continue
            
            imo_attr = valid_df.groupby('imo')[attr].apply(lambda x: x.dropna().unique())
            
            for imo, values in imo_attr.items():
                if len(values) > 1:
                    # For numeric attributes, check if variation is significant
                    if attr in ['grossTonnage', 'length', 'width']:
                        values_numeric = [v for v in values if pd.notna(v)]
                        if len(values_numeric) > 1:
                            variation = (max(values_numeric) - min(values_numeric)) / max(values_numeric)
                            if variation > 0.1:  # More than 10% variation
                                inconsistencies.append({
                                    "imo": int(imo),
                                    "attribute": attr,
                                    "values": [float(v) if pd.notna(v) else None for v in values],
                                    "variation_pct": round(variation * 100, 2),
                                    "anomaly_type": "attribute_inconsistency",
                                    "confidence": min(1.0, variation)
                                })
                    else:
                        inconsistencies.append({
                            "imo": int(imo),
                            "attribute": attr,
                            "values": list(values),
                            "anomaly_type": "attribute_inconsistency",
                            "confidence": 0.7
                        })
        
        return inconsistencies
    
    def detect_ais_movement_anomalies(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect unusual AIS movement patterns.
        """
        anomalies = []
        
        # Check for unrealistic speeds
        if 'last_position_speed' in df.columns:
            high_speed = df[df['last_position_speed'] > 50]  # >50 knots is very fast
            for idx, row in high_speed.iterrows():
                anomalies.append({
                    "imo": row.get('imo'),
                    "mmsi": row.get('mmsi'),
                    "speed": row['last_position_speed'],
                    "anomaly_type": "unrealistic_speed",
                    "confidence": 0.8
                })
        
        return anomalies
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two vessel names."""
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        n1 = str(name1).upper().strip()
        n2 = str(name2).upper().strip()
        
        if n1 == n2:
            return 1.0
        
        # Simple Jaccard similarity on characters
        set1 = set(n1)
        set2 = set(n2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def run_all_detections(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Run all anomaly detection methods."""
        return {
            "suspicious_name_changes": self.detect_suspicious_name_changes(df),
            "mmsi_switching_patterns": self.detect_mmsi_switching_patterns(df),
            "attribute_inconsistencies": self.detect_attribute_inconsistencies(df),
            "ais_movement_anomalies": self.detect_ais_movement_anomalies(df)
        }


# =============================================================================
# SECTION 4: ALERT GENERATION SYSTEM
# =============================================================================

@dataclass
class DataQualityAlert:
    """Data quality alert with full context."""
    alert_id: str
    alert_type: str
    severity: str
    affected_records: List[Dict]
    description: str
    rule_or_model: str
    confidence_score: float
    suggested_actions: List[str]
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolution_details: Optional[str] = None
    requires_human_review: bool = False


class AlertGenerationSystem:
    """
    System for generating and managing data quality alerts.
    """
    
    SEVERITY_LEVELS = {
        "critical": 4,
        "high": 3,
        "medium": 2,
        "low": 1
    }
    
    def __init__(self):
        self.alerts: List[DataQualityAlert] = []
        self.alert_thresholds = {
            "invalid_imo": "high",
            "invalid_mmsi": "high",
            "duplicate_record": "medium",
            "conflicting_identifiers": "critical",
            "suspicious_name_change": "medium",
            "mmsi_reuse": "high",
            "attribute_inconsistency": "medium",
            "temporal_inconsistency": "low",
            "geographic_anomaly": "medium"
        }
    
    def generate_alert(
        self,
        alert_type: str,
        affected_records: List[Dict],
        description: str,
        rule_or_model: str,
        confidence_score: float = 1.0,
        custom_severity: Optional[str] = None
    ) -> DataQualityAlert:
        """Generate a new data quality alert."""
        
        alert_id = f"ALERT_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        severity = custom_severity or self.alert_thresholds.get(alert_type, "medium")
        
        # Determine suggested actions based on alert type
        suggested_actions = self._get_suggested_actions(alert_type)
        
        # Determine if human review is required
        requires_human = self._requires_human_review(alert_type, severity, confidence_score)
        
        alert = DataQualityAlert(
            alert_id=alert_id,
            alert_type=alert_type,
            severity=severity,
            affected_records=affected_records,
            description=description,
            rule_or_model=rule_or_model,
            confidence_score=confidence_score,
            suggested_actions=suggested_actions,
            requires_human_review=requires_human
        )
        
        self.alerts.append(alert)
        return alert
    
    def _get_suggested_actions(self, alert_type: str) -> List[str]:
        """Get suggested investigation actions for an alert type."""
        actions = {
            "invalid_imo": [
                "Verify IMO number against official IMO registry",
                "Check if IMO was recently assigned to vessel",
                "Contact vessel operator for confirmation"
            ],
            "invalid_mmsi": [
                "Verify MMSI with national maritime authority",
                "Check for recent MMSI reassignment",
                "Cross-reference with AIS provider data"
            ],
            "conflicting_identifiers": [
                "Compare all records for both identifiers",
                "Check temporal sequence of identifier usage",
                "Verify against vessel registry databases",
                "Flag for human expert review"
            ],
            "suspicious_name_change": [
                "Review historical vessel names",
                "Check for common misspellings",
                "Verify against port state control records"
            ],
            "mmsi_reuse": [
                "Check if MMSI was legally transferred",
                "Verify vessel scrapping/retirement records",
                "Check for spoofing indicators"
            ],
            "attribute_inconsistency": [
                "Compare attribute values with authoritative source",
                "Check for data entry errors",
                "Review update history"
            ]
        }
        return actions.get(alert_type, ["Review affected records", "Consult domain expert"])
    
    def _requires_human_review(
        self,
        alert_type: str,
        severity: str,
        confidence_score: float
    ) -> bool:
        """Determine if alert requires human review."""
        # Always require human review for critical alerts
        if severity == "critical":
            return True
        
        # Require review for high severity with low confidence
        if severity == "high" and confidence_score < 0.8:
            return True
        
        # Specific alert types always need review
        human_review_types = ["conflicting_identifiers", "mmsi_reuse", "suspicious_name_change"]
        if alert_type in human_review_types:
            return True
        
        return False
    
    def get_alerts_for_review(self) -> List[DataQualityAlert]:
        """Get all alerts that require human review."""
        return [a for a in self.alerts if a.requires_human_review and not a.resolved]
    
    def get_alert_summary(self) -> Dict:
        """Get summary of all alerts."""
        summary = {
            "total_alerts": len(self.alerts),
            "by_severity": defaultdict(int),
            "by_type": defaultdict(int),
            "pending_review": 0,
            "resolved": 0
        }
        
        for alert in self.alerts:
            summary["by_severity"][alert.severity] += 1
            summary["by_type"][alert.alert_type] += 1
            if alert.requires_human_review and not alert.resolved:
                summary["pending_review"] += 1
            if alert.resolved:
                summary["resolved"] += 1
        
        return dict(summary)


# =============================================================================
# SECTION 5: INTEGRATED VALIDATION PIPELINE
# =============================================================================

class IntegratedValidationPipeline:
    """
    Integrated pipeline combining all validation components.
    """
    
    def __init__(self):
        self.rule_validator = DataValidationPipeline()
        self.anomaly_detector = AIAnomalyDetector()
        self.alert_system = AlertGenerationSystem()
    
    def run_full_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run complete validation pipeline on dataset."""
        print("\n" + "="*80)
        print("STAGE 2: DATA VALIDATION & QUALITY MONITORING PIPELINE")
        print("="*80 + "\n")
        
        results = {
            "rule_based_validation": {},
            "anomaly_detection": {},
            "alerts": [],
            "summary": {}
        }
        
        # Step 1: Rule-based validation
        print("📋 Running rule-based validation...")
        validation_df = self.rule_validator.validate_dataframe(df)
        results["rule_based_validation"] = self.rule_validator.generate_validation_summary(validation_df)
        
        # Generate alerts for failed validations
        failed_validations = validation_df[~validation_df["is_valid"]]
        for rule_id in failed_validations["rule_id"].unique():
            rule_failures = failed_validations[failed_validations["rule_id"] == rule_id]
            affected = rule_failures[["imo", "mmsi", "value", "error_message"]].to_dict("records")
            
            alert = self.alert_system.generate_alert(
                alert_type=f"validation_failure_{rule_id}",
                affected_records=affected[:100],  # Limit records
                description=f"Rule {rule_id} failed for {len(rule_failures)} records",
                rule_or_model=rule_id,
                confidence_score=1.0
            )
            results["alerts"].append(alert.__dict__)
        
        print(f"   ✓ Validated {len(df)} records")
        print(f"   ✓ Found {results['rule_based_validation']['failed']} validation failures")
        
        # Step 2: AI-assisted anomaly detection
        print("\n🤖 Running AI-assisted anomaly detection...")
        anomalies = self.anomaly_detector.run_all_detections(df)
        results["anomaly_detection"] = {k: len(v) for k, v in anomalies.items()}
        
        # Generate alerts for anomalies
        for anomaly_type, anomaly_list in anomalies.items():
            if anomaly_list:
                alert = self.alert_system.generate_alert(
                    alert_type=anomaly_type,
                    affected_records=anomaly_list[:50],
                    description=f"Detected {len(anomaly_list)} {anomaly_type} anomalies",
                    rule_or_model="AI_ANOMALY_DETECTOR",
                    confidence_score=0.85
                )
                results["alerts"].append(alert.__dict__)
        
        print(f"   ✓ Suspicious name changes: {results['anomaly_detection'].get('suspicious_name_changes', 0)}")
        print(f"   ✓ MMSI switching patterns: {results['anomaly_detection'].get('mmsi_switching_patterns', 0)}")
        print(f"   ✓ Attribute inconsistencies: {results['anomaly_detection'].get('attribute_inconsistencies', 0)}")
        
        # Step 3: Generate summary
        results["summary"] = self.alert_system.get_alert_summary()
        results["summary"]["alerts_requiring_review"] = len(self.alert_system.get_alerts_for_review())
        
        print(f"\n📊 Alert Summary:")
        print(f"   Total alerts: {results['summary']['total_alerts']}")
        print(f"   Requiring human review: {results['summary']['alerts_requiring_review']}")
        
        print("\n" + "="*80)
        print("VALIDATION PIPELINE COMPLETE")
        print("="*80)
        
        return results


# =============================================================================
# DEMONSTRATION CODE
# =============================================================================

def demonstrate_validation_rules():
    """Demonstrate validation rules with Python examples."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║           STAGE 2: DATA VALIDATION & QUALITY MONITORING                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

PYTHON CODE EXAMPLES FOR VALIDATION:
====================================
""")
    
    print("""
# 1. IMO Validation with Checksum
# --------------------------------
def validate_imo(imo: int) -> tuple[bool, str]:
    '''
    Validate IMO number format and checksum.
    IMO format: 7 digits, last digit is checksum.
    Checksum: (d1×7 + d2×6 + d3×5 + d4×4 + d5×3 + d6×2) mod 10
    '''
    if imo < 1000000 or imo > 9999999:
        return False, "IMO must be 7 digits"
    
    imo_str = str(imo)
    weights = [7, 6, 5, 4, 3, 2]
    checksum = sum(int(imo_str[i]) * weights[i] for i in range(6))
    expected = checksum % 10
    actual = int(imo_str[6])
    
    if expected != actual:
        return False, f"Checksum failed: expected {expected}, got {actual}"
    return True, "Valid"

# Example
print(validate_imo(9528574))  # (True, 'Valid')
print(validate_imo(9528575))  # (False, 'Checksum failed...')


# 2. MMSI Validation
# -------------------
def validate_mmsi(mmsi: int) -> tuple[bool, str]:
    '''Validate MMSI format (9 digits, valid MID)'''
    mmsi_str = str(mmsi)
    if len(mmsi_str) != 9:
        return False, f"MMSI must be 9 digits, got {len(mmsi_str)}"
    
    mid = int(mmsi_str[:3])  # Maritime Identification Digits
    if mid < 200 or mid > 775:
        return False, f"Invalid MID: {mid}"
    return True, "Valid"

# Example
print(validate_mmsi(636013854))  # (True, 'Valid') - Liberia
print(validate_mmsi(123456789))  # (False, 'Invalid MID')


# 3. Geographic Coordinate Validation
# -------------------------------------
def validate_coordinates(lat: float, lon: float) -> tuple[bool, str]:
    '''Validate latitude and longitude ranges'''
    if lat < -90 or lat > 90:
        return False, f"Invalid latitude: {lat}"
    if lon < -180 or lon > 180:
        return False, f"Invalid longitude: {lon}"
    if lat == 0 and lon == 0:
        return False, "Null Island (0,0) - suspicious"
    return True, "Valid"


# 4. Timestamp Validation
# ------------------------
from datetime import datetime, timedelta

def validate_timestamp(ts: datetime) -> tuple[bool, str]:
    '''Validate timestamp is not in future and not too old'''
    now = datetime.utcnow()
    if ts > now + timedelta(hours=24):
        return False, "Future timestamp"
    if ts < now - timedelta(days=365):
        return False, "Timestamp too old"
    return True, "Valid"


# 5. Batch Validation Pipeline
# -----------------------------
def validate_vessel_record(record: dict) -> list[dict]:
    '''Run all validations on a single record'''
    results = []
    
    # IMO validation
    if 'imo' in record and record['imo']:
        valid, msg = validate_imo(record['imo'])
        results.append({'field': 'imo', 'valid': valid, 'message': msg})
    
    # MMSI validation  
    if 'mmsi' in record and record['mmsi']:
        valid, msg = validate_mmsi(record['mmsi'])
        results.append({'field': 'mmsi', 'valid': valid, 'message': msg})
    
    # Coordinate validation
    if 'last_position_latitude' in record:
        valid, msg = validate_coordinates(
            record.get('last_position_latitude', 0),
            record.get('last_position_longitude', 0)
        )
        results.append({'field': 'coordinates', 'valid': valid, 'message': msg})
    
    return results


# 6. AI-Assisted Anomaly Detection: Name Similarity
# --------------------------------------------------
from difflib import SequenceMatcher

def calculate_name_similarity(name1: str, name2: str) -> float:
    '''Calculate similarity between vessel names'''
    if not name1 or not name2:
        return 0.0
    n1, n2 = name1.upper().strip(), name2.upper().strip()
    return SequenceMatcher(None, n1, n2).ratio()

def detect_suspicious_name_change(old_name: str, new_name: str) -> dict:
    '''Detect if name change is suspicious'''
    similarity = calculate_name_similarity(old_name, new_name)
    return {
        'old_name': old_name,
        'new_name': new_name,
        'similarity': similarity,
        'suspicious': similarity < 0.3,  # Very different names
        'confidence': 1 - similarity
    }

# Example
print(detect_suspicious_name_change("MARCO", "MARCO POLO"))
# {'old_name': 'MARCO', 'new_name': 'MARCO POLO', 'similarity': 0.67, 'suspicious': False}

print(detect_suspicious_name_change("MARCO", "SUNSHINE"))
# {'old_name': 'MARCO', 'new_name': 'SUNSHINE', 'suspicious': True, 'confidence': 0.85}
""")
    
    return None


# Aliases for backward compatibility
ValidationPipeline = DataValidationPipeline
DataValidator = ValidationRule


if __name__ == "__main__":
    demonstrate_validation_rules()
