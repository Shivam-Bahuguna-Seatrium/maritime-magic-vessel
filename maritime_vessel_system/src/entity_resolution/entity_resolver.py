"""
Stage 5, 6 & 7: Vessel Entity Resolution with Human-in-the-Loop and RLHF
========================================================================

This module implements:
- Stage 5: Entity resolution mechanism for determining vessel identity
- Stage 6: Human-in-the-loop verification for ambiguous matches
- Stage 7: RLHF-based feedback loop for continuous improvement

The system combines deterministic rules, similarity scoring, and human validation
to accurately resolve vessel identities while maintaining full traceability.
"""

import uuid
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import json


# =============================================================================
# STAGE 5: ENTITY RESOLUTION ENGINE
# =============================================================================

class MatchType(str, Enum):
    """Types of entity matches."""
    EXACT_IMO = "exact_imo_match"
    EXACT_MMSI = "exact_mmsi_match"
    HIGH_SIMILARITY = "high_similarity_match"
    PROBABLE_MATCH = "probable_match"
    UNCERTAIN = "uncertain_match"
    NO_MATCH = "no_match"


@dataclass
class SimilarityScore:
    """Detailed similarity score between two records."""
    overall_score: float
    attribute_scores: Dict[str, float]
    matching_attributes: List[str]
    conflicting_attributes: List[str]
    confidence: float
    evidence: List[str]


@dataclass
class EntityResolutionResult:
    """Result of entity resolution between records."""
    record_a_id: str
    record_b_id: str
    match_type: MatchType
    similarity_score: SimilarityScore
    is_same_entity: bool
    confidence: float
    requires_review: bool
    resolution_evidence: List[str]
    resolved_at: datetime = field(default_factory=datetime.utcnow)


class VesselEntityResolutionEngine:
    """
    Engine for resolving vessel identities across records.
    
    Combines:
    - Deterministic rules (exact IMO match)
    - Attribute similarity scoring
    - Historical identifier tracking
    - Confidence scoring
    """
    
    # Configurable thresholds
    HIGH_CONFIDENCE_THRESHOLD = 0.9
    REVIEW_REQUIRED_THRESHOLD = 0.6
    LOW_CONFIDENCE_THRESHOLD = 0.3
    
    # Attribute weights for similarity calculation
    ATTRIBUTE_WEIGHTS = {
        "imo": 0.30,
        "name": 0.20,
        "mmsi": 0.10,
        "length": 0.10,
        "width": 0.08,
        "gross_tonnage": 0.07,
        "built_year": 0.08,
        "builder": 0.07
    }
    
    def __init__(self, feedback_store: Optional['FeedbackStore'] = None):
        self.feedback_store = feedback_store
        self.resolution_cache: Dict[str, EntityResolutionResult] = {}
        self._load_learned_thresholds()
    
    def _load_learned_thresholds(self):
        """Load thresholds adjusted from RLHF feedback."""
        if self.feedback_store:
            learned = self.feedback_store.get_learned_thresholds()
            if learned:
                self.HIGH_CONFIDENCE_THRESHOLD = learned.get(
                    "high_confidence", self.HIGH_CONFIDENCE_THRESHOLD
                )
                self.REVIEW_REQUIRED_THRESHOLD = learned.get(
                    "review_required", self.REVIEW_REQUIRED_THRESHOLD
                )
    
    def resolve_entities(
        self,
        record_a: Dict,
        record_b: Dict
    ) -> EntityResolutionResult:
        """
        Determine if two records represent the same vessel entity.
        
        Resolution Strategy:
        1. Check deterministic rules (exact IMO match)
        2. Calculate attribute similarity scores
        3. Apply confidence scoring
        4. Determine if human review is needed
        """
        # Generate cache key
        cache_key = self._generate_cache_key(record_a, record_b)
        if cache_key in self.resolution_cache:
            return self.resolution_cache[cache_key]
        
        record_a_id = str(record_a.get("imo") or record_a.get("mmsi") or uuid.uuid4())
        record_b_id = str(record_b.get("imo") or record_b.get("mmsi") or uuid.uuid4())
        
        # Step 1: Deterministic rule check
        deterministic_result = self._check_deterministic_rules(record_a, record_b)
        if deterministic_result:
            return deterministic_result
        
        # Step 2: Calculate similarity scores
        similarity_score = self._calculate_similarity(record_a, record_b)
        
        # Step 3: Determine match type and confidence
        match_type, is_same, confidence = self._determine_match(similarity_score)
        
        # Step 4: Check if human review is needed
        requires_review = self._requires_human_review(
            match_type, confidence, record_a, record_b
        )
        
        # Build evidence
        evidence = self._build_resolution_evidence(
            record_a, record_b, similarity_score, match_type
        )
        
        result = EntityResolutionResult(
            record_a_id=record_a_id,
            record_b_id=record_b_id,
            match_type=match_type,
            similarity_score=similarity_score,
            is_same_entity=is_same,
            confidence=confidence,
            requires_review=requires_review,
            resolution_evidence=evidence
        )
        
        self.resolution_cache[cache_key] = result
        return result
    
    def _generate_cache_key(self, record_a: Dict, record_b: Dict) -> str:
        """Generate unique cache key for a record pair."""
        id_a = record_a.get("imo") or record_a.get("mmsi") or id(record_a)
        id_b = record_b.get("imo") or record_b.get("mmsi") or id(record_b)
        combined = f"{min(id_a, id_b)}_{max(id_a, id_b)}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _check_deterministic_rules(
        self,
        record_a: Dict,
        record_b: Dict
    ) -> Optional[EntityResolutionResult]:
        """
        Check deterministic rules for exact matches.
        
        Rule 1: Exact IMO match (both valid) = Same vessel
        Rule 2: Same valid MMSI + similar name = Probable same vessel
        """
        imo_a = record_a.get("imo")
        imo_b = record_b.get("imo")
        
        # Rule 1: Exact IMO match
        if imo_a and imo_b and imo_a > 0 and imo_b > 0:
            if imo_a == imo_b:
                return EntityResolutionResult(
                    record_a_id=str(imo_a),
                    record_b_id=str(imo_b),
                    match_type=MatchType.EXACT_IMO,
                    similarity_score=SimilarityScore(
                        overall_score=1.0,
                        attribute_scores={"imo": 1.0},
                        matching_attributes=["imo"],
                        conflicting_attributes=[],
                        confidence=1.0,
                        evidence=["Exact IMO match"]
                    ),
                    is_same_entity=True,
                    confidence=1.0,
                    requires_review=False,
                    resolution_evidence=["Deterministic rule: Exact IMO match"]
                )
        
        return None
    
    def _calculate_similarity(self, record_a: Dict, record_b: Dict) -> SimilarityScore:
        """Calculate comprehensive similarity score between records."""
        attribute_scores = {}
        evidence = []
        
        # IMO comparison
        imo_a, imo_b = record_a.get("imo"), record_b.get("imo")
        if imo_a and imo_b and imo_a > 0 and imo_b > 0:
            attribute_scores["imo"] = 1.0 if imo_a == imo_b else 0.0
            if imo_a == imo_b:
                evidence.append(f"IMO match: {imo_a}")
        
        # Name similarity
        name_a, name_b = record_a.get("name", ""), record_b.get("name", "")
        if name_a and name_b:
            name_sim = self._calculate_name_similarity(name_a, name_b)
            attribute_scores["name"] = name_sim
            if name_sim > 0.8:
                evidence.append(f"Name similarity: {name_sim:.2%}")
        
        # MMSI comparison
        mmsi_a, mmsi_b = record_a.get("mmsi"), record_b.get("mmsi")
        if mmsi_a and mmsi_b:
            attribute_scores["mmsi"] = 1.0 if mmsi_a == mmsi_b else 0.0
            if mmsi_a == mmsi_b:
                evidence.append(f"MMSI match: {mmsi_a}")
        
        # Dimension comparisons (with tolerance)
        for dim in ["length", "width"]:
            val_a = record_a.get(dim)
            val_b = record_b.get(dim)
            if val_a and val_b and val_a > 0 and val_b > 0:
                diff_pct = abs(val_a - val_b) / max(val_a, val_b)
                attribute_scores[dim] = max(0, 1 - diff_pct / 0.05)  # 5% tolerance
                if attribute_scores[dim] > 0.9:
                    evidence.append(f"{dim} match: {val_a}")
        
        # Gross tonnage comparison
        gt_a = record_a.get("grossTonnage")
        gt_b = record_b.get("grossTonnage")
        if gt_a and gt_b and gt_a > 0 and gt_b > 0:
            diff_pct = abs(gt_a - gt_b) / max(gt_a, gt_b)
            attribute_scores["gross_tonnage"] = max(0, 1 - diff_pct / 0.05)
        
        # Build year comparison
        year_a = record_a.get("builtYear")
        year_b = record_b.get("builtYear")
        if year_a and year_b:
            attribute_scores["built_year"] = 1.0 if year_a == year_b else 0.0
            if year_a == year_b:
                evidence.append(f"Built year match: {year_a}")
        
        # Builder comparison
        builder_a = record_a.get("shipBuilder", "")
        builder_b = record_b.get("shipBuilder", "")
        if builder_a and builder_b:
            builder_sim = self._calculate_name_similarity(builder_a, builder_b)
            attribute_scores["builder"] = builder_sim
        
        # Calculate weighted overall score
        total_weight = 0
        weighted_sum = 0
        for attr, weight in self.ATTRIBUTE_WEIGHTS.items():
            if attr in attribute_scores:
                weighted_sum += attribute_scores[attr] * weight
                total_weight += weight
        
        overall = weighted_sum / total_weight if total_weight > 0 else 0.0
        
        # Determine matching and conflicting attributes
        matching = [k for k, v in attribute_scores.items() if v > 0.9]
        conflicting = [k for k, v in attribute_scores.items() if v < 0.3]
        
        # Calculate confidence based on how many attributes we could compare
        attribute_coverage = len(attribute_scores) / len(self.ATTRIBUTE_WEIGHTS)
        confidence = overall * attribute_coverage
        
        return SimilarityScore(
            overall_score=round(overall, 4),
            attribute_scores=attribute_scores,
            matching_attributes=matching,
            conflicting_attributes=conflicting,
            confidence=round(confidence, 4),
            evidence=evidence
        )
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two vessel names using multiple methods."""
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        n1 = name1.upper().strip()
        n2 = name2.upper().strip()
        
        if n1 == n2:
            return 1.0
        
        # Method 1: Character-level Jaccard
        set1 = set(n1.replace(" ", ""))
        set2 = set(n2.replace(" ", ""))
        jaccard = len(set1 & set2) / len(set1 | set2) if set1 | set2 else 0
        
        # Method 2: Token overlap (word-level)
        tokens1 = set(n1.split())
        tokens2 = set(n2.split())
        token_overlap = len(tokens1 & tokens2) / len(tokens1 | tokens2) if tokens1 | tokens2 else 0
        
        # Method 3: Substring containment
        containment = 1.0 if n1 in n2 or n2 in n1 else 0.0
        
        # Weight and combine
        combined = (jaccard * 0.4) + (token_overlap * 0.4) + (containment * 0.2)
        
        return round(combined, 4)
    
    def _determine_match(
        self,
        similarity: SimilarityScore
    ) -> Tuple[MatchType, bool, float]:
        """Determine match type and whether entities are the same."""
        score = similarity.overall_score
        confidence = similarity.confidence
        
        if score >= self.HIGH_CONFIDENCE_THRESHOLD:
            return MatchType.HIGH_SIMILARITY, True, confidence
        elif score >= self.REVIEW_REQUIRED_THRESHOLD:
            return MatchType.PROBABLE_MATCH, True, confidence
        elif score >= self.LOW_CONFIDENCE_THRESHOLD:
            return MatchType.UNCERTAIN, False, confidence
        else:
            return MatchType.NO_MATCH, False, confidence
    
    def _requires_human_review(
        self,
        match_type: MatchType,
        confidence: float,
        record_a: Dict,
        record_b: Dict
    ) -> bool:
        """Determine if human review is required."""
        # Always review uncertain matches
        if match_type == MatchType.UNCERTAIN:
            return True
        
        # Review probable matches with conflicting attributes
        if match_type == MatchType.PROBABLE_MATCH:
            return True
        
        # Review if there are conflicting identifiers
        imo_a, imo_b = record_a.get("imo"), record_b.get("imo")
        mmsi_a, mmsi_b = record_a.get("mmsi"), record_b.get("mmsi")
        
        if imo_a and imo_b and imo_a > 0 and imo_b > 0:
            if imo_a != imo_b and mmsi_a == mmsi_b:
                return True  # Same MMSI, different IMO
        
        return False
    
    def _build_resolution_evidence(
        self,
        record_a: Dict,
        record_b: Dict,
        similarity: SimilarityScore,
        match_type: MatchType
    ) -> List[str]:
        """Build evidence trail for the resolution decision."""
        evidence = [
            f"Resolution type: {match_type.value}",
            f"Overall similarity: {similarity.overall_score:.2%}",
            f"Confidence: {similarity.confidence:.2%}"
        ]
        
        evidence.extend(similarity.evidence)
        
        if similarity.matching_attributes:
            evidence.append(f"Matching attributes: {', '.join(similarity.matching_attributes)}")
        if similarity.conflicting_attributes:
            evidence.append(f"Conflicting attributes: {', '.join(similarity.conflicting_attributes)}")
        
        return evidence


# =============================================================================
# STAGE 6: HUMAN-IN-THE-LOOP VERIFICATION
# =============================================================================

class ReviewStatus(str, Enum):
    """Status of a human review item."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    ESCALATED = "escalated"


class ReviewDecision(str, Enum):
    """Human reviewer decisions."""
    CONFIRMED_SAME = "confirmed_same_entity"
    CONFIRMED_DIFFERENT = "confirmed_different_entities"
    MERGE_RECORDS = "merge_records"
    FLAG_INVESTIGATION = "flag_for_investigation"
    UNCERTAIN = "uncertain"


@dataclass
class HumanReviewItem:
    """Item queued for human review."""
    review_id: str
    resolution_result: EntityResolutionResult
    vessel_records: List[Dict]
    priority_score: float
    status: ReviewStatus = ReviewStatus.PENDING
    assigned_to: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    # Historical context
    historical_data: Dict = field(default_factory=dict)
    similar_past_decisions: List[Dict] = field(default_factory=list)


@dataclass
class ReviewFeedback:
    """Feedback from human reviewer."""
    review_id: str
    reviewer_id: str
    decision: ReviewDecision
    confidence: float
    reasoning: str
    additional_evidence: List[str]
    reviewed_at: datetime = field(default_factory=datetime.utcnow)
    time_spent_seconds: int = 0


class HumanReviewInterface:
    """
    Interface for human-in-the-loop identity verification.
    
    Responsibilities:
    - Queue uncertain matches for review
    - Present conflicting records with context
    - Collect and store reviewer decisions
    - Route decisions to feedback system
    """
    
    def __init__(self, feedback_store: 'FeedbackStore'):
        self.feedback_store = feedback_store
        self.review_queue: Dict[str, HumanReviewItem] = {}
        self.completed_reviews: Dict[str, ReviewFeedback] = {}
    
    def queue_for_review(
        self,
        resolution_result: EntityResolutionResult,
        record_a: Dict,
        record_b: Dict
    ) -> HumanReviewItem:
        """Queue a resolution result for human review."""
        review_id = f"REV_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        # Calculate priority score (higher = more urgent)
        priority = self._calculate_priority(resolution_result, record_a, record_b)
        
        # Get historical context
        historical = self._get_historical_context(record_a, record_b)
        
        # Find similar past decisions
        similar = self.feedback_store.find_similar_decisions(
            resolution_result.similarity_score
        )
        
        review_item = HumanReviewItem(
            review_id=review_id,
            resolution_result=resolution_result,
            vessel_records=[record_a, record_b],
            priority_score=priority,
            historical_data=historical,
            similar_past_decisions=similar
        )
        
        self.review_queue[review_id] = review_item
        return review_item
    
    def _calculate_priority(
        self,
        result: EntityResolutionResult,
        record_a: Dict,
        record_b: Dict
    ) -> float:
        """Calculate priority score for review."""
        priority = 0.5  # Base priority
        
        # Higher priority for uncertain matches with high data completeness
        if result.match_type == MatchType.UNCERTAIN:
            priority += 0.2
        
        # Higher priority if both records have valid IMO
        if record_a.get("imo", 0) > 0 and record_b.get("imo", 0) > 0:
            priority += 0.15
        
        # Higher priority for conflicting identifiers
        if (record_a.get("imo") != record_b.get("imo") and 
            record_a.get("mmsi") == record_b.get("mmsi")):
            priority += 0.25
        
        return min(1.0, priority)
    
    def _get_historical_context(self, record_a: Dict, record_b: Dict) -> Dict:
        """Get historical vessel data for context."""
        return {
            "record_a_history": {
                "imo": record_a.get("imo"),
                "name": record_a.get("name"),
                "flag": record_a.get("flag"),
                "update_date": record_a.get("UpdateDate")
            },
            "record_b_history": {
                "imo": record_b.get("imo"),
                "name": record_b.get("name"),
                "flag": record_b.get("flag"),
                "update_date": record_b.get("UpdateDate")
            }
        }
    
    def submit_review(
        self,
        review_id: str,
        reviewer_id: str,
        decision: ReviewDecision,
        confidence: float,
        reasoning: str,
        additional_evidence: List[str] = None,
        time_spent_seconds: int = 0
    ) -> ReviewFeedback:
        """Submit a human review decision."""
        if review_id not in self.review_queue:
            raise ValueError(f"Review {review_id} not found in queue")
        
        feedback = ReviewFeedback(
            review_id=review_id,
            reviewer_id=reviewer_id,
            decision=decision,
            confidence=confidence,
            reasoning=reasoning,
            additional_evidence=additional_evidence or [],
            time_spent_seconds=time_spent_seconds
        )
        
        # Mark review as completed
        review_item = self.review_queue[review_id]
        review_item.status = ReviewStatus.COMPLETED
        
        # Store feedback
        self.completed_reviews[review_id] = feedback
        
        # Send to feedback store for learning
        self.feedback_store.record_feedback(review_item, feedback)
        
        return feedback
    
    def get_pending_reviews(self, limit: int = 10) -> List[HumanReviewItem]:
        """Get pending reviews sorted by priority."""
        pending = [r for r in self.review_queue.values() 
                   if r.status == ReviewStatus.PENDING]
        return sorted(pending, key=lambda x: x.priority_score, reverse=True)[:limit]
    
    def generate_review_context(self, review_id: str) -> Dict:
        """Generate context for a reviewer to make a decision."""
        if review_id not in self.review_queue:
            return {}
        
        item = self.review_queue[review_id]
        result = item.resolution_result
        records = item.vessel_records
        
        return {
            "review_id": review_id,
            "match_type": result.match_type.value,
            "confidence": result.confidence,
            "similarity_score": result.similarity_score.overall_score,
            "matching_attributes": result.similarity_score.matching_attributes,
            "conflicting_attributes": result.similarity_score.conflicting_attributes,
            "evidence": result.resolution_evidence,
            "record_comparison": {
                "record_a": {
                    "imo": records[0].get("imo"),
                    "mmsi": records[0].get("mmsi"),
                    "name": records[0].get("name"),
                    "flag": records[0].get("flag"),
                    "vessel_type": records[0].get("vessel_type"),
                    "length": records[0].get("length"),
                    "width": records[0].get("width"),
                    "built_year": records[0].get("builtYear"),
                    "builder": records[0].get("shipBuilder")
                },
                "record_b": {
                    "imo": records[1].get("imo"),
                    "mmsi": records[1].get("mmsi"),
                    "name": records[1].get("name"),
                    "flag": records[1].get("flag"),
                    "vessel_type": records[1].get("vessel_type"),
                    "length": records[1].get("length"),
                    "width": records[1].get("width"),
                    "built_year": records[1].get("builtYear"),
                    "builder": records[1].get("shipBuilder")
                }
            },
            "historical_data": item.historical_data,
            "similar_past_decisions": item.similar_past_decisions,
            "suggested_decision": self._suggest_decision(item)
        }
    
    def _suggest_decision(self, item: HumanReviewItem) -> Dict:
        """Suggest a decision based on similar past decisions."""
        if not item.similar_past_decisions:
            return {"decision": None, "confidence": 0}
        
        # Count past decisions
        decision_counts = defaultdict(int)
        for past in item.similar_past_decisions:
            decision_counts[past.get("decision")] += 1
        
        if decision_counts:
            most_common = max(decision_counts.items(), key=lambda x: x[1])
            total = sum(decision_counts.values())
            return {
                "decision": most_common[0],
                "confidence": most_common[1] / total,
                "based_on": total
            }
        
        return {"decision": None, "confidence": 0}


# =============================================================================
# STAGE 7: RLHF-BASED FEEDBACK LOOP
# =============================================================================

@dataclass
class FeedbackRecord:
    """Record of human feedback for learning."""
    feedback_id: str
    resolution_type: MatchType
    similarity_score: float
    attribute_scores: Dict[str, float]
    human_decision: ReviewDecision
    human_confidence: float
    was_correct: Optional[bool] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


class FeedbackStore:
    """
    Store for collecting and learning from human feedback.
    
    Implements RLHF-like mechanism to:
    - Improve validation rules
    - Adjust similarity thresholds
    - Improve entity resolution accuracy
    - Reduce false alerts
    """
    
    def __init__(self):
        self.feedback_records: List[FeedbackRecord] = []
        self.learned_thresholds: Dict[str, float] = {}
        self.learned_weights: Dict[str, float] = {}
        self.pattern_statistics: Dict[str, Dict] = defaultdict(lambda: {
            "total": 0,
            "confirmed_same": 0,
            "confirmed_different": 0
        })
    
    def record_feedback(
        self,
        review_item: HumanReviewItem,
        feedback: ReviewFeedback
    ):
        """Record human feedback for learning."""
        result = review_item.resolution_result
        
        record = FeedbackRecord(
            feedback_id=feedback.review_id,
            resolution_type=result.match_type,
            similarity_score=result.similarity_score.overall_score,
            attribute_scores=result.similarity_score.attribute_scores,
            human_decision=feedback.decision,
            human_confidence=feedback.confidence
        )
        
        self.feedback_records.append(record)
        
        # Update pattern statistics
        self._update_statistics(record)
        
        # Trigger learning if enough samples
        if len(self.feedback_records) % 50 == 0:
            self._learn_from_feedback()
    
    def _update_statistics(self, record: FeedbackRecord):
        """Update statistics for learning."""
        # Bin similarity score
        score_bin = f"{int(record.similarity_score * 10) / 10:.1f}"
        
        self.pattern_statistics[score_bin]["total"] += 1
        
        if record.human_decision in [ReviewDecision.CONFIRMED_SAME, ReviewDecision.MERGE_RECORDS]:
            self.pattern_statistics[score_bin]["confirmed_same"] += 1
        elif record.human_decision == ReviewDecision.CONFIRMED_DIFFERENT:
            self.pattern_statistics[score_bin]["confirmed_different"] += 1
    
    def _learn_from_feedback(self):
        """Learn new thresholds and weights from accumulated feedback."""
        if len(self.feedback_records) < 20:
            return
        
        # Learn optimal thresholds
        self._learn_thresholds()
        
        # Learn optimal attribute weights
        self._learn_weights()
        
        print(f"[RLHF] Updated thresholds from {len(self.feedback_records)} samples")
    
    def _learn_thresholds(self):
        """Learn optimal confidence thresholds."""
        # Find similarity scores where humans confirmed "same entity"
        same_scores = [
            r.similarity_score for r in self.feedback_records
            if r.human_decision in [ReviewDecision.CONFIRMED_SAME, ReviewDecision.MERGE_RECORDS]
        ]
        
        # Find similarity scores where humans confirmed "different entities"
        diff_scores = [
            r.similarity_score for r in self.feedback_records
            if r.human_decision == ReviewDecision.CONFIRMED_DIFFERENT
        ]
        
        if same_scores and diff_scores:
            # High confidence threshold: 90th percentile of "same" decisions
            self.learned_thresholds["high_confidence"] = sorted(same_scores)[
                int(len(same_scores) * 0.1)
            ]
            
            # Review required threshold: midpoint between same and different
            min_same = min(same_scores)
            max_diff = max(diff_scores)
            self.learned_thresholds["review_required"] = (min_same + max_diff) / 2
    
    def _learn_weights(self):
        """Learn optimal attribute weights from feedback."""
        # Analyze which attributes were most predictive of human decisions
        attribute_importance = defaultdict(lambda: {"same": [], "different": []})
        
        for record in self.feedback_records:
            for attr, score in record.attribute_scores.items():
                if record.human_decision in [ReviewDecision.CONFIRMED_SAME, ReviewDecision.MERGE_RECORDS]:
                    attribute_importance[attr]["same"].append(score)
                elif record.human_decision == ReviewDecision.CONFIRMED_DIFFERENT:
                    attribute_importance[attr]["different"].append(score)
        
        # Calculate new weights based on discriminative power
        for attr, scores in attribute_importance.items():
            if scores["same"] and scores["different"]:
                mean_same = sum(scores["same"]) / len(scores["same"])
                mean_diff = sum(scores["different"]) / len(scores["different"])
                
                # Higher weight for attributes that better discriminate
                discrimination = abs(mean_same - mean_diff)
                self.learned_weights[attr] = discrimination
        
        # Normalize weights
        total = sum(self.learned_weights.values()) if self.learned_weights else 1
        for attr in self.learned_weights:
            self.learned_weights[attr] /= total
    
    def get_learned_thresholds(self) -> Dict[str, float]:
        """Get learned thresholds."""
        return self.learned_thresholds
    
    def get_learned_weights(self) -> Dict[str, float]:
        """Get learned attribute weights."""
        return self.learned_weights
    
    def find_similar_decisions(
        self,
        similarity_score: SimilarityScore,
        limit: int = 5
    ) -> List[Dict]:
        """Find similar past decisions for context."""
        similar = []
        
        for record in self.feedback_records[-100:]:  # Look at recent feedback
            score_diff = abs(record.similarity_score - similarity_score.overall_score)
            
            if score_diff < 0.1:  # Similar overall score
                similar.append({
                    "similarity_score": record.similarity_score,
                    "decision": record.human_decision.value,
                    "confidence": record.human_confidence
                })
        
        return sorted(similar, key=lambda x: x["confidence"], reverse=True)[:limit]
    
    def get_learning_statistics(self) -> Dict:
        """Get statistics about the learning process."""
        return {
            "total_feedback_records": len(self.feedback_records),
            "learned_thresholds": self.learned_thresholds,
            "learned_weights": self.learned_weights,
            "pattern_statistics": dict(self.pattern_statistics),
            "decisions_breakdown": self._get_decision_breakdown()
        }
    
    def _get_decision_breakdown(self) -> Dict:
        """Get breakdown of human decisions."""
        breakdown = defaultdict(int)
        for record in self.feedback_records:
            breakdown[record.human_decision.value] += 1
        return dict(breakdown)


# =============================================================================
# DEMONSTRATION CODE
# =============================================================================

def demonstrate_entity_resolution():
    """Demonstrate the entity resolution system with HITL and RLHF."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║         STAGE 5, 6 & 7: ENTITY RESOLUTION WITH HITL AND RLHF                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

SYSTEM OVERVIEW:
================
    ┌─────────────────────────────────────────────────────────────────┐
    │                  ENTITY RESOLUTION ENGINE                        │
    │  (Deterministic Rules + Similarity Scoring + Confidence)         │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
           ┌──────────────────┐          ┌──────────────────┐
           │   HIGH CONFIDENCE │          │  LOW CONFIDENCE   │
           │   AUTO-RESOLVED   │          │  NEEDS REVIEW     │
           └──────────────────┘          └──────────────────┘
                                                   │
                                                   ▼
                              ┌───────────────────────────────────┐
                              │     HUMAN-IN-THE-LOOP REVIEW       │
                              │  • Present conflicting records     │
                              │  • Show similarity scores          │
                              │  • Display historical context      │
                              │  • Suggest decision based on past  │
                              └───────────────────────────────────┘
                                                   │
                                                   ▼
                              ┌───────────────────────────────────┐
                              │        RLHF FEEDBACK LOOP          │
                              │  • Record human decisions          │
                              │  • Learn optimal thresholds        │
                              │  • Adjust attribute weights        │
                              │  • Reduce future ambiguity         │
                              └───────────────────────────────────┘


PYTHON CODE EXAMPLES:
=====================
""")
    
    print("""
# 1. Initialize the Entity Resolution System
# -------------------------------------------
feedback_store = FeedbackStore()
resolution_engine = VesselEntityResolutionEngine(feedback_store)
review_interface = HumanReviewInterface(feedback_store)


# 2. Entity Resolution Example - Exact IMO Match
# -----------------------------------------------
record1 = {
    "imo": 9528574,
    "mmsi": 636013854,
    "name": "MARCO",
    "flag": "LR",
    "length": 225,
    "builtYear": 2009
}
record2 = {
    "imo": 9528574,
    "mmsi": 636013855,  # Different MMSI
    "name": "MARCO POLO",
    "flag": "LR",
    "length": 225,
    "builtYear": 2009
}

result = resolution_engine.resolve_entities(record1, record2)
print(f"Match type: {result.match_type.value}")
print(f"Is same entity: {result.is_same_entity}")
print(f"Confidence: {result.confidence:.2%}")
print(f"Evidence: {result.resolution_evidence}")


# 3. Uncertain Match Requiring Review
# -------------------------------------
record_a = {
    "imo": 9528574,
    "mmsi": 636013854,
    "name": "MARCO",
    "length": 225
}
record_b = {
    "imo": 9528575,  # Different IMO
    "mmsi": 636013854,  # Same MMSI
    "name": "MARKO",  # Similar name
    "length": 226
}

result = resolution_engine.resolve_entities(record_a, record_b)
print(f"Requires review: {result.requires_review}")

if result.requires_review:
    # Queue for human review
    review_item = review_interface.queue_for_review(result, record_a, record_b)
    print(f"Review ID: {review_item.review_id}")
    print(f"Priority: {review_item.priority_score:.2f}")
    
    # Generate review context
    context = review_interface.generate_review_context(review_item.review_id)
    print(f"Suggested decision: {context['suggested_decision']}")


# 4. Human Review Submission
# ---------------------------
feedback = review_interface.submit_review(
    review_id=review_item.review_id,
    reviewer_id="expert_001",
    decision=ReviewDecision.CONFIRMED_SAME,
    confidence=0.85,
    reasoning="Same MMSI with minor name variation suggests same vessel",
    additional_evidence=["Checked external registry"],
    time_spent_seconds=120
)
print(f"Review submitted: {feedback.decision.value}")


# 5. RLHF Learning Statistics
# ----------------------------
stats = feedback_store.get_learning_statistics()
print(f"Total feedback records: {stats['total_feedback_records']}")
print(f"Learned thresholds: {stats['learned_thresholds']}")
print(f"Learned weights: {stats['learned_weights']}")


# 6. Similarity Scoring Details
# ------------------------------
def calculate_vessel_similarity(rec1, rec2):
    '''Calculate detailed similarity between vessel records'''
    
    scores = {}
    
    # Name similarity (using multiple methods)
    name1, name2 = rec1.get('name', ''), rec2.get('name', '')
    if name1 and name2:
        n1, n2 = name1.upper(), name2.upper()
        
        # Jaccard similarity
        chars1, chars2 = set(n1), set(n2)
        jaccard = len(chars1 & chars2) / len(chars1 | chars2)
        
        # Token overlap
        tokens1, tokens2 = set(n1.split()), set(n2.split())
        token_sim = len(tokens1 & tokens2) / len(tokens1 | tokens2) if tokens1 | tokens2 else 0
        
        scores['name'] = (jaccard + token_sim) / 2
    
    # Exact matches
    scores['imo'] = 1.0 if rec1.get('imo') == rec2.get('imo') else 0.0
    scores['mmsi'] = 1.0 if rec1.get('mmsi') == rec2.get('mmsi') else 0.0
    
    # Dimensional similarity (5% tolerance)
    for dim in ['length', 'width']:
        v1, v2 = rec1.get(dim), rec2.get(dim)
        if v1 and v2:
            diff = abs(v1 - v2) / max(v1, v2)
            scores[dim] = max(0, 1 - diff / 0.05)
    
    # Weighted average
    weights = {'imo': 0.3, 'name': 0.25, 'mmsi': 0.15, 'length': 0.15, 'width': 0.15}
    total = sum(weights[k] * scores.get(k, 0) for k in weights)
    
    return {'overall': total, 'attribute_scores': scores}

# Example usage
similarity = calculate_vessel_similarity(record1, record2)
print(f"Overall similarity: {similarity['overall']:.2%}")
""")
    
    # Live demonstration
    print("\n" + "="*60)
    print("LIVE DEMONSTRATION")
    print("="*60 + "\n")
    
    # Initialize system
    feedback_store = FeedbackStore()
    resolution_engine = VesselEntityResolutionEngine(feedback_store)
    review_interface = HumanReviewInterface(feedback_store)
    
    # Demo 1: Exact IMO match
    print("1. Testing Exact IMO Match:")
    r1 = {"imo": 9528574, "mmsi": 636013854, "name": "MARCO", "length": 225}
    r2 = {"imo": 9528574, "mmsi": 636013855, "name": "MARCO POLO", "length": 225}
    result = resolution_engine.resolve_entities(r1, r2)
    print(f"   Match type: {result.match_type.value}")
    print(f"   Same entity: {result.is_same_entity}")
    print(f"   Confidence: {result.confidence:.2%}")
    
    # Demo 2: Uncertain match
    print("\n2. Testing Uncertain Match:")
    r3 = {"imo": 1000000, "mmsi": 212222100, "name": "XF", "length": 399}
    r4 = {"imo": 1000000, "mmsi": 413209020, "name": "YONGXIN156", "length": 112}
    result2 = resolution_engine.resolve_entities(r3, r4)
    print(f"   Match type: {result2.match_type.value}")
    print(f"   Similarity: {result2.similarity_score.overall_score:.2%}")
    print(f"   Requires review: {result2.requires_review}")
    
    # Demo 3: Queue for review
    if result2.requires_review:
        print("\n3. Queueing for Human Review:")
        review_item = review_interface.queue_for_review(result2, r3, r4)
        print(f"   Review ID: {review_item.review_id}")
        print(f"   Priority: {review_item.priority_score:.2f}")
        
        context = review_interface.generate_review_context(review_item.review_id)
        print(f"   Matching attributes: {context['matching_attributes']}")
        print(f"   Conflicting attributes: {context['conflicting_attributes']}")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    demonstrate_entity_resolution()
