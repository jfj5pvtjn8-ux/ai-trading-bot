"""
Advanced Zone Classification System

Categorizes liquidity zones by strength, type, time relevance, and quality.
Supports multi-timeframe zone aggregation and confidence scoring.

Key Features:
- Zone strength classification (weak/medium/strong)
- Zone type detection (support/resistance/supply/demand)
- Time relevance scoring (fresh/aged/stale)
- Quality assessment (high/medium/low)
- MTF zone aggregation and ranking
- Zone confluence detection
- Volume profile integration

This module provides the final layer of zone analysis before trading decisions.
"""

from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

from .enums import ZoneQuality, TimeRelevance, ZoneReaction
from .models import ClassifiedZone, LiquidityZone


# ============================================================================
# ZONE CLASSIFIER
# ============================================================================

class ZoneClassifier:
    """
    Professional zone classification and ranking system.
    
    Takes liquidity zones and classifies them by multiple criteria
    to identify the highest-quality zones for trading.
    
    Usage:
        classifier = ZoneClassifier()
        
        # Classify zones from liquidity map
        zones = liquidity_map.get_zones_for_timeframe("5m")
        classified = classifier.classify_zones(zones, current_price=50000.0)
        
        # Get highest-priority zones
        best_zones = classifier.get_top_zones(classified, max_zones=5)
    """
    
    def __init__(
        self,
        fresh_threshold_hours: int = 24,
        recent_threshold_days: int = 7,
        aged_threshold_days: int = 30,
        min_confidence: float = 0.5,
    ):
        """
        Initialize zone classifier.
        
        Args:
            fresh_threshold_hours: Hours for zone to be "fresh"
            recent_threshold_days: Days for zone to be "recent"
            aged_threshold_days: Days before zone is "stale"
            min_confidence: Minimum confidence to consider zone
        """
        self.fresh_threshold_hours = fresh_threshold_hours
        self.recent_threshold_days = recent_threshold_days
        self.aged_threshold_days = aged_threshold_days
        self.min_confidence = min_confidence
    
    # ========================================================================
    # MAIN CLASSIFICATION
    # ========================================================================
    
    def classify_zones(
        self,
        zones: List[Any],  # List[LiquidityZone]
        current_price: float,
        current_timestamp: Optional[int] = None,
    ) -> List[ClassifiedZone]:
        """
        Classify all zones with comprehensive scoring.
        
        Args:
            zones: List of LiquidityZone objects
            current_price: Current market price
            current_timestamp: Current timestamp (ms), or None for now
            
        Returns:
            List of ClassifiedZone objects with full classification
        """
        if current_timestamp is None:
            current_timestamp = int(datetime.now().timestamp() * 1000)
        
        classified_zones = []
        
        for zone in zones:
            if not zone.is_active:
                continue
            
            # Calculate days old
            zone_age_ms = current_timestamp - zone.created_ts
            days_old = zone_age_ms / (1000 * 60 * 60 * 24)
            
            # Calculate distance from current price
            zone_mid = (zone.price_low + zone.price_high) / 2
            distance_pct = abs(current_price - zone_mid) / current_price
            
            # Classify time relevance
            time_relevance = self._classify_time_relevance(days_old)
            
            # Determine last reaction (if tested)
            last_reaction = self._determine_last_reaction(zone)
            
            # Calculate quality
            quality = self._calculate_quality(
                zone, days_old, time_relevance, last_reaction
            )
            
            # Calculate confidence score
            confidence = self._calculate_confidence(
                zone, days_old, distance_pct, time_relevance
            )
            
            # Calculate priority score (for ranking)
            priority = self._calculate_priority(
                zone, confidence, distance_pct, time_relevance
            )
            
            # Calculate risk score
            risk = self._calculate_risk(
                zone, time_relevance, last_reaction, distance_pct
            )
            
            # Create classified zone
            classified = ClassifiedZone(
                zone_id=zone.zone_id,
                timeframe=zone.timeframe,
                zone_type=zone.zone_type,
                price_low=zone.price_low,
                price_high=zone.price_high,
                quality=quality,
                time_relevance=time_relevance,
                last_reaction=last_reaction,
                confidence_score=confidence,
                strength_score=priority / 100.0,  # Normalize priority to 0-1
                touch_count=zone.touch_count,
                successful_bounces=zone.touch_count if zone.is_active else 0,
                volume=zone.volume,
                created_ts=zone.created_ts,
                last_touch_ts=zone.last_touch_ts,
                confluence_count=zone.confluence_count,
                timeframes_present=[zone.timeframe],
                is_active=zone.is_active,
                is_invalidated=not zone.is_active,
            )
            
            # Only include zones meeting minimum confidence
            if confidence >= self.min_confidence:
                classified_zones.append(classified)
        
        return classified_zones
    
    # ========================================================================
    # TIME RELEVANCE
    # ========================================================================
    
    def _classify_time_relevance(self, days_old: float) -> TimeRelevance:
        """Classify how fresh/relevant a zone is."""
        hours_old = days_old * 24
        
        if hours_old < self.fresh_threshold_hours:
            return TimeRelevance.FRESH
        elif days_old < self.recent_threshold_days:
            return TimeRelevance.RECENT
        elif days_old < self.aged_threshold_days:
            return TimeRelevance.AGED
        else:
            return TimeRelevance.STALE
    
    # ========================================================================
    # REACTION ANALYSIS
    # ========================================================================
    
    def _determine_last_reaction(self, zone) -> ZoneReaction:
        """Determine how price reacted to this zone last time."""
        
        if zone.touch_count == 0:
            return ZoneReaction.PENDING
        
        # If zone is broken, it was a breakthrough
        if not zone.is_active:
            return ZoneReaction.BREAKTHROUGH
        
        # If touched multiple times and still active, it's been respected
        if zone.touch_count >= 2:
            return ZoneReaction.STRONG_BOUNCE
        
        # Single touch and still active
        if zone.touch_count == 1:
            return ZoneReaction.WEAK_BOUNCE
        
        return ZoneReaction.PENDING
    
    # ========================================================================
    # QUALITY ASSESSMENT
    # ========================================================================
    
    def _calculate_quality(
        self,
        zone,
        days_old: float,
        time_relevance: TimeRelevance,
        last_reaction: ZoneReaction,
    ) -> ZoneQuality:
        """Calculate overall zone quality."""
        
        # Broken zones are invalid
        if not zone.is_active:
            return ZoneQuality.INVALID
        
        # Score various factors
        score = 0
        
        # Strength factor (0-3 points)
        if zone.strength == "strong":
            score += 3
        elif zone.strength == "medium":
            score += 2
        else:
            score += 1
        
        # Time relevance (0-2 points)
        if time_relevance == TimeRelevance.FRESH:
            score += 2
        elif time_relevance == TimeRelevance.RECENT:
            score += 1
        
        # Reaction quality (0-2 points)
        if last_reaction == ZoneReaction.STRONG_BOUNCE:
            score += 2
        elif last_reaction == ZoneReaction.WEAK_BOUNCE:
            score += 1
        
        # Confluence (0-2 points)
        if zone.confluence_count >= 3:
            score += 2
        elif zone.confluence_count >= 2:
            score += 1
        
        # Volume (0-1 point)
        if zone.volume > 1000000:  # Adjust for your symbol
            score += 1
        
        # Classify based on total score
        if score >= 8:
            return ZoneQuality.EXCELLENT
        elif score >= 6:
            return ZoneQuality.GOOD
        elif score >= 4:
            return ZoneQuality.AVERAGE
        elif score >= 2:
            return ZoneQuality.POOR
        else:
            return ZoneQuality.INVALID
    
    # ========================================================================
    # CONFIDENCE SCORING
    # ========================================================================
    
    def _calculate_confidence(
        self,
        zone,
        days_old: float,
        distance_pct: float,
        time_relevance: TimeRelevance,
    ) -> float:
        """
        Calculate confidence score (0.0 to 1.0).
        
        Higher confidence = more reliable zone for trading.
        """
        confidence = 0.0
        
        # Base confidence from strength (0.0 to 0.3)
        strength_map = {"weak": 0.1, "medium": 0.2, "strong": 0.3}
        confidence += strength_map.get(zone.strength, 0.1)
        
        # Time relevance (0.0 to 0.25)
        time_map = {
            TimeRelevance.FRESH: 0.25,
            TimeRelevance.RECENT: 0.20,
            TimeRelevance.AGED: 0.10,
            TimeRelevance.STALE: 0.05,
        }
        confidence += time_map.get(time_relevance, 0.05)
        
        # Touch count - more touches = more confident (0.0 to 0.2)
        touch_bonus = min(0.2, zone.touch_count * 0.05)
        confidence += touch_bonus
        
        # Confluence - multi-TF agreement (0.0 to 0.15)
        confluence_bonus = min(0.15, zone.confluence_count * 0.05)
        confidence += confluence_bonus
        
        # Distance penalty - too far = less confident (0.0 to -0.1)
        if distance_pct > 0.05:  # More than 5% away
            distance_penalty = min(0.1, (distance_pct - 0.05) * 2)
            confidence -= distance_penalty
        
        # Volume factor (0.0 to 0.1)
        if zone.volume > 500000:  # Adjust for your symbol
            confidence += 0.1
        elif zone.volume > 100000:
            confidence += 0.05
        
        return max(0.0, min(1.0, confidence))
    
    # ========================================================================
    # PRIORITY SCORING (FOR RANKING)
    # ========================================================================
    
    def _calculate_priority(
        self,
        zone,
        confidence: float,
        distance_pct: float,
        time_relevance: TimeRelevance,
    ) -> float:
        """
        Calculate priority score (0.0 to 100.0) for ranking zones.
        
        Higher priority = should be considered first for trades.
        """
        priority = 0.0
        
        # Confidence is the biggest factor (0-40 points)
        priority += confidence * 40
        
        # Proximity to current price (0-25 points)
        # Closer zones are higher priority
        if distance_pct < 0.01:  # Within 1%
            priority += 25
        elif distance_pct < 0.02:  # Within 2%
            priority += 20
        elif distance_pct < 0.05:  # Within 5%
            priority += 15
        elif distance_pct < 0.10:  # Within 10%
            priority += 10
        else:
            priority += 5
        
        # Time relevance (0-15 points)
        time_scores = {
            TimeRelevance.FRESH: 15,
            TimeRelevance.RECENT: 12,
            TimeRelevance.AGED: 8,
            TimeRelevance.STALE: 4,
        }
        priority += time_scores.get(time_relevance, 4)
        
        # Zone strength (0-10 points)
        strength_scores = {"strong": 10, "medium": 7, "weak": 4}
        priority += strength_scores.get(zone.strength, 4)
        
        # Confluence bonus (0-10 points)
        priority += min(10, zone.confluence_count * 3)
        
        return min(100.0, priority)
    
    # ========================================================================
    # RISK SCORING
    # ========================================================================
    
    def _calculate_risk(
        self,
        zone,
        time_relevance: TimeRelevance,
        last_reaction: ZoneReaction,
        distance_pct: float,
    ) -> float:
        """
        Calculate risk score (0.0 to 1.0).
        
        Higher risk = more likely to fail.
        """
        risk = 0.0
        
        # Old zones are riskier (0.0 to 0.3)
        time_risk = {
            TimeRelevance.FRESH: 0.0,
            TimeRelevance.RECENT: 0.1,
            TimeRelevance.AGED: 0.2,
            TimeRelevance.STALE: 0.3,
        }
        risk += time_risk.get(time_relevance, 0.3)
        
        # Untested zones are riskier (0.0 to 0.2)
        if last_reaction == ZoneReaction.PENDING:
            risk += 0.2
        elif last_reaction == ZoneReaction.WEAK_BOUNCE:
            risk += 0.1
        
        # Weak zones are riskier (0.0 to 0.2)
        if zone.strength == "weak":
            risk += 0.2
        elif zone.strength == "medium":
            risk += 0.1
        
        # No confluence is riskier (0.0 to 0.15)
        if zone.confluence_count == 0:
            risk += 0.15
        elif zone.confluence_count == 1:
            risk += 0.10
        
        # Very far zones are riskier (0.0 to 0.15)
        if distance_pct > 0.10:
            risk += 0.15
        elif distance_pct > 0.05:
            risk += 0.10
        
        return min(1.0, risk)
    
    # ========================================================================
    # QUERY & RANKING
    # ========================================================================
    
    def get_top_zones(
        self,
        classified_zones: List[ClassifiedZone],
        max_zones: int = 5,
        min_quality: ZoneQuality = ZoneQuality.AVERAGE,
    ) -> List[ClassifiedZone]:
        """
        Get top N zones ranked by priority.
        
        Args:
            classified_zones: List of classified zones
            max_zones: Maximum number to return
            min_quality: Minimum quality threshold
            
        Returns:
            Top zones sorted by priority score
        """
        # Filter by quality
        quality_order = [
            ZoneQuality.EXCELLENT,
            ZoneQuality.GOOD,
            ZoneQuality.AVERAGE,
            ZoneQuality.POOR,
            ZoneQuality.INVALID,
        ]
        min_quality_idx = quality_order.index(min_quality)
        
        filtered = [
            z for z in classified_zones
            if quality_order.index(z.quality) <= min_quality_idx
        ]
        
        # Sort by priority score (descending)
        sorted_zones = sorted(filtered, key=lambda z: z.priority_score, reverse=True)
        
        return sorted_zones[:max_zones]
    
    def get_excellent_zones(
        self,
        classified_zones: List[ClassifiedZone]
    ) -> List[ClassifiedZone]:
        """Get only excellent quality zones."""
        return [z for z in classified_zones if z.quality == ZoneQuality.EXCELLENT]
    
    def get_low_risk_zones(
        self,
        classified_zones: List[ClassifiedZone],
        max_risk: float = 0.3,
    ) -> List[ClassifiedZone]:
        """Get zones with risk below threshold."""
        return [z for z in classified_zones if z.risk_score <= max_risk]
    
    def get_nearby_zones(
        self,
        classified_zones: List[ClassifiedZone],
        max_distance_pct: float = 0.05,
    ) -> List[ClassifiedZone]:
        """Get zones within distance threshold of current price."""
        return [
            z for z in classified_zones
            if z.distance_from_price_pct <= max_distance_pct
        ]
    
    def get_confluence_zones(
        self,
        classified_zones: List[ClassifiedZone],
        min_timeframes: int = 2,
    ) -> List[ClassifiedZone]:
        """Get zones with multi-timeframe confluence."""
        return [
            z for z in classified_zones
            if z.confluence_count >= min_timeframes
        ]
    
    def rank_by_risk_reward(
        self,
        classified_zones: List[ClassifiedZone],
    ) -> List[ClassifiedZone]:
        """
        Rank zones by risk/reward ratio.
        
        Lower risk + higher confidence = better risk/reward.
        """
        scored = []
        for zone in classified_zones:
            # Risk/reward score (0.0 to 1.0, higher = better)
            rr_score = zone.confidence_score * (1.0 - zone.risk_score)
            scored.append((zone, rr_score))
        
        # Sort by risk/reward score (descending)
        sorted_zones = sorted(scored, key=lambda x: x[1], reverse=True)
        
        return [zone for zone, _ in sorted_zones]
    
    # ========================================================================
    # ANALYSIS & REPORTING
    # ========================================================================
    
    def get_zone_summary(
        self,
        classified_zones: List[ClassifiedZone]
    ) -> Dict[str, Any]:
        """Get summary statistics for classified zones."""
        if not classified_zones:
            return {"total": 0}
        
        return {
            "total": len(classified_zones),
            "by_quality": {
                "excellent": len([z for z in classified_zones if z.quality == ZoneQuality.EXCELLENT]),
                "good": len([z for z in classified_zones if z.quality == ZoneQuality.GOOD]),
                "average": len([z for z in classified_zones if z.quality == ZoneQuality.AVERAGE]),
                "poor": len([z for z in classified_zones if z.quality == ZoneQuality.POOR]),
            },
            "by_time": {
                "fresh": len([z for z in classified_zones if z.time_relevance == TimeRelevance.FRESH]),
                "recent": len([z for z in classified_zones if z.time_relevance == TimeRelevance.RECENT]),
                "aged": len([z for z in classified_zones if z.time_relevance == TimeRelevance.AGED]),
                "stale": len([z for z in classified_zones if z.time_relevance == TimeRelevance.STALE]),
            },
            "avg_confidence": sum(z.confidence_score for z in classified_zones) / len(classified_zones),
            "avg_risk": sum(z.risk_score for z in classified_zones) / len(classified_zones),
            "avg_priority": sum(z.priority_score for z in classified_zones) / len(classified_zones),
            "with_confluence": len([z for z in classified_zones if z.confluence_count >= 2]),
        }
