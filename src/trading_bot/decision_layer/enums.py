"""
Enumerations for decision layer components.

Centralized location for all enum types used across the decision layer.
"""

from enum import Enum


# ============================================================================
# TREND ENUMS
# ============================================================================

class TrendDirection(Enum):
    """Trend direction states."""
    STRONG_BULLISH = "strong_bullish"
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"
    STRONG_BEARISH = "strong_bearish"


class TrendStrength(Enum):
    """Trend strength classification."""
    VERY_WEAK = "very_weak"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


# ============================================================================
# ZONE CLASSIFICATION ENUMS
# ============================================================================

class ZoneQuality(Enum):
    """Overall quality assessment of a zone."""
    EXCELLENT = "excellent"  # High confidence, strong, fresh, good volume
    GOOD = "good"            # Solid zone, decent confidence
    AVERAGE = "average"      # Acceptable but not ideal
    POOR = "poor"            # Weak, old, or low volume
    INVALID = "invalid"      # Broken or no longer relevant


class TimeRelevance(Enum):
    """How recent/relevant a zone is."""
    FRESH = "fresh"      # Less than 24 hours old
    RECENT = "recent"    # 1-7 days old
    AGED = "aged"        # 7-30 days old
    STALE = "stale"      # Over 30 days old


class ZoneReaction(Enum):
    """How price reacted to the zone."""
    STRONG_BOUNCE = "strong_bounce"      # Clear rejection
    WEAK_BOUNCE = "weak_bounce"          # Hesitation
    BREAKTHROUGH = "breakthrough"        # Broken through
    PENDING = "pending"                  # Not yet tested
