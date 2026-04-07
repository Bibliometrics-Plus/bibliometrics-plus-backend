"""
Formatting helpers for numbers and labels.
"""

from __future__ import annotations

import math


def format_int(value: int | float | None) -> str:
    """Format whole-number metrics safely."""
    if value is None:
        return "0"
    return f"{int(value):,}"


def format_pct(numerator: int | float, denominator: int | float) -> str:
    """Convert a fraction into a readable percentage string."""
    if not denominator:
        return "0.0%"
    return f"{(numerator / denominator) * 100:.1f}%"


def format_number(value: int | float | None, decimals: int = 1) -> str:
    """Format a generic number safely."""
    if value is None:
        return "0"
    if isinstance(value, float) and math.isnan(value):
        return "0"
    return f"{value:,.{decimals}f}"


def format_distance_km(value: int | float | None) -> str:
    """Format a distance value in kilometers."""
    if value is None:
        return "0.0 km"
    if isinstance(value, float) and math.isnan(value):
        return "0.0 km"
    return f"{value:,.1f} km"
