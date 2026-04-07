"""Deterministic chart summaries used beneath dashboard visualizations."""

from __future__ import annotations

import math

import pandas as pd

from app.services.formatters import format_int, format_number


def _safe_label(value: object) -> str:
    """Return a readable label for chart text."""
    if value is None:
        return "Unknown"
    text = str(value).strip()
    return text or "Unknown"


def _format_value(value: object, decimals: int = 0) -> str:
    """Format chart values safely for short summaries."""
    if value is None:
        return "0"
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)

    if math.isnan(numeric_value):
        return "0"
    if decimals == 0:
        return format_int(numeric_value)
    return format_number(numeric_value, decimals=decimals)


def summarize_ranking(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    metric_label: str,
    decimals: int = 0,
    context_col: str | None = None,
) -> str:
    """Summarize a descending ranking chart."""
    if df.empty:
        return "No data is available for this ranking in the current filtered view."

    ranked = df.sort_values(value_col, ascending=False).reset_index(drop=True)
    top_row = ranked.iloc[0]
    top_label = _safe_label(top_row[label_col])
    top_value = _format_value(top_row[value_col], decimals=decimals)
    summary = f"{top_label} is highest with {top_value} {metric_label.lower()}."

    if context_col and context_col in ranked.columns:
        summary = f"{top_label} in {_safe_label(top_row[context_col])} is highest with {top_value} {metric_label.lower()}."

    if len(ranked) > 1:
        second_row = ranked.iloc[1]
        gap_value = float(top_row[value_col]) - float(second_row[value_col])
        gap = _format_value(gap_value, decimals=decimals)
        summary += f" It leads {_safe_label(second_row[label_col])} by {gap}."

    return summary


def summarize_time_series(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    metric_label: str,
    decimals: int = 0,
) -> str:
    """Summarize a single-metric time series."""
    if df.empty:
        return "No time-series data is available for this view."

    ordered = df.sort_values(x_col).reset_index(drop=True)
    first_row = ordered.iloc[0]
    last_row = ordered.iloc[-1]
    peak_row = ordered.loc[ordered[y_col].idxmax()]
    start_value = float(first_row[y_col])
    end_value = float(last_row[y_col])
    delta_value = end_value - start_value
    direction = "increased" if delta_value >= 0 else "decreased"
    delta = _format_value(abs(delta_value), decimals=decimals)
    peak_value = _format_value(peak_row[y_col], decimals=decimals)

    return (
        f"{metric_label} {direction} by {delta} from {_safe_label(first_row[x_col])} "
        f"to {_safe_label(last_row[x_col])}. The peak appears in {_safe_label(peak_row[x_col])} "
        f"at {peak_value}."
    )


def summarize_heatmap(
    df: pd.DataFrame,
    row_col: str,
    column_col: str,
    value_col: str,
    value_label: str,
    decimals: int = 0,
) -> str:
    """Summarize a heatmap by calling out its strongest and weakest cells."""
    if df.empty:
        return "No comparison values are available for this matrix."

    ranked = df.sort_values(value_col, ascending=False).reset_index(drop=True)
    top_row = ranked.iloc[0]
    bottom_row = ranked.iloc[-1]

    top_value = _format_value(top_row[value_col], decimals=decimals)
    bottom_value = _format_value(bottom_row[value_col], decimals=decimals)

    return (
        f"The strongest cell is {_safe_label(top_row[row_col])} for {_safe_label(top_row[column_col])} "
        f"at {top_value} {value_label.lower()}. The lightest cell is "
        f"{_safe_label(bottom_row[row_col])} for {_safe_label(bottom_row[column_col])} at "
        f"{bottom_value}."
    )


def summarize_grouped_bars(
    df: pd.DataFrame,
    group_col: str,
    category_col: str,
    value_col: str,
    value_label: str,
    decimals: int = 0,
) -> str:
    """Summarize grouped bar charts by identifying the overall leader and top segment."""
    if df.empty:
        return "No grouped comparison data is available for the current view."

    totals = df.groupby(group_col, as_index=False)[value_col].sum()
    leading_group = totals.sort_values(value_col, ascending=False).iloc[0]
    top_segment = df.sort_values(value_col, ascending=False).iloc[0]

    return (
        f"{_safe_label(leading_group[group_col])} has the largest combined total in this view. "
        f"The strongest single bar is {_safe_label(top_segment[category_col])} for "
        f"{_safe_label(top_segment[group_col])} at {_format_value(top_segment[value_col], decimals=decimals)} "
        f"{value_label.lower()}."
    )


def summarize_diverging_bars(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    direction_col: str,
    decimals: int = 0,
) -> str:
    """Summarize above/below benchmark charts."""
    if df.empty:
        return "No benchmark comparison is available for this branch."

    ranked = df.copy()
    ranked["__abs_value"] = ranked[value_col].abs()
    strongest = ranked.sort_values("__abs_value", ascending=False).iloc[0]
    return (
        f"{_safe_label(strongest[label_col])} shows the largest gap versus the system average. "
        f"It is {_safe_label(strongest[direction_col]).lower()} by "
        f"{_format_value(abs(float(strongest[value_col])), decimals=decimals)}."
    )
