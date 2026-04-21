"""
Shared chart helpers for the Bibliometrics+ dashboard.

Using reusable Altair helpers keeps the visual language consistent and also
makes it easier to maintain accessibility choices such as color and tooltip
clarity in one place.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

from app.styles.theme import get_theme_tokens

SYSTEM_DOMAIN = ["TPL", "OPL", "Montreal", "Unassigned"]


def _system_range(theme: dict[str, str]) -> list[str]:
    return [theme["primary"], theme["secondary"], theme["accent"], "#94A3B8"]


def _comparison_range(theme: dict[str, str]) -> list[str]:
    return [theme["primary"], theme["accent"], theme["secondary"]]


def _base_chart(df: pd.DataFrame, height: int = 340) -> alt.Chart:
    """Create a base chart without chart-level config."""
    return alt.Chart(df).properties(height=height)


def _encoding_field_name(encoding: str) -> str:
    """Return the raw field name from an Altair shorthand encoding."""
    return encoding.split(":", 1)[0]


def _legend_title_from_encoding(encoding: str) -> str:
    """Generate a human-readable legend title from an encoding string."""
    field_name = _encoding_field_name(encoding)
    return field_name.replace("_", " ").title()


def _axis_title_from_encoding(encoding: str) -> str:
    """Generate a readable axis title from an encoding string."""
    return _legend_title_from_encoding(encoding)


def _apply_theme(chart: alt.Chart) -> alt.Chart:
    """Apply the shared visual configuration to the completed chart object."""
    theme = get_theme_tokens()
    return (
        chart.configure(
            background=theme["surface"],
        )
        .configure_axis(
            labelColor=theme["text"],
            titleColor=theme["text"],
            gridColor=theme["chart_grid"],
            domainColor=theme["border"],
            tickColor=theme["border"],
            labelFontSize=12,
            titleFontSize=13,
        )
        .configure_view(strokeOpacity=0, fill=theme["surface"])
        .configure_legend(labelColor=theme["text"], titleColor=theme["text"], labelFontSize=12, titleFontSize=13)
        .configure_title(color=theme["text"], fontSize=16)
    )


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    tooltip: list[str],
    color: str | None = None,
    sort: str = "-x",
    height: int = 340,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a consistent bar chart."""
    theme = get_theme_tokens()
    bar_color = color or theme["primary"]
    return _apply_theme(
        _base_chart(df, height=height).mark_bar(
            color=bar_color,
            cornerRadiusTopRight=6,
            cornerRadiusBottomRight=6,
        ).encode(
            x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
            y=alt.Y(y, sort=sort, title=y_title or _axis_title_from_encoding(y)),
            tooltip=tooltip,
        )
    )


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    tooltip: list[str],
    color: str | None = None,
    height: int = 340,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a consistent line chart with visible data points."""
    theme = get_theme_tokens()
    line_color = color or theme["secondary"]
    return _apply_theme(
        _base_chart(df, height=height).mark_line(
            color=line_color,
            strokeWidth=3,
            point=alt.OverlayMarkDef(filled=True, size=75, color=line_color),
        ).encode(
            x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
            y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
            tooltip=tooltip,
        )
    )


def grouped_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    tooltip: list[str],
    height: int = 340,
    legend_title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a grouped bar chart for side-by-side comparisons."""
    theme = get_theme_tokens()
    color_field = _encoding_field_name(color)
    color_range = _system_range(theme) if color_field == "system_name" else _comparison_range(theme)
    return _apply_theme(
        _base_chart(df, height=height).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
            x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
            y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
            color=alt.Color(
                color,
                legend=alt.Legend(title=legend_title or _legend_title_from_encoding(color)),
                scale=alt.Scale(range=color_range),
            ),
            tooltip=tooltip,
            xOffset=color,
        )
    )


def faceted_comparison_bar_chart(
    df: pd.DataFrame,
    metric_col: str,
    category_col: str,
    value_col: str,
    tooltip: list[str],
    height: int = 260,
    column_title: str = "Metric",
    y_title: str = "Library system",
) -> alt.Chart:
    """Build faceted horizontal bars so each metric keeps its own readable scale."""
    theme = get_theme_tokens()
    labels_df = df.copy()
    labels_df["__bm_value_label"] = labels_df[value_col].apply(
        lambda value: f"{value:,.0f}" if pd.notnull(value) else ""
    )
    color_scale = alt.Scale(domain=SYSTEM_DOMAIN, range=_system_range(theme))

    bars = _base_chart(labels_df, height=height).mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6).encode(
        x=alt.X(
            f"{value_col}:Q",
            title=None,
            axis=alt.Axis(format=",.0f"),
        ),
        y=alt.Y(
            f"{category_col}:N",
            title=y_title,
            sort=alt.EncodingSortField(field=value_col, order="descending"),
        ),
        color=alt.Color(
            f"{category_col}:N",
            scale=color_scale,
            legend=None,
        ),
        tooltip=tooltip,
    )
    labels = _base_chart(labels_df, height=height).mark_text(
        align="left",
        baseline="middle",
        dx=6,
        fontSize=11,
        color=theme["text"],
    ).encode(
        x=alt.X(f"{value_col}:Q", title=None),
        y=alt.Y(f"{category_col}:N", sort=alt.EncodingSortField(field=value_col, order="descending")),
        text=alt.Text("__bm_value_label:N"),
    )
    chart = (
        (bars + labels)
        .facet(
            column=alt.Column(
                f"{metric_col}:N",
                title=column_title,
                header=alt.Header(
                    titleOrient="bottom",
                    titleColor=theme["text"],
                    labelColor=theme["text"],
                    labelFontSize=13,
                ),
            )
        )
        .resolve_scale(x="independent")
        .properties(spacing=18)
    )
    return _apply_theme(chart)


def normalized_metric_dot_chart(
    df: pd.DataFrame,
    metric_col: str,
    category_col: str,
    value_col: str,
    tooltip: list[str],
    height: int = 220,
    column_title: str = "Coverage measure",
    y_title: str = "Library system",
) -> alt.Chart:
    """Build a per-measure normalized dot chart for mixed-unit comparisons."""
    theme = get_theme_tokens()
    chart_df = df.copy()
    chart_df["__bm_metric_max"] = chart_df.groupby(metric_col)[value_col].transform("max").replace(0, 1)
    chart_df["__bm_normalized_value"] = chart_df[value_col] / chart_df["__bm_metric_max"]
    chart_df["__bm_zero"] = 0.0
    chart_df["__bm_full"] = 1.0
    chart_df["__bm_value_label"] = chart_df[value_col].apply(
        lambda value: f"{value:,.1f}" if isinstance(value, float) and not float(value).is_integer() else f"{value:,.0f}"
    )
    color_scale = alt.Scale(domain=SYSTEM_DOMAIN, range=_system_range(theme))

    baseline = _base_chart(chart_df, height=height).mark_rule(color=theme["border"], strokeWidth=5).encode(
        x=alt.X("__bm_zero:Q", title="Relative strength within each measure", scale=alt.Scale(domain=[0, 1.05])),
        x2=alt.X2("__bm_full:Q"),
        y=alt.Y(f"{category_col}:N", title=y_title),
    )
    points = _base_chart(chart_df, height=height).mark_circle(size=220, stroke=theme["surface"], strokeWidth=2).encode(
        x=alt.X(
            "__bm_normalized_value:Q",
            title="Relative strength within each measure",
            axis=alt.Axis(format="%", tickCount=3),
            scale=alt.Scale(domain=[0, 1.05]),
        ),
        y=alt.Y(f"{category_col}:N", title=y_title),
        color=alt.Color(f"{category_col}:N", scale=color_scale, legend=None),
        tooltip=tooltip,
    )
    labels = _base_chart(chart_df, height=height).mark_text(
        align="left",
        baseline="middle",
        dx=8,
        fontSize=11,
        color=theme["text"],
    ).encode(
        x=alt.X("__bm_normalized_value:Q", title="Relative strength within each measure", scale=alt.Scale(domain=[0, 1.05])),
        y=alt.Y(f"{category_col}:N", title=y_title),
        text=alt.Text("__bm_value_label:N"),
    )
    chart = (
        (baseline + points + labels)
        .facet(
            column=alt.Column(
                f"{metric_col}:N",
                title=column_title,
                header=alt.Header(
                    titleOrient="bottom",
                    titleColor=theme["text"],
                    labelColor=theme["text"],
                    labelFontSize=13,
                ),
            )
        )
        .properties(spacing=18)
    )
    return _apply_theme(chart)


def area_line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    tooltip: list[str],
    color: str | None = None,
    height: int = 340,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a layered area + line chart for trend storytelling."""
    theme = get_theme_tokens()
    line_color = color or theme["primary"]
    area = _base_chart(df, height=height).mark_area(
        color=line_color,
        opacity=0.18,
        line={"color": line_color, "strokeWidth": 3},
    ).encode(
        x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
        y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
        tooltip=tooltip,
    )
    points = _base_chart(df, height=height).mark_circle(
        color=line_color,
        size=90,
        stroke=theme["surface"],
        strokeWidth=1.5,
    ).encode(
        x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
        y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
        tooltip=tooltip,
    )
    return _apply_theme(area + points)


def lollipop_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    tooltip: list[str],
    color: str | None = None,
    sort: str = "-x",
    height: int = 340,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a lollipop chart to make rankings feel lighter and more intentional."""
    theme = get_theme_tokens()
    mark_color = color or theme["primary"]
    rules = _base_chart(df, height=height).mark_rule(color=theme["border"], strokeWidth=2).encode(
        x=alt.value(0),
        x2=alt.X2(x),
        y=alt.Y(y, sort=sort, title=y_title or _axis_title_from_encoding(y)),
        tooltip=tooltip,
    )
    points = _base_chart(df, height=height).mark_circle(
        color=mark_color,
        size=180,
        stroke=theme["surface"],
        strokeWidth=1.5,
    ).encode(
        x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
        y=alt.Y(y, sort=sort, title=y_title or _axis_title_from_encoding(y)),
        tooltip=tooltip,
    )
    return _apply_theme(rules + points)


def heatmap_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    tooltip: list[str],
    scheme: str | None = None,
    height: int = 320,
    legend_title: str = "Higher values",
    show_values: bool = True,
    value_decimals: int = 0,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a heatmap for compact cross-category comparison."""
    theme = get_theme_tokens()
    chart_df = df.copy()
    value_field = _encoding_field_name(color)
    color_scale = (
        alt.Scale(scheme=scheme)
        if scheme
        else alt.Scale(range=[theme["surface_alt"], "#93C5FD", theme["primary_dark"]])
    )
    base = _base_chart(chart_df, height=height)
    heatmap = base.mark_rect(cornerRadius=6).encode(
        x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
        y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
        color=alt.Color(
            color,
            scale=color_scale,
            legend=alt.Legend(title=legend_title, gradientLength=140),
        ),
        tooltip=tooltip,
    )

    if show_values and value_field in chart_df.columns:
        format_spec = f",.{value_decimals}f" if value_decimals > 0 else ",.0f"
        chart_df["__bm_heatmap_label"] = chart_df[value_field].apply(
            lambda value: f"{value:{format_spec}}" if pd.notnull(value) else ""
        )
        numeric_values = pd.to_numeric(chart_df[value_field], errors="coerce")
        threshold = float(numeric_values.median()) if numeric_values.notna().any() else 0.0
        chart_df["__bm_heatmap_label_tone"] = numeric_values.apply(
            lambda value: "light" if pd.notnull(value) and value >= threshold else "dark"
        )
        base = _base_chart(chart_df, height=height)
        labels = base.mark_text(fontSize=11, fontWeight="bold").encode(
            x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
            y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
            text=alt.Text("__bm_heatmap_label:N"),
            color=alt.condition(
                "datum.__bm_heatmap_label_tone === 'light'",
                alt.value(theme["surface"]),
                alt.value(theme["text"]),
            ),
            tooltip=tooltip,
        )
        return _apply_theme(heatmap + labels)

    return _apply_theme(heatmap)


def diverging_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    tooltip: list[str],
    color_field: str,
    positive_label: str,
    negative_label: str,
    height: int = 320,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    """Build a diverging bar chart for above/below-average style comparisons."""
    theme = get_theme_tokens()
    return _apply_theme(
        _base_chart(df, height=height).mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6).encode(
            x=alt.X(x, title=x_title or _axis_title_from_encoding(x)),
            y=alt.Y(y, title=y_title or _axis_title_from_encoding(y)),
            color=alt.Color(
                color_field,
                scale=alt.Scale(
                    domain=[negative_label, positive_label],
                    range=[theme["danger"], theme["secondary"]],
                ),
                legend=alt.Legend(title="Direction"),
            ),
            tooltip=tooltip,
        )
    )
