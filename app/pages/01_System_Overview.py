"""System overview page for Bibliometrics+."""

from __future__ import annotations

import pandas as pd
import sys
from pathlib import Path
import streamlit as st

# Streamlit executes each page file directly, so I add the repo root first
# before importing shared `app.*` modules.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.charts import grouped_bar_chart, heatmap_chart, lollipop_chart
from app.components.layout import (
    configure_page,
    render_app_shell,
    render_chart_guide,
    render_chart_summary,
    render_filter_summary,
    render_footer,
    render_page_header,
    render_section_intro,
)
from app.components.tables import show_dataframe
from app.services.chart_insights import summarize_grouped_bars, summarize_heatmap, summarize_ranking
from app.services.filters import render_global_filters
from app.services.formatters import format_int, format_pct
from app.services.queries import get_system_coverage
from app.styles.theme import apply_theme


configure_page("Bibliometrics+ | System Overview")
apply_theme()
filters = render_global_filters()
render_app_shell("System Overview")

render_page_header(
    "System Overview",
    "Compare the real operational coverage of Toronto, Montreal, and Ottawa before drilling into individual KPI or EDI pages.",
)
render_filter_summary(
    [
        "Comparisons are presented at the system level to maintain consistency across cities.",
        "System coverage varies by source and by metric.",
        "KPI depth and EDI context are not distributed equally across all systems.",
    ]
)

coverage_df = get_system_coverage()
coverage_df["accessibility_share"] = coverage_df.apply(
    lambda row: format_pct(row["items_with_accessibility"], row["collection_items"]),
    axis=1,
)
coverage_df["accessibility_share_value"] = coverage_df.apply(
    lambda row: (row["items_with_accessibility"] / row["collection_items"] * 100) if row["collection_items"] else 0,
    axis=1,
)

card_1, card_2, card_3 = st.columns(3)
with card_1:
    st.metric("Systems Compared", format_int(len(coverage_df)))
with card_2:
    st.metric("Total Libraries", format_int(coverage_df["libraries"].sum()))
with card_3:
    st.metric("KPI Coverage Range", f"{int(coverage_df['min_year'].min())} - {int(coverage_df['max_year'].max())}")

top_left, top_right = st.columns(2)
with top_left:
    render_section_intro(
        "Library Footprint",
        "This ranking compares how many library locations are represented per system in the current data environment.",
    )
    st.altair_chart(
        lollipop_chart(
            coverage_df,
            x="libraries:Q",
            y="system_name:N",
            tooltip=["system_name", "libraries"],
        ),
        width="stretch",
    )
    render_chart_guide("Each dot marks one library system. Farther-right dots mean more library locations, and the blue color is only highlighting the ranking.")
    render_chart_summary(
        summarize_ranking(
            coverage_df,
            label_col="system_name",
            value_col="libraries",
            metric_label="libraries",
        )
    )

with top_right:
    render_section_intro(
        "Collection Footprint",
        "Collection coverage is strongest for Montreal in the current data environment, which supports deeper collection-level analysis for that system.",
    )
    st.altair_chart(
        lollipop_chart(
            coverage_df,
            x="collection_items:Q",
            y="system_name:N",
            tooltip=["system_name", "collection_items", "items_with_accessibility"],
            color="#0F9D76",
        ),
        width="stretch",
    )
    render_chart_guide("Farther-right dots mean more collection records in the loaded data. The green color highlights the ranking only; it does not represent a second metric.")
    render_chart_summary(
        summarize_ranking(
            coverage_df,
            label_col="system_name",
            value_col="collection_items",
            metric_label="collection items",
        )
    )

st.info(
    "Coverage note: Toronto and Montreal currently have the strongest branch KPI history, while Ottawa currently has the strongest branch-to-community EDI context."
)

long_df = pd.melt(
    coverage_df[["system_name", "total_circulation", "total_visits", "total_registrations"]],
    id_vars=["system_name"],
    var_name="metric_name",
    value_name="metric_value",
)
long_df["metric_name"] = long_df["metric_name"].map(
    {
        "total_circulation": "Circulation",
        "total_visits": "Visits",
        "total_registrations": "Registrations",
    }
)

render_section_intro(
    "Operational Coverage Comparison",
    "This matrix provides a compact comparison of operational scale across systems.",
)
st.altair_chart(
    grouped_bar_chart(
        long_df,
        x="metric_name:N",
        y="metric_value:Q",
        color="system_name:N",
        tooltip=["system_name", "metric_name", "metric_value"],
        height=360,
        legend_title="Library system",
    ),
    width="stretch",
)
render_chart_guide("Each metric category groups one bar per system, so users can compare Toronto, Ottawa, and Montreal directly without decoding color intensity.")
render_chart_summary(
    summarize_grouped_bars(
        long_df,
        group_col="metric_name",
        category_col="system_name",
        value_col="metric_value",
        value_label="total activity",
    )
)

coverage_matrix_df = pd.melt(
    coverage_df[
        [
            "system_name",
            "kpi_years",
            "distinct_formats",
            "accessibility_share_value",
        ]
    ],
    id_vars=["system_name"],
    var_name="coverage_measure",
    value_name="coverage_value",
)
coverage_matrix_df["coverage_measure"] = coverage_matrix_df["coverage_measure"].map(
    {
        "kpi_years": "KPI Years",
        "distinct_formats": "Distinct Formats",
        "accessibility_share_value": "Accessibility Coverage %",
    }
)

render_section_intro(
    "Coverage Profile Matrix",
    "This view highlights where each system is strongest in terms of historical coverage, format breadth, and accessibility representation.",
)
st.altair_chart(
    heatmap_chart(
        coverage_matrix_df,
        x="coverage_measure:N",
        y="system_name:N",
        color="coverage_value:Q",
        tooltip=["system_name", "coverage_measure", "coverage_value"],
        height=220,
        legend_title="Higher coverage in this view",
    ),
    width="stretch",
)
render_chart_guide("This matrix now uses darker cells for higher values and prints the exact number inside each cell, so the color supports the result instead of replacing it.")
render_chart_summary(
    summarize_heatmap(
        coverage_matrix_df,
        row_col="system_name",
        column_col="coverage_measure",
        value_col="coverage_value",
        value_label="coverage",
    )
)

render_section_intro(
    "System Coverage Table",
    "This table provides a detailed view of system coverage, year ranges, and collection depth by city.",
)
show_dataframe(
    coverage_df[
        [
            "system_name",
            "libraries",
            "kpi_years",
            "min_year",
            "max_year",
            "total_circulation",
            "total_visits",
            "total_registrations",
            "collection_items",
            "items_with_year",
            "items_with_accessibility",
            "accessibility_share",
            "distinct_formats",
        ]
    ]
)
render_footer()
