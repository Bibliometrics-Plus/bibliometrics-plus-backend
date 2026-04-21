"""
Home page for Bibliometrics+.

This page introduces the project and summarizes the data currently loaded into
the system.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Streamlit runs page files like standalone scripts, so I add the repository
# root to `sys.path` here to make imports such as `app.services...` resolve
# consistently when the dashboard is launched from the terminal.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.charts import bar_chart, grouped_bar_chart
from app.components.layout import (
    configure_page,
    render_app_shell,
    render_chart_guide,
    render_chart_summary,
    render_footer,
    render_hero,
    render_page_header,
    render_section_intro,
)
from app.components.tables import show_dataframe
from app.services.filters import render_global_filters
from app.services.formatters import format_int
from app.services.chart_insights import summarize_grouped_bars, summarize_ranking
from app.services.queries import get_home_summary, get_system_comparison_chart, get_system_coverage
from app.styles.theme import apply_theme


configure_page("Bibliometrics+")
apply_theme()
filters = render_global_filters()
render_app_shell("Home")

render_hero(
    "Bibliometrics+",
    (
        "An AI & EDI-driven library usage analytics platform that combines "
        "branch KPIs, collection analytics, accessibility indicators, and "
        "community-context analysis using live institutional data across Toronto, Montreal, and Ottawa."
    ),
    chips=[
        "Real data only",
        "AI-supported insights",
        "EDI-aware analytics",
        "Cross-city coverage",
    ],
)

render_page_header(
    "Executive Overview",
    "A high-level summary of the data, system coverage, and analytical scope available across the application.",
)

summary_df = get_home_summary()
summary = summary_df.iloc[0]

metric_1, metric_2, metric_3 = st.columns(3)
with metric_1:
    st.metric("Libraries Loaded", format_int(summary["libraries"]))
with metric_2:
    st.metric("Collection Items", format_int(summary["collection_items"]))
with metric_3:
    st.metric("Branch KPI Rows", format_int(summary["branch_kpi_rows"]))

metric_4, metric_5, metric_6 = st.columns(3)
with metric_4:
    st.metric("Circulation Rows", format_int(summary["circulation_rows"]))
with metric_5:
    st.metric("Accessible Items", format_int(summary["accessibility_items"]))
with metric_6:
    st.metric("Branch Context Rows", format_int(summary["ottawa_edi_rows"]))

comparison_df = get_system_comparison_chart()
coverage_df = get_system_coverage()
coverage_matrix_df = coverage_df.melt(
    id_vars=["system_name"],
    value_vars=["libraries", "kpi_years", "distinct_formats"],
    var_name="coverage_measure",
    value_name="coverage_value",
)
coverage_matrix_df["coverage_measure"] = coverage_matrix_df["coverage_measure"].map(
    {
        "libraries": "Libraries",
        "kpi_years": "KPI Years",
        "distinct_formats": "Formats",
    }
)

overview_tab, compare_tab, detail_tab = st.tabs(["Overview", "Compare", "Detail"])

with overview_tab:
    left_col, right_col = st.columns((1.25, 1))
    with left_col:
        render_section_intro(
            "System-Level Operating Scale",
            "Start with the broadest view: this ranking compares the circulation volume currently represented for each system.",
        )
        st.altair_chart(
            bar_chart(
                comparison_df,
                x="total_circulation:Q",
                y="system_name:N",
                tooltip=["system_name", "total_circulation"],
                height=320,
                color="#1C7ED6",
                x_title="Total circulation",
                y_title="Library system",
            ),
            width="stretch",
        )
        render_chart_guide("Longer bars mean higher total circulation. This blue color is only a highlight color here, not a separate category.")
        render_chart_summary(
            summarize_ranking(
                comparison_df,
                label_col="system_name",
                value_col="total_circulation",
                metric_label="total circulation",
            )
        )

    with right_col:
        render_section_intro(
            "Analytical Coverage",
            (
                "The application supports high-level system comparison, mid-level KPI and EDI exploration, "
                "and branch-level inspection using the currently available data."
            ),
        )
        st.markdown(
            """
            - `System Overview` compares Toronto, Montreal, and Ottawa using live coverage.
            - `KPI Analysis` focuses on branch-level circulation, visits, registrations, and trends.
            - `EDI Analytics` compares accessibility, publication year, and format indicators across all three systems, with deeper branch context where available.
            - `Branch Explorer` supports branch-level inspection.
            - `AI Insights` summarizes filtered query results.
            """
        )

with compare_tab:
    render_section_intro(
        "Coverage Matrix",
        "This matrix supports comparison tasks by showing which systems are stronger in terms of library footprint, KPI history, and format breadth.",
    )
    st.altair_chart(
        grouped_bar_chart(
            coverage_matrix_df,
            x="coverage_measure:N",
            y="coverage_value:Q",
            color="system_name:N",
            tooltip=["system_name", "coverage_measure", "coverage_value"],
            height=240,
            legend_title="Library system",
        ),
        width="stretch",
    )
    render_chart_guide("Each coverage category now groups one bar per system, so the comparison reads directly from bar height instead of color shading.")
    render_chart_summary(
        summarize_grouped_bars(
            coverage_matrix_df,
            group_col="coverage_measure",
            category_col="system_name",
            value_col="coverage_value",
            value_label="coverage",
        )
    )

with detail_tab:
    render_section_intro(
        "Live Data Coverage",
        "Use the detailed table for inspection, filtering, and validation of the coverage shown at the overview and comparison levels.",
    )
    show_dataframe(coverage_df)
render_footer()
