"""
KPI page for Bibliometrics+.

This page focuses on branch-level operating metrics supported by the available
data sources.
"""

from __future__ import annotations

import sys
from pathlib import Path
import streamlit as st

# Streamlit loads this page as a top-level script, so I add the repository
# root to the import path before importing shared application modules.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.charts import area_line_chart, bar_chart, grouped_bar_chart
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
from app.services.chart_insights import summarize_grouped_bars, summarize_ranking, summarize_time_series
from app.services.filters import render_global_filters
from app.services.formatters import format_int
from app.services.queries import (
    get_circulation_trend,
    get_collection_format_distribution,
    get_kpi_snapshot,
    get_metric_breakdown,
    get_top_branches_by_metric,
)
from app.styles.theme import apply_theme


configure_page("Bibliometrics+ | KPI Dashboard")
apply_theme()
filters = render_global_filters()
render_app_shell("KPI Dashboard")

render_page_header(
    "KPI Analysis",
    "Track circulation, visits, registrations, and collection format distribution using the live operational records loaded into the platform.",
)
render_filter_summary(
    [
        f"System: {filters.system}",
        f"Branch: {filters.branch}",
        f"Years: {filters.year_start} to {filters.year_end}",
    ]
)

snapshot_df = get_kpi_snapshot(filters)
snapshot = snapshot_df.iloc[0]

metric_1, metric_2, metric_3, metric_4 = st.columns(4)
with metric_1:
    st.metric("Branches in Scope", format_int(snapshot["branches"]))
with metric_2:
    st.metric("Total Circulation", format_int(snapshot["total_circulation"]))
with metric_3:
    st.metric("Total Visits", format_int(snapshot["total_visits"]))
with metric_4:
    st.metric("Total Registrations", format_int(snapshot["total_registrations"]))

trend_df = get_circulation_trend(filters)
top_branches_df = get_top_branches_by_metric(filters, metric="circulation")
metric_breakdown_df = get_metric_breakdown(filters)
format_df = get_collection_format_distribution(filters)
rank_metric = st.selectbox(
    "Ranking Metric",
    ["circulation", "visits", "registrations"],
    format_func=lambda value: value.replace("_", " ").title(),
)
top_branches_df = get_top_branches_by_metric(filters, metric=rank_metric)

overview_tab, compare_tab, detail_tab = st.tabs(["Overview", "Compare", "Detail"])

with overview_tab:
    upper_left, upper_right = st.columns(2)
    with upper_left:
        render_section_intro(
            "Circulation Trend",
            "Start with the highest-level time task: this view emphasizes change over time in the selected scope.",
        )
        if trend_df.empty:
            st.info("No branch KPI circulation trend is available for the current filter selection.")
        else:
            st.altair_chart(
                area_line_chart(
                    trend_df,
                    x="year:O",
                    y="total_circulation:Q",
                    tooltip=["year", "total_circulation"],
                    height=360,
                ),
                width="stretch",
            )
            render_chart_guide("The filled area and line both show total circulation over time. Higher peaks mean stronger circulation in that year.")
            render_chart_summary(
                summarize_time_series(
                    trend_df,
                    x_col="year",
                    y_col="total_circulation",
                    metric_label="Circulation",
                )
            )

    with upper_right:
        render_section_intro(
            f"Top Branches by {rank_metric.replace('_', ' ').title()}",
            "This ranking supports comparison tasks by highlighting the strongest branches in the current scope.",
        )
        if top_branches_df.empty:
            st.info("No branch ranking is available for the current filter selection.")
        else:
            st.altair_chart(
                bar_chart(
                    top_branches_df,
                    x="metric_value:Q",
                    y="branch:N",
                    tooltip=["branch", "system_name", "metric_value"],
                    height=360,
                    x_title=rank_metric.replace("_", " ").title(),
                    y_title="Branch",
                ),
                width="stretch",
            )
            render_chart_guide("Longer bars mean stronger results for the selected ranking metric, so the leading branches are easy to compare directly.")
            render_chart_summary(
                summarize_ranking(
                    top_branches_df,
                    label_col="branch",
                    value_col="metric_value",
                    metric_label=rank_metric.replace("_", " "),
                    context_col="system_name",
                )
            )

with compare_tab:
    render_section_intro(
        "Operational Metric Comparison",
        "This matrix supports mid-level comparison by showing where circulation, visits, and registrations are most concentrated.",
    )
    if metric_breakdown_df.empty:
        st.info("No KPI comparison data is available for the current selection.")
    else:
        st.altair_chart(
            grouped_bar_chart(
                metric_breakdown_df,
                x="system_name:N",
                y="metric_value:Q",
                color="metric_name:N",
                tooltip=["system_name", "metric_name", "metric_value"],
                height=360,
                legend_title="KPI metric",
            ),
            width="stretch",
        )
        render_chart_guide("Each system now shows one bar per KPI metric. This makes the comparison readable without relying on color intensity.")
        render_chart_summary(
            summarize_grouped_bars(
                metric_breakdown_df,
                group_col="system_name",
                category_col="metric_name",
                value_col="metric_value",
                value_label="total KPI activity",
            )
        )

    render_section_intro(
        "Collection Format Distribution",
        "This ranking provides a secondary structural view of the collection through the loaded format records.",
    )
    if format_df.empty:
        st.info("No collection format data is available for the current filter selection.")
    else:
        st.altair_chart(
            bar_chart(
                format_df.head(15),
                x="item_count:Q",
                y="format:N",
                tooltip=["format", "item_count"],
                color="#0F9D76",
                height=420,
                x_title="Items",
                y_title="Format",
            ),
            width="stretch",
        )
        render_chart_guide("Longer bars mean more items in the selected scope. The green color only highlights the ranking.")
        render_chart_summary(
            summarize_ranking(
                format_df.head(15),
                label_col="format",
                value_col="item_count",
                metric_label="items",
            )
        )

with detail_tab:
    render_section_intro(
        "Detailed KPI Table",
        "Use the detailed table for inspection, filtering, and exact value lookup after the overview and comparison views.",
    )
    if top_branches_df.empty:
        st.info("No branch table is available for the current selection.")
    else:
        show_dataframe(top_branches_df)
render_footer()
