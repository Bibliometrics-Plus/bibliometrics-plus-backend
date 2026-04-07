"""Branch explorer page for Bibliometrics+."""

from __future__ import annotations

import pandas as pd
import sys
from pathlib import Path
import streamlit as st

# I add the repo root here because Streamlit executes pages like standalone
# scripts, which otherwise breaks absolute `app.*` imports.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.charts import area_line_chart, diverging_bar_chart, grouped_bar_chart
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
from app.services.chart_insights import summarize_diverging_bars, summarize_grouped_bars, summarize_time_series
from app.services.filters import render_global_filters
from app.services.formatters import format_int
from app.services.queries import get_branch_benchmark, get_branch_kpi_trend, get_branch_profile, get_ottawa_edi_priority
from app.styles.theme import apply_theme


configure_page("Bibliometrics+ | Branch Explorer")
apply_theme()
filters = render_global_filters()
render_app_shell("Branch Explorer")

render_page_header(
    "Branch Explorer",
    "Inspect an individual branch in more detail, including KPI trends and Ottawa community-context indicators where those records are available.",
)

if filters.is_all_branches:
    st.info("Select a branch in the sidebar to open the branch explorer.")
    st.stop()

render_filter_summary(
    [
        f"System: {filters.system}",
        f"Branch: {filters.branch}",
        f"Years: {filters.year_start} to {filters.year_end}",
    ]
)

profile_df = get_branch_profile(filters.branch, filters.system)
trend_df = get_branch_kpi_trend(filters.branch, filters.year_start, filters.year_end, filters.system)
benchmark_df = get_branch_benchmark(filters.branch, filters.year_start, filters.year_end, filters.system)

if profile_df.empty:
    st.warning("No branch profile could be found for the selected branch.")
    st.stop()

profile = profile_df.iloc[0]
card_1, card_2, card_3 = st.columns(3)
with card_1:
    st.metric("System", profile["system_name"])
with card_2:
    st.metric("City", profile["city"] or "Unknown")
with card_3:
    st.metric("Branch Code", profile["branch_code"] or "N/A")
st.caption(f"Address: {profile['address'] or 'Address unavailable in the source records.'}")

render_section_intro(
    "Branch Profile",
    f"{profile['name']} is represented under the {profile['system_name']} system. This section focuses on operating trends and the contextual detail currently linked to this branch.",
)

trend_long = pd.melt(
    trend_df,
    id_vars=["year"],
    value_vars=["circulation", "visits", "registrations"],
    var_name="metric_name",
    value_name="metric_value",
)
trend_long["metric_name"] = trend_long["metric_name"].str.title()
benchmark_delta_df = pd.DataFrame()
if not benchmark_df.empty:
    benchmark = benchmark_df.iloc[0]
    benchmark_delta_df = pd.DataFrame(
        [
            {
                "metric_name": "Circulation",
                "delta_value": benchmark["circulation"] - benchmark["avg_circulation"],
                "direction": "Above System Average" if benchmark["circulation"] >= benchmark["avg_circulation"] else "Below System Average",
            },
            {
                "metric_name": "Visits",
                "delta_value": benchmark["visits"] - benchmark["avg_visits"],
                "direction": "Above System Average" if benchmark["visits"] >= benchmark["avg_visits"] else "Below System Average",
            },
            {
                "metric_name": "Registrations",
                "delta_value": benchmark["registrations"] - benchmark["avg_registrations"],
                "direction": "Above System Average" if benchmark["registrations"] >= benchmark["avg_registrations"] else "Below System Average",
            },
        ]
    )

overview_tab, compare_tab, detail_tab = st.tabs(["Overview", "Compare", "Detail"])

with overview_tab:
    left_col, right_col = st.columns(2)
    with left_col:
        render_section_intro(
            "Branch KPI Trend",
            "Start with the broadest branch view: this trend shows how circulation changes over time across the selected years.",
        )
        if trend_df.empty:
            st.info("No branch KPI trend data is available for this branch.")
        else:
            st.altair_chart(
                area_line_chart(
                    trend_df,
                    x="year:O",
                    y="circulation:Q",
                    tooltip=["year", "circulation", "visits", "registrations"],
                    height=360,
                ),
                width="stretch",
            )
            render_chart_guide("The line tracks circulation across the selected years, so higher points mean stronger branch circulation.")
            render_chart_summary(
                summarize_time_series(
                    trend_df,
                    x_col="year",
                    y_col="circulation",
                    metric_label="Branch circulation",
                )
            )

    with right_col:
        render_section_intro(
            "Metric Mix by Year",
            "This matrix supports mid-level inspection by comparing circulation, visits, and registrations year by year.",
        )
        if trend_long.empty:
            st.info("No yearly KPI mix is available for this branch.")
        else:
            st.altair_chart(
                grouped_bar_chart(
                    trend_long,
                    x="year:O",
                    y="metric_value:Q",
                    color="metric_name:N",
                    tooltip=["year", "metric_name", "metric_value"],
                    height=360,
                    legend_title="KPI metric",
                ),
                width="stretch",
            )
            render_chart_guide("Each year now shows separate bars for circulation, visits, and registrations so the branch mix can be compared without decoding a color scale.")
            render_chart_summary(
                summarize_grouped_bars(
                    trend_long,
                    group_col="year",
                    category_col="metric_name",
                    value_col="metric_value",
                    value_label="activity",
                )
            )

with compare_tab:
    render_section_intro(
        "Branch vs System Average",
        "This comparison highlights whether the selected branch is performing above or below the average branch in the same system.",
    )
    if benchmark_df.empty:
        st.info("No benchmark comparison is available for this branch.")
    else:
        compare_cols = st.columns(3)
        with compare_cols[0]:
            st.metric("Branch Circulation", format_int(benchmark["circulation"]), delta=format_int(benchmark["circulation"] - benchmark["avg_circulation"]))
        with compare_cols[1]:
            st.metric("Branch Visits", format_int(benchmark["visits"]), delta=format_int(benchmark["visits"] - benchmark["avg_visits"]))
        with compare_cols[2]:
            st.metric("Branch Registrations", format_int(benchmark["registrations"]), delta=format_int(benchmark["registrations"] - benchmark["avg_registrations"]))
        st.altair_chart(
            diverging_bar_chart(
                benchmark_delta_df,
                x="delta_value:Q",
                y="metric_name:N",
                color_field="direction:N",
                positive_label="Above System Average",
                negative_label="Below System Average",
                tooltip=["metric_name", "direction", "delta_value"],
                height=260,
            ),
            width="stretch",
        )
        render_chart_guide("Bars to the right show the branch is above the system average, and bars to the left show it is below.")
        render_chart_summary(
            summarize_diverging_bars(
                benchmark_delta_df,
                label_col="metric_name",
                value_col="delta_value",
                direction_col="direction",
            )
        )

    if profile["system_name"] == "OPL":
        render_section_intro(
            "Ottawa EDI Context",
            "Ottawa branches can also be inspected through the available ward-level context indicators.",
        )
        ottawa_edi_df = get_ottawa_edi_priority(filters)
        if ottawa_edi_df.empty:
            st.info("No Ottawa EDI context row was found for this branch.")
        else:
            show_dataframe(ottawa_edi_df)
    else:
        st.caption("Ottawa-specific EDI context appears only for Ottawa Public Library branches.")

with detail_tab:
    render_section_intro(
        "Detailed Branch Values",
        "Use the detailed table for exact values after reviewing the branch summary and comparison views.",
    )
    if benchmark_df.empty:
        st.info("No benchmark comparison is available for this branch.")
    else:
        show_dataframe(benchmark_df)
render_footer()
