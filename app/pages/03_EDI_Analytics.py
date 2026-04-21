"""EDI analytics page for Bibliometrics+."""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import streamlit as st

# This keeps `app.*` imports stable when Streamlit runs the page file directly.
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
from app.services.queries import (
    get_accessibility_distribution,
    get_collection_format_distribution,
    get_ottawa_edi_priority,
    get_publication_year_distribution,
    get_system_coverage,
    get_toronto_neighbourhood_context,
)
from app.styles.theme import apply_theme


def _top_non_null(df: pd.DataFrame, column: str, limit: int = 15) -> pd.DataFrame:
    """Return the highest rows for a context indicator."""
    if df.empty or column not in df.columns:
        return pd.DataFrame()
    return df.dropna(subset=[column]).sort_values(column, ascending=False).head(limit).copy()


def _render_ranked_context_chart(
    df: pd.DataFrame,
    *,
    title: str,
    description: str,
    x_field: str,
    x_title: str,
    tooltip: list[str],
    summary_metric_label: str,
    guide_text: str,
    color: str = "#2563EB",
    decimals: int = 1,
    note: str | None = None,
) -> None:
    """Render a reusable ranked context chart with guide and summary."""
    render_section_intro(title, description)
    if df.empty:
        st.info("No data is available for this context view in the current filter selection.")
        return

    st.altair_chart(
        bar_chart(
            df,
            x=f"{x_field}:Q",
            y="branch_name:N",
            tooltip=tooltip,
            color=color,
            height=400,
            x_title=x_title,
            y_title="Branch",
        ),
        width="stretch",
    )
    if note:
        st.caption(note)
    render_chart_guide(guide_text)
    render_chart_summary(
        summarize_ranking(
            df,
            label_col="branch_name",
            value_col=x_field,
            metric_label=summary_metric_label,
            decimals=decimals,
        )
    )


configure_page("Bibliometrics+ | EDI Analytics")
apply_theme()
filters = render_global_filters()
render_app_shell("EDI Analytics")

render_page_header(
    "EDI Analytics",
    "Examine accessibility, collection age, format diversity, and community-context indicators across Toronto, Montreal, and Ottawa.",
)
render_filter_summary(
    [
        f"System: {filters.system}",
        f"Branch: {filters.branch}",
        f"Years: {filters.year_start} to {filters.year_end}",
        "Collection-based equity indicators are available across all three systems, while Ottawa includes the richest branch-level community-context layer in the current data environment.",
    ]
)

if filters.system == "All Systems":
    st.info(
        "This page begins with cross-city equity indicators for Toronto, Montreal, and Ottawa, then adds the deeper branch-to-neighbourhood context currently available for Toronto and Ottawa."
    )
elif filters.system == "OPL":
    st.info(
        "Ottawa includes both collection-based equity indicators and branch-level community-context indicators in the current data environment."
    )
elif filters.system == "TPL":
    st.info(
        "Toronto includes both collection-based equity indicators and branch-level neighbourhood context through the TPL neighbourhood profile table."
    )
else:
    st.info(
        "This view emphasizes collection-based equity indicators for the selected system. Montreal is currently strongest on collection-based EDI coverage, while Toronto and Ottawa also include branch-linked context datasets."
    )

access_df = get_accessibility_distribution(filters)
year_df = get_publication_year_distribution(filters)
format_df = get_collection_format_distribution(filters)
toronto_context_df = get_toronto_neighbourhood_context(filters) if filters.system in {"All Systems", "TPL"} else pd.DataFrame()
ottawa_edi_df = get_ottawa_edi_priority(filters) if filters.system in {"All Systems", "OPL"} else pd.DataFrame()
system_coverage_df = get_system_coverage()
system_coverage_df["accessibility_share_value"] = system_coverage_df.apply(
    lambda row: (row["items_with_accessibility"] / row["collection_items"] * 100) if row["collection_items"] else 0,
    axis=1,
)
system_coverage_df["publication_year_share_value"] = system_coverage_df.apply(
    lambda row: (row["items_with_year"] / row["collection_items"] * 100) if row["collection_items"] else 0,
    axis=1,
)
cross_city_indicator_df = system_coverage_df.melt(
    id_vars=["system_name"],
    value_vars=["accessibility_share_value", "publication_year_share_value", "distinct_formats"],
    var_name="indicator",
    value_name="indicator_value",
)
cross_city_indicator_df["indicator"] = cross_city_indicator_df["indicator"].map(
    {
        "accessibility_share_value": "Accessibility Coverage %",
        "publication_year_share_value": "Publication Year Coverage %",
        "distinct_formats": "Distinct Formats",
    }
)
overview_tab, compare_tab, detail_tab = st.tabs(["Overview", "Compare", "Detail"])

with overview_tab:
    top_left, top_right = st.columns(2)
    with top_left:
        if filters.system == "All Systems":
            render_section_intro(
                "Accessibility Coverage by System",
                "This view compares accessibility representation across all three library systems using the currently available collection data.",
            )
            st.altair_chart(
                bar_chart(
                    system_coverage_df,
                    x="accessibility_share_value:Q",
                    y="system_name:N",
                    tooltip=["system_name", "items_with_accessibility", "collection_items", "accessibility_share_value"],
                    color="#0F9D76",
                    height=320,
                    x_title="Accessibility coverage %",
                    y_title="Library system",
                ),
                width="stretch",
            )
            render_chart_guide("Longer bars mean a higher share of collection items with accessibility metadata. The green color simply highlights the ranking.")
            render_chart_summary(
                summarize_ranking(
                    system_coverage_df,
                    label_col="system_name",
                    value_col="accessibility_share_value",
                    metric_label="accessibility coverage percentage",
                    decimals=1,
                )
            )
        else:
            render_section_intro(
                "Accessible Format Coverage",
                "This ranking supports overview tasks by showing which accessibility-related formats are most visible in the selected scope.",
            )
            if access_df.empty:
                st.info("No accessibility format data is available for the current filter selection.")
            else:
                st.altair_chart(
                    bar_chart(
                        access_df,
                        x="item_count:Q",
                        y="accessibility_format:N",
                        tooltip=["accessibility_format", "item_count"],
                        color="#0F9D76",
                        height=360,
                        x_title="Items",
                        y_title="Accessibility format",
                    ),
                    width="stretch",
                )
                render_chart_guide("Longer bars mean those accessibility-related formats appear more often in the selected collection scope.")
                render_chart_summary(
                    summarize_ranking(
                        access_df,
                        label_col="accessibility_format",
                        value_col="item_count",
                        metric_label="items",
                    )
                )

    with top_right:
        if filters.system == "All Systems":
            render_section_intro(
                "Publication Year Coverage by System",
                "This view compares how fully publication-year metadata is represented across Toronto, Montreal, and Ottawa.",
            )
            st.altair_chart(
                bar_chart(
                    system_coverage_df,
                    x="publication_year_share_value:Q",
                    y="system_name:N",
                    tooltip=["system_name", "items_with_year", "collection_items", "publication_year_share_value"],
                    color="#2563EB",
                    height=320,
                    x_title="Publication year coverage %",
                    y_title="Library system",
                ),
                width="stretch",
            )
            render_chart_guide("Longer bars mean a larger share of collection items have publication-year metadata.")
            render_chart_summary(
                summarize_ranking(
                    system_coverage_df,
                    label_col="system_name",
                    value_col="publication_year_share_value",
                    metric_label="publication year coverage percentage",
                    decimals=1,
                )
            )
        else:
            render_section_intro(
                "Publication Year Distribution",
                "This trend view supports change-over-time reading by showing collection age and recency.",
            )
            if year_df.empty:
                st.info("No publication year data is available for the current filter selection.")
            else:
                st.altair_chart(
                    area_line_chart(
                        year_df,
                        x="publication_year:O",
                        y="item_count:Q",
                        tooltip=["publication_year", "item_count"],
                        color="#0B5FFF",
                        height=360,
                    ),
                    width="stretch",
                )
                render_chart_guide("Higher peaks show publication years that appear more often in the loaded collection data.")
                render_chart_summary(
                    summarize_time_series(
                        year_df,
                        x_col="publication_year",
                        y_col="item_count",
                        metric_label="Publication-year item count",
                    )
                )

with compare_tab:
    render_section_intro(
        "Collection Format Diversity",
        "This ranking supports comparison across formats within the current collection scope.",
    )
    if format_df.empty:
        st.info("No collection format diversity data is available for the current filter selection.")
    else:
        st.altair_chart(
            bar_chart(
                format_df.head(20),
                x="item_count:Q",
                y="format:N",
                tooltip=["format", "item_count"],
                color="#0B5FFF",
                height=420,
                x_title="Items",
                y_title="Format",
            ),
            width="stretch",
        )
        render_chart_guide("Longer bars mean more items in the selected collection scope, so the ranking can be read directly from bar length.")
        render_chart_summary(
            summarize_ranking(
                format_df.head(20),
                label_col="format",
                value_col="item_count",
                metric_label="items",
            )
        )

    if filters.system == "All Systems":
        render_section_intro(
            "Three-City EDI Indicator Comparison",
            "This grouped comparison makes the main cross-city equity-related indicators easier to read than a color-intensity matrix.",
        )
        st.altair_chart(
            grouped_bar_chart(
                cross_city_indicator_df,
                x="indicator:N",
                y="indicator_value:Q",
                color="system_name:N",
                tooltip=["system_name", "indicator", "indicator_value"],
                height=240,
                legend_title="Library system",
            ),
            width="stretch",
        )
        render_chart_guide("Each indicator now groups one bar per city, so the comparison reads directly from bar height instead of color shading.")
        render_chart_summary(
            summarize_grouped_bars(
                cross_city_indicator_df,
                group_col="indicator",
                category_col="system_name",
                value_col="indicator_value",
                value_label="indicator value",
                decimals=1,
            )
        )

    if filters.system in {"All Systems", "TPL"}:
        render_section_intro(
            "Toronto Neighbourhood Context",
            "Toronto branches are linked to the TPL neighbourhood profile table, which adds income, housing, age, and language context around each branch.",
        )
        if toronto_context_df.empty:
            st.info("No Toronto neighbourhood profile rows are available for the current filter selection.")
        else:
            toronto_overview_df = _top_non_null(toronto_context_df, "low_income_lim_at_pct")
            _render_ranked_context_chart(
                toronto_overview_df,
                title="Low-Income Neighbourhood Ranking",
                description="This ranking highlights Toronto branches linked to neighbourhoods with higher low-income prevalence.",
                x_field="low_income_lim_at_pct",
                x_title="Low-Income Measure (%)",
                tooltip=[
                    "branch_name",
                    "neighbourhood_no",
                    "neighbourhood_name",
                    "tsns_designation",
                    "median_after_tax_income_2020",
                    "low_income_lim_at_pct",
                ],
                summary_metric_label="low-income measure percentage",
                guide_text="Longer bars mean the branch is linked to a neighbourhood with a higher low-income measure after tax.",
                color="#1C7ED6",
                decimals=1,
            )
            with st.expander("View Toronto supporting table"):
                show_dataframe(toronto_context_df)

    if filters.system in {"All Systems", "OPL"}:
        render_section_intro(
            "Ottawa Ward Context",
            "Ottawa branches are linked to ward-level context indicators and an existing EDI priority score.",
        )
        if ottawa_edi_df.empty:
            st.info("No Ottawa EDI priority rows are available for the current filter selection.")
        else:
            ottawa_overview_df = _top_non_null(ottawa_edi_df, "edi_priority_score")
            _render_ranked_context_chart(
                ottawa_overview_df,
                title="Ottawa EDI Priority Ranking",
                description="This ranking highlights Ottawa branches with the strongest overall EDI priority score in the current data model.",
                x_field="edi_priority_score",
                x_title="EDI Priority Score",
                tooltip=[
                    "branch_name",
                    "ward_name",
                    "edi_priority_score",
                    "core_housing_need_pct",
                    "age_0_14",
                    "age_65_plus",
                    "immigrants",
                ],
                summary_metric_label="EDI priority score",
                guide_text="Longer bars mean a higher overall Ottawa EDI priority score for that branch's ward context.",
                color="#0F9D76",
                decimals=1,
            )
            with st.expander("View Ottawa supporting table"):
                show_dataframe(ottawa_edi_df)
    else:
        st.caption("Ottawa ward context is hidden here because the current filter is focused on a non-Ottawa system.")

with detail_tab:
    if filters.system in {"All Systems", "TPL"}:
        render_section_intro(
            "Toronto Neighbourhood Detail",
            "These views break Toronto context into housing, age, and language dimensions linked to branch neighbourhoods.",
        )
        if toronto_context_df.empty:
            st.info("No Toronto neighbourhood profile rows are available for the current filter selection.")
        else:
            toronto_housing_df = _top_non_null(toronto_context_df, "core_housing_need_pct")
            toronto_shelter_df = _top_non_null(toronto_context_df, "shelter_cost_30_plus_pct")
            housing_left, housing_right = st.columns(2)
            with housing_left:
                _render_ranked_context_chart(
                    toronto_housing_df,
                    title="Toronto Housing Need",
                    description="Branches located in neighbourhoods with higher core housing need.",
                    x_field="core_housing_need_pct",
                    x_title="Core Housing Need (%)",
                    tooltip=["branch_name", "neighbourhood_name", "core_housing_need_pct", "median_after_tax_income_2020"],
                    summary_metric_label="core housing need percentage",
                    guide_text="Longer bars mean the branch is linked to a neighbourhood where a larger share of residents are in core housing need.",
                    color="#0F9D76",
                    decimals=1,
                )
            with housing_right:
                _render_ranked_context_chart(
                    toronto_shelter_df,
                    title="Toronto Shelter Cost Pressure",
                    description="Branches located in neighbourhoods where more households spend 30% or more of income on shelter.",
                    x_field="shelter_cost_30_plus_pct",
                    x_title="Shelter Cost 30%+ (%)",
                    tooltip=["branch_name", "neighbourhood_name", "shelter_cost_30_plus_pct", "median_after_tax_income_2020"],
                    summary_metric_label="shelter cost burden percentage",
                    guide_text="Longer bars mean greater neighbourhood shelter-cost pressure around that branch.",
                    color="#F97316",
                    decimals=1,
                )

            toronto_youth_df = _top_non_null(toronto_context_df, "age_0_14_pct")
            toronto_senior_df = _top_non_null(toronto_context_df, "age_65_plus_pct")
            age_left, age_right = st.columns(2)
            with age_left:
                _render_ranked_context_chart(
                    toronto_youth_df,
                    title="Toronto Youth Profile",
                    description="Branches linked to neighbourhoods with larger child populations.",
                    x_field="age_0_14_pct",
                    x_title="Residents Ages 0-14 (%)",
                    tooltip=["branch_name", "neighbourhood_name", "age_0_14_pct"],
                    summary_metric_label="child population percentage",
                    guide_text="Longer bars mean a higher share of residents ages 0 to 14 in the linked neighbourhood.",
                    color="#2563EB",
                    decimals=1,
                )
            with age_right:
                _render_ranked_context_chart(
                    toronto_senior_df,
                    title="Toronto Older-Adult Profile",
                    description="Branches linked to neighbourhoods with larger older-adult populations.",
                    x_field="age_65_plus_pct",
                    x_title="Residents Ages 65+ (%)",
                    tooltip=["branch_name", "neighbourhood_name", "age_65_plus_pct"],
                    summary_metric_label="older-adult population percentage",
                    guide_text="Longer bars mean a higher share of residents ages 65 and over in the linked neighbourhood.",
                    color="#7C3AED",
                    decimals=1,
                )

            toronto_language_df = _top_non_null(toronto_context_df, "non_official_languages_count")
            _render_ranked_context_chart(
                toronto_language_df,
                title="Toronto Language Diversity Context",
                description="This ranking highlights branches linked to neighbourhoods with larger counts of residents speaking non-official languages.",
                x_field="non_official_languages_count",
                x_title="Non-Official Languages Count",
                tooltip=["branch_name", "neighbourhood_name", "non_official_languages_count", "tsns_designation"],
                summary_metric_label="non-official language count",
                guide_text="Longer bars mean a larger neighbourhood count of non-official languages around that branch.",
                color="#1C7ED6",
                decimals=0,
                note="This is a neighbourhood count rather than a percentage, so larger neighbourhoods may naturally rank higher.",
            )

    if filters.system in {"All Systems", "OPL"}:
        if not ottawa_edi_df.empty:
            render_section_intro(
                "Ottawa Ward Detail",
                "These views break Ottawa context into housing, age, immigration, and overall priority dimensions linked to branch wards.",
            )
            ottawa_housing_df = _top_non_null(ottawa_edi_df, "core_housing_need_pct")
            ottawa_priority_df = _top_non_null(ottawa_edi_df, "edi_priority_score")
            ottawa_left, ottawa_right = st.columns(2)
            with ottawa_left:
                _render_ranked_context_chart(
                    ottawa_housing_df,
                    title="Ottawa Housing Need",
                    description="Branches linked to wards with higher core housing need.",
                    x_field="core_housing_need_pct",
                    x_title="Core Housing Need (%)",
                    tooltip=["branch_name", "ward_name", "core_housing_need_pct", "edi_priority_score"],
                    summary_metric_label="core housing need percentage",
                    guide_text="Longer bars mean a higher share of residents in core housing need in the branch's ward.",
                    color="#0F9D76",
                    decimals=1,
                )
            with ottawa_right:
                _render_ranked_context_chart(
                    ottawa_priority_df,
                    title="Ottawa Priority Score",
                    description="Branches with higher overall Ottawa EDI priority scores.",
                    x_field="edi_priority_score",
                    x_title="EDI Priority Score",
                    tooltip=["branch_name", "ward_name", "edi_priority_score", "core_housing_need_pct"],
                    summary_metric_label="EDI priority score",
                    guide_text="Longer bars mean a higher overall Ottawa EDI priority score for the branch's ward context.",
                    color="#2563EB",
                    decimals=1,
                )

            ottawa_youth_df = _top_non_null(ottawa_edi_df, "age_0_14")
            ottawa_senior_df = _top_non_null(ottawa_edi_df, "age_65_plus")
            ottawa_age_left, ottawa_age_right = st.columns(2)
            with ottawa_age_left:
                _render_ranked_context_chart(
                    ottawa_youth_df,
                    title="Ottawa Youth Profile",
                    description="Branches linked to wards with larger child populations.",
                    x_field="age_0_14",
                    x_title="Residents Ages 0-14",
                    tooltip=["branch_name", "ward_name", "age_0_14"],
                    summary_metric_label="child population count",
                    guide_text="Longer bars mean a larger count of residents ages 0 to 14 in the branch's ward.",
                    color="#F97316",
                    decimals=0,
                )
            with ottawa_age_right:
                _render_ranked_context_chart(
                    ottawa_senior_df,
                    title="Ottawa Older-Adult Profile",
                    description="Branches linked to wards with larger older-adult populations.",
                    x_field="age_65_plus",
                    x_title="Residents Ages 65+",
                    tooltip=["branch_name", "ward_name", "age_65_plus"],
                    summary_metric_label="older-adult population count",
                    guide_text="Longer bars mean a larger count of residents ages 65 and over in the branch's ward.",
                    color="#7C3AED",
                    decimals=0,
                )

            ottawa_immigrant_df = _top_non_null(ottawa_edi_df, "immigrants")
            _render_ranked_context_chart(
                ottawa_immigrant_df,
                title="Ottawa Immigration Context",
                description="This ranking highlights branches linked to wards with larger immigrant populations.",
                x_field="immigrants",
                x_title="Immigrant Population",
                tooltip=["branch_name", "ward_name", "immigrants", "edi_priority_score"],
                summary_metric_label="immigrant population count",
                guide_text="Longer bars mean a larger immigrant population count in the branch's ward.",
                color="#1C7ED6",
                decimals=0,
                note="This is a ward-level count rather than a percentage, so larger wards may naturally rank higher.",
            )
    else:
        render_section_intro(
            "Detailed Equity Indicators",
            "Montreal currently contributes collection-based EDI indicators such as accessibility coverage, publication year coverage, and format diversity. A branch-linked Montreal neighbourhood context view is not yet wired into this dashboard.",
        )
render_footer()
