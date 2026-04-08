"""
03_EDI_Analytics.py

Purpose:
- analyze accessibility and collection diversity
- compare representation across library systems
- support the EDI-focused part of Bibliometrics+

Important design choice:
- use live data when available
- use clearly labeled generated demo data when subject or accessibility data is incomplete
"""

import streamlit as st
import altair as alt

from app.components.shared_styles import apply_shared_styles, render_brand, render_page_intro
from app.services.supabase_connector import check_connection
from app.services.dashboard_utils import run_query, demo_accessibility, demo_publication_year, demo_subjects


def style_chart(chart):
    return chart.configure_view(
        strokeOpacity=0
    ).configure_axis(
        labelColor="#24324A",
        titleColor="#24324A",
        gridColor="rgba(36,50,74,0.12)",
        domainColor="rgba(36,50,74,0.20)",
        tickColor="rgba(36,50,74,0.20)",
        labelFontSize=13,
        titleFontSize=15
    ).configure_title(
        color="#24324A",
        fontSize=18
    )

st.set_page_config(
    page_title="Bibliometrics+ | EDI Analytics",
    page_icon="📚",
    layout="wide"
)

apply_shared_styles()
render_brand()

render_page_intro(
    "EDI Analytics",
    "Accessibility, collection diversity, publication patterns, and neighbourhood context indicators that support EDI-aware library analysis."
)

status = check_connection()
if status.mode == "SUPABASE":
    st.success("Connected to Supabase database.")
else:
    st.warning("Running in demo mode.")

st.sidebar.markdown(
    """
    <div style="color:#24324A; font-size:2rem; font-weight:700; margin-bottom:0.75rem;">
        EDI Filters
    </div>
    """,
    unsafe_allow_html=True
)

selected_system = st.sidebar.selectbox(
    "Library System",
    ["All Libraries", "Ottawa", "Toronto", "Montreal"]
)

system_map = {
    "Ottawa": "OPL",
    "Toronto": "TPL",
    "Montreal": "MPL"
}

if selected_system == "All Libraries":
    filter_clause = ""
    filter_params = {}
else:
    filter_clause = "AND l.system_name = :selected_system"
    filter_params = {"selected_system": system_map[selected_system]}

# Accessibility format distribution
st.subheader("Accessible Format Distribution")

sql_accessible = f"""
SELECT
    ci.accessibility_format,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
WHERE ci.accessibility_format IS NOT NULL
{filter_clause}
GROUP BY ci.accessibility_format
ORDER BY item_count DESC;
"""

df_access = run_query(sql_accessible, filter_params)

used_demo_access = False
if df_access.empty:
    df_access = demo_accessibility(selected_system)
    used_demo_access = True

if used_demo_access:
    st.info("Generated demo accessibility data is being displayed because source accessibility values are currently sparse or missing.")

access_chart = alt.Chart(df_access).mark_bar().encode(
    x=alt.X("item_count:Q", title="Number of Items"),
    y=alt.Y("accessibility_format:N", sort="-x", title="Accessibility Format"),
    tooltip=["accessibility_format", "item_count"]
).properties(height=350)

st.altair_chart(style_chart(access_chart), width="stretch")

with st.expander("View supporting table"):
    st.dataframe(df_access, width="stretch", hide_index=True)

# Publication-year distribution
st.subheader("Publication Year Distribution")

sql_pub_year = f"""
SELECT
    ci.publication_year,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
WHERE ci.publication_year IS NOT NULL
{filter_clause}
GROUP BY ci.publication_year
ORDER BY ci.publication_year;
"""

df_year = run_query(sql_pub_year, filter_params)

used_demo_year = False
if df_year.empty:
    df_year = demo_publication_year(selected_system)
    used_demo_year = True

if used_demo_year:
    st.info("Generated demo publication-year data is being displayed because source publication-year values are incomplete.")

year_chart = alt.Chart(df_year).mark_line(point=True).encode(
    x=alt.X("publication_year:O", title="Publication Year"),
    y=alt.Y("item_count:Q", title="Items"),
    tooltip=["publication_year", "item_count"]
).properties(height=350)

st.altair_chart(style_chart(year_chart), width="stretch")

with st.expander("View supporting table"):
    st.dataframe(df_year, width="stretch", hide_index=True)

# Collection format diversity
st.subheader("Collection Format Diversity")

sql_format = f"""
SELECT
    ci.format,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
WHERE ci.format IS NOT NULL
{filter_clause}
GROUP BY ci.format
ORDER BY item_count DESC;
"""

df_format = run_query(sql_format, filter_params)

if not df_format.empty:
    format_chart = alt.Chart(df_format).mark_bar().encode(
        x=alt.X("item_count:Q", title="Number of Items"),
        y=alt.Y("format:N", sort="-x", title="Format"),
        tooltip=["format", "item_count"]
    ).properties(height=350)

    st.altair_chart(style_chart(format_chart), width="stretch")

    with st.expander("View supporting table"):
        st.dataframe(df_format, width="stretch", hide_index=True)
else:
    st.info("Collection format diversity data is not currently available for this filter selection.")

# Toronto Neighbourhood Context
st.subheader("Toronto Neighbourhood Context")

if selected_system in ["All Libraries", "Toronto"]:
    sql_neighbourhood = """
    SELECT
        l.name AS branch_name,
        l.neighbourhood_no,
        l.neighbourhood_name,
        t.tsns_designation,
        t.median_after_tax_income_2020,
        t.low_income_lim_at_pct,
        t.core_housing_need_pct,
        t.shelter_cost_30_plus_pct,
        t.age_0_14_pct,
        t.age_65_plus_pct,
        t.non_official_languages_count
    FROM library l
    JOIN tpl_neighbourhood_profile t
        ON l.neighbourhood_no = t.neighbourhood_no
    WHERE l.system_name = 'TPL'
    ORDER BY
        t.low_income_lim_at_pct DESC NULLS LAST,
        t.median_after_tax_income_2020 ASC NULLS LAST;
    """

    df_neighbourhood = run_query(sql_neighbourhood)

    if not df_neighbourhood.empty:
        neighbourhood_chart = alt.Chart(df_neighbourhood.head(15)).mark_bar().encode(
            x=alt.X("low_income_lim_at_pct:Q", title="Low-Income Measure (%)"),
            y=alt.Y("branch_name:N", sort="-x", title="Toronto Branch"),
            tooltip=[
                "branch_name",
                "neighbourhood_no",
                "neighbourhood_name",
                "tsns_designation",
                "median_after_tax_income_2020",
                "low_income_lim_at_pct"
            ]
        ).properties(height=420)

        st.altair_chart(style_chart(neighbourhood_chart), width="stretch")

        with st.expander("View supporting table"):
            st.dataframe(df_neighbourhood, width="stretch", hide_index=True)

        st.subheader("Housing Need & Affordability Context")

        housing_df = (
            df_neighbourhood.dropna(subset=["core_housing_need_pct"])
            .sort_values("core_housing_need_pct", ascending=False)
            .head(15)
        )

        shelter_df = (
            df_neighbourhood.dropna(subset=["shelter_cost_30_plus_pct"])
            .sort_values("shelter_cost_30_plus_pct", ascending=False)
            .head(15)
        )

        h1, h2 = st.columns(2)

        with h1:
            st.caption("Branches located in neighbourhoods with higher core housing need.")
            housing_chart = alt.Chart(housing_df).mark_bar().encode(
                x=alt.X("core_housing_need_pct:Q", title="Core Housing Need (%)"),
                y=alt.Y("branch_name:N", sort="-x", title="Toronto Branch"),
                tooltip=[
                    "branch_name",
                    "neighbourhood_name",
                    "core_housing_need_pct",
                    "median_after_tax_income_2020"
                ]
            ).properties(height=400)
            st.altair_chart(style_chart(housing_chart), width="stretch")

        with h2:
            st.caption("Branches located in neighbourhoods where more households spend 30%+ of income on shelter.")
            shelter_chart = alt.Chart(shelter_df).mark_bar().encode(
                x=alt.X("shelter_cost_30_plus_pct:Q", title="Shelter Cost 30%+ (%)"),
                y=alt.Y("branch_name:N", sort="-x", title="Toronto Branch"),
                tooltip=[
                    "branch_name",
                    "neighbourhood_name",
                    "shelter_cost_30_plus_pct",
                    "median_after_tax_income_2020"
                ]
            ).properties(height=400)
            st.altair_chart(style_chart(shelter_chart), width="stretch")

        st.subheader("Age Profile by Neighbourhood")

        youth_df = (
            df_neighbourhood.dropna(subset=["age_0_14_pct"])
            .sort_values("age_0_14_pct", ascending=False)
            .head(15)
        )

        senior_df = (
            df_neighbourhood.dropna(subset=["age_65_plus_pct"])
            .sort_values("age_65_plus_pct", ascending=False)
            .head(15)
        )

        a1, a2 = st.columns(2)

        with a1:
            st.caption("Branches linked to neighbourhoods with larger child populations.")
            youth_chart = alt.Chart(youth_df).mark_bar().encode(
                x=alt.X("age_0_14_pct:Q", title="Residents Ages 0–14"),
                y=alt.Y("branch_name:N", sort="-x", title="Toronto Branch"),
                tooltip=["branch_name", "neighbourhood_name", "age_0_14_pct"]
            ).properties(height=400)
            st.altair_chart(style_chart(youth_chart), width="stretch")

        with a2:
            st.caption("Branches linked to neighbourhoods with larger older-adult populations.")
            senior_chart = alt.Chart(senior_df).mark_bar().encode(
                x=alt.X("age_65_plus_pct:Q", title="Residents Ages 65+"),
                y=alt.Y("branch_name:N", sort="-x", title="Toronto Branch"),
                tooltip=["branch_name", "neighbourhood_name", "age_65_plus_pct"]
            ).properties(height=400)
            st.altair_chart(style_chart(senior_chart), width="stretch")

        st.subheader("Language Diversity Context")

        language_df = (
            df_neighbourhood.dropna(subset=["non_official_languages_count"])
            .sort_values("non_official_languages_count", ascending=False)
            .head(15)
        )

        st.caption("This is a neighbourhood context count, not a percentage, so larger neighbourhoods may naturally rank higher.")
        language_chart = alt.Chart(language_df).mark_bar().encode(
            x=alt.X("non_official_languages_count:Q", title="Non-Official Languages Count"),
            y=alt.Y("branch_name:N", sort="-x", title="Toronto Branch"),
            tooltip=[
                "branch_name",
                "neighbourhood_name",
                "non_official_languages_count",
                "tsns_designation"
            ]
        ).properties(height=420)

        st.altair_chart(style_chart(language_chart), width="stretch")
    else:
        st.info("Toronto neighbourhood profile data is not currently available.")
else:
    df_neighbourhood = None
    st.info("Neighbourhood context is currently available for Toronto only.")

# Subject diversity
st.subheader("Subject Diversity in the Collection")

sql_subject_diversity = f"""
SELECT
    s.subject_name,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
JOIN collection_item_subject cis
    ON ci.item_id = cis.item_id
JOIN subject s
    ON cis.subject_id = s.subject_id
WHERE s.subject_name IS NOT NULL
{filter_clause}
GROUP BY s.subject_name
ORDER BY item_count DESC
LIMIT 15;
"""

df_subject = run_query(sql_subject_diversity, filter_params)

used_demo_subject = False
if df_subject.empty:
    df_subject = demo_subjects(selected_system)
    used_demo_subject = True

if used_demo_subject:
    st.info("Generated demo subject representation is being displayed until subject mapping data is loaded into Supabase.")

subject_chart = alt.Chart(df_subject).mark_bar().encode(
    x=alt.X("item_count:Q", title="Number of Items"),
    y=alt.Y("subject_name:N", sort="-x", title="Subject Area"),
    tooltip=["subject_name", "item_count"]
).properties(height=400)

st.altair_chart(style_chart(subject_chart), width="stretch")

with st.expander("View supporting table"):
    st.dataframe(df_subject, width="stretch", hide_index=True)

# Automated insight summary
st.subheader("Automated Insight Summary")

scope_text = selected_system if selected_system != "All Libraries" else "the full dataset"

summary_lines = []
data_notes = []

top_subject = df_subject.iloc[0]["subject_name"]
top_subject_count = int(df_subject.iloc[0]["item_count"])
summary_lines.append(
    f"For **{scope_text}**, the most represented subject area is **{top_subject}**, with approximately **{top_subject_count:,} items**."
)

if not df_format.empty:
    top_format = df_format.iloc[0]["format"]
    top_format_count = int(df_format.iloc[0]["item_count"])
    summary_lines.append(
        f"The most common collection format is **{top_format}**, with roughly **{top_format_count:,} items**."
    )

if not df_access.empty:
    top_access = df_access.iloc[0]["accessibility_format"]
    top_access_count = int(df_access.iloc[0]["item_count"])
    summary_lines.append(
        f"The most visible accessibility-support format is **{top_access}**, with about **{top_access_count:,} items**."
    )

if df_neighbourhood is not None and not df_neighbourhood.empty:
    top_housing = (
        df_neighbourhood.dropna(subset=["core_housing_need_pct"])
        .sort_values("core_housing_need_pct", ascending=False)
        .iloc[0]
    )

    top_youth = (
        df_neighbourhood.dropna(subset=["age_0_14_pct"])
        .sort_values("age_0_14_pct", ascending=False)
        .iloc[0]
    )

    top_senior = (
        df_neighbourhood.dropna(subset=["age_65_plus_pct"])
        .sort_values("age_65_plus_pct", ascending=False)
        .iloc[0]
    )

    top_language = (
        df_neighbourhood.dropna(subset=["non_official_languages_count"])
        .sort_values("non_official_languages_count", ascending=False)
        .iloc[0]
    )

    summary_lines.append(
        f"In Toronto neighbourhood context data, **{top_housing['branch_name']}** is linked to the highest visible core housing need at approximately **{top_housing['core_housing_need_pct']:.1f}%**."
    )

    summary_lines.append(
        f"**{top_youth['branch_name']}** is linked to a neighbourhood with approximately **{int(top_youth['age_0_14_pct']):,}** residents ages 0–14, while **{top_senior['branch_name']}** is linked to a neighbourhood with approximately **{int(top_senior['age_65_plus_pct']):,}** residents ages 65+."
    )

    summary_lines.append(
        f"Language diversity context is also visible around **{top_language['branch_name']}**, which is linked to a neighbourhood with approximately **{int(top_language['non_official_languages_count']):,}** residents reporting non-official languages."
    )

if used_demo_subject:
    data_notes.append("Subject diversity is currently based on generated demo data because subject mapping is not yet fully loaded.")

if used_demo_access:
    data_notes.append("Accessibility distribution is currently based on generated demo data because source accessibility values are sparse or incomplete.")

if used_demo_year:
    data_notes.append("Publication-year distribution is currently based on generated demo data because publication-year coverage is incomplete.")

summary_lines.append(
    "This EDI view helps identify where collection diversity, accessibility support, and representation measures are available, and where additional source data still needs to be loaded."
)

st.info("\n\n".join(summary_lines))

if data_notes:
    st.warning("**Data caveats:**\n\n- " + "\n- ".join(data_notes))

st.caption("Bibliometrics+ | EDI & Collection Diversity Analytics")
