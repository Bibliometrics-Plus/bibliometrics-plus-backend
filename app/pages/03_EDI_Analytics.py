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

from services.supabase_connector import check_connection
from services.dashboard_utils import run_query, demo_accessibility, demo_publication_year, demo_subjects



# Page setup

st.set_page_config(
    page_title="Bibliometrics+ | EDI Analytics",
    page_icon="",
    layout="wide"
)

st.title("EDI & Collection Diversity Analytics")
st.caption("Accessibility, collection diversity, and representation indicators.")


# Connection banner

status = check_connection()

if status.mode == "SUPABASE":
    st.success("Connected to Supabase database.")
else:
    st.warning("Running in demo mode.")



# Sidebar filter

st.sidebar.markdown("## EDI Filters")

selected_system = st.sidebar.selectbox(
    "Library System",
    ["All Libraries", "Ottawa", "Toronto", "Montreal"]
)

if selected_system == "All Libraries":
    filter_clause = ""
    filter_params = {}
else:
    filter_clause = "AND l.system_name = :selected_system"
    filter_params = {"selected_system": selected_system}



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

st.altair_chart(access_chart, use_container_width=True)
st.dataframe(df_access, use_container_width=True)



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

st.altair_chart(year_chart, use_container_width=True)
st.dataframe(df_year, use_container_width=True)



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

    st.altair_chart(format_chart, use_container_width=True)
    st.dataframe(df_format, use_container_width=True)



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

st.altair_chart(subject_chart, use_container_width=True)
st.dataframe(df_subject, use_container_width=True)


# Ottawa branch EDI priority ranking

if selected_system in ["All Libraries", "Ottawa"]:
    st.subheader("Ottawa Branch EDI Priority Ranking")

    sql_ottawa_edi = """
    SELECT
        branch_name,
        ward_name,
        core_housing_need_pct,
        age_0_14,
        age_65_plus,
        immigrants,
        edi_priority_score
    FROM ottawa_branch_edi_priority
    ORDER BY edi_priority_score DESC;
    """

    df_ottawa_edi = run_query(sql_ottawa_edi)
    st.write("Ottawa EDI rows:", len(df_ottawa_edi))
    st.dataframe(df_ottawa_edi, use_container_width=True)

    if df_ottawa_edi.empty:
        st.warning("No Ottawa EDI priority data is available yet.")
    else:
        # make numeric columns numeric
        numeric_cols = [
            "core_housing_need_pct",
            "age_0_14",
            "age_65_plus",
            "immigrants",
            "edi_priority_score",
        ]

        for col in numeric_cols:
            df_ottawa_edi[col] = df_ottawa_edi[col].astype(str).str.replace(",", "", regex=False)
            df_ottawa_edi[col] = df_ottawa_edi[col].astype(float)

        st.caption(
            "This ranking uses Ottawa ward-level census context linked to library branches. "
            "Higher scores indicate stronger indicators of potential equity-related service need."
        )

        top_n = st.slider(
            "Show top Ottawa branches",
            min_value=5,
            max_value=min(20, len(df_ottawa_edi)),
            value=min(10, len(df_ottawa_edi)),
            key="ottawa_edi_top_n"
        )

        top_df = df_ottawa_edi.head(top_n).copy()

        ottawa_edi_chart = alt.Chart(top_df).mark_bar().encode(
            x=alt.X("edi_priority_score:Q", title="EDI Priority Score"),
            y=alt.Y("branch_name:N", sort="-x", title="Ottawa Branch"),
            tooltip=[
                "branch_name",
                "ward_name",
                alt.Tooltip("core_housing_need_pct:Q", title="Core Housing Need %"),
                alt.Tooltip("age_0_14:Q", title="Age 0-14"),
                alt.Tooltip("age_65_plus:Q", title="Age 65+"),
                alt.Tooltip("immigrants:Q", title="Immigrants"),
                alt.Tooltip("edi_priority_score:Q", title="EDI Score")
            ]
        ).properties(height=420)

        st.altair_chart(ottawa_edi_chart, use_container_width=True)

        st.dataframe(
            top_df[
                [
                    "branch_name",
                    "ward_name",
                    "core_housing_need_pct",
                    "age_0_14",
                    "age_65_plus",
                    "immigrants",
                    "edi_priority_score",
                ]
            ],
            use_container_width=True
        )

        top_branch = top_df.iloc[0]["branch_name"]
        top_ward = top_df.iloc[0]["ward_name"]

        st.info(
            f"""
The current Ottawa EDI ranking identifies **{top_branch}** in **{top_ward}**
as the highest-priority branch based on ward-level indicators including
housing need, youth population, seniors, and immigrant population.

This score should be interpreted as an **equity-context prioritization**
measure rather than a full service-gap model, because Ottawa branch-level
usage metrics are still being expanded.
"""
        )
# Automated insight summary

st.subheader("Automated Insight Summary")

scope_text = selected_system if selected_system != "All Libraries" else "the full dataset"

top_subject = df_subject.iloc[0]["subject_name"]
top_subject_count = int(df_subject.iloc[0]["item_count"])

top_format = None
top_format_count = None

if not df_format.empty:
    top_format = df_format.iloc[0]["format"]
    top_format_count = int(df_format.iloc[0]["item_count"])

subject_note = "This subject insight uses generated demo data." if used_demo_subject else "This subject insight uses live Supabase data."
access_note = "Accessibility distribution currently uses generated demo data." if used_demo_access else "Accessibility distribution currently uses live Supabase data."

extra_line = ""
if top_format is not None:
    extra_line = f"\nThe most common collection format is **{top_format}** with approximately **{top_format_count:,} items**."

st.info(
    f"""
For {scope_text}, the most represented subject area is **{top_subject}**
with approximately **{top_subject_count:,} items**.{extra_line}

{subject_note}
{access_note}
"""
)

st.caption("Bibliometrics+ | EDI & Collection Diversity Analytics")