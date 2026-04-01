"""
04_AI_Insights.py

Purpose:
- present an interpretation layer on top of the KPI and EDI data
- generate narrative summaries that help explain what the dashboard shows
- clearly distinguish between live data and demo-generated estimates
"""

import streamlit as st
import altair as alt

from services.dashboard_utils import run_query, demo_subjects, demo_accessibility



# Page setup

st.set_page_config(
    page_title="Bibliometrics+ | AI Insights",
    page_icon="",
    layout="wide"
)

st.title("AI Insights")
st.caption("Narrative interpretation layer for operational and EDI patterns.")


# Sidebar filter

selected_system = st.sidebar.selectbox(
    "Library System",
    ["All Libraries", "Ottawa", "Toronto", "Montreal"],
    key="ai_system"
)

if selected_system == "All Libraries":
    filter_clause = ""
    filter_params = {}
else:
    filter_clause = "AND l.system_name = :selected_system"
    filter_params = {"selected_system": selected_system}



# Load circulation trend

sql_trend = f"""
SELECT
    bk.year,
    SUM(bk.circulation) AS total_circulation
FROM branch_kpi bk
JOIN library l
    ON bk.library_id = l.library_id
WHERE bk.circulation IS NOT NULL
{filter_clause}
GROUP BY bk.year
ORDER BY bk.year;
"""

df_trend = run_query(sql_trend, filter_params)



# Load system comparison

sql_system = """
SELECT
    l.system_name AS system,
    SUM(bk.circulation) AS total_circulation
FROM branch_kpi bk
JOIN library l
    ON bk.library_id = l.library_id
WHERE bk.circulation IS NOT NULL
GROUP BY l.system_name
ORDER BY total_circulation DESC;
"""

df_system = run_query(sql_system)


# Load subject representation

sql_subject = f"""
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
LIMIT 10;
"""

df_subject = run_query(sql_subject, filter_params)

used_demo_subject = False
if df_subject.empty:
    df_subject = demo_subjects(selected_system)
    used_demo_subject = True


# Load accessibility distribution

sql_access = f"""
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

df_access = run_query(sql_access, filter_params)

used_demo_access = False
if df_access.empty:
    df_access = demo_accessibility(selected_system)
    used_demo_access = True



# Executive narrative

st.subheader("Executive Narrative")

scope = selected_system if selected_system != "All Libraries" else "the full dataset"

if not df_trend.empty:
    first_year = int(df_trend["year"].min())
    last_year = int(df_trend["year"].max())

    first_val = float(df_trend.loc[df_trend["year"] == first_year, "total_circulation"].values[0])
    last_val = float(df_trend.loc[df_trend["year"] == last_year, "total_circulation"].values[0])

    direction = "declined" if last_val < first_val else "increased"

    st.info(
        f"""
For {scope}, total circulation has **{direction}** between {first_year} and {last_year},
moving from approximately **{first_val:,.0f}** to **{last_val:,.0f}** loans.
"""
    )



# Interpretive findings

top_subject = df_subject.iloc[0]["subject_name"]
top_subject_count = int(df_subject.iloc[0]["item_count"])

top_access = df_access.iloc[0]["accessibility_format"]
top_access_count = int(df_access.iloc[0]["item_count"])

st.subheader("Interpretive Findings")
st.write(f"- The strongest currently visible subject area is **{top_subject}** ({top_subject_count:,} items).")
st.write(f"- The most visible accessibility-related format is **{top_access}** ({top_access_count:,} items).")
st.write(f"- Subject insight source: {'Generated demo data' if used_demo_subject else 'Live Supabase data'}")
st.write(f"- Accessibility insight source: {'Generated demo data' if used_demo_access else 'Live Supabase data'}")


# Visual support charts

c1, c2 = st.columns(2)

with c1:
    st.markdown("**Top Subject Representation**")
    st.altair_chart(
        alt.Chart(df_subject).mark_bar().encode(
            x=alt.X("item_count:Q", title="Items"),
            y=alt.Y("subject_name:N", sort="-x", title="Subject")
        ).properties(height=320),
        use_container_width=True
    )

with c2:
    st.markdown("**Accessibility Signal**")
    st.altair_chart(
        alt.Chart(df_access).mark_bar().encode(
            x=alt.X("item_count:Q", title="Items"),
            y=alt.Y("accessibility_format:N", sort="-x", title="Accessibility Format")
        ).properties(height=320),
        use_container_width=True
    )





st.caption("Bibliometrics+ | AI Insights")