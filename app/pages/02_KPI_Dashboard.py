"""
02_KPI_Dashboard.py

Purpose:
- show operational performance metrics
- compare circulation across branches and systems
- display long-term circulation trends
- provide category-level insight where possible

Important design choice:
- use live Supabase data where available
- use generated demo category popularity only if subject-linked borrowing data is not yet ready
"""

from __future__ import annotations

import streamlit as st
import altair as alt

from services.supabase_connector import check_connection
from services.dashboard_utils import run_query, demo_top_borrowed_categories


# Page setup

st.set_page_config(
    page_title="Bibliometrics+ | KPI Dashboard",
    page_icon="",
    layout="wide"
)

st.title("KPI Dashboard")
st.caption("Operational usage KPIs calculated from Supabase tables, with synthetic fallback only where source data is incomplete.")


# Connection banner

status = check_connection()

if status.mode == "SUPABASE":
    st.success("Database: Connected to Supabase PostgreSQL successfully.")
else:
    st.warning("Database connection unavailable. Using generated demo data where needed.")



# Sidebar filter

st.sidebar.markdown("## KPI Filters")

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



# Snapshot cards

st.subheader("System-Wide Snapshot")

df_libraries = run_query("SELECT COUNT(*) AS n FROM library;")
df_items = run_query("SELECT COUNT(*) AS n FROM collection_item;")
df_tx = run_query("SELECT COUNT(*) AS n FROM circulation_transaction;")
df_kpi = run_query("SELECT COUNT(*) AS n FROM branch_kpi;")


def get_count(df, col="n", fallback=0):
    """
    Small helper for displaying card values safely.
    """
    if df.empty or col not in df.columns or df.iloc[0][col] is None:
        return fallback
    return int(df.iloc[0][col])


col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Libraries", f"{get_count(df_libraries, fallback=243):,}")

with col2:
    st.metric("Collection Items", f"{get_count(df_items, fallback=4350):,}")

with col3:
    st.metric("Circulation Transactions", f"{get_count(df_tx, fallback=7368):,}")

with col4:
    st.metric("Branch KPI Rows", f"{get_count(df_kpi, fallback=1339):,}")



# Circulation by library branch

st.subheader("Circulation by Library")

sql_circ_by_library = f"""
SELECT
    l.name AS library,
    SUM(bk.circulation) AS total_circulation
FROM branch_kpi bk
JOIN library l
    ON bk.library_id = l.library_id
WHERE bk.circulation IS NOT NULL
{filter_clause}
GROUP BY l.name
ORDER BY total_circulation DESC
LIMIT 15;
"""

df_circ_by_library = run_query(sql_circ_by_library, filter_params)

if not df_circ_by_library.empty:
    chart = alt.Chart(df_circ_by_library).mark_bar().encode(
        x=alt.X("total_circulation:Q", title="Total Circulation"),
        y=alt.Y("library:N", sort="-x", title="Library"),
        tooltip=["library", "total_circulation"]
    ).properties(height=500)

    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df_circ_by_library, use_container_width=True)
else:
    st.info("Branch-level KPI data is not currently available for this filter selection.")



# Circulation by system

st.subheader("Circulation by Library System")

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

if not df_system.empty:
    system_chart = alt.Chart(df_system).mark_bar().encode(
        x=alt.X("system:N", title="Library System"),
        y=alt.Y("total_circulation:Q", title="Total Circulation"),
        tooltip=["system", "total_circulation"]
    ).properties(height=320)

    st.altair_chart(system_chart, use_container_width=True)
    st.dataframe(df_system, use_container_width=True)



# Trend over time

st.subheader("Circulation Trend Over Time")

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

if not df_trend.empty:
    line = alt.Chart(df_trend).mark_line(point=True).encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("total_circulation:Q", title="Total Circulation"),
        tooltip=["year", "total_circulation"]
    ).properties(height=320)

    st.altair_chart(line, use_container_width=True)
    st.dataframe(df_trend, use_container_width=True)



# Collection format mix

st.subheader("Collection Format Distribution")

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


# Top borrowed categories / subjects

st.subheader("Top Borrowed Subjects / Categories")

sql_subjects = f"""
SELECT
    s.subject_name AS category,
    COUNT(*) AS borrow_count
FROM circulation_transaction ct
JOIN collection_item ci
    ON ct.item_id = ci.item_id
JOIN collection_item_subject cis
    ON ci.item_id = cis.item_id
JOIN subject s
    ON cis.subject_id = s.subject_id
JOIN library l
    ON ci.library_id = l.library_id
WHERE s.subject_name IS NOT NULL
{filter_clause}
GROUP BY s.subject_name
ORDER BY borrow_count DESC
LIMIT 10;
"""

df_top_categories = run_query(sql_subjects, filter_params)

used_demo_subjects = False

# If subject-linked circulation is not ready yet, use generated demo categories.
if df_top_categories.empty:
    df_top_categories = demo_top_borrowed_categories(selected_system)
    used_demo_subjects = True

if used_demo_subjects:
    st.info("Generated demo category popularity is being shown here until subject-linked borrowing data is fully loaded into Supabase.")

top_chart = alt.Chart(df_top_categories).mark_bar().encode(
    x=alt.X("borrow_count:Q", title="Borrow Count"),
    y=alt.Y("category:N", sort="-x", title="Category"),
    tooltip=["category", "borrow_count"]
).properties(height=350)

st.altair_chart(top_chart, use_container_width=True)
st.dataframe(df_top_categories, use_container_width=True)


# AI-style summary

st.subheader("AI Insight Summary")

if not df_trend.empty:
    earliest_year = int(df_trend["year"].min())
    latest_year = int(df_trend["year"].max())

    earliest_value = float(df_trend.loc[df_trend["year"] == earliest_year, "total_circulation"].values[0])
    latest_value = float(df_trend.loc[df_trend["year"] == latest_year, "total_circulation"].values[0])

    trend_direction = "declined" if latest_value < earliest_value else "increased"

    st.info(
        f"""
Between {earliest_year} and {latest_year}, total library circulation has **{trend_direction}**.

The system recorded approximately **{earliest_value:,.0f} loans** in {earliest_year},
compared to **{latest_value:,.0f} loans** in {latest_year}.

This operational view helps identify long-term changes in demand and branch performance.
"""
    )

st.caption("Bibliometrics+ | KPI Dashboard")