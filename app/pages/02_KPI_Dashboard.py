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

from app.components.shared_styles import apply_shared_styles, render_brand, render_page_intro, render_metric_card
from app.services.supabase_connector import check_connection
from app.services.dashboard_utils import run_query, demo_top_borrowed_categories


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


def get_count(df, col="n", fallback=0):
    if df.empty or col not in df.columns or df.iloc[0][col] is None:
        return fallback
    return int(df.iloc[0][col])


def first_value(df, col, fallback=None):
    if df.empty or col not in df.columns or df.iloc[0][col] is None:
        return fallback
    return df.iloc[0][col]


st.set_page_config(
    page_title="Bibliometrics+ | KPI Dashboard",
    page_icon="📚",
    layout="wide"
)

apply_shared_styles()
render_brand()

render_page_intro(
    "KPI Dashboard",
    "Operational usage KPIs, circulation trends, and system-level comparisons across the current dashboard view."
)

status = check_connection()
if status.mode == "SUPABASE":
    st.success(f"Database: {status.message}")
else:
    st.warning("Database connection unavailable. Using generated demo data where needed.")

st.sidebar.markdown(
    """
    <div style="color:#24324A; font-size:2rem; font-weight:700; margin-bottom:0.75rem;">
        KPI Filters
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

# Snapshot cards
st.subheader("System-Wide Snapshot")

df_libraries = run_query(f"""
SELECT COUNT(*) AS n
FROM library l
WHERE 1=1
{filter_clause}
""", filter_params)

df_items = run_query(f"""
SELECT COUNT(*) AS n
FROM collection_item ci
JOIN library l ON ci.library_id = l.library_id
WHERE 1=1
{filter_clause}
""", filter_params)

df_tx = run_query(f"""
SELECT COUNT(*) AS n
FROM circulation_transaction ct
JOIN collection_item ci ON ct.item_id = ci.item_id
JOIN library l ON ci.library_id = l.library_id
WHERE 1=1
{filter_clause}
""", filter_params)

df_kpi = run_query(f"""
SELECT COUNT(*) AS n
FROM branch_kpi bk
JOIN library l ON bk.library_id = l.library_id
WHERE 1=1
{filter_clause}
""", filter_params)

c1, c2, c3, c4 = st.columns(4)

with c1:
    render_metric_card("Libraries", f"{get_count(df_libraries, fallback=243):,}")
with c2:
    render_metric_card("Collection Items", f"{get_count(df_items, fallback=4350):,}")
with c3:
    render_metric_card("Circulation Transactions", f"{get_count(df_tx, fallback=7368):,}")
with c4:
    render_metric_card("Branch KPI Rows", f"{get_count(df_kpi, fallback=1339):,}")

# Circulation by library branch
st.subheader("Circulation by Library")

df_circ_by_library = run_query(f"""
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
""", filter_params)

if not df_circ_by_library.empty:
    chart = alt.Chart(df_circ_by_library).mark_bar().encode(
        x=alt.X("total_circulation:Q", title="Total Circulation"),
        y=alt.Y("library:N", sort="-x", title="Library"),
        tooltip=["library", "total_circulation"]
    ).properties(height=420)

    st.altair_chart(style_chart(chart), width="stretch")

    with st.expander("View supporting table"):
        st.dataframe(df_circ_by_library, width="stretch", hide_index=True)
else:
    st.info("Branch-level KPI data is not currently available for this filter selection.")

# Circulation by system
st.subheader("Circulation by Library System")

if selected_system != "All Libraries":
    st.info("System comparison is most useful when 'All Libraries' is selected.")
    df_system = run_query(f"""
    SELECT
        l.system_name AS system,
        SUM(bk.circulation) AS total_circulation
    FROM branch_kpi bk
    JOIN library l
        ON bk.library_id = l.library_id
    WHERE bk.circulation IS NOT NULL
    {filter_clause}
    GROUP BY l.system_name
    ORDER BY total_circulation DESC;
    """, filter_params)
else:
    df_system = run_query("""
    SELECT
        l.system_name AS system,
        SUM(bk.circulation) AS total_circulation
    FROM branch_kpi bk
    JOIN library l
        ON bk.library_id = l.library_id
    WHERE bk.circulation IS NOT NULL
      AND bk.year = (
          SELECT MAX(year)
          FROM branch_kpi
          WHERE circulation IS NOT NULL
      )
    GROUP BY l.system_name
    ORDER BY total_circulation DESC;
    """)

if not df_system.empty and selected_system == "All Libraries":
    system_chart = alt.Chart(df_system).mark_bar().encode(
        x=alt.X("system:N", title="Library System"),
        y=alt.Y("total_circulation:Q", title="Total Circulation"),
        tooltip=["system", "total_circulation"]
    ).properties(height=320)

    st.altair_chart(style_chart(system_chart), width="stretch")

    with st.expander("View supporting table"):
        st.dataframe(df_system, width="stretch", hide_index=True)
elif selected_system == "All Libraries":
    st.info("System-level circulation data is not currently available.")

# Trend over time
st.subheader("Circulation Trend Over Time")

df_trend = run_query(f"""
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
""", filter_params)

if not df_trend.empty:
    line = alt.Chart(df_trend).mark_line(point=True).encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("total_circulation:Q", title="Total Circulation"),
        tooltip=["year", "total_circulation"]
    ).properties(height=320)

    st.altair_chart(style_chart(line), width="stretch")

    with st.expander("View supporting table"):
        st.dataframe(df_trend, width="stretch", hide_index=True)
else:
    st.info("Yearly circulation trend data is not currently available.")

# Collection format mix
st.subheader("Collection Format Distribution")

df_format = run_query(f"""
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
""", filter_params)

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
    st.info("Collection format distribution is not currently available.")

# Top borrowed categories
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

st.altair_chart(style_chart(top_chart), width="stretch")

with st.expander("View supporting table"):
    st.dataframe(df_top_categories, width="stretch", hide_index=True)

# AI-style summary
st.subheader("AI Insight Summary")

summary_lines = []
data_notes = []

scope = selected_system if selected_system != "All Libraries" else "all library systems"

if not df_trend.empty:
    earliest_year = int(df_trend["year"].min())
    latest_year = int(df_trend["year"].max())

    earliest_value = float(df_trend.loc[df_trend["year"] == earliest_year, "total_circulation"].values[0])
    latest_value = float(df_trend.loc[df_trend["year"] == latest_year, "total_circulation"].values[0])

    trend_direction = "declined" if latest_value < earliest_value else "increased"

    if earliest_value != 0:
        pct_change = ((latest_value - earliest_value) / earliest_value) * 100
        summary_lines.append(
            f"Across **{scope}**, total circulation has **{trend_direction}** from **{earliest_year}** to **{latest_year}**, changing from **{earliest_value:,.0f}** to **{latest_value:,.0f}** loans (**{pct_change:+.1f}%**)."
        )
    else:
        summary_lines.append(
            f"Across **{scope}**, total circulation has **{trend_direction}** from **{earliest_year}** to **{latest_year}**, changing from **{earliest_value:,.0f}** to **{latest_value:,.0f}** loans."
        )
else:
    data_notes.append("Trend analysis is unavailable because yearly circulation data could not be loaded.")

if not df_system.empty:
    top_system = first_value(df_system, "system", "Unknown")
    top_system_value = first_value(df_system, "total_circulation", 0)
    summary_lines.append(
        f"The highest recorded circulation at the system level is currently **{top_system}**, with approximately **{top_system_value:,.0f}** total loans."
    )
else:
    data_notes.append("System-level comparison could not be generated.")

if not df_circ_by_library.empty:
    top_branch = first_value(df_circ_by_library, "library", "Unknown")
    top_branch_value = first_value(df_circ_by_library, "total_circulation", 0)
    summary_lines.append(
        f"At the branch level, **{top_branch}** appears as the strongest visible library in the current filter selection, with about **{top_branch_value:,.0f}** recorded circulations."
    )
else:
    data_notes.append("Branch-level KPI data is currently missing for this filter selection.")

if not df_format.empty:
    top_format = first_value(df_format, "format", "Unknown")
    top_format_count = first_value(df_format, "item_count", 0)
    summary_lines.append(
        f"In the available collection data, **{top_format}** is the most common format, with roughly **{top_format_count:,.0f}** items."
    )
else:
    data_notes.append("Collection format distribution is unavailable for the current filter.")

if not df_top_categories.empty:
    top_category = first_value(df_top_categories, "category", "Unknown")
    top_category_count = first_value(df_top_categories, "borrow_count", 0)
    summary_lines.append(
        f"The most borrowed visible category is **{top_category}**, with approximately **{top_category_count:,.0f}** borrow events."
    )
    if used_demo_subjects:
        data_notes.append(
            "Category popularity is currently based on generated demo data because subject-linked borrowing records are not yet fully available in Supabase."
        )
else:
    data_notes.append("Borrowed category analysis could not be generated.")

if not df_trend.empty and not df_system.empty:
    summary_lines.append(
        "This suggests that the KPI dashboard is already useful for spotting demand shifts, comparing systems, and identifying where deeper branch- or category-level follow-up may be needed."
    )

if summary_lines:
    st.info("\n\n".join(summary_lines))

if data_notes:
    st.warning("**Data caveats:**\n\n- " + "\n- ".join(data_notes))

st.caption("Bibliometrics+ | KPI Dashboard")
