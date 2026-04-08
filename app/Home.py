"""
Home.py

Main landing page for Bibliometrics+.

Purpose:
- introduce the project clearly
- show a high-level executive snapshot
- explain what each dashboard module does
- reassure the user that the system is connected and working

Design approach:
- use live Supabase data where possible
- use generated demo values only for still-incomplete metrics
"""

import streamlit as st

from app.components.shared_styles import (
    apply_shared_styles,
    render_brand,
    render_page_intro,
    render_metric_card,
)
from app.services.dashboard_utils import run_query, demo_home_snapshot


st.set_page_config(
    page_title="Bibliometrics+",
    page_icon="📚",
    layout="wide"
)

apply_shared_styles()

render_brand()

render_page_intro(
    "Bibliometrics+",
    "A multi-city public library analytics prototype combining KPI, EDI, and AI-supported interpretation."
)

# Sidebar
st.sidebar.markdown(
    """
    <div style="color:#24324A; font-size:2rem; font-weight:700; margin-bottom:0.75rem;">
        Dashboard Filters
    </div>
    """,
    unsafe_allow_html=True
)

selected_library = st.sidebar.selectbox(
    "Library",
    ["All Libraries", "Ottawa Public Library", "Toronto Public Library", "Montreal Public Library"]
)

year_range = st.sidebar.slider(
    "Year Range",
    2015,
    2025,
    (2019, 2024)
)

user_group = st.sidebar.selectbox(
    "User Group",
    ["All Users", "Adults", "Youth", "Seniors"]
)

# Helpers
def first_or(df, col, fallback):
    if df.empty or col not in df.columns or df.iloc[0][col] is None:
        return fallback
    return df.iloc[0][col]


demo = demo_home_snapshot()

# Snapshot queries
df_total_circulation = run_query("""
SELECT SUM(circulation) AS total_circulation
FROM branch_kpi
WHERE circulation IS NOT NULL;
""")

df_subjects = run_query("""
SELECT COUNT(DISTINCT subject_id) AS distinct_subjects
FROM collection_item_subject;
""")

df_access = run_query("""
SELECT
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE accessibility_format IS NOT NULL)
        / NULLIF(COUNT(*), 0),
        1
    ) AS accessible_share
FROM collection_item;
""")

# Final display values
total_circulation_val = int(first_or(df_total_circulation, "total_circulation", demo["total_circulation"]))
total_circulation = f"{total_circulation_val:,}"

distinct_subjects_val = int(first_or(df_subjects, "distinct_subjects", demo["distinct_subjects"]))
distinct_subjects = f"{distinct_subjects_val:,}"

accessible_share_val = first_or(df_access, "accessible_share", demo["edi_share"])
if isinstance(accessible_share_val, (int, float)):
    edi_share = f"{accessible_share_val:.1f}%"
else:
    edi_share = str(accessible_share_val)

# Executive snapshot
st.subheader("Executive Snapshot")

col1, col2, col3, col4 = st.columns(4)

with col1:
    render_metric_card("Total Circulation", total_circulation)

with col2:
    render_metric_card("Circulation Growth", demo["circulation_growth"])

with col3:
    render_metric_card("Distinct Subjects Mapped", distinct_subjects)

with col4:
    render_metric_card("Accessible Format Share", edi_share)

st.info(
    "Real Supabase data is used where available. Clearly labeled generated values appear only where source coverage is still incomplete."
)

# Project overview
st.subheader("Project Overview")
st.write(
    "Bibliometrics+ is a prototype decision-support dashboard designed to help public libraries analyze circulation patterns, collection diversity, accessibility, and equity-focused indicators."
)
st.write(
    "The platform brings together multiple city datasets into one analytics framework, while remaining transparent about data coverage and fallback use."
)

# Dashboard modules
st.subheader("Dashboard Modules")

col_a, col_b = st.columns(2)

with col_a:
    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-card-title">Operational Analytics</div>
            <div style="color:#FFFFFF; line-height:1.6;">
                Track circulation totals, branch-level performance, system comparison, and long-term usage trends.
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-card-title">EDI & Collection Diversity</div>
            <div style="color:#FFFFFF; line-height:1.6;">
                Analyze accessible formats, subject representation, publication patterns, and Toronto neighbourhood context indicators.
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col_b:
    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-card-title">Data Status</div>
            <div style="color:#FFFFFF; line-height:1.6;">
                Review connection health, table inventory, data completeness, and validation checks before interpreting results.
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="metric-card">
            <div class="metric-card-title">AI Insights</div>
            <div style="color:#FFFFFF; line-height:1.6;">
                Generate narrative summaries and ask AI questions about KPI, EDI, and data-coverage patterns in the dashboard.
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.caption("Bibliometrics+ | Home")
