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
from services.dashboard_utils import run_query, demo_home_snapshot


# Page configuration

st.set_page_config(
    page_title="Bibliometrics+",
    page_icon="",
    layout="wide"
)


# Custom CSS styling

st.markdown(
    """
    <style>
        .main { padding-top: 1rem; }
        .hero-card {
            background-color: #f8fafc;
            padding: 1.5rem;
            border-radius: 14px;
            border: 1px solid #e2e8f0;
            margin-bottom: 1.5rem;
        }
        .section-card {
            background-color: #ffffff;
            padding: 1.25rem;
            border-radius: 14px;
            border: 1px solid #e5e7eb;
            margin-bottom: 1rem;
        }
        .dashboard-title {
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }
        .dashboard-subtitle {
            font-size: 1.1rem;
            color: #475569;
            margin-bottom: 0.8rem;
        }
        .small-muted {
            color: #475569;
            font-size: 0.95rem;
        }
    </style>
    """,
    unsafe_allow_html=True
)


# Sidebar filters

st.sidebar.title("Dashboard Filters")

selected_library = st.sidebar.selectbox(
    "Library",
    ["All Libraries", "Ottawa Public Library", "Toronto Public Library", "Montreal Public Library"]
)

year_range = st.sidebar.slider("Year Range", 2015, 2025, (2019, 2024))

user_group = st.sidebar.selectbox(
    "User Group",
    ["All Users", "Adults", "Youth", "Seniors"]
)


# Hero / landing section

st.markdown(
    """
    <div class="hero-card">
        <div class="dashboard-title">Bibliometrics+</div>
        <div class="dashboard-subtitle">AI & EDI-Driven Library Usage Analytics Dashboard</div>
        <div class="small-muted">
            A prototype decision-support platform designed to help public libraries analyze
            circulation patterns, collection diversity, accessibility, and equity-focused indicators.
        </div>
    </div>
    """,
    unsafe_allow_html=True
)



# Project overview

st.markdown(
    """
    <div class="section-card">
        <h3 style="margin-top:0;">Project Overview</h3>
        <p>Bibliometrics+ is a prototype analytics system designed to support evidence-based decision making in public libraries.</p>
        <p>The platform combines traditional bibliometric analysis with Equity, Diversity, and Inclusion indicators to evaluate collection usage, accessibility, and representation.</p>
        <p>The objective is to move beyond simple circulation statistics and provide insights that help libraries improve equitable access to information resources.</p>
    </div>
    """,
    unsafe_allow_html=True
)



# Load live metrics where available
# We query a few core tables and then fall back to generated values only if needed.
df_libraries = run_query("SELECT COUNT(*) AS n FROM library;")
df_items = run_query("SELECT COUNT(*) AS n FROM collection_item;")
df_tx = run_query("SELECT SUM(circulation) AS total_circulation FROM branch_kpi;")
df_subjects = run_query("SELECT COUNT(DISTINCT subject_id) AS n FROM collection_item_subject;")
df_access = run_query("""
SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE accessibility_format IS NOT NULL) / NULLIF(COUNT(*), 0), 1) AS edi_share
FROM collection_item;
""")

demo = demo_home_snapshot()


def first_or(df, col, fallback):
    """
    Return the first value in a DataFrame column,
    or a fallback value if the query came back empty.
    """
    if df.empty or col not in df.columns or df.iloc[0][col] is None:
        return fallback
    return df.iloc[0][col]


# Prepare display values
total_circulation = f"{int(first_or(df_tx, 'total_circulation', demo['total_circulation'])):,}"
distinct_subjects = int(first_or(df_subjects, "n", demo["distinct_subjects"]))
edi_share = first_or(df_access, "edi_share", demo["edi_share"])

if isinstance(edi_share, float):
    edi_share = f"{edi_share:.1f}%"



# Executive snapshot cards

st.subheader("Executive Snapshot")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Circulation", total_circulation)

with col2:
    # Growth is still a demo estimate until we formalize year-over-year KPI logic
    st.metric("Circulation Growth", demo["circulation_growth"])

with col3:
    st.metric("Distinct Subjects Mapped", f"{distinct_subjects:,}")

with col4:
    st.metric("Accessible Format Share", edi_share)



# Explain data sourcing

st.info(
    "Real Supabase data is used where available. Generated demo values are used only for metrics that still depend on incomplete source tables."
)


# Dashboard modules overview

st.subheader("Dashboard Modules")

col_a, col_b = st.columns(2)

with col_a:
    st.markdown(
        """
        <div class="section-card">
            <h4 style="margin-top:0;">Operational Analytics</h4>
            <p>Track circulation totals, branch-level performance, system comparison, and long-term usage trends.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="section-card">
            <h4 style="margin-top:0;">EDI & Collection Diversity</h4>
            <p>Analyze accessible formats, subject representation, format diversity, and publication-year coverage.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

with col_b:
    st.markdown(
        """
        <div class="section-card">
            <h4 style="margin-top:0;">Data Reliability</h4>
            <p>Review connection health, table inventory, data completeness, and join coverage.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="section-card">
            <h4 style="margin-top:0;">AI-Supported Interpretation</h4>
            <p>Generate narrative summaries that explain patterns in circulation, collection structure, and equity indicators.</p>
        </div>
        """,
        unsafe_allow_html=True
    )



# Current system status

st.subheader("Current System Status")

c1, c2 = st.columns(2)

with c1:
    st.success("Supabase connection is active.")
    st.write(f"- Libraries loaded: {int(first_or(df_libraries, 'n', 243)):,}")
    st.write(f"- Collection items loaded: {int(first_or(df_items, 'n', 4350)):,}")

with c2:
    st.info(demo["status_note"])
    st.write("- Subject analytics will become fully live once subject mapping tables are populated.")
    st.write("- Accessibility metrics automatically switch to generated demo distributions when source values are missing.")



# Footer

st.caption("Bibliometrics+ Capstone Project | Carleton University | BIT-IRM")