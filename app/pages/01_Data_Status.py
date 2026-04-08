"""
01_Data_Status.py

Purpose:
- show whether the app is connected to Supabase
- show table inventory and approximate row counts
- surface data-quality checks so the dashboard looks transparent and trustworthy
"""

import streamlit as st

from app.components.shared_styles import (
    apply_shared_styles,
    render_brand,
    render_page_intro,
)
from app.services.supabase_connector import check_connection, get_table_row_counts
from app.services.dashboard_utils import run_query, demo_quality_checks


st.set_page_config(
    page_title="Bibliometrics+ | Data Status",
    page_icon="📚",
    layout="wide"
)

apply_shared_styles()
render_brand()

render_page_intro(
    "Data Status",
    "Connection health, table inventory, and validation checks that help keep the dashboard transparent and trustworthy."
)

# Sidebar
st.sidebar.markdown(
    """
    <div style="color:#24324A; font-size:2rem; font-weight:700; margin-bottom:0.75rem;">
        Data Status
    </div>
    """,
    unsafe_allow_html=True
)

schema = "public"

# Connection + table inventory
status = check_connection()
table_counts_df, table_status = get_table_row_counts(schema=schema)

if status.mode == "SUPABASE":
    st.success(status.message)
else:
    st.warning(status.message)

st.subheader("Connection Status & Table Inventory")
st.caption("Review the current database connection mode and approximate table row counts.")

with st.expander("View table inventory", expanded=True):
    st.dataframe(table_counts_df, width="stretch", hide_index=True)

# Live quality checks
nulls_df = run_query("""
SELECT * FROM (
    SELECT
        'collection_item.accessibility_format' AS field,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE accessibility_format IS NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%' AS null_rate
    FROM collection_item

    UNION ALL

    SELECT
        'collection_item.publication_year',
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE publication_year IS NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%'
    FROM collection_item

    UNION ALL

    SELECT
        'circulation_transaction.item_id',
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE item_id IS NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%'
    FROM circulation_transaction

    UNION ALL

    SELECT
        'circulation_transaction.group_id',
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE group_id IS NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%'
    FROM circulation_transaction
) q;
""")

duplicates_df = run_query("""
SELECT * FROM (
    SELECT
        'library.library_id' AS field,
        COUNT(*) - COUNT(DISTINCT library_id) AS duplicate_rows
    FROM library

    UNION ALL

    SELECT
        'collection_item.item_id',
        COUNT(*) - COUNT(DISTINCT item_id)
    FROM collection_item

    UNION ALL

    SELECT
        'circulation_transaction.transaction_id',
        COUNT(*) - COUNT(DISTINCT transaction_id)
    FROM circulation_transaction

    UNION ALL

    SELECT
        'branch_kpi.kpi_id',
        COUNT(*) - COUNT(DISTINCT kpi_id)
    FROM branch_kpi
) q;
""")

joins_df = run_query("""
SELECT * FROM (
    SELECT
        'collection_item -> library' AS relationship,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE library_id IS NOT NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%' AS coverage
    FROM collection_item

    UNION ALL

    SELECT
        'circulation_transaction -> collection_item',
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE item_id IS NOT NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%'
    FROM circulation_transaction

    UNION ALL

    SELECT
        'circulation_transaction -> user_group',
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE group_id IS NOT NULL)
            / NULLIF(COUNT(*), 0),
            1
        )::text || '%'
    FROM circulation_transaction

    UNION ALL

    SELECT
        'collection_item -> subject mapping',
        CASE
            WHEN (SELECT COUNT(*) FROM collection_item) = 0 THEN '0.0%'
            ELSE ROUND(
                100.0 * (SELECT COUNT(DISTINCT item_id) FROM collection_item_subject)
                / NULLIF((SELECT COUNT(*) FROM collection_item), 0),
                1
            )::text || '%'
        END
) q;
""")

# Demo fallback
demo = demo_quality_checks()

if nulls_df.empty:
    nulls_df = demo["nulls"]

if duplicates_df.empty:
    duplicates_df = demo["duplicates"]

if joins_df.empty:
    joins_df = demo["joins"]

# Validation section
st.subheader("Quick Validation Checks")
st.caption("Check missing values, duplicate records, and join coverage before interpreting dashboard results.")

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown("**Null / Missing Values**")
    st.dataframe(nulls_df, width="stretch", hide_index=True)

with c2:
    st.markdown("**Duplicate Records**")
    st.dataframe(duplicates_df, width="stretch", hide_index=True)

with c3:
    st.markdown("**Join Coverage**")
    st.dataframe(joins_df, width="stretch", hide_index=True)

# Notes
st.subheader("Known Data Limitations & Notes")
st.caption("These notes explain current data gaps and fallback behavior used in the dashboard.")

st.write("- Toronto currently has the strongest branch-level KPI coverage through `branch_kpi`.")
st.write("- Montreal currently has stronger `collection_item` coverage than branch-level KPI coverage.")
st.write("- Ottawa currently has library metadata loaded, but still requires fuller KPI and collection integration.")
st.write("- Some dashboard views use clearly labeled generated fallback data where source coverage is incomplete.")
st.write("- Subject-linked borrowing remains less complete than branch-level KPI and collection-format coverage.")

st.caption("Bibliometrics+ | Data Status")
