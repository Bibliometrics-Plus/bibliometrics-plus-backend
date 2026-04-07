"""
dashboard_utils.py

Shared helper functions for the Bibliometrics+ dashboard.

Why this file exists:
- keeps repeated logic out of the page files
- runs SQL safely without crashing the app
- generates synthetic demo data when some source tables are still incomplete
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text

from services.supabase_connector import check_connection, get_engine


# Database query helper

def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    Safely run a SQL query and return the results as a DataFrame.
    """
    status = check_connection()

    if status.mode != "SUPABASE":
        return pd.DataFrame()
    import streamlit as st
    st.write("DEBUG run_query status is being used")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as e:
        import streamlit as st
        st.error(f"run_query failed: {repr(e)}")
        return pd.DataFrame()


# Small helper used to scale demo data by library system

def _multiplier(selected_system: str) -> float:
    """
    Returns a simple scaling factor so generated demo data changes
    slightly depending on the selected library system.

    This makes the filters feel more realistic during demos even when
    live data is not available for that metric yet.
    """
    if selected_system == "Ottawa":
        return 0.95
    if selected_system == "Toronto":
        return 1.20
    if selected_system == "Montreal":
        return 0.85
    return 1.0


# Synthetic demo data: accessibility formats

def demo_accessibility(selected_system: str = "All Libraries") -> pd.DataFrame:
    """
    Generates synthetic accessibility-format data.

    Used when:
    - accessibility values are missing or sparse in Supabase
    - we still want the EDI page to remain complete and presentation-ready
    """
    scale = _multiplier(selected_system)

    data = [
        ("Large Print", int(280 * scale)),
        ("Audiobook", int(240 * scale)),
        ("eAccessible", int(190 * scale)),
        ("DAISY", int(95 * scale)),
        ("Braille", int(42 * scale)),
        ("Screen Reader Compatible", int(135 * scale)),
    ]

    return pd.DataFrame(data, columns=["accessibility_format", "item_count"])


# Synthetic demo data: publication years

def demo_publication_year(selected_system: str = "All Libraries") -> pd.DataFrame:
    """
    Generates synthetic publication-year data for the collection.

    This gives the EDI page a complete line chart even if publication-year
    data is incomplete or missing in the live source tables.
    """
    scale = _multiplier(selected_system)

    years = list(range(2010, 2025))
    base = [95, 110, 125, 140, 155, 168, 182, 195, 220, 245, 270, 295, 310, 285, 260]
    vals = [int(v * scale) for v in base]

    return pd.DataFrame({"publication_year": years, "item_count": vals})


# Synthetic demo data: subject diversity

def demo_subjects(selected_system: str = "All Libraries") -> pd.DataFrame:
    """
    Generates synthetic subject-representation data.

    This is especially useful because subject mapping is often the last
    part of the schema to be fully loaded.
    """
    scale = _multiplier(selected_system)

    subjects = [
        ("Children's Literature", 520),
        ("Language Learning", 470),
        ("Canadian History", 430),
        ("Health & Wellness", 395),
        ("Career Development", 360),
        ("Indigenous Studies", 335),
        ("Science & Technology", 315),
        ("Mental Health", 290),
        ("Immigration & Settlement", 265),
        ("Black Studies", 240),
        ("Gender Studies", 220),
        ("Accessibility & Disability", 205),
        ("Financial Literacy", 195),
        ("Climate & Environment", 180),
        ("Civic Engagement", 170),
    ]

    subjects = [(name, int(count * scale)) for name, count in subjects]

    return pd.DataFrame(subjects, columns=["subject_name", "item_count"])


# Synthetic demo data: top borrowed categories

def demo_top_borrowed_categories(selected_system: str = "All Libraries") -> pd.DataFrame:
    """
    Generates synthetic popularity data for top borrowed categories.

    Used in the KPI page when subject-linked borrowing data is not yet
    fully available in Supabase.
    """
    scale = _multiplier(selected_system)

    data = [
        ("Children's Literature", 12800),
        ("DVD / Film", 11800),
        ("Language Learning", 10400),
        ("Graphic Novels", 9700),
        ("Career Resources", 8900),
        ("Popular Fiction", 8600),
        ("New Arrivals", 8100),
        ("Audiobooks", 7600),
        ("Health & Wellness", 7100),
        ("Digital Learning", 6650),
    ]

    data = [(name, int(count * scale)) for name, count in data]

    return pd.DataFrame(data, columns=["category", "borrow_count"])


# Synthetic demo data: data-quality diagnostics

def demo_quality_checks() -> dict[str, pd.DataFrame]:
    """
    Generates demo data-quality tables for the Data Status page.

    These are used only if live SQL checks are unavailable.
    """
    nulls = pd.DataFrame(
        [
            ("collection_item.accessibility_format", "27.4%"),
            ("collection_item.publication_year", "11.8%"),
            ("circulation_transaction.item_id", "31.2%"),
            ("circulation_transaction.group_id", "28.5%"),
            ("collection_item.library_id", "4.7%"),
        ],
        columns=["field", "null_rate"],
    )

    duplicates = pd.DataFrame(
        [
            ("library.library_id", 0),
            ("collection_item.item_id", 0),
            ("circulation_transaction.transaction_id", 0),
            ("branch_kpi.kpi_id", 0),
        ],
        columns=["field", "duplicate_rows"],
    )

    joins = pd.DataFrame(
        [
            ("collection_item -> library", "95.3%"),
            ("circulation_transaction -> collection_item", "68.8%"),
            ("circulation_transaction -> user_group", "71.5%"),
            ("collection_item -> subject mapping", "0.0%"),
        ],
        columns=["relationship", "coverage"],
    )

    return {"nulls": nulls, "duplicates": duplicates, "joins": joins}


# Synthetic demo data: home-page snapshot metrics

def demo_home_snapshot() -> dict[str, Any]:
    """
    Provides fallback values for the Home page summary cards.

    These are only used when a live metric is not yet available.
    """
    return {
        "total_circulation": 24992899,
        "circulation_growth": "-4.8%",
        "distinct_subjects": 15,
        "edi_share": "18.6%",
        "status_note": "Some cards use generated demo estimates where source tables are still incomplete."
    }