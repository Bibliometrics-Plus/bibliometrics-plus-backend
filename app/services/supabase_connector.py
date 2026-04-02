"""
Supabase Database Connector (PostgreSQL)

This module centralizes database connection logic for the Streamlit app.
It uses the shared SQLAlchemy engine from db.py, which loads DATABASE_URL
from the project .env file.

This keeps Streamlit pages, loaders, and validation scripts on the same
database connection while still supporting Demo Mode if Supabase is unavailable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import engine as db_engine


@dataclass
class DBStatus:
    connected: bool
    mode: str
    message: str


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    return db_engine


def check_connection() -> DBStatus:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return DBStatus(
            connected=True,
            mode="SUPABASE",
            message="Connected to Supabase PostgreSQL successfully."
        )
    except Exception as e:
        return DBStatus(
            connected=False,
            mode="DEMO",
            message=f"Supabase connection failed. Switching to Demo Mode. (Reason: {e})"
        )


def get_table_row_counts(schema: str = "public") -> Tuple[pd.DataFrame, DBStatus]:
    status = check_connection()

    if status.mode == "DEMO":
        demo = pd.DataFrame(
            {
                "table_name": [
                    "library",
                    "collection_item",
                    "subject",
                    "circulation_transaction",
                    "user_group",
                    "branch_kpi",
                ],
                "row_count": [12, 4500, 300, 82000, 6, 250],
            }
        )
        return demo, status

    query = """
    SELECT
        relname AS table_name,
        n_live_tup AS row_count
    FROM pg_stat_user_tables
    WHERE schemaname = :schema
    ORDER BY n_live_tup DESC;
    """

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"schema": schema})

    return df, status
