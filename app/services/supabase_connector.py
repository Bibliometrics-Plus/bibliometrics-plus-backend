"""
Supabase Database Connector (PostgreSQL)

This module centralizes all database connection logic so that:
- Pages don't duplicate connection code
- We can cleanly support "Demo Mode" if Supabase is unavailable
- The project stays readable for teammates

We connect using the Supabase Postgres credentials stored in:
.streamlit/secrets.toml
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class DBStatus:
    connected: bool
    mode: str  # "SUPABASE" or "DEMO"
    message: str


def _build_connection_string() -> str:
    """
    Builds a SQLAlchemy Postgres connection string from Streamlit secrets.
    """
    host = st.secrets.get("SUPABASE_DB_HOST", "")
    port = st.secrets.get("SUPABASE_DB_PORT", "5432")
    dbname = st.secrets.get("SUPABASE_DB_NAME", "postgres")
    user = st.secrets.get("SUPABASE_DB_USER", "postgres")
    password = st.secrets.get("SUPABASE_DB_PASSWORD", "")

    # Format: postgresql+psycopg2://user:password@host:port/dbname
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """
    Creates and caches the SQLAlchemy engine so we don't reconnect on every rerun.
    """
    conn_str = _build_connection_string()
    return create_engine(conn_str, pool_pre_ping=True)


def check_connection() -> DBStatus:
    """
    Attempts a simple SELECT 1 query to confirm the database connection.
    If it fails, we return Demo Mode status instead of crashing.
    """
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
    """
    Returns a dataframe of table row counts if connected.
    If not connected, returns sample/demo counts instead.
    """
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
                    "kpi_fact"
                ],
                "row_count": [12, 4500, 300, 82000, 6, 250]
            }
        )
        return demo, status

    # Real query: list tables + estimated row count
    query = f"""
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