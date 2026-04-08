"""
Database connection helpers for the Streamlit app.

This module centralizes connection logic so every page can use the same engine,
connection checks, and cached query execution path.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

@dataclass
class DBStatus:
    connected: bool
    message: str


def _build_connection_string() -> str:
    """
    Resolve a PostgreSQL connection string from either `.env` or Streamlit
    secrets.

    The repo already uses `.env` for local scripts, so the app supports both
    patterns instead of forcing only one.
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    host = st.secrets.get("SUPABASE_DB_HOST", "")
    port = st.secrets.get("SUPABASE_DB_PORT", "5432")
    dbname = st.secrets.get("SUPABASE_DB_NAME", "postgres")
    user = st.secrets.get("SUPABASE_DB_USER", "postgres")
    password = st.secrets.get("SUPABASE_DB_PASSWORD", "")
    if host and password:
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    raise ValueError("No DATABASE_URL found in .env and no .secrets configured")


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """Create and cache the SQLAlchemy engine."""
    return create_engine(_build_connection_string(), pool_pre_ping=True)


@st.cache_data(show_spinner=False, ttl=600)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    Execute a SQL query and return a dataframe.

    This is the single shared query helper that every page uses. Having one
    function reduces duplication and gives us consistent caching behavior.
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def check_connection() -> DBStatus:
    """Confirm that the dashboard can reach the live database."""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return DBStatus(True, "Connected to the live Bibliometrics+ database.")
    except Exception as exc:  # pragma: no cover - defensive UI path
        return DBStatus(False, f"Database connection failed: {exc}")
