"""
Table helpers used across the dashboard.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


def show_dataframe(df: pd.DataFrame) -> None:
    """
    Display a dataframe consistently.

    I keep table rendering in one helper so formatting changes can be made once
    and then flow to every page.
    """
    st.dataframe(df, width="stretch", hide_index=True)
