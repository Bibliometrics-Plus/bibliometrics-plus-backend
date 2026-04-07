"""
Shared filter UI and helper functions.

Keeping filters here lets every page share the same system, branch, and year
logic. That is important because inconsistent filters make dashboards feel
unfinished very quickly.
"""

from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from app.services.db import run_query
from app.styles.theme import THEME_MODE_KEY, get_theme_mode, set_theme_mode


PENDING_SYSTEM_KEY = "pending_global_system"
PENDING_BRANCH_KEY = "pending_global_branch"


@dataclass
class DashboardFilters:
    system: str
    branch: str
    year_start: int
    year_end: int

    @property
    def is_all_systems(self) -> bool:
        return self.system == "All Systems"

    @property
    def is_all_branches(self) -> bool:
        return self.branch == "All Branches"


def get_system_options() -> list[str]:
    """Return the list of available systems."""
    df = run_query(
        """
        SELECT DISTINCT COALESCE(system_name, 'Unassigned') AS system_name
        FROM library
        ORDER BY system_name;
        """
    )
    return ["All Systems"] + df["system_name"].tolist()


def get_branch_options(system: str) -> list[str]:
    """Return branches for the selected system."""
    if system == "All Systems":
        df = run_query(
            """
            SELECT DISTINCT name
            FROM library
            WHERE name IS NOT NULL
            ORDER BY name;
            """
        )
    else:
        df = run_query(
            """
            SELECT DISTINCT name
            FROM library
            WHERE COALESCE(system_name, 'Unassigned') = :system_name
              AND name IS NOT NULL
            ORDER BY name;
            """,
            {"system_name": system},
        )
    return ["All Branches"] + df["name"].tolist()


def get_available_year_range() -> tuple[int, int]:
    """
    Build the global year range from the real operational tables.

    I use the combined branch KPI and circulation year coverage so the sidebar
    reflects the actual real-data range available across the dashboard.
    """
    df = run_query(
        """
        WITH year_bounds AS (
            SELECT MIN(year) AS min_year, MAX(year) AS max_year
            FROM branch_kpi
            UNION ALL
            SELECT
                MIN(EXTRACT(YEAR FROM borrow_date))::INT AS min_year,
                MAX(EXTRACT(YEAR FROM borrow_date))::INT AS max_year
            FROM circulation_transaction
        )
        SELECT MIN(min_year) AS min_year, MAX(max_year) AS max_year
        FROM year_bounds;
        """
    )
    min_year = int(df.iloc[0]["min_year"] or 2012)
    max_year = int(df.iloc[0]["max_year"] or min_year)
    return min_year, max_year


def render_global_filters() -> DashboardFilters:
    """Render shared sidebar filters and return the selected values."""
    # When another page or action wants to push the user into a branch-specific
    # view, I store that intent in separate pending keys first. Streamlit does
    # not let me mutate an already-instantiated widget key in the same run, so
    # I apply those pending values here before the widgets are created.
    pending_system = st.session_state.pop(PENDING_SYSTEM_KEY, None)
    pending_branch = st.session_state.pop(PENDING_BRANCH_KEY, None)
    if pending_system is not None:
        st.session_state["global_system"] = pending_system
    if pending_branch is not None:
        st.session_state["global_branch"] = pending_branch

    st.sidebar.title("Explore the Data")
    st.sidebar.caption("Start broad, then narrow the view by system, branch, and year range.")

    active_theme_mode = get_theme_mode()
    theme_mode = st.sidebar.selectbox(
        "Color mode",
        ["Light", "Dark"],
        index=0 if active_theme_mode == "Light" else 1,
        key=f"{THEME_MODE_KEY}_selector",
        help="Switch between light and dark display modes while preserving contrast and readability.",
    )
    set_theme_mode(theme_mode)

    systems = get_system_options()
    if st.session_state.get("global_system") not in systems:
        st.session_state["global_system"] = "All Systems"
    selected_system = st.sidebar.selectbox(
        "Choose a library system",
        systems,
        key="global_system",
        help="Use All Systems for comparison, or choose one system to focus the analysis.",
    )

    branches = get_branch_options(selected_system)
    if st.session_state.get("global_branch") not in branches:
        st.session_state["global_branch"] = "All Branches"
    selected_branch = st.sidebar.selectbox(
        "Choose a branch",
        branches,
        key="global_branch",
        help="Branch-level pages work best when a single branch is selected.",
    )

    min_year, max_year = get_available_year_range()
    if "global_year_range" not in st.session_state:
        st.session_state["global_year_range"] = (min_year, max_year)
    selected_years = st.sidebar.slider(
        "Select a year range",
        min_value=min_year,
        max_value=max_year,
        value=st.session_state["global_year_range"],
        key="global_year_range",
    )

    st.sidebar.markdown(
        """
        **How to use**
        - Use Home and System Overview for the broadest view.
        - Use KPI Analysis and EDI Analytics to compare patterns.
        - Use Branch Explorer or Library Access for branch-level detail.
        """
    )

    return DashboardFilters(
        system=selected_system,
        branch=selected_branch,
        year_start=selected_years[0],
        year_end=selected_years[1],
    )


def set_branch_filter(branch_name: str, system_name: str | None = None) -> None:
    """
    Update the shared filter state from the top search bar.

    This gives the search a direct effect on the dashboard instead of acting as
    a passive text field.
    """
    st.session_state[PENDING_BRANCH_KEY] = branch_name
    if system_name:
        st.session_state[PENDING_SYSTEM_KEY] = system_name
