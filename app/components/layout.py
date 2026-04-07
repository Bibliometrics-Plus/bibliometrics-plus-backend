"""
Reusable layout helpers for the Streamlit app.

These helpers keep page headers and section wrappers consistent across the
application.
"""

from __future__ import annotations

import streamlit as st

from app.services.db import check_connection
from app.services.filters import set_branch_filter
from app.services.formatters import format_int
from app.services.queries import get_searchable_branches, get_shell_summary


PAGE_LINKS = [
    ("Home", "app/Home.py", ["home", "overview", "executive"]),
    ("System Overview", "app/pages/01_System_Overview.py", ["system", "systems", "comparison", "overview"]),
    ("KPI Analysis", "app/pages/02_KPI_Dashboard.py", ["kpi", "metrics", "performance", "dashboard"]),
    ("EDI Analytics", "app/pages/03_EDI_Analytics.py", ["edi", "equity", "accessibility", "analytics"]),
    ("Branch Explorer", "app/pages/04_Branch_Explorer.py", ["branch", "branches", "explorer", "profile"]),
    ("Data Quality", "app/pages/05_Data_Quality.py", ["data", "quality", "coverage"]),
    ("AI Insights", "app/pages/06_AI_Insights.py", ["ai", "insights", "assistant", "chat"]),
    ("Methods & Definitions", "app/pages/07_Methodology.py", ["methods", "methodology", "definitions"]),
    ("Library Access", "app/pages/08_Library_Map.py", ["library", "map", "locator", "access"]),
]


def configure_page(page_title: str) -> None:
    """
    Apply Streamlit page configuration.

    I use the same layout everywhere because the dashboard is dense and benefits
    from a wide content area on almost every page.
    """
    st.set_page_config(
        page_title=page_title,
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def _status_badge(label: str, tone: str = "neutral") -> str:
    """Build a shared shell badge."""
    return f'<span class="bm-status-pill bm-status-pill--{tone}">{label}</span>'


def _panel_tone(seed_text: str) -> str:
    """Choose a stable tone class so the panels feel varied without randomness."""
    tones = ["primary", "secondary", "accent", "shell"]
    tone_index = sum(ord(char) for char in seed_text) % len(tones)
    return tones[tone_index]


def render_app_shell(page_name: str) -> None:
    """
    Render the shared application chrome.

    This includes the brand area, page context, search field, and a clear
    Supabase connection/status bar.
    """
    status = check_connection()
    shell = get_shell_summary().iloc[0]

    left_col, middle_col, right_col = st.columns((1.25, 1.05, 1.7))
    with left_col:
        st.markdown(
            """
            <div class="bm-topbar-brand">
                <div class="bm-brand-mark">B+</div>
                <div>
                    <div class="bm-brand-title">Bibliometrics+</div>
                    <div class="bm-brand-subtitle">AI & EDI-Driven Library Usage Analytics for Public Libraries</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with middle_col:
        st.markdown(
            f"""
            <div class="bm-topbar-page">
                <div class="bm-topbar-label">Viewing</div>
                <div class="bm-topbar-value">{page_name}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with right_col:
        query = st.text_input(
            "Search pages or branches",
            placeholder="Search a page or branch",
            key=f"shell_search_{page_name.lower().replace(' ', '_')}",
            label_visibility="collapsed",
        )

    tone = "good" if status.connected else "warn"
    st.markdown(
        f"""
        <div class="bm-connection-bar">
            <div class="bm-connection-left">
                {_status_badge('Supabase Connected' if status.connected else 'Database Unavailable', tone)}
                {_status_badge(f"{format_int(shell['systems'])} systems")}
                {_status_badge(f"{format_int(shell['libraries'])} libraries")}
                {_status_badge(f"{int(shell['min_kpi_year'])} to {int(shell['max_kpi_year'])} KPI years")}
                {_status_badge(f"{format_int(shell['branch_kpi_rows'])} KPI rows")}
            </div>
            <div class="bm-connection-right">{status.message}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if query:
        render_global_search_results(query)


def render_global_search_results(search_query: str) -> None:
    """Show page and branch matches for the shell search."""
    query = search_query.strip().lower()
    if not query:
        return

    page_matches = [
        item for item in PAGE_LINKS if query in item[0].lower() or any(query in alias for alias in item[2])
    ]
    branch_df = get_searchable_branches()
    branch_match_mask = (
        branch_df["name"].str.lower().str.contains(query, na=False)
        | branch_df["system_name"].str.lower().str.contains(query, na=False)
        | branch_df["city"].fillna("").str.lower().str.contains(query, na=False)
    )
    branch_matches = branch_df[branch_match_mask].head(6)

    st.markdown('<div class="bm-search-results">', unsafe_allow_html=True)
    left_col, right_col = st.columns(2)

    with left_col:
        st.markdown("**Page Matches**")
        if page_matches:
            for label, path, _aliases in page_matches:
                st.page_link(path, label=label)
        else:
            st.caption("No page matches found.")

    with right_col:
        st.markdown("**Branch Matches**")
        if branch_matches.empty:
            st.caption("No branch matches found.")
        else:
            for row in branch_matches.to_dict(orient="records"):
                location_text = f"{row['city']}" if row["city"] else "City unavailable"
                label = f"Use {row['name']} ({row['system_name']} | {location_text})"
                if st.button(label, key=f"search_branch_{row['system_name']}_{row['name']}"):
                    set_branch_filter(row["name"], row["system_name"])
                    st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def render_page_header(title: str, subtitle: str) -> None:
    """Show a consistent page title and subtitle."""
    st.markdown(f'<div class="bm-page-title">{title}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="bm-page-subtitle">{subtitle}</div>', unsafe_allow_html=True)


def render_hero(title: str, body: str, chips: list[str] | None = None) -> None:
    """
    Render a prominent hero section for the home page.

    Chips are useful for quickly reinforcing the dashboard scope and the fact
    that the app now runs on real data only.
    """
    chip_markup = ""
    if chips:
        joined = "".join(f'<span class="bm-chip">{chip}</span>' for chip in chips)
        chip_markup = f'<div class="bm-chip-row">{joined}</div>'

    st.markdown(
        f"""
        <div class="bm-hero">
            <div class="bm-hero-grid">
                <div>
                    <h1>{title}</h1>
                    <p>{body}</p>
                    {chip_markup}
                </div>
                <div class="bm-hero-aside">
                    <div class="bm-hero-kicker">Platform Focus</div>
                    <strong>Usage, equity, and access</strong>
                    <div>Designed to support system comparison, branch exploration, and grounded interpretation across public library data.</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_intro(title: str, body: str) -> None:
    """Render a short section intro card."""
    tone = _panel_tone(title)
    st.markdown(
        f"""
        <div class="bm-section-card bm-section-card--{tone}">
            <h3>{title}</h3>
            <p>{body}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chart_guide(text: str) -> None:
    """Render a compact note that explains how to read a visualization."""
    st.markdown(
        f"""
        <div class="bm-chart-guide">
            <strong>How to read this:</strong> {text}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chart_summary(summary: str) -> None:
    """Render a concise automated summary directly under a visualization."""
    st.markdown(
        f"""
        <div class="bm-chart-summary">
            <div class="bm-chart-summary-label">Automated insight</div>
            <div>{summary}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_filter_summary(lines: list[str]) -> None:
    """
    Show the currently active filters so the user always knows what the
    dashboard is summarizing.
    """
    tone = _panel_tone(" ".join(lines[:2]))
    summary_html = "".join(f"<div>{line}</div>" for line in lines)
    st.markdown(
        f'<div class="bm-filter-summary bm-filter-summary--{tone}"><strong>Active View</strong>{summary_html}</div>',
        unsafe_allow_html=True,
    )


def render_footer() -> None:
    """Render the shared footer at the bottom of each page."""
    st.markdown(
        """
        <div class="bm-footer">
            <div class="bm-footer-title">Bibliometrics+</div>
            <div class="bm-footer-copy">
                Public library usage analytics across KPI, EDI, accessibility, and AI-assisted interpretation.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
