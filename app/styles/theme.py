"""
Shared visual theme for the Bibliometrics+ Streamlit dashboard.

This file keeps all of the dashboard styling decisions in one place so the
pages do not each invent their own CSS. That makes the final app look more
consistent and also makes it easier for me to explain the design choices.
"""

from __future__ import annotations

import streamlit as st


THEME_MODE_KEY = "bm_theme_mode"
THEME_QUERY_PARAM = "theme"

LIGHT_THEME = {
    "primary": "#2563EB",
    "primary_dark": "#1E3A8A",
    "secondary": "#059669",
    "secondary_dark": "#065F46",
    "accent": "#F97316",
    "accent_dark": "#C2410C",
    "background": "#EAF1F5",
    "surface": "#FFFFFF",
    "surface_alt": "#EEF5F8",
    "border": "#C8D5E1",
    "text": "#102033",
    "muted": "#5F7288",
    "success": "#0F8B5F",
    "warning": "#B7791F",
    "danger": "#DC2626",
    "shell": "#101826",
    "shell_soft": "#1A2434",
    "shell_text": "#F4F8FA",
    "chart_grid": "#D8E8E3",
    "panel_bg": "linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(245, 249, 252, 0.95))",
    "panel_blue": "linear-gradient(180deg, rgba(236, 244, 255, 0.98), rgba(225, 237, 255, 0.95))",
    "panel_green": "linear-gradient(180deg, rgba(236, 251, 246, 0.98), rgba(225, 245, 237, 0.95))",
    "panel_orange": "linear-gradient(180deg, rgba(255, 245, 236, 0.98), rgba(255, 238, 226, 0.95))",
    "panel_shell": "linear-gradient(180deg, rgba(235, 240, 248, 0.98), rgba(226, 233, 244, 0.95))",
    "input_bg": "#FFFFFF",
    "input_border": "#C8D5E1",
    "chip_bg": "rgba(16, 24, 38, 0.06)",
    "chip_border": "rgba(16, 24, 38, 0.10)",
    "overlay_soft": "rgba(255,255,255,0.14)",
    "overlay_strong": "rgba(255,255,255,0.94)",
}

DARK_THEME = {
    "primary": "#60A5FA",
    "primary_dark": "#2563EB",
    "secondary": "#34D399",
    "secondary_dark": "#059669",
    "accent": "#FB923C",
    "accent_dark": "#F97316",
    "background": "#0B1220",
    "surface": "#111A2B",
    "surface_alt": "#162235",
    "border": "#243248",
    "text": "#E6EEF8",
    "muted": "#A9B9CC",
    "success": "#34D399",
    "warning": "#FBBF24",
    "danger": "#F87171",
    "shell": "#060B16",
    "shell_soft": "#0E1728",
    "shell_text": "#F4F8FA",
    "chart_grid": "#334155",
    "panel_bg": "linear-gradient(180deg, rgba(17, 26, 43, 0.98), rgba(12, 20, 35, 0.96))",
    "panel_blue": "linear-gradient(180deg, rgba(21, 37, 66, 0.98), rgba(16, 30, 55, 0.96))",
    "panel_green": "linear-gradient(180deg, rgba(16, 43, 34, 0.98), rgba(12, 34, 28, 0.96))",
    "panel_orange": "linear-gradient(180deg, rgba(56, 30, 15, 0.98), rgba(40, 23, 12, 0.96))",
    "panel_shell": "linear-gradient(180deg, rgba(20, 28, 44, 0.98), rgba(14, 20, 32, 0.96))",
    "input_bg": "#162235",
    "input_border": "#334155",
    "chip_bg": "rgba(255,255,255,0.08)",
    "chip_border": "rgba(255,255,255,0.12)",
    "overlay_soft": "rgba(255,255,255,0.10)",
    "overlay_strong": "rgba(17, 26, 43, 0.98)",
}


def get_theme_mode() -> str:
    """Return the current application theme mode."""
    session_mode = st.session_state.get(THEME_MODE_KEY)
    if session_mode in {"Light", "Dark"}:
        return session_mode

    query_mode = st.query_params.get(THEME_QUERY_PARAM, "Light")
    if isinstance(query_mode, list):
        query_mode = query_mode[0] if query_mode else "Light"
    if query_mode not in {"Light", "Dark"}:
        query_mode = "Light"

    st.session_state[THEME_MODE_KEY] = query_mode
    return query_mode


def get_theme_tokens() -> dict[str, str]:
    """Return the active theme token set."""
    return DARK_THEME if get_theme_mode() == "Dark" else LIGHT_THEME


def set_theme_mode(mode: str) -> None:
    """
    Persist the theme mode across pages.

    Streamlit page navigation can rebuild individual page scripts, so I store
    the mode in both session state and query params to keep the user's choice
    stable while moving around the app.
    """
    normalized_mode = mode if mode in {"Light", "Dark"} else "Light"
    st.session_state[THEME_MODE_KEY] = normalized_mode
    st.query_params[THEME_QUERY_PARAM] = normalized_mode


def apply_theme() -> None:
    """
    Inject a consistent, accessible dashboard theme into Streamlit.

    The palette intentionally uses blue and green as requested while keeping
    contrast high enough for readable text, borders, and controls.
    """
    theme = get_theme_tokens()
    st.markdown(
        f"""
        <style>
            :root {{
                --bm-primary: {theme["primary"]};
                --bm-primary-dark: {theme["primary_dark"]};
                --bm-secondary: {theme["secondary"]};
                --bm-secondary-dark: {theme["secondary_dark"]};
                --bm-accent: {theme["accent"]};
                --bm-accent-dark: {theme["accent_dark"]};
                --bm-bg: {theme["background"]};
                --bm-surface: {theme["surface"]};
                --bm-surface-alt: {theme["surface_alt"]};
                --bm-border: {theme["border"]};
                --bm-text: {theme["text"]};
                --bm-muted: {theme["muted"]};
                --bm-success: {theme["success"]};
                --bm-warning: {theme["warning"]};
                --bm-danger: {theme["danger"]};
                --bm-shell: {theme["shell"]};
                --bm-shell-soft: {theme["shell_soft"]};
                --bm-shell-text: {theme["shell_text"]};
            }}

            .stApp {{
                background:
                    radial-gradient(circle at top right, rgba(37, 99, 235, 0.14), transparent 24%),
                    radial-gradient(circle at top left, rgba(5, 150, 105, 0.12), transparent 24%),
                    linear-gradient(180deg, var(--bm-surface-alt) 0%, var(--bm-bg) 100%);
                color: var(--bm-text);
            }}

            .main .block-container {{
                padding-top: 1.15rem;
                padding-bottom: 2.5rem;
                max-width: 1500px;
            }}

            section[data-testid="stSidebar"] {{
                background: linear-gradient(180deg, var(--bm-shell) 0%, var(--bm-shell-soft) 100%);
                border-right: 1px solid rgba(255, 255, 255, 0.08);
            }}

            section[data-testid="stSidebar"] * {{
                color: var(--bm-shell-text);
            }}

            section[data-testid="stSidebar"] label,
            section[data-testid="stSidebar"] .stMarkdown,
            section[data-testid="stSidebar"] .stCaption,
            section[data-testid="stSidebar"] p,
            section[data-testid="stSidebar"] li,
            section[data-testid="stSidebar"] span {{
                color: var(--bm-shell-text);
            }}

            section[data-testid="stSidebar"] [data-baseweb="select"] > div,
            section[data-testid="stSidebar"] [data-baseweb="select"] input,
            section[data-testid="stSidebar"] [data-baseweb="select"] span {{
                color: var(--bm-text) !important;
            }}

            section[data-testid="stSidebar"] [data-baseweb="select"] * {{
                -webkit-text-fill-color: var(--bm-text) !important;
            }}

            section[data-testid="stSidebar"] [data-baseweb="select"] > div > div,
            section[data-testid="stSidebar"] [data-baseweb="select"] > div > div > div,
            section[data-testid="stSidebar"] [data-baseweb="select"] [role="combobox"],
            section[data-testid="stSidebar"] [data-baseweb="select"] [role="combobox"] *,
            section[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] div {{
                color: var(--bm-text) !important;
                opacity: 1 !important;
            }}

            section[data-testid="stSidebar"] [data-baseweb="select"] > div {{
                background: {theme["input_bg"]} !important;
                border-color: {theme["input_border"]} !important;
            }}

            [data-baseweb="select"] > div,
            div[data-testid="stTextInput"] input,
            div[data-testid="stTextArea"] textarea,
            .stMultiSelect [data-baseweb="select"] > div {{
                background: {theme["input_bg"]} !important;
                color: var(--bm-text) !important;
                border: 1px solid {theme["input_border"]} !important;
                -webkit-text-fill-color: var(--bm-text) !important;
            }}

            [data-baseweb="popover"] [role="listbox"] {{
                background: #FFFFFF !important;
                background-color: #FFFFFF !important;
                border: 1px solid #C8D5E1 !important;
                border-radius: 14px !important;
                box-shadow: 0 18px 36px rgba(15, 31, 48, 0.18) !important;
            }}

            [data-baseweb="popover"],
            [data-baseweb="popover"] > div,
            [data-baseweb="menu"],
            [data-baseweb="menu"] > div,
            [data-baseweb="menu"] ul,
            div[role="listbox"],
            ul[role="listbox"] {{
                background: #FFFFFF !important;
                background-color: #FFFFFF !important;
                border-color: #C8D5E1 !important;
            }}

            [data-baseweb="popover"] [role="option"] {{
                color: #102033 !important;
                background: transparent !important;
                background-color: transparent !important;
                -webkit-text-fill-color: #102033 !important;
                opacity: 1 !important;
            }}

            [data-baseweb="popover"] [role="option"] *,
            [data-baseweb="popover"] [role="option"] div,
            [data-baseweb="popover"] [role="option"] span,
            [data-baseweb="popover"] [role="option"] p {{
                color: #102033 !important;
                -webkit-text-fill-color: #102033 !important;
                opacity: 1 !important;
            }}

            li[role="option"],
            li[role="option"] *,
            div[role="option"],
            div[role="option"] *,
            [data-baseweb="menu"] li,
            [data-baseweb="menu"] li *,
            [data-baseweb="menu"] div,
            [data-baseweb="menu"] div *,
            [data-baseweb="popover"] ul li,
            [data-baseweb="popover"] ul li *,
            [data-baseweb="popover"] [id*="option"],
            [data-baseweb="popover"] [id*="option"] * {{
                color: #102033 !important;
                -webkit-text-fill-color: #102033 !important;
                opacity: 1 !important;
            }}

            [data-baseweb="popover"] [role="option"]:hover,
            [data-baseweb="popover"] [role="option"][aria-selected="true"],
            li[role="option"]:hover,
            li[role="option"][aria-selected="true"],
            div[role="option"]:hover,
            div[role="option"][aria-selected="true"],
            [data-baseweb="menu"] li:hover,
            [data-baseweb="menu"] li[aria-selected="true"],
            [data-baseweb="popover"] ul li:hover,
            [data-baseweb="popover"] ul li[aria-selected="true"] {{
                background: #EEF5F8 !important;
                background-color: #EEF5F8 !important;
            }}

            section[data-testid="stSidebar"] [data-baseweb="select"] svg {{
                fill: var(--bm-muted) !important;
            }}

            section[data-testid="stSidebar"] .stSlider [data-baseweb="slider"] * {{
                color: var(--bm-shell-text) !important;
            }}

            section[data-testid="stSidebar"] code {{
                background: {theme["overlay_soft"]};
                color: var(--bm-shell-text);
                padding: 0.12rem 0.35rem;
                border-radius: 8px;
            }}

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {{
                padding-top: 0.5rem;
            }}

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a {{
                border-radius: 14px;
                margin: 0.18rem 0;
                padding: 0.45rem 0.55rem;
                background: transparent;
            }}

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover {{
                background: rgba(255,255,255,0.08);
            }}

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] {{
                background: linear-gradient(90deg, rgba(37,99,235,0.22), rgba(5,150,105,0.18));
                border-left: 3px solid var(--bm-accent);
            }}

            .bm-page-title {{
                font-size: 2.15rem;
                font-weight: 800;
                line-height: 1.05;
                color: var(--bm-text);
                margin-bottom: 0.35rem;
            }}

            .bm-page-subtitle {{
                color: var(--bm-muted);
                font-size: 1rem;
                margin-bottom: 1.15rem;
                max-width: 56rem;
            }}

            .bm-hero {{
                background: linear-gradient(135deg, var(--bm-shell) 0%, var(--bm-primary-dark) 100%);
                color: #FFFFFF;
                border-radius: 22px;
                padding: 1.6rem 1.7rem;
                margin-bottom: 1.2rem;
                box-shadow: 0 20px 44px rgba(6, 32, 54, 0.14);
                border: 1px solid rgba(255,255,255,0.08);
            }}

            .bm-hero h1 {{
                margin: 0 0 0.35rem 0;
                font-size: 2.4rem;
                line-height: 1.05;
                color: #FFFFFF;
            }}

            .bm-hero p {{
                margin: 0;
                color: rgba(255, 255, 255, 0.92);
                font-size: 1rem;
                max-width: 60rem;
            }}

            .bm-hero-grid {{
                display: grid;
                grid-template-columns: 1.35fr 0.95fr;
                gap: 1rem;
                align-items: stretch;
            }}

            .bm-hero-aside {{
                background: rgba(255,255,255,0.1);
                border: 1px solid rgba(255,255,255,0.12);
                border-radius: 18px;
                padding: 1rem 1.1rem;
                display: flex;
                flex-direction: column;
                justify-content: center;
            }}

            .bm-hero-kicker {{
                font-size: 0.78rem;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: rgba(255,255,255,0.72);
                margin-bottom: 0.45rem;
                font-weight: 700;
            }}

            .bm-hero-aside strong {{
                font-size: 1.55rem;
                display: block;
                margin-bottom: 0.2rem;
            }}

            .bm-section-card {{
                background: {theme["panel_bg"]};
                border: 1px solid var(--bm-border);
                border-radius: 18px;
                padding: 1.1rem 1.15rem;
                margin-bottom: 1rem;
                box-shadow: 0 14px 34px rgba(15, 31, 48, 0.06);
                position: relative;
                overflow: hidden;
            }}

            .bm-section-card::before {{
                content: "";
                position: absolute;
                left: 0;
                top: 0;
                bottom: 0;
                width: 5px;
                background: linear-gradient(180deg, var(--bm-primary), var(--bm-secondary));
            }}

            .bm-section-card--primary {{
                background: {theme["panel_blue"]};
            }}

            .bm-section-card--secondary {{
                background: {theme["panel_green"]};
            }}

            .bm-section-card--accent {{
                background: {theme["panel_orange"]};
            }}

            .bm-section-card--shell {{
                background: {theme["panel_shell"]};
            }}

            .bm-section-card h3,
            .bm-section-card h4 {{
                color: var(--bm-text);
                margin-top: 0;
            }}

            .bm-chip-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 0.5rem;
                margin: 0.55rem 0 0.25rem;
            }}

            .bm-chip {{
                background: {theme["chip_bg"]};
                border: 1px solid {theme["chip_border"]};
                border-radius: 999px;
                padding: 0.35rem 0.75rem;
                font-size: 0.86rem;
                color: var(--bm-text);
            }}

            .bm-filter-summary {{
                background: {theme["panel_bg"]};
                border: 1px solid var(--bm-border);
                border-radius: 16px;
                padding: 0.85rem 1rem;
                margin-bottom: 1rem;
            }}

            .bm-filter-summary--primary {{
                background: {theme["panel_blue"]};
            }}

            .bm-filter-summary--secondary {{
                background: {theme["panel_green"]};
            }}

            .bm-filter-summary--accent {{
                background: {theme["panel_orange"]};
            }}

            .bm-filter-summary--shell {{
                background: {theme["panel_shell"]};
            }}

            div[data-testid="metric-container"],
            div[data-testid="stMetric"] {{
                background: {theme["panel_blue"]};
                border: 1px solid rgba(37, 99, 235, 0.20);
                border-radius: 18px;
                padding: 0.9rem 1rem;
                box-shadow: 0 14px 30px rgba(15, 31, 48, 0.07);
                border-top: 4px solid var(--bm-primary);
            }}

            div[data-testid="metric-container"] label,
            div[data-testid="stMetric"] label,
            div[data-testid="metric-container"] [data-testid="stMetricLabel"],
            div[data-testid="stMetric"] [data-testid="stMetricLabel"],
            div[data-testid="metric-container"] [data-testid="stMetricLabel"] *,
            div[data-testid="stMetric"] [data-testid="stMetricLabel"] * {{
                color: var(--bm-text) !important;
                -webkit-text-fill-color: var(--bm-text) !important;
                opacity: 1 !important;
                font-weight: 700;
            }}

            div[data-testid="metric-container"] [data-testid="stMetricValue"],
            div[data-testid="stMetric"] [data-testid="stMetricValue"],
            div[data-testid="metric-container"] [data-testid="stMetricValue"] *,
            div[data-testid="stMetric"] [data-testid="stMetricValue"] * {{
                font-size: 2rem;
                font-weight: 800;
                color: var(--bm-text) !important;
                -webkit-text-fill-color: var(--bm-text) !important;
                opacity: 1 !important;
            }}

            div[data-testid="metric-container"] [data-testid="stMetricDelta"],
            div[data-testid="stMetric"] [data-testid="stMetricDelta"],
            div[data-testid="metric-container"] [data-testid="stMetricDelta"] *,
            div[data-testid="stMetric"] [data-testid="stMetricDelta"] * {{
                font-weight: 700;
            }}

            div[data-testid="stMetric"] p,
            div[data-testid="stMetric"] span,
            div[data-testid="stMetric"] label {{
                color: var(--bm-text) !important;
            }}

            .bm-topbar-brand {{
                display: flex;
                align-items: center;
                gap: 0.85rem;
                background: linear-gradient(135deg, var(--bm-shell) 0%, var(--bm-shell-soft) 100%);
                color: var(--bm-shell-text);
                border-radius: 18px;
                padding: 0.95rem 1rem;
                min-height: 88px;
                box-shadow: 0 18px 34px rgba(8, 19, 28, 0.18);
            }}

            .bm-brand-mark {{
                width: 44px;
                height: 44px;
                border-radius: 14px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 800;
                font-size: 1rem;
                background: linear-gradient(135deg, var(--bm-primary), var(--bm-secondary));
                color: #ffffff;
            }}

            .bm-brand-title {{
                font-size: 1.03rem;
                font-weight: 800;
                color: var(--bm-shell-text);
            }}

            .bm-brand-subtitle {{
                font-size: 0.78rem;
                color: rgba(244, 248, 250, 0.74);
            }}

            .bm-topbar-page {{
                background: {theme["panel_bg"]};
                border: 1px solid var(--bm-border);
                border-radius: 18px;
                padding: 0.95rem 1rem;
                min-height: 88px;
                display: flex;
                flex-direction: column;
                justify-content: center;
            }}

            .bm-topbar-label {{
                font-size: 0.76rem;
                text-transform: uppercase;
                letter-spacing: 0.06em;
                color: var(--bm-muted);
                margin-bottom: 0.3rem;
            }}

            .bm-topbar-value {{
                font-size: 1.15rem;
                font-weight: 800;
                color: var(--bm-text);
            }}

            .bm-connection-bar {{
                margin: 0.9rem 0 1rem;
                padding: 0.9rem 1rem;
                border-radius: 18px;
                background: {theme["panel_bg"]};
                border: 1px solid var(--bm-border);
                display: flex;
                gap: 0.9rem;
                justify-content: space-between;
                align-items: center;
                flex-wrap: wrap;
                box-shadow: 0 14px 34px rgba(15, 31, 48, 0.06);
            }}

            .bm-connection-left {{
                display: flex;
                gap: 0.55rem;
                flex-wrap: wrap;
            }}

            .bm-connection-right {{
                color: var(--bm-muted);
                font-size: 0.92rem;
            }}

            .bm-status-pill {{
                display: inline-flex;
                align-items: center;
                padding: 0.35rem 0.72rem;
                border-radius: 999px;
                font-size: 0.83rem;
                font-weight: 700;
                border: 1px solid var(--bm-border);
                background: var(--bm-surface);
                color: var(--bm-text);
            }}

            .bm-status-pill--good {{
                background: rgba(29, 143, 95, 0.12);
                color: var(--bm-secondary_dark);
                border-color: rgba(29, 143, 95, 0.25);
            }}

            .bm-status-pill--warn {{
                background: rgba(183, 121, 31, 0.12);
                color: #8A5A13;
                border-color: rgba(183, 121, 31, 0.24);
            }}

            .bm-search-results {{
                background: {theme["panel_bg"]};
                border: 1px solid var(--bm-border);
                border-radius: 18px;
                padding: 1rem 1rem 0.6rem;
                margin: 0.3rem 0 1rem;
                box-shadow: 0 14px 32px rgba(15, 31, 48, 0.06);
            }}

            .bm-map-panel {{
                background: {theme["panel_blue"]};
                border: 1px solid var(--bm-border);
                border-radius: 20px;
                padding: 1rem 1rem 0.35rem;
                margin-bottom: 1rem;
                box-shadow: 0 16px 36px rgba(15, 31, 48, 0.08);
            }}

            .bm-chart-guide {{
                margin: 0.55rem 0 0.85rem;
                padding: 0.72rem 0.9rem;
                border-radius: 14px;
                background: {theme["overlay_strong"]};
                border: 1px solid var(--bm-border);
                color: var(--bm-text);
                font-size: 0.92rem;
            }}

            .bm-chart-summary {{
                margin: 0.55rem 0 1.1rem;
                padding: 0.9rem 1rem;
                border-radius: 16px;
                background: {theme["panel_green"]};
                border: 1px solid rgba(5, 150, 105, 0.18);
                color: var(--bm-text);
                box-shadow: 0 12px 28px rgba(15, 31, 48, 0.06);
            }}

            .bm-chart-summary-label {{
                font-size: 0.76rem;
                text-transform: uppercase;
                letter-spacing: 0.07em;
                color: var(--bm-secondary_dark);
                font-weight: 800;
                margin-bottom: 0.35rem;
            }}

            .bm-map-legend {{
                display: flex;
                flex-wrap: wrap;
                gap: 0.65rem;
                margin-bottom: 0.8rem;
            }}

            .bm-legend-item {{
                display: inline-flex;
                align-items: center;
                gap: 0.45rem;
                padding: 0.38rem 0.7rem;
                border-radius: 999px;
                background: {theme["overlay_strong"]};
                border: 1px solid var(--bm-border);
                color: var(--bm-text);
                font-size: 0.84rem;
                font-weight: 700;
            }}

            .bm-legend-dot {{
                width: 11px;
                height: 11px;
                border-radius: 999px;
                display: inline-block;
            }}

            .bm-search-card {{
                background: linear-gradient(135deg, rgba(16,24,38,0.98), rgba(37,99,235,0.92));
                color: white;
                border-radius: 20px;
                padding: 1rem 1.1rem;
                margin-bottom: 1rem;
                box-shadow: 0 18px 34px rgba(8, 19, 28, 0.18);
            }}

            .bm-search-card h3 {{
                color: white;
                margin: 0 0 0.35rem 0;
            }}

            .bm-search-card p {{
                color: rgba(255,255,255,0.86);
                margin: 0;
            }}

            .bm-footer {{
                margin-top: 2rem;
                padding: 1rem 1.1rem;
                border-top: 1px solid var(--bm-border);
                color: var(--bm-muted);
            }}

            .bm-footer-title {{
                font-weight: 800;
                color: var(--bm-text);
                margin-bottom: 0.2rem;
            }}

            div[data-testid="stTextInput"] input {{
                border-radius: 14px;
                border: 1px solid var(--bm-border);
                background: {theme["input_bg"]};
                color: var(--bm-text);
            }}

            .stTabs [data-baseweb="tab-list"] {{
                gap: 0.35rem;
                background: {theme["panel_shell"]};
                padding: 0.35rem;
                border-radius: 999px;
                border: 1px solid var(--bm-border);
                width: fit-content;
            }}

            .stTabs [data-baseweb="tab"] {{
                border-radius: 999px;
                border: 1px solid transparent;
                background: transparent;
                color: var(--bm-muted);
                font-weight: 700;
                padding: 0.42rem 0.95rem;
            }}

            .stTabs [aria-selected="true"] {{
                background: var(--bm-primary);
                color: white;
                border-color: var(--bm-primary);
                box-shadow: 0 8px 20px rgba(37, 99, 235, 0.22);
            }}

            .stAlert {{
                border-radius: 16px;
                border-width: 1px;
            }}

            .stAlert[data-baseweb="notification"] {{
                background: {theme["panel_bg"]};
            }}

            .stAlert [data-testid="stMarkdownContainer"] p {{
                color: var(--bm-text);
            }}

            [data-testid="stHorizontalBlock"] > div:has(> div[data-testid="metric-container"]) {{
                align-self: stretch;
            }}

            .stSelectbox label,
            .stSlider label,
            .stTextArea label,
            .stMultiSelect label,
            div[data-testid="stWidgetLabel"],
            div[data-testid="stWidgetLabel"] *,
            .stTextInput label {{
                font-weight: 700;
                color: var(--bm-text) !important;
                -webkit-text-fill-color: var(--bm-text) !important;
                opacity: 1 !important;
            }}

            div[data-testid="stDataFrame"] {{
                border: 1px solid var(--bm-border);
                border-radius: 18px;
                overflow: hidden;
                background: {theme["panel_bg"]};
            }}

            .stButton > button,
            .stFormSubmitButton > button {{
                width: 100%;
                border-radius: 14px;
                border: 1px solid var(--bm-primary);
                background: linear-gradient(135deg, var(--bm-primary) 0%, var(--bm-secondary) 100%);
                color: #ffffff !important;
                font-weight: 800;
                box-shadow: 0 12px 24px rgba(15, 31, 48, 0.18);
            }}

            .stButton > button:hover,
            .stFormSubmitButton > button:hover {{
                border-color: var(--bm-accent);
                filter: brightness(1.03);
            }}

            .stButton > button p,
            .stButton > button span,
            .stFormSubmitButton > button p,
            .stFormSubmitButton > button span {{
                color: #ffffff !important;
                -webkit-text-fill-color: #ffffff !important;
                opacity: 1 !important;
            }}

            @media (max-width: 900px) {{
                .bm-hero-grid {{
                    grid-template-columns: 1fr;
                }}
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
