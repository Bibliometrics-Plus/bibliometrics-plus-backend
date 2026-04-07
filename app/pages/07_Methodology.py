"""Methodology page for Bibliometrics+."""

from __future__ import annotations

import sys
from pathlib import Path
import streamlit as st

# This keeps the shared package imports working in Streamlit's multipage mode.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.layout import configure_page, render_app_shell, render_footer, render_page_header, render_section_intro
from app.services.filters import render_global_filters
from app.styles.theme import apply_theme


configure_page("Bibliometrics+ | Methodology")
apply_theme()
render_global_filters()
render_app_shell("Methodology")

render_page_header(
    "Methodology",
    "This page explains the application structure, metric definitions, and the analytical choices behind the visualizations.",
)

render_section_intro(
    "Project Scope",
    "Bibliometrics+ is an AI & EDI-driven library usage analytics application designed to compare public library usage and collection patterns across systems while surfacing equity-related context where supporting data exists.",
)

st.markdown(
    """
    ### Real Data Sources
    - `branch_kpi` supports branch-level circulation, visits, and registrations for Toronto and Montreal.
    - `collection_item` supports collection analytics, publication year coverage, and accessibility analytics.
    - `ottawa_branch_edi_priority` and related Ottawa tables support branch-level equity-context analysis.
    - `library_statistics`, `user_group_stats`, and neighbourhood/context tables support additional system-level coverage notes.

    ### KPI Logic
    - Circulation, visits, and registrations are aggregated from the real `branch_kpi` table.
    - System comparisons summarize the branch KPI rows currently available for each system.
    - Branch explorer comparisons benchmark a selected branch against the average branch in the same system.

    ### EDI Logic
    - Accessibility views are based on the real `accessibility_format` field in `collection_item`.
    - Collection age and recency views are based on the real `publication_year` field.
    - Toronto neighbourhood context comes from the real `tpl_neighbourhood_profile` table joined to branch records.
    - Ottawa branch EDI rows come from the precomputed Ottawa equity-context tables already loaded in the database.
    - Montreal currently contributes collection-based EDI indicators in the dashboard, but not a branch-linked neighbourhood-context layer.

    ### Why Some Planned Charts Were Redesigned
    - Subject analytics were part of the original plan, but the live `subject` and `collection_item_subject` tables are currently empty.
    - To avoid unsupported placeholder content, those visuals were replaced with real format, accessibility, publication-year, and branch KPI views.

    ### AI Grounding
    - The AI page does not read the whole database directly.
    - The dashboard first queries the filtered KPI and EDI results.
    - Those results are then passed to the model as grounding context.
    - If the data in scope is limited, the AI is expected to say that clearly.
    """
    )
render_footer()
