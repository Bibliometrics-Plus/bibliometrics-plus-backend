"""Data quality and coverage page for Bibliometrics+."""

from __future__ import annotations

import sys
from pathlib import Path
import streamlit as st

# This ensures the shared `app.*` modules are importable when the page is run
# by Streamlit as an individual script.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.layout import configure_page, render_app_shell, render_footer, render_page_header, render_section_intro
from app.components.tables import show_dataframe
from app.services.filters import render_global_filters
from app.services.formatters import format_pct
from app.services.queries import get_data_quality_overview, get_supported_metrics_notes, get_table_inventory
from app.styles.theme import apply_theme


configure_page("Bibliometrics+ | Data Quality")
apply_theme()
filters = render_global_filters()
render_app_shell("Data Quality")

render_page_header(
    "Data Quality & Coverage",
    "Review data completeness, coverage, and source availability across the analytical areas used in the application.",
)

st.info(
    "This page reports application-wide data coverage and support status. "
    f"The current sidebar scope is {filters.system} / {filters.branch}, but the audit values below summarize the overall loaded environment."
)

overview_df = get_data_quality_overview()
overview = overview_df.iloc[0]

metric_1, metric_2, metric_3 = st.columns(3)
with metric_1:
    st.metric("Publication Year Coverage", format_pct(overview["items_with_publication_year"], overview["collection_items"]))
with metric_2:
    st.metric("Accessibility Coverage", format_pct(overview["items_with_accessibility"], overview["collection_items"]))
with metric_3:
    st.metric("Toronto Context Rows", int(overview["toronto_edi_rows"]))

metric_4, metric_5 = st.columns(2)
with metric_4:
    st.metric("Ottawa EDI Rows", int(overview["ottawa_edi_rows"]))
with metric_5:
    st.metric("Subject Links", int(overview["item_subject_links"]))

render_section_intro(
    "Supported Analytics Status",
    "Only supported analytical areas are surfaced in the application. Unsupported areas are identified explicitly.",
)
show_dataframe(get_supported_metrics_notes())

render_section_intro(
    "Public Table Inventory",
    "These row counts help confirm which tables provide enough depth to support the analytical views.",
)
show_dataframe(get_table_inventory())
render_footer()
