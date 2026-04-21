"""AI insights page for Bibliometrics+."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Streamlit loads this page directly, so I add the repository root before
# importing the shared `app.*` modules used by the AI page.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.layout import configure_page, render_app_shell, render_filter_summary, render_footer, render_page_header, render_section_intro
from app.services.ai_service import AIServiceError, ask_openai, dataframe_context, has_openai_api_key
from app.services.filters import render_global_filters
from app.services.queries import get_accessibility_distribution, get_circulation_trend, get_kpi_snapshot, get_ottawa_edi_priority
from app.styles.theme import apply_theme


configure_page("Bibliometrics+ | AI Insights")
apply_theme()
filters = render_global_filters()
render_app_shell("AI Insights")

render_page_header(
    "AI Insights",
    "Ask questions about the current view using AI responses based on the filtered query results.",
)
render_filter_summary(
    [
        f"System: {filters.system}",
        f"Branch: {filters.branch}",
        f"Years: {filters.year_start} to {filters.year_end}",
        "Responses are generated from the current filtered results in scope.",
    ]
)

render_section_intro(
    "Ask Bibliometrics+",
    "This assistant summarizes the current KPI and EDI evidence and is expected to signal when the available context is limited.",
)

has_openai_key = has_openai_api_key()
if not has_openai_key:
    st.warning(
        "AI responses are unavailable until `OPENAI_API_KEY` is added to the environment or Streamlit secrets. "
        "The rest of the dashboard remains fully live on database-backed data."
    )

prompt_option = st.selectbox(
    "Suggested Prompt",
    [
        "Executive summary",
        "KPI trend explanation",
        "Branch comparison",
        "EDI summary",
        "Custom question",
    ],
)

default_prompt_map = {
    "Executive summary": (
        "Summarize the most important KPI and EDI patterns in the current filtered view "
        "and explain what a stakeholder should pay attention to."
    ),
    "KPI trend explanation": "Explain the most important circulation and operational KPI trends in the current filtered view.",
    "Branch comparison": "Compare the current branch or system view against the broader operating context and highlight the main strengths or concerns.",
    "EDI summary": "Explain the most important accessibility, collection age, and Ottawa EDI findings visible in the current filtered view.",
    "Custom question": "",
}
default_question = default_prompt_map[prompt_option]
question = st.text_area("Ask Bibliometrics+", value=default_question, height=120)

if st.button("Generate AI Insight", type="primary", disabled=not has_openai_key):
    with st.spinner("Generating response from the current filtered results..."):
        try:
            snapshot_df = get_kpi_snapshot(filters)
            trend_df = get_circulation_trend(filters)
            access_df = get_accessibility_distribution(filters)
            ottawa_edi_df = get_ottawa_edi_priority(filters) if filters.system in {"All Systems", "OPL"} else None

            contexts = [
                dataframe_context("kpi_snapshot", snapshot_df),
                dataframe_context("circulation_trend", trend_df),
                dataframe_context("accessibility_distribution", access_df),
            ]
            if ottawa_edi_df is not None and not ottawa_edi_df.empty:
                contexts.append(dataframe_context("ottawa_edi_priority", ottawa_edi_df))

            answer = ask_openai(question, contexts)
            st.success("Response generated from the current filtered context.")
            st.write(answer)
        except AIServiceError as exc:
            st.error(str(exc))
        except Exception as exc:  # pragma: no cover - defensive UI path
            st.error(f"AI generation failed: {exc}")
render_footer()
