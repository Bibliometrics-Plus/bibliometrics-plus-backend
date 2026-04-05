"""
04_AI_Insights.py

Purpose:
- present an interpretation layer on top of the KPI and EDI data
- generate narrative summaries that help explain what the dashboard shows
- clearly distinguish between live data and demo-generated estimates
"""

from __future__ import annotations

import os
import pandas as pd
import streamlit as st
import altair as alt
from openai import OpenAI

from app.components.shared_styles import apply_shared_styles, render_brand, render_page_intro
from app.services.supabase_connector import check_connection, get_table_row_counts
from app.services.dashboard_utils import (
    run_query,
    demo_subjects,
    demo_accessibility,
    demo_publication_year,
)

def style_chart(chart):
    return chart.configure_view(
        strokeOpacity=0
    ).configure_axis(
        labelColor="#24324A",
        titleColor="#24324A",
        gridColor="rgba(36,50,74,0.12)",
        domainColor="rgba(36,50,74,0.20)",
        tickColor="rgba(36,50,74,0.20)",
        labelFontSize=13,
        titleFontSize=15
    ).configure_title(
        color="#24324A",
        fontSize=18
    )



def get_openai_client():
    api_key = st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    return OpenAI(api_key=api_key)


def df_preview(df, max_rows=8):
    if df is None or df.empty:
        return "No rows available."
    try:
        return df.head(max_rows).to_markdown(index=False)
    except Exception:
        return df.head(max_rows).to_string(index=False)


st.set_page_config(
    page_title="Bibliometrics+ | AI Insights",
    page_icon="📚",
    layout="wide"
)

apply_shared_styles()
render_brand()

render_page_intro(
    "AI Insights",
    "AI-assisted interpretation of KPI, EDI, and data-status patterns using the current dashboard filter context."
)

st.sidebar.markdown(
    """
    <div style="color:#24324A; font-size:2rem; font-weight:700; margin-bottom:0.75rem;">
        AI Filters
    </div>
    """,
    unsafe_allow_html=True
)

selected_system = st.sidebar.selectbox(
    "Library System",
    ["All Libraries", "Ottawa", "Toronto", "Montreal"],
    key="ai_system"
)

system_map = {
    "Ottawa": "OPL",
    "Toronto": "TPL",
    "Montreal": "MPL"
}

if selected_system == "All Libraries":
    filter_clause = ""
    filter_params = {}
else:
    filter_clause = "AND l.system_name = :selected_system"
    filter_params = {"selected_system": system_map[selected_system]}

# Data status context
db_status = check_connection()
table_counts_df, _ = get_table_row_counts(schema="public")

# Circulation trend
sql_trend = f"""
SELECT
    bk.year,
    SUM(bk.circulation) AS total_circulation
FROM branch_kpi bk
JOIN library l
    ON bk.library_id = l.library_id
WHERE bk.circulation IS NOT NULL
{filter_clause}
GROUP BY bk.year
ORDER BY bk.year;
"""
df_trend = run_query(sql_trend, filter_params)

# System comparison
sql_system = f"""
SELECT
    l.system_name AS system,
    SUM(bk.circulation) AS total_circulation
FROM branch_kpi bk
JOIN library l
    ON bk.library_id = l.library_id
WHERE bk.circulation IS NOT NULL
{filter_clause}
GROUP BY l.system_name
ORDER BY total_circulation DESC;
"""
df_system = run_query(sql_system, filter_params)

# Subject representation
sql_subject = f"""
SELECT
    s.subject_name,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
JOIN collection_item_subject cis
    ON ci.item_id = cis.item_id
JOIN subject s
    ON cis.subject_id = s.subject_id
WHERE s.subject_name IS NOT NULL
{filter_clause}
GROUP BY s.subject_name
ORDER BY item_count DESC
LIMIT 10;
"""
df_subject = run_query(sql_subject, filter_params)

used_demo_subject = False
if df_subject.empty:
    df_subject = demo_subjects(selected_system)
    used_demo_subject = True

# Accessibility distribution
sql_access = f"""
SELECT
    ci.accessibility_format,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
WHERE ci.accessibility_format IS NOT NULL
{filter_clause}
GROUP BY ci.accessibility_format
ORDER BY item_count DESC;
"""
df_access = run_query(sql_access, filter_params)

used_demo_access = False
if df_access.empty:
    df_access = demo_accessibility(selected_system)
    used_demo_access = True

# Publication-year distribution
sql_pub_year = f"""
SELECT
    ci.publication_year,
    COUNT(*) AS item_count
FROM collection_item ci
JOIN library l
    ON ci.library_id = l.library_id
WHERE ci.publication_year IS NOT NULL
{filter_clause}
GROUP BY ci.publication_year
ORDER BY ci.publication_year;
"""
df_year = run_query(sql_pub_year, filter_params)

used_demo_year = False
if df_year.empty:
    df_year = demo_publication_year(selected_system)
    used_demo_year = True

# Toronto neighbourhood context
df_neighbourhood = pd.DataFrame()
if selected_system in ["All Libraries", "Toronto"]:
    sql_neighbourhood = """
    SELECT
        l.name AS branch_name,
        l.neighbourhood_no,
        l.neighbourhood_name,
        t.tsns_designation,
        t.median_after_tax_income_2020,
        t.low_income_lim_at_pct,
        t.core_housing_need_pct,
        t.shelter_cost_30_plus_pct,
        t.age_0_14_pct,
        t.age_65_plus_pct,
        t.non_official_languages_count
    FROM library l
    JOIN tpl_neighbourhood_profile t
        ON l.neighbourhood_no = t.neighbourhood_no
    WHERE l.system_name = 'TPL'
    ORDER BY
        t.low_income_lim_at_pct DESC NULLS LAST,
        t.median_after_tax_income_2020 ASC NULLS LAST;
    """
    df_neighbourhood = run_query(sql_neighbourhood)

# Executive narrative
st.subheader("Executive Narrative")

scope = selected_system if selected_system != "All Libraries" else "the full dataset"

if not df_trend.empty:
    first_year = int(df_trend["year"].min())
    last_year = int(df_trend["year"].max())

    first_val = float(df_trend.loc[df_trend["year"] == first_year, "total_circulation"].values[0])
    last_val = float(df_trend.loc[df_trend["year"] == last_year, "total_circulation"].values[0])

    direction = "declined" if last_val < first_val else "increased"

    st.info(
        f"""
For {scope}, total circulation has **{direction}** between {first_year} and {last_year},
moving from approximately **{first_val:,.0f}** to **{last_val:,.0f}** loans.
"""
    )
else:
    st.info("A live circulation trend is not currently available for this filter selection.")

# Interpretive findings
top_subject = df_subject.iloc[0]["subject_name"]
top_subject_count = int(df_subject.iloc[0]["item_count"])

top_access = df_access.iloc[0]["accessibility_format"]
top_access_count = int(df_access.iloc[0]["item_count"])

st.subheader("Interpretive Findings")
st.write(f"- The strongest currently visible subject area is **{top_subject}** ({top_subject_count:,} items).")
st.write(f"- The most visible accessibility-related format is **{top_access}** ({top_access_count:,} items).")
st.write(f"- Subject insight source: {'Generated demo data' if used_demo_subject else 'Live Supabase data'}")
st.write(f"- Accessibility insight source: {'Generated demo data' if used_demo_access else 'Live Supabase data'}")

# Support charts
c1, c2 = st.columns(2)

with c1:
    st.markdown("**Top Subject Representation**")
    subject_chart = alt.Chart(df_subject).mark_bar().encode(
        x=alt.X("item_count:Q", title="Items"),
        y=alt.Y("subject_name:N", sort="-x", title="Subject")
    ).properties(height=320)
    st.altair_chart(style_chart(subject_chart), width="stretch")

with c2:
    st.markdown("**Accessibility Signal**")
    access_chart = alt.Chart(df_access).mark_bar().encode(
        x=alt.X("item_count:Q", title="Items"),
        y=alt.Y("accessibility_format:N", sort="-x", title="Accessibility Format")
    ).properties(height=320)
    st.altair_chart(style_chart(access_chart), width="stretch")

# AI client
client = get_openai_client()
if client is None:
    st.warning("OPENAI_API_KEY is missing, so live AI chat is not available yet.")
else:
    st.success("OpenAI client loaded.")

st.subheader("Ask the AI About These Insights")
st.caption("The AI assistant uses data summaries from Data Status, KPI, EDI, and Toronto neighbourhood context where available.")


def build_ai_context():
    scope_text = selected_system if selected_system != "All Libraries" else "all library systems"

    context = f"""
You are helping interpret a public-library analytics dashboard.

Current filter scope: {scope_text}

DATABASE / DATA STATUS
Connection mode: {db_status.mode}
Connection message: {db_status.message}

Table inventory preview:
{df_preview(table_counts_df)}

KPI PAGE CONTEXT

Circulation trend:
{df_preview(df_trend)}

System comparison:
{df_preview(df_system)}

EDI PAGE CONTEXT

Accessibility distribution:
{df_preview(df_access)}

Publication-year distribution:
{df_preview(df_year)}

Subject representation:
{df_preview(df_subject)}

Toronto neighbourhood context:
{df_preview(df_neighbourhood)}

Known coverage note:
- TPL currently has the strongest branch_kpi coverage.
- MPL currently has stronger collection_item coverage.
- OPL currently has more limited KPI/collection coverage.
- Subject-linked borrowing data may still rely on demo fallback in some views.

Rules:
- Be concise and practical.
- Distinguish live data from demo/fallback data.
- Do not invent missing facts.
- If data coverage is incomplete, say so clearly.
- Use plain language suitable for librarians, instructors, and non-technical users.
- When discussing neighbourhood data, treat core_housing_need_pct and shelter_cost_30_plus_pct as percentages.
- Treat age_0_14_pct and age_65_plus_pct as counts if values look like counts rather than percentages.
- Treat non_official_languages_count as a count, not a percentage.
"""

    if used_demo_subject:
        context += "\nSubject data note: subject representation is currently based on generated demo data.\n"
    if used_demo_access:
        context += "\nAccessibility data note: accessibility distribution is currently based on generated demo data.\n"
    if used_demo_year:
        context += "\nPublication-year note: publication-year distribution is currently based on generated demo data.\n"
    if df_neighbourhood.empty:
        context += "\nNeighbourhood context note: Toronto neighbourhood context is unavailable or not applicable for the current filter.\n"

    return context


if "ai_messages" not in st.session_state:
    st.session_state.ai_messages = [
        {
            "role": "assistant",
            "content": "Ask me about circulation trends, accessibility patterns, subject diversity, neighbourhood context, or data coverage in the current dashboard view."
        }
    ]

for message in st.session_state.ai_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Ask about the current dashboard view..."):
    st.session_state.ai_messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        if client is None:
            reply = "OPENAI_API_KEY is missing, so live AI chat is not available yet."
            st.markdown(reply)
        else:
            try:
                context = build_ai_context()

                stream = client.responses.create(
                    model="gpt-5",
                    input=[
                        {
                            "role": "developer",
                            "content": (
                                "You are an analytics assistant for a public library dashboard. "
                                "Answer only from the supplied dashboard context. "
                                "If data is incomplete or demo-based, say that clearly. "
                                "Be practical, concise, and easy for non-technical users to understand."
                            ),
                        },
                        {
                            "role": "user",
                            "content": f"{context}\n\nUser question: {prompt}",
                        },
                    ],
                    stream=True,
                )

                chunks = []
                placeholder = st.empty()

                for event in stream:
                    if event.type == "response.output_text.delta":
                        chunks.append(event.delta)
                        placeholder.markdown("".join(chunks))

                reply = "".join(chunks) if chunks else "No response was returned."

            except Exception as e:
                reply = f"AI response failed: {e}"
                st.error(reply)

    st.session_state.ai_messages.append({"role": "assistant", "content": reply})

st.caption("Bibliometrics+ | AI Insights")
