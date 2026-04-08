import streamlit as st


def apply_shared_styles():
    st.markdown(
        """
        <style>
            :root {
                --bg-main: #04113C;
                --bg-card: #071B5A;
                --bg-soft: #DDE6F2;
                --bg-soft-2: #CFE1F7;
                --text-main: #FFFFFF;
                --text-soft: #D6E4FF;
                --text-dark: #24324A;
                --text-accent: #0B4EA2;
                --gold: #F2C94C;
                --border-glow: rgba(45, 168, 255, 0.35);
            }

            .stApp,
            [data-testid="stAppViewContainer"] {
                background: var(--bg-main) !important;
                color: var(--text-main) !important;
            }

            [data-testid="stHeader"],
            [data-testid="stToolbar"] {
                background: transparent !important;
            }

            .block-container {
                padding-top: 1.2rem !important;
                padding-bottom: 2rem !important;
            }

            h1, h2, h3, h4, h5, h6 {
                color: #F4F8FF !important;
                font-weight: 800 !important;
            }

            .stCaption {
                color: #C8D8F2 !important;
            }

            .brand-line {
                color: var(--gold);
                font-size: 1.35rem;
                font-weight: 800;
                margin-bottom: 0.15rem;
            }

            .shared-hero {
                background: linear-gradient(135deg, #0A1E63 0%, #157FD6 100%);
                padding: 1.35rem 1.5rem;
                border-radius: 20px;
                border: 1px solid var(--border-glow);
                margin-bottom: 1.25rem;
                box-shadow: 0 0 18px rgba(21, 127, 214, 0.18);
            }

            .shared-hero h3 {
                margin: 0 0 0.45rem 0 !important;
                color: #FFFFFF !important;
            }

            .shared-hero p {
                margin: 0 !important;
                color: #F4F8FF !important;
                line-height: 1.6 !important;
            }

            .metric-card {
                background: rgba(7, 27, 90, 0.96);
                border: 1px solid var(--border-glow);
                border-radius: 18px;
                padding: 1rem 1.1rem;
                min-height: 130px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                box-shadow: 0 0 12px rgba(45, 168, 255, 0.10);
            }

            .metric-card-title {
                color: #CFE1F7 !important;
                font-size: 1rem;
                font-weight: 700;
                line-height: 1.35;
                margin-bottom: 0.7rem;
            }

            .metric-card-value {
                color: #FFFFFF !important;
                font-size: 2.3rem;
                font-weight: 800;
                line-height: 1;
            }

            div[data-testid="stAlert"] {
                background-color: #DDE6F2 !important;
                border: 1px solid #C7D3E6 !important;
                border-radius: 16px !important;
            }

            div[data-testid="stAlert"] p {
                color: #0B4EA2 !important;
                font-size: 1rem !important;
                line-height: 1.55 !important;
            }

            section[data-testid="stSidebar"] {
                background: #DDE3EC !important;
            }

            section[data-testid="stSidebar"] * {
                color: #24324A !important;
            }

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a {
                color: #24324A !important;
                border-radius: 12px !important;
            }

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a * {
                color: #24324A !important;
            }

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] {
                background-color: #C3D4EC !important;
                border-radius: 12px !important;
            }

            section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] * {
                color: #1E4F94 !important;
                font-weight: 700 !important;
            }

            section[data-testid="stSidebar"] hr {
                border-color: #BCCCE3 !important;
            }

            div[data-testid="stDataFrame"] {
                border-radius: 16px !important;
                overflow: hidden !important;
                border: 1px solid rgba(45, 168, 255, 0.30) !important;
            }

            div[data-testid="stDataFrame"] * {
                color: #FFFFFF !important;
            }

            div[data-testid="stDataFrame"] thead tr th {
                background-color: #0B2C7D !important;
                color: #FFFFFF !important;
                font-weight: 700 !important;
            }

            div[data-testid="stDataFrame"] tbody tr td,
            div[data-testid="stDataFrame"] tbody th {
                background-color: #071B5A !important;
                color: #FFFFFF !important;
                border-color: rgba(255,255,255,0.08) !important;
            }
            /* Chat messages */
            [data-testid="stChatMessage"] {
                color: #FFFFFF !important;
            }

            [data-testid="stChatMessage"] * {
                color: #FFFFFF !important;
            }

            /* User and assistant markdown in chat */
            [data-testid="stChatMessage"] .stMarkdown,
            [data-testid="stChatMessage"] .stMarkdown p,
            [data-testid="stChatMessage"] .stMarkdown div,
            [data-testid="stChatMessage"] span {
                color: #FFFFFF !important;
            }

            /* Chat input text */
            [data-testid="stChatInput"] textarea,
            [data-testid="stChatInput"] input,
            [data-testid="stChatInput"] * {
                color: #24324A !important;
            }
            
            [data-testid="stExpander"] summary {
                color: #FFFFFF !important;
                -webkit-text-fill-color: #FFFFFF !important;
                background: #071B5A !important;
            }

            [data-testid="stExpander"] summary span,
            [data-testid="stExpander"] summary p,
            [data-testid="stExpander"] summary div {
                color: #FFFFFF !important;
                -webkit-text-fill-color: #FFFFFF !important;
            }

            [data-testid="stExpander"] details[open] summary span,
            [data-testid="stExpander"] details[open] summary p,
            [data-testid="stExpander"] details[open] summary div {
                color: #FFFFFF !important;
                -webkit-text-fill-color: #FFFFFF !important;
            }


        </style>
        """,
        unsafe_allow_html=True,
    )


def render_brand():
    st.markdown("<div class='brand-line'>Bibliometrics+</div>", unsafe_allow_html=True)


def render_page_intro(title: str, subtitle: str):
    st.markdown(
        f"""
        <div class="shared-hero">
            <h3>{title}</h3>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_card(title: str, value: str):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-card-title">{title}</div>
            <div class="metric-card-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
