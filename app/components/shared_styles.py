import streamlit as st


def apply_shared_styles():
    st.markdown(
        """
        <style>
            :root {
                --bg-main: #04113C;
                --bg-card: #071B5A;
                --bg-soft: #DDE6F2;
                --border-glow: rgba(45, 168, 255, 0.45);
                --text-main: #FFFFFF;
                --text-dark: #0B4EA2;
                --text-soft: #D6E4FF;
            }

            .stApp {
                background: #04113C !important;
                color: var(--text-main) !important;
            }

            [data-testid="stAppViewContainer"] {
                background: #04113C !important;
            }

            [data-testid="stHeader"] {
                background: transparent !important;
            }

            .block-container {
                padding-top: 1.2rem;
                padding-bottom: 2rem;
            }

            h1, h2, h3, h4, h5, h6 {
                color: #FFFFFF !important;
                font-weight: 800 !important;
            }

            p, label, div, span {
                color: inherit;
            }

            .shared-hero {
                background: linear-gradient(135deg, #0A1E63 0%, #157FD6 100%);
                padding: 1.4rem 1.6rem;
                border-radius: 20px;
                border: 1px solid var(--border-glow);
                margin-bottom: 1.25rem;
                box-shadow: 0 0 14px rgba(21, 127, 214, 0.18);
            }

            .shared-hero h3 {
                margin-top: 0 !important;
                margin-bottom: 0.5rem !important;
                color: #FFFFFF !important;
            }

            .shared-hero p {
                margin-bottom: 0 !important;
                color: #F4F8FF !important;
                line-height: 1.6;
            }

            div[data-testid="stMetric"] {
                background: rgba(7, 27, 90, 0.95) !important;
                border: 1px solid var(--border-glow) !important;
                border-radius: 16px !important;
                padding: 1rem 1rem !important;
                box-shadow: 0 0 10px rgba(45, 168, 255, 0.10) !important;
            }

            div[data-testid="stMetricLabel"] {
                color: #FFFFFF !important;
                opacity: 1 !important;
                font-weight: 700 !important;
            }

            div[data-testid="stMetricValue"] {
                color: #FFFFFF !important;
                font-weight: 800 !important;
            }

            div[data-testid="stAlert"] {
                background-color: #DDE6F2 !important;
                border: 1px solid #C7D3E6 !important;
                border-radius: 16px !important;
            }

            div[data-testid="stAlert"] p {
                color: #0B4EA2 !important;
                font-size: 1rem !important;
                line-height: 1.5 !important;
            }

            div[data-testid="stSidebar"] {
                background: #DDE3EC !important;
            }

            div[data-testid="stSidebar"] * {
                color: #24324A !important;
            }

            div[data-testid="stSidebarNav"] a[aria-current="page"] {
                background-color: #C3D4EC !important;
                color: #1E4F94 !important;
                font-weight: 700 !important;
                border-radius: 12px !important;
            }

            div[data-testid="stSidebarNav"] a:hover {
                background-color: #CCD9EE !important;
                border-radius: 12px !important;
            }

            section[data-testid="stSidebar"] hr,
            div[data-testid="stSidebar"] hr,
            hr {
                border-color: #BCCCE3 !important;
            }
            
            div[data-testid="stDataFrame"] {
                border-radius: 14px !important;
                overflow: hidden !important;
            }

            .stMarkdown, .stCaption {
                color: var(--text-main) !important;
            }

            .stCaption {
                color: #C8D8F2 !important;
            }
            .metric-card {
                background: rgba(7, 27, 90, 0.95);
                border: 1px solid rgba(45, 168, 255, 0.45);
                border-radius: 18px;
                padding: 1rem 1.1rem;
                min-height: 140px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                box-shadow: 0 0 10px rgba(45, 168, 255, 0.10);
            }

            .metric-card-title {
                color: #CFE1F7 !important;
                font-size: 1rem;
                font-weight: 700;
                line-height: 1.4;
                margin-bottom: 0.8rem;
            }

            .metric-card-value {
                color: #FFFFFF !important;
                font-size: 2.6rem;
                font-weight: 800;
                line-height: 1;
            }

        </style>
        """,
        unsafe_allow_html=True,
    )
