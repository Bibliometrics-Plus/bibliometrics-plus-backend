# app.py
import streamlit as st
import pandas as pd
from sqlalchemy import text
from db import engine

# Basic page config
st.set_page_config(
    page_title="Bibliometrics+ – Ottawa Demo",
    layout="wide",
)

# ---------- Helpers ----------

@st.cache_data
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a SQL query against Supabase and return a DataFrame."""
    params = params or {}
    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


@st.cache_data
def get_years() -> list[int]:
    """Get the list of years from user_group_stats (Dataset #1)."""
    try:
        df = run_query(
            "SELECT DISTINCT year FROM user_group_stats ORDER BY year;"
        )
    except Exception as e:
        st.error(f"Could not fetch years from user_group_stats: {e}")
        return []
    return df["year"].tolist()


# ---------- Main UI ----------

def main():
    st.title("📊 Bibliometrics+ – Ottawa Library Usage (Sprint 2 Demo)")

    # Sidebar filters
    st.sidebar.header("Filters")

    years = get_years()
    if not years:
        st.stop()  # nothing to show, error already rendered

    selected_year = st.sidebar.selectbox("Year", years, index=len(years) - 1)
    st.sidebar.markdown("**System:** Ottawa Public Library")

    # ---------- KPI Row ----------

    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

    # Total loans (from circulation_transaction)
    loans_df = run_query(
        """
        SELECT COALESCE(SUM(loan_count), 0) AS total_loans
        FROM circulation_transaction
        WHERE EXTRACT(YEAR FROM borrow_date) = :year;
        """,
        {"year": int(selected_year)},
    )
    total_loans = int(loans_df["total_loans"].iloc[0])

    # Total cardholders (from user_group_stats)
    cards_df = run_query(
        """
        SELECT COALESCE(SUM(cardholder_count), 0) AS total_cardholders
        FROM user_group_stats
        WHERE year = :year;
        """,
        {"year": int(selected_year)},
    )
    total_cardholders = int(cards_df["total_cardholders"].iloc[0])

    # Branch count (from library)
    branches_df = run_query(
        """
        SELECT COUNT(*) AS branch_count
        FROM library
        WHERE system_name = 'Ottawa';
        """
    )
    branch_count = int(branches_df["branch_count"].iloc[0])

    with kpi_col1:
        st.metric("Total loans", f"{total_loans:,}")
    with kpi_col2:
        st.metric("Total cardholders", f"{total_cardholders:,}")
    with kpi_col3:
        st.metric("Ottawa branches", branch_count)

    st.markdown("---")

    # ---------- Charts & Tables ----------

    # Loans by branch
    st.subheader(f"Loans by branch – {selected_year}")
    loans_by_branch = run_query(
        """
        SELECT l.name AS branch,
               SUM(c.loan_count) AS total_loans
        FROM circulation_transaction c
        JOIN library l ON c.library_id = l.library_id
        WHERE EXTRACT(YEAR FROM c.borrow_date) = :year
          AND l.system_name = 'Ottawa'
        GROUP BY l.name
        ORDER BY total_loans DESC;
        """,
        {"year": int(selected_year)},
    )

    if loans_by_branch.empty:
        st.info("No circulation data for this year.")
    else:
        st.bar_chart(loans_by_branch.set_index("branch")["total_loans"])

    col_left, col_right = st.columns(2)

    # Cardholders by neighbourhood
    with col_left:
        st.subheader(f"Cardholders by neighbourhood – {selected_year}")
        card_by_neigh = run_query(
            """
            SELECT neighbourhood,
                   cardholder_count
            FROM user_group_stats
            WHERE year = :year
            ORDER BY cardholder_count DESC;
            """,
            {"year": int(selected_year)},
        )

        if card_by_neigh.empty:
            st.info("No cardholder stats for this year.")
        else:
            st.bar_chart(
                card_by_neigh.set_index("neighbourhood")["cardholder_count"]
            )

    # Top requested titles (from most-requested ETL)
    with col_right:
        st.subheader("Top 10 most requested titles (Ottawa)")
        top_titles = run_query(
            """
            SELECT
                ci.title,
                COALESCE(string_agg(a.name, ', '), 'Unknown') AS authors,
                ci.format,
                ci.request_count
            FROM collection_item ci
            LEFT JOIN collection_item_author cia ON ci.item_id = cia.item_id
            LEFT JOIN author a ON cia.author_id = a.author_id
            WHERE ci.request_count IS NOT NULL
            GROUP BY ci.item_id
            ORDER BY ci.request_count DESC
            LIMIT 10;
            """
        )

        if top_titles.empty:
            st.info("No 'most requested titles' data loaded yet.")
        else:
            st.dataframe(top_titles)


if __name__ == "__main__":
    main()