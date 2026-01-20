# load_user_group_stats_from_cardholders.py
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine


# point to backend/data exactly like check_data_files.py
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "Ottawa_Public_Library_1_year_active_cardholders_by_Neighbourhood_(ONS)_2024.csv"

def main():
    df = pd.read_csv(CSV_PATH)

    # Adjust these to match real header names
    rename_map = {
        "Neighbourhood": "neighbourhood",
        "AgeGroup": "age_group",
        "ActiveCardholders": "cardholder_count",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["year"] = 2024

    processed = 0
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                INSERT INTO user_group_stats (
                    group_id,
                    neighbourhood,
                    year,
                    cardholder_count
                )
                VALUES (
                    (SELECT group_id FROM user_group WHERE age_group = :age_group LIMIT 1),
                    :neighbourhood,
                    :year,
                    :cardholder_count
                );
                """),
                {
                    "age_group": getattr(row, "age_group", None),
                    "neighbourhood": getattr(row, "neighbourhood", None),
                    "year": row.year,
                    "cardholder_count": int(getattr(row, "cardholder_count", 0) or 0),
                },
            )
            processed += 1

    print(f"Inserted {processed} cardholder stats rows.")

if __name__ == "__main__":
    main()
