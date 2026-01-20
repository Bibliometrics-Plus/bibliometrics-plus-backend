import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine


# point to backend/data exactly like check_data_files.py
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CSV_PATH = Path(__file__).resolve().parent / "data" / "Ottawa_Public_Library_Locations_2024.csv"


def load_libraries():
    df = pd.read_csv(CSV_PATH)

    # Map CSV columns -> our schema
    df_mapped = (
        df.rename(
            columns={
                "Name": "name",
                "Street_Address": "address",
                "City": "city",
            }
        )[["name", "address", "city"]]
        .dropna(subset=["name"])
        .drop_duplicates()
    )

    with engine.begin() as conn:
        for _, row in df_mapped.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO library (name, address, city)
                    VALUES (:name, :address, :city)
                    ON CONFLICT (name, address) DO NOTHING;
                    """
                ),
                {
                    "name": row["name"],
                    "address": row["address"],
                    "city": row["city"],
                },
            )

    print(f"Processed {len(df_mapped)} rows from CSV.")
    print("Insert complete (duplicates skipped).")


if __name__ == "__main__":
    load_libraries()
