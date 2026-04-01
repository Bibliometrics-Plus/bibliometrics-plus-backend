import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "raw" / "montreal" / "bottin-des-biblio_2024.csv"

def load_libraries():
    # Quebec "bottin" uses semicolon separator
    df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8")

    # Keep ONLY Montreal rows (this file includes all of Quebec)
    df = df[df["MUNICIPALITÉ"].astype(str).str.contains("Montréal", na=False)]

    # Use service point as the branch name (best match to "library")
    # Fallback to "NOM DE LA BIBLIOTHÈQUE" if needed
    name_series = df.get("NOM DU POINT DE SERVICE")
    if name_series is None:
        name_series = df["NOM DE LA BIBLIOTHÈQUE"]

    df_mapped = pd.DataFrame(
        {
            "name": name_series.astype(str).str.strip(),
            "address": df["ADRESSE"].astype(str).str.strip(),
            "city": "Montréal",
        }
    )

    df_mapped = (
        df_mapped.dropna(subset=["name"])
        .drop_duplicates(subset=["name", "address"])
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

    print(f"Processed {len(df_mapped)} Montreal library rows from bottin.")
    print("Insert complete (duplicates skipped).")

if __name__ == "__main__":
    load_libraries()
