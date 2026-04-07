import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "raw" / "montreal" / "bottin-des-biblio_2024.csv"
SYSTEM_NAME = "Montreal"
CITY_NAME = "Montréal"


def build_library_frame() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8", dtype=str)

    # Keep ONLY Montreal rows (this file includes all of Quebec)
    df = df[df["MUNICIPALITÉ"].astype(str).str.contains("Montréal", na=False)].copy()

    # The branch name lives in "NOM DE LA BIBLIOTHÈQUE".
    # "NOM DU POINT DE SERVICE" is often the system label
    # "Bibliothèques de Montréal", which is too generic to use as a branch name.
    branch_name = df["NOM DE LA BIBLIOTHÈQUE"].fillna("").astype(str).str.strip()
    point_service = df["NOM DU POINT DE SERVICE"].fillna("").astype(str).str.strip()
    address = df["ADRESSE"].fillna("").astype(str).str.strip()

    df_mapped = pd.DataFrame(
        {
            "name": branch_name.mask(branch_name == "", point_service),
            "address": address,
            "city": CITY_NAME,
            "system_name": SYSTEM_NAME,
        }
    )

    df_mapped = df_mapped[df_mapped["name"] != ""].drop_duplicates(subset=["name", "address"])
    return df_mapped


def upsert_library(conn, row: dict) -> None:
    existing_id = conn.execute(
        text(
            """
            SELECT library_id
            FROM library
            WHERE name = :name
              AND (
                    address = :address
                 OR address IS NULL
                 OR :address = ''
              )
            ORDER BY CASE WHEN address = :address THEN 0 ELSE 1 END
            LIMIT 1;
            """
        ),
        {"name": row["name"], "address": row["address"]},
    ).scalar()

    if existing_id is not None:
        conn.execute(
            text(
                """
                UPDATE library
                SET address = CASE
                        WHEN (address IS NULL OR address = '') AND :address <> '' THEN :address
                        ELSE address
                    END,
                    city = :city,
                    system_name = :system_name
                WHERE library_id = :library_id;
                """
            ),
            {
                "library_id": existing_id,
                "address": row["address"],
                "city": row["city"],
                "system_name": row["system_name"],
            },
        )
        return

    conn.execute(
        text(
            """
            INSERT INTO library (name, address, city, system_name)
            VALUES (:name, :address, :city, :system_name)
            ON CONFLICT (name, address)
            DO UPDATE SET
                city = EXCLUDED.city,
                system_name = EXCLUDED.system_name;
            """
        ),
        row,
    )


def cleanup_legacy_rows(conn) -> int:
    deleted = conn.execute(
        text(
            """
            DELETE FROM library l
            WHERE l.name = 'Bibliothèques de Montréal'
              AND l.system_name = :system_name
              AND NOT EXISTS (
                  SELECT 1
                  FROM collection_item ci
                  WHERE ci.library_id = l.library_id
              );
            """
        ),
        {"system_name": SYSTEM_NAME},
    )
    return deleted.rowcount or 0

def load_libraries():
    df_mapped = build_library_frame()
    processed = 0

    with engine.begin() as conn:
        for _, row in df_mapped.iterrows():
            upsert_library(conn, row.to_dict())
            processed += 1
        deleted = cleanup_legacy_rows(conn)

    print(f"Processed {processed} Montreal library rows from bottin.")
    print(f"Cleaned up {deleted} legacy generic Montreal library rows.")

if __name__ == "__main__":
    load_libraries()
