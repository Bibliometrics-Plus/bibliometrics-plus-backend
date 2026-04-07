from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import engine


# This loader keeps the yearly Quebec statistics files inside the shared team
# table named library_statistics. We use stat_year to store multiple years
# without creating a separate Montreal-only destination.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw" / "montreal"

YEARLY_SOURCE_FILES = {
    2018: DATA_DIR / "statistiques_bibliotheques_quebec_2018.csv",
    2019: DATA_DIR / "statistiques_bibliotheques_quebec_2019.csv",
    2020: DATA_DIR / "statistiques_bibliotheques_quebec_2020_2025-10-07.csv",
    2021: DATA_DIR / "statistiques_bibliotheques_quebec_2021_2025-10-08.csv",
    2022: DATA_DIR / "statistiques_bibliotheques_quebec_2022_2025-10-08.csv",
    2023: DATA_DIR / "statistiques_bibliotheques_quebec_2023_maj_2025-09.csv",
    2024: DATA_DIR / "statistiques_bibliotheques_quebec_2024.csv",
}


def to_int(value):
    """
    Convert numeric-looking French CSV values into Python integers.

    The source files mix commas, spaces, and non-breaking spaces, so the loader
    normalizes those before trying to cast them.
    """
    if pd.isna(value) or str(value).strip() == "":
        return None

    normalized = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return int(float(normalized))
    except Exception:
        return None


def to_float(value):
    """
    Convert numeric-looking French CSV values into Python floats.
    """
    if pd.isna(value) or str(value).strip() == "":
        return None

    normalized = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return float(normalized)
    except Exception:
        return None


def library_name_column(df: pd.DataFrame) -> str:
    """
    The 2018 file uses a slightly different library-name header than later years.
    This helper keeps the main load loop simple.
    """
    if "Bibliothèque ou Centre régional" in df.columns:
        return "Bibliothèque ou Centre régional"
    return "Bibliothèque ou CRSBP"


def read_montreal_rows(csv_path: Path) -> pd.DataFrame:
    """
    Read one yearly statistics file and keep only Montreal-region rows.
    """
    df = pd.read_csv(csv_path, sep=";", dtype=str, encoding="utf-8", engine="python")
    return df[df["Région administrative"].fillna("").str.contains("Montr", case=False, na=False)].copy()


def ensure_table(conn) -> None:
    """
    Extend the existing statistics table so it can store multiple years.
    """
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS library_statistics (
                id SERIAL PRIMARY KEY,
                stat_year INTEGER NOT NULL,
                library_name TEXT NOT NULL,
                category TEXT,
                population_served INTEGER,
                total_collection INTEGER,
                documents_per_capita DOUBLE PRECISION,
                books_per_capita DOUBLE PRECISION,
                digital_titles BIGINT,
                digital_books BIGINT,
                electronic_serials BIGINT,
                acquisitions_total BIGINT,
                refresh_rate_percent DOUBLE PRECISION,
                total_loans BIGINT,
                total_visits BIGINT,
                total_virtual_visits BIGINT,
                total_revenue NUMERIC,
                total_expenditure NUMERIC,
                source_file TEXT
            );
            """
        )
    )
    conn.execute(text("ALTER TABLE library_statistics ADD COLUMN IF NOT EXISTS stat_year INTEGER;"))
    conn.execute(text("ALTER TABLE library_statistics ADD COLUMN IF NOT EXISTS source_file TEXT;"))
    conn.execute(
        text(
            """
            UPDATE library_statistics
            SET stat_year = COALESCE(stat_year, 2024),
                source_file = COALESCE(source_file, 'statistiques_bibliotheques_quebec_2024.csv')
            WHERE stat_year IS NULL OR source_file IS NULL;
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS library_statistics_year_name_idx
            ON library_statistics (stat_year, library_name);
            """
        )
    )


def upsert_year(conn, year: int, csv_path: Path) -> int:
    """
    Upsert one year's Montreal statistics into the normalized table.
    """
    df = read_montreal_rows(csv_path)
    name_col = library_name_column(df)

    processed = 0

    for _, row in df.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO library_statistics (
                    stat_year,
                    library_name,
                    category,
                    population_served,
                    total_collection,
                    documents_per_capita,
                    books_per_capita,
                    digital_titles,
                    digital_books,
                    electronic_serials,
                    acquisitions_total,
                    refresh_rate_percent,
                    total_loans,
                    total_visits,
                    total_virtual_visits,
                    total_revenue,
                    total_expenditure,
                    source_file
                )
                VALUES (
                    :stat_year,
                    :library_name,
                    :category,
                    :population_served,
                    :total_collection,
                    :documents_per_capita,
                    :books_per_capita,
                    :digital_titles,
                    :digital_books,
                    :electronic_serials,
                    :acquisitions_total,
                    :refresh_rate_percent,
                    :total_loans,
                    :total_visits,
                    :total_virtual_visits,
                    :total_revenue,
                    :total_expenditure,
                    :source_file
                )
                ON CONFLICT (stat_year, library_name)
                DO UPDATE SET
                    category = EXCLUDED.category,
                    population_served = EXCLUDED.population_served,
                    total_collection = EXCLUDED.total_collection,
                    documents_per_capita = EXCLUDED.documents_per_capita,
                    books_per_capita = EXCLUDED.books_per_capita,
                    digital_titles = EXCLUDED.digital_titles,
                    digital_books = EXCLUDED.digital_books,
                    electronic_serials = EXCLUDED.electronic_serials,
                    acquisitions_total = EXCLUDED.acquisitions_total,
                    refresh_rate_percent = EXCLUDED.refresh_rate_percent,
                    total_loans = EXCLUDED.total_loans,
                    total_visits = EXCLUDED.total_visits,
                    total_virtual_visits = EXCLUDED.total_virtual_visits,
                    total_revenue = EXCLUDED.total_revenue,
                    total_expenditure = EXCLUDED.total_expenditure,
                    source_file = EXCLUDED.source_file;
                """
            ),
            {
                "stat_year": year,
                "library_name": row.get(name_col),
                "category": row.get("Catégorie de la bibl."),
                "population_served": to_int(row.get("Population desservie")),
                "total_collection": to_int(row.get("Coll. / Tous les documents")),
                "documents_per_capita": to_float(row.get("Coll. / Documents par hab.")),
                "books_per_capita": to_float(row.get("Coll. / Livres par hab.")),
                "digital_titles": to_int(row.get("Coll. / Ress. numériques - Titres (Total)")),
                "digital_books": to_int(row.get("Coll. / Livres num. (Total)")),
                "electronic_serials": to_int(row.get("Coll. / Publ. en série électr. - Titres (Total)")),
                "acquisitions_total": to_int(row.get("Acquis. / Livres impr. (Total)")),
                "refresh_rate_percent": to_float(row.get("Acquis. / Livres impr. - Taux rafraîchissement (%)")),
                "total_loans": to_int(row.get("Prêts / Tous les doc. (Total)")),
                "total_visits": to_int(row.get("Visites (Total)")),
                "total_virtual_visits": to_int(row.get("Visites virtuelles (Total)")),
                "total_revenue": to_float(row.get("Revenus / Tous les revenus ($)")),
                "total_expenditure": to_float(row.get("Dép. fonct. / Toutes les dépenses ($)")),
                "source_file": csv_path.name,
            },
        )
        processed += 1

    return processed


def main() -> None:
    available_files = {year: path for year, path in YEARLY_SOURCE_FILES.items() if path.exists()}

    with engine.begin() as conn:
        ensure_table(conn)
        total_rows = 0
        for year, csv_path in sorted(available_files.items()):
            conn.execute(
                text("DELETE FROM library_statistics WHERE stat_year = :stat_year"),
                {"stat_year": year},
            )
            loaded = upsert_year(conn, year, csv_path)
            total_rows += loaded
            print(f"[Montreal stats] {year}: upserted {loaded} rows from {csv_path.name}")

        # The old Montreal-only yearly table is replaced by the shared statistics table.
        conn.execute(text("DROP TABLE IF EXISTS montreal_library_statistics"))

    print(f"[Montreal stats] Finished yearly statistics load. Source files processed: {len(available_files)}")
    print(f"[Montreal stats] Total rows upserted this run: {total_rows}")


if __name__ == "__main__":
    main()
