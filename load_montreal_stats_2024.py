import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
SRC = BASE_DIR / "data" / "raw" / "montreal" / "statistiques_bibliotheques_quebec_2024.csv"
OUT = BASE_DIR / "data" / "raw" / "montreal" / "statistiques_montreal_only_2024_semicolon.csv"


def to_int(x):
    if pd.isna(x) or str(x).strip() == "":
        return None
    s = str(x).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return int(float(s))
    except Exception:
        return None


def to_float(x):
    if pd.isna(x) or str(x).strip() == "":
        return None
    s = str(x).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return None


def main():
    df = pd.read_csv(SRC, sep=";", dtype=str, encoding="utf-8", engine="python")

    region_col = "Région administrative"
    df = df[df[region_col].fillna("").str.contains("Montr", case=False, na=False)].copy()

    df.to_csv(OUT, sep=";", index=False, encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS library_statistics (
                id SERIAL PRIMARY KEY,
                stat_year INTEGER,
                library_name TEXT,
                category TEXT,
                population_served INTEGER,
                total_collection INTEGER,
                documents_per_capita FLOAT,
                books_per_capita FLOAT,
                digital_titles BIGINT,
                digital_books BIGINT,
                electronic_serials BIGINT,
                acquisitions_total BIGINT,
                refresh_rate_percent FLOAT,
                total_loans BIGINT,
                total_visits BIGINT,
                total_virtual_visits BIGINT,
                total_revenue NUMERIC,
                total_expenditure NUMERIC,
                source_file TEXT
            );
        """))
        conn.execute(text("ALTER TABLE library_statistics ADD COLUMN IF NOT EXISTS stat_year INTEGER;"))
        conn.execute(text("ALTER TABLE library_statistics ADD COLUMN IF NOT EXISTS source_file TEXT;"))

        conn.execute(text("DELETE FROM library_statistics WHERE stat_year = 2024 OR stat_year IS NULL;"))

        for _, row in df.iterrows():
            conn.execute(
                text("""
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
                """),
                {
                    "stat_year": 2024,
                    "library_name": row.get("Bibliothèque ou Centre régional"),
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
                    "source_file": SRC.name,
                },
            )

    print(f"Montreal statistics 2024 loaded: {len(df)} rows")


if __name__ == "__main__":
    main()
