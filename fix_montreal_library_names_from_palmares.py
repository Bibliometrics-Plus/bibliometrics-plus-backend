import pandas as pd
from sqlalchemy import text
from db import engine

PALMARES_PATH = "data/raw/montreal/palmaresparbibliodu20150413au20150426.tsv"

def main():
    df = pd.read_csv(PALMARES_PATH, sep="\t", dtype=str).fillna("")
    libs = sorted(set([x.strip() for x in df["Nom Bibliothèque"].tolist() if x.strip()]))

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for name in libs:
            exists = conn.execute(
                text("SELECT 1 FROM library WHERE name = :name AND city = 'Montréal' LIMIT 1"),
                {"name": name},
            ).scalar()

            if exists:
                skipped += 1
                continue

            conn.execute(
                text("""
                    INSERT INTO library (name, city, address)
                    VALUES (:name, 'Montréal', NULL)
                """),
                {"name": name},
            )
            inserted += 1

    print("DONE ✅ Added Montreal library names from palmares")
    print("Unique palmares libraries:", len(libs))
    print("Inserted:", inserted)
    print("Skipped (already existed):", skipped)

if __name__ == "__main__":
    main()
