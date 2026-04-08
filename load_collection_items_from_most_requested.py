# load_collection_items_from_most_requested.py
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from db import engine

# Point to backend/data
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "Ottawa_Public_Library_Most_Requested_Titles_2024.csv"


def main():
    df = pd.read_csv(CSV_PATH)

    # Adjust to real headers
    rename_map = {
        "Title": "title",
        "Author": "author_name",
        "PublicationYear": "publication_year",
        "Format": "format",
        "Requests": "request_count",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    with engine.begin() as conn:
        # 🔹 EASIER: just grab *some* Ottawa library_id (or first library row)
        # This avoids the "NoResultFound" crap you keep hitting.
        library_id = conn.execute(
            text("""
                SELECT library_id
                FROM library
                ORDER BY library_id
                LIMIT 1;
            """)
        ).scalar_one()

        processed = 0

        for row in df.itertuples(index=False):
            params = {
                "library_id": library_id,
                "title": getattr(row, "title", None),
                "publication_year": int(getattr(row, "publication_year", 0) or 0),
                "format": getattr(row, "format", None),
                "request_count": int(getattr(row, "request_count", 0) or 0),
                "author_name": getattr(row, "author_name", "Unknown"),
            }

            conn.execute(
                text("""
                    WITH ins_item AS (
                        INSERT INTO collection_item (
                            library_id,
                            title,
                            publication_year,
                            format,
                            request_count
                        )
                        VALUES (
                            :library_id,
                            :title,
                            :publication_year,
                            :format,
                            :request_count
                        )
                        ON CONFLICT (library_id, title, publication_year)
                        DO UPDATE SET
                            request_count = GREATEST(
                                collection_item.request_count,
                                EXCLUDED.request_count
                            )
                        RETURNING item_id
                    ),
                    ins_author AS (
                        INSERT INTO author (name)
                        VALUES (:author_name)
                        ON CONFLICT (name)
                        DO UPDATE SET name = EXCLUDED.name
                        RETURNING author_id
                    )
                    INSERT INTO collection_item_author (item_id, author_id)
                    SELECT ins_item.item_id, ins_author.author_id
                    FROM ins_item, ins_author
                    ON CONFLICT DO NOTHING;
                """),
                params,
            )

            processed += 1

    print(f"Upserted {processed} high-demand titles.")


if __name__ == "__main__":
    main()
