import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
FILE_NAME = "palmaresparbibliodu20141110au20141123.tsv"
CSV_PATH = BASE_DIR / "data" / "raw" / "montreal" / FILE_NAME

def extract_author(title: str):
    if "/" in title:
        return title.split("/")[-1].replace(".", "").strip()
    return None

def clean_title(title: str):
    if "/" in title:
        return title.split("/")[0].strip()
    return title.strip()

def normalize_library_name(name: str) -> str:
    """
    Convert palmares library strings like:
    "Bibliothèque d'Ahuntsic Adultes - Nouveautés"
    -> "Bibliothèque d'Ahuntsic"
    """
    s = (name or "").strip()

    # Many rows start with the base library then extra qualifiers.
    # Keep only the first part before " Adultes" or " Jeunes" or " - "
    for cut in [" Adultes", " Jeunes", " - "]:
        if cut in s:
            s = s.split(cut)[0].strip()

    return s

def find_library_id(conn, raw_name: str):
    base = normalize_library_name(raw_name)

    # Try exact match first
    lib_id = conn.execute(
        text("SELECT library_id FROM library WHERE name = :n LIMIT 1"),
        {"n": base},
    ).scalar()

    if lib_id is not None:
        return lib_id

    # Try partial match (safe fallback)
    lib_id = conn.execute(
        text("SELECT library_id FROM library WHERE name ILIKE :q LIMIT 1"),
        {"q": f"%{base}%"},
    ).scalar()

    return lib_id

def load_palmares():
    df = pd.read_csv(CSV_PATH, sep="\t", dtype=str).fillna("")

    inserted_items = 0
    inserted_authors = 0
    inserted_links = 0
    skipped_no_library_match = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            raw_title = row.get("Titre", "").strip()
            raw_library_name = row.get("Nom Bibliothèque", "").strip()
            secteur = row.get("Secteur", "").strip()

            if not raw_title or not raw_library_name:
                continue

            library_id = find_library_id(conn, raw_library_name)
            if library_id is None:
                skipped_no_library_match += 1
                continue

            title = clean_title(raw_title)
            author_name = extract_author(raw_title)

            # Check if item exists already (same title + library)
            item_id = conn.execute(
                text("""
                    SELECT item_id
                    FROM collection_item
                    WHERE title = :title AND library_id = :library_id
                    LIMIT 1
                """),
                {"title": title, "library_id": library_id},
            ).scalar()

            # If not exists, insert it
            if item_id is None:
                item_id = conn.execute(
                    text("""
                        INSERT INTO collection_item (library_id, title, format)
                        VALUES (:library_id, :title, :format)
                        RETURNING item_id
                    """),
                    {"library_id": library_id, "title": title, "format": secteur},
                ).scalar()
                inserted_items += 1

            # Author insert/find
            author_id = None
            if author_name:
                author_id = conn.execute(
                    text("SELECT author_id FROM author WHERE name = :name LIMIT 1"),
                    {"name": author_name},
                ).scalar()

                if author_id is None:
                    author_id = conn.execute(
                        text("INSERT INTO author (name) VALUES (:name) RETURNING author_id"),
                        {"name": author_name},
                    ).scalar()
                    inserted_authors += 1

                # Link item <-> author (avoid duplicates)
                exists_link = conn.execute(
                    text("""
                        SELECT 1 FROM collection_item_author
                        WHERE item_id = :item_id AND author_id = :author_id
                        LIMIT 1
                    """),
                    {"item_id": item_id, "author_id": author_id},
                ).scalar()

                if exists_link is None:
                    conn.execute(
                        text("""
                            INSERT INTO collection_item_author (item_id, author_id)
                            VALUES (:item_id, :author_id)
                        """),
                        {"item_id": item_id, "author_id": author_id},
                    )
                    inserted_links += 1

    print("DONE ✅ Montreal palmares load finished")
    print("Inserted collection_item:", inserted_items)
    print("Inserted author:", inserted_authors)
    print("Inserted collection_item_author links:", inserted_links)
    print("Skipped rows (no library match):", skipped_no_library_match)

if __name__ == "__main__":
    load_palmares()
