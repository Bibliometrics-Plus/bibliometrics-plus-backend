from __future__ import annotations

import csv
import re
import tempfile
from pathlib import Path

from sqlalchemy import text

from db import engine


# The shared collection_item table enforces global uniqueness on (title, publication_year),
# so this Montreal catalog has to be aggregated to that same level in order to fit the
# existing schema correctly.
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "raw" / "montreal" / "donnees_ouverte.csv"
SYSTEM_NAME = "Montreal"


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).strip().split())
    return cleaned or None


def fit_varchar(value: str | None, limit: int = 255) -> str | None:
    """
    Trim text to fit the existing schema column width when needed.
    """
    cleaned = clean_text(value)
    if cleaned is None:
        return None
    return cleaned[:limit]


def normalize_lookup(value: str | None) -> str:
    if value is None:
        return ""

    normalized = clean_text(value) or ""
    normalized = normalized.lower()
    normalized = (
        normalized.replace("’", "'")
        .replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("ë", "e")
        .replace("à", "a")
        .replace("â", "a")
        .replace("î", "i")
        .replace("ï", "i")
        .replace("ô", "o")
        .replace("ö", "o")
        .replace("ù", "u")
        .replace("û", "u")
        .replace("ç", "c")
    )
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def parse_branch_label(localisation: str | None) -> str | None:
    if localisation is None:
        return None
    first_segment = localisation.split("/", 1)[0].strip()
    first_segment = re.sub(r"^\([^)]+\)\s*", "", first_segment).strip()
    return clean_text(first_segment)


def clean_author_name(value: str | None) -> str | None:
    cleaned = clean_text(value)
    if cleaned is None:
        return None
    return cleaned.rstrip(",").strip() or None


def parse_publication_year(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"(18|19|20)\d{2}", value)
    if not match:
        return None
    return int(match.group(0))


def classify_accessibility(localisation: str | None, document_type: str | None) -> str | None:
    lookup = f"{normalize_lookup(localisation)} {normalize_lookup(document_type)}"
    if "gros caracteres" in lookup:
        return "Large Print"
    if "livre audio" in lookup or "audio" in lookup:
        return "Audiobook"
    return None


def build_library_lookup(conn) -> dict[str, tuple[int | None, str]]:
    lookup: dict[str, tuple[int | None, str]] = {}
    rows = conn.execute(
        text(
            """
            SELECT library_id, name
            FROM library
            WHERE system_name = :system_name
            """
        ),
        {"system_name": SYSTEM_NAME},
    ).fetchall()

    for library_id, name in rows:
        normalized_name = normalize_lookup(name)
        lookup[normalized_name] = (library_id, name)

        shorter_name = normalized_name.replace("bibliotheque ", "", 1).strip()
        if shorter_name:
            lookup[shorter_name] = (library_id, name)

    aliases = {
        "ahuntsic": "Bibliothèque d'Ahuntsic",
        "cartierville": "Bibliothèque de Cartierville",
        "c d neiges": "Bibliothèque de Côte-des-Neiges",
        "interculturelle": "Bibliothèque interculturelle",
        "benny": "Bibliothèque Benny",
        "jean corbeil": "Bibliothèque Jean-Corbeil",
        "riv d prairies": "Bibliothèque de Rivière-des-Prairies",
        "p a trembles": "Bibliothèque de Pointe-aux-Trembles",
        "saint michel": "Bibliothèque de Saint-Michel",
        "le prevost": "Bibliothèque Le Prévost",
        "parc extension": "Bibliothèque de Parc-Extension",
        "la petite patrie": "Bibliothèque de La Petite-Patrie",
        "rosemont": "Bibliothèque de Rosemont",
        "plateau m r": "Bibliothèque du Plateau-Mont-Royal",
        "mercier": "Bibliothèque Mercier",
        "langelier": "Bibliothèque Langelier",
        "maisonneuve": "Bibliothèque Maisonneuve",
        "frontenac": "Bibliothèque Frontenac",
        "jacqueline de repentigny": "Bibliothèque Jacqueline-De Repentigny",
        "marie uguay": "Bibliothèque Marie-Uguay",
        "saint charles": "Bibliothèque Saint-Charles",
        "rejean ducharme": "Bibliothèque Réjean-Ducharme",
        "saint leonard": "Bibliothèque de Saint-Léonard",
        "outremont": "Bibliothèque Mordecai-Richler",
        "saul bellow": "Bibliothèque Saul-Bellow",
        "l ile bizard": "Bibliothèque de L'Île-Bizard",
        "l octogone": "Bibliothèque L'Octogone",
        "verdun": "Bibliothèque de Verdun",
        "hochelaga": "Bibliothèque Hochelaga",
        "biblio courrier": "Biblio-Courrier",
    }

    for alias, target_name in aliases.items():
        target_key = normalize_lookup(target_name)
        if target_key in lookup:
            lookup[alias] = lookup[target_key]

    return lookup


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    """
    Write a temporary CSV used for fast PostgreSQL COPY loading.
    """
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def bulk_merge_shared_tables(
    item_map: dict[tuple[str, int | None], dict],
    author_names: set[str],
    link_keys: set[tuple[str, int | None, str]],
) -> tuple[int, int, int]:
    """
    Use temp staging tables plus PostgreSQL COPY so the final merge runs much faster
    than sending thousands of small batches over the network.
    """
    item_rows = [
        (
            row["library_id"],
            row["title"],
            row["format"],
            row["publication_year"],
            row["accessibility_format"],
            row["request_count"],
        )
        for row in item_map.values()
    ]
    author_rows = [(name,) for name in sorted(author_names)]
    link_rows = [
        (title, publication_year, author_name)
        for title, publication_year, author_name in sorted(
            link_keys,
            key=lambda row: (row[0], row[1] if row[1] is not None else -1, row[2]),
        )
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        items_csv = temp_path / "items.csv"
        authors_csv = temp_path / "authors.csv"
        links_csv = temp_path / "links.csv"

        write_csv(
            items_csv,
            ["library_id", "title", "format", "publication_year", "accessibility_format", "request_count"],
            item_rows,
        )
        write_csv(authors_csv, ["name"], author_rows)
        write_csv(links_csv, ["title", "publication_year", "author_name"], link_rows)

        raw_conn = engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            cursor.execute("SET statement_timeout = 0;")
            cursor.execute(
                """
                CREATE TEMP TABLE stage_montreal_items (
                    library_id INTEGER,
                    title TEXT,
                    format TEXT,
                    publication_year INTEGER,
                    accessibility_format TEXT,
                    request_count INTEGER
                ) ON COMMIT DROP;
                """
            )
            cursor.execute(
                """
                CREATE TEMP TABLE stage_montreal_authors (
                    name TEXT
                ) ON COMMIT DROP;
                """
            )
            cursor.execute(
                """
                CREATE TEMP TABLE stage_montreal_links (
                    title TEXT,
                    publication_year INTEGER,
                    author_name TEXT
                ) ON COMMIT DROP;
                """
            )

            with items_csv.open("r", encoding="utf-8") as handle:
                cursor.copy_expert(
                    """
                    COPY stage_montreal_items (
                        library_id,
                        title,
                        format,
                        publication_year,
                        accessibility_format,
                        request_count
                    )
                    FROM STDIN WITH CSV HEADER
                    """,
                    handle,
                )

            with authors_csv.open("r", encoding="utf-8") as handle:
                cursor.copy_expert(
                    "COPY stage_montreal_authors (name) FROM STDIN WITH CSV HEADER",
                    handle,
                )

            with links_csv.open("r", encoding="utf-8") as handle:
                cursor.copy_expert(
                    "COPY stage_montreal_links (title, publication_year, author_name) FROM STDIN WITH CSV HEADER",
                    handle,
                )

            cursor.execute(
                """
                INSERT INTO collection_item (
                    library_id,
                    title,
                    format,
                    publication_year,
                    accessibility_format,
                    request_count
                )
                SELECT
                    library_id,
                    title,
                    format,
                    publication_year,
                    accessibility_format,
                    request_count
                FROM stage_montreal_items
                ON CONFLICT (title, publication_year)
                DO UPDATE SET
                    library_id = COALESCE(collection_item.library_id, EXCLUDED.library_id),
                    format = COALESCE(EXCLUDED.format, collection_item.format),
                    accessibility_format = COALESCE(EXCLUDED.accessibility_format, collection_item.accessibility_format),
                    request_count = GREATEST(COALESCE(collection_item.request_count, 0), COALESCE(EXCLUDED.request_count, 0));
                """
            )
            items_written = cursor.rowcount or 0

            cursor.execute(
                """
                INSERT INTO author (name)
                SELECT DISTINCT name
                FROM stage_montreal_authors
                ON CONFLICT (name) DO NOTHING;
                """
            )
            authors_written = cursor.rowcount or 0

            cursor.execute(
                """
                INSERT INTO collection_item_author (item_id, author_id)
                SELECT DISTINCT
                    ci.item_id,
                    a.author_id
                FROM stage_montreal_links s
                JOIN collection_item ci
                  ON ci.title = s.title
                 AND COALESCE(ci.publication_year, -1) = COALESCE(s.publication_year, -1)
                JOIN author a
                  ON a.name = s.author_name
                ON CONFLICT DO NOTHING;
                """
            )
            links_written = cursor.rowcount or 0

            raw_conn.commit()
        finally:
            raw_conn.close()

    return items_written, authors_written, links_written


def main() -> None:
    total_rows_read = 0
    malformed_rows = 0
    matched_rows = 0
    unmatched_rows = 0

    item_map: dict[tuple[str, int | None], dict] = {}
    author_names: set[str] = set()
    link_keys: set[tuple[str, int | None, str]] = set()

    with engine.connect() as conn:
        library_lookup = build_library_lookup(conn)

    with CSV_PATH.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)

        for row in reader:
            total_rows_read += 1
            if len(row) != len(header):
                malformed_rows += 1
                continue

            record = dict(zip(header, row))
            title = fit_varchar(record.get("Titre"))
            if title is None:
                continue

            branch_label = parse_branch_label(record.get("Localisation"))
            library_match = library_lookup.get(normalize_lookup(branch_label))
            library_id = library_match[0] if library_match else None

            if library_id is None:
                unmatched_rows += 1
                continue
            matched_rows += 1

            publication_year = parse_publication_year(record.get("Annee"))
            item_key = (title, publication_year)
            annual_loans = record.get("Nombre-prets-annee")
            annual_loans_int = int(annual_loans) if str(annual_loans).isdigit() else None

            existing = item_map.get(item_key)
            candidate = {
                "library_id": library_id,
                "title": title,
                "format": fit_varchar(record.get("Type-document")),
                "publication_year": publication_year,
                "accessibility_format": fit_varchar(
                    classify_accessibility(record.get("Localisation"), record.get("Type-document"))
                ),
                "request_count": annual_loans_int,
            }

            if existing is None:
                item_map[item_key] = candidate
            else:
                if existing["library_id"] is None and library_id is not None:
                    existing["library_id"] = library_id
                if annual_loans_int is not None:
                    existing["request_count"] = max(existing["request_count"] or 0, annual_loans_int)
                if existing["accessibility_format"] is None:
                    existing["accessibility_format"] = candidate["accessibility_format"]

            author_name = clean_author_name(record.get("Auteur"))
            if author_name is not None:
                author_names.add(author_name)
                link_keys.add((title, publication_year, author_name))

            if total_rows_read % 250000 == 0:
                print(
                    f"[Montreal open data] scanned {total_rows_read:,} rows "
                    f"(matched: {matched_rows:,}, malformed: {malformed_rows:,}, "
                    f"distinct titles so far: {len(item_map):,})"
                )

    items_written, authors_written, links_written = bulk_merge_shared_tables(
        item_map,
        author_names,
        link_keys,
    )

    print(f"[Montreal open data] rows read: {total_rows_read}")
    print(f"[Montreal open data] malformed rows skipped: {malformed_rows}")
    print(f"[Montreal open data] matched rows: {matched_rows}")
    print(f"[Montreal open data] unmatched rows: {unmatched_rows}")
    print(f"[Montreal open data] distinct canonical items written: {items_written}")
    print(f"[Montreal open data] distinct authors written: {authors_written}")
    print(f"[Montreal open data] item-author links written: {links_written}")


if __name__ == "__main__":
    main()
