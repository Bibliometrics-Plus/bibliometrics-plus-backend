from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import engine
from load_montreal_branch_kpis_from_bm_stats import clean_text, resolve_library_id
from load_montreal_open_data_catalog import build_library_lookup


# These Montreal workbooks contain branch-level collection composition and
# self-service metrics. Collection composition still needs its own shared table,
# but self-service metrics can be folded directly into branch_kpi.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw" / "montreal"

COLLECTION_FORMAT_WORKBOOK = DATA_DIR / "pdrcollectionformat.xls"
COLLECTION_LANGUAGE_WORKBOOK = DATA_DIR / "pdrcollectionlanguepublic.xls"
COLLECTION_BOOKS_WORKBOOK = DATA_DIR / "pdrcollectionlivres.xlsx"
SELF_SERVICE_WORKBOOK = DATA_DIR / "pdrlibre-service.xls"
SOURCE_CITY = "Montreal"


def to_int(value) -> int | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def to_float(value) -> float | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def normalize_branch_label(value: str | None) -> str | None:
    """
    Strip workbook prefixes like "(AHC)" so we can match the cleaned library table.
    """
    cleaned = clean_text(value)
    if cleaned is None or cleaned.lower().startswith("total"):
        return None

    if cleaned.startswith("(") and ") " in cleaned:
        return cleaned.split(") ", 1)[1]
    return cleaned


def safe_cell(row, index: int):
    """
    Some workbook rows are shorter than the visual header width, so direct iloc
    access can fail. This helper returns None instead of raising an IndexError.
    """
    if index >= len(row):
        return None
    return row.iloc[index]


def ensure_tables(conn) -> None:
    """
    Create shared cross-city tables for workbook metrics that do not have a
    direct home in the original schema.
    """
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS library_collection_profile (
                row_id SERIAL PRIMARY KEY,
                library_id INTEGER NOT NULL REFERENCES library(library_id),
                stat_year INTEGER NOT NULL,
                source_city TEXT NOT NULL,
                profile_type TEXT NOT NULL,
                audience TEXT,
                category_name TEXT NOT NULL,
                item_count INTEGER,
                share DOUBLE PRECISION,
                source_workbook TEXT NOT NULL,
                UNIQUE (library_id, stat_year, source_city, profile_type, audience, category_name, source_workbook)
            );
            """
        )
    )
    branch_kpi_columns = [
        "self_service_loans_total BIGINT",
        "self_service_loans BIGINT",
        "self_service_loans_share DOUBLE PRECISION",
        "self_service_renewals_total BIGINT",
        "self_service_renewals BIGINT",
        "self_service_renewals_share DOUBLE PRECISION",
        "self_service_returns_total BIGINT",
        "self_service_returns BIGINT",
        "self_service_returns_share DOUBLE PRECISION",
    ]
    for column_def in branch_kpi_columns:
        conn.execute(text(f"ALTER TABLE branch_kpi ADD COLUMN IF NOT EXISTS {column_def};"))


def load_collection_by_format(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(COLLECTION_FORMAT_WORKBOOK)
    inserted = 0
    categories = ["Nouveautés", "Livres", "Périodiques", "Audio", "Vidéo", "Électronique", "Multisupports", "Autres", "TOTAL"]
    insert_sql = text(
        """
        INSERT INTO library_collection_profile (
            library_id,
            stat_year,
            source_city,
            profile_type,
            audience,
            category_name,
            item_count,
            share,
            source_workbook
        )
        VALUES (
            :library_id,
            :stat_year,
            :source_city,
            :profile_type,
            :audience,
            :category_name,
            :item_count,
            :share,
            :source_workbook
        )
        ON CONFLICT (library_id, stat_year, source_city, profile_type, audience, category_name, source_workbook)
        DO UPDATE SET
            item_count = EXCLUDED.item_count,
            share = EXCLUDED.share;
        """
    )

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = workbook.parse(sheet_name=sheet_name, header=None).iloc[5:].copy()
        batch = []

        for _, row in df.iterrows():
            branch_name = normalize_branch_label(row.iloc[0])
            if branch_name is None:
                continue

            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            for offset, category in enumerate(categories, start=1):
                batch.append(
                    {
                        "library_id": library_id,
                        "stat_year": year,
                        "source_city": SOURCE_CITY,
                        "profile_type": "format",
                        "audience": None,
                        "category_name": category,
                        "item_count": to_int(safe_cell(row, offset)),
                        "share": None,
                        "source_workbook": COLLECTION_FORMAT_WORKBOOK.name,
                    }
                )
                inserted += 1

                if len(batch) >= 1000:
                    conn.execute(insert_sql, batch)
                    batch.clear()

        if batch:
            conn.execute(insert_sql, batch)

    return inserted


def load_collection_by_language(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(COLLECTION_LANGUAGE_WORKBOOK)
    inserted = 0
    insert_sql = text(
        """
        INSERT INTO library_collection_profile (
            library_id,
            stat_year,
            source_city,
            profile_type,
            audience,
            category_name,
            item_count,
            share,
            source_workbook
        )
        VALUES (
            :library_id,
            :stat_year,
            :source_city,
            :profile_type,
            :audience,
            :category_name,
            :item_count,
            :share,
            :source_workbook
        )
        ON CONFLICT (library_id, stat_year, source_city, profile_type, audience, category_name, source_workbook)
        DO UPDATE SET
            item_count = EXCLUDED.item_count,
            share = EXCLUDED.share;
        """
    )

    language_specs = [
        ("Secteur Adulte", "TOTAL", 1, 2),
        ("Secteur Adulte", "français", 3, None),
        ("Secteur Adulte", "anglais", 4, None),
        ("Secteur Adulte", "autres", 5, None),
        ("Secteur Jeune", "TOTAL", 7, 8),
        ("Secteur Jeune", "français", 9, None),
        ("Secteur Jeune", "anglais", 10, None),
        ("Secteur Jeune", "autres", 11, None),
        ("Sans secteur", "TOTAL", 13, None),
        ("TOTAL", "TOTAL", 15, None),
        ("TOTAL", "français", 16, 17),
        ("TOTAL", "anglais", 18, 19),
        ("TOTAL", "autres", 20, 21),
    ]

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = workbook.parse(sheet_name=sheet_name, header=None).iloc[6:].copy()
        batch = []

        for _, row in df.iterrows():
            branch_name = normalize_branch_label(row.iloc[0])
            if branch_name is None:
                continue

            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            for audience, language_name, count_col, share_col in language_specs:
                batch.append(
                    {
                        "library_id": library_id,
                        "stat_year": year,
                        "source_city": SOURCE_CITY,
                        "profile_type": "language",
                        "audience": audience,
                        "category_name": language_name,
                        "item_count": to_int(safe_cell(row, count_col)),
                        "share": to_float(safe_cell(row, share_col)) if share_col is not None else None,
                        "source_workbook": COLLECTION_LANGUAGE_WORKBOOK.name,
                    }
                )
                inserted += 1

                if len(batch) >= 1000:
                    conn.execute(insert_sql, batch)
                    batch.clear()

        if batch:
            conn.execute(insert_sql, batch)

    return inserted


def load_collection_books(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(COLLECTION_BOOKS_WORKBOOK)
    inserted = 0
    insert_sql = text(
        """
        INSERT INTO library_collection_profile (
            library_id,
            stat_year,
            source_city,
            profile_type,
            audience,
            category_name,
            item_count,
            share,
            source_workbook
        )
        VALUES (
            :library_id,
            :stat_year,
            :source_city,
            :profile_type,
            :audience,
            :category_name,
            :item_count,
            :share,
            :source_workbook
        )
        ON CONFLICT (library_id, stat_year, source_city, profile_type, audience, category_name, source_workbook)
        DO UPDATE SET
            item_count = EXCLUDED.item_count,
            share = EXCLUDED.share;
        """
    )

    specs = [
        ("ADULTE", "docum.", 1, None),
        ("ADULTE", "fiction", 2, None),
        ("ADULTE", "autres", 3, None),
        ("ADULTE", "total", 4, None),
        ("JEUNE", "docum.", 5, None),
        ("JEUNE", "fiction", 6, None),
        ("JEUNE", "autres", 7, None),
        ("JEUNE", "total", 8, None),
        ("TOTAL", "docum.", 9, 13),
        ("TOTAL", "fiction", 10, 14),
        ("TOTAL", "autres", 11, 15),
        ("TOTAL", "total", 12, None),
    ]

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = workbook.parse(sheet_name=sheet_name, header=None).iloc[6:].copy()
        batch = []

        for _, row in df.iterrows():
            branch_name = normalize_branch_label(row.iloc[0])
            if branch_name is None:
                continue

            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            for audience, book_type, count_col, share_col in specs:
                batch.append(
                    {
                        "library_id": library_id,
                        "stat_year": year,
                        "source_city": SOURCE_CITY,
                        "profile_type": "book_type",
                        "audience": audience,
                        "category_name": book_type,
                        "item_count": to_int(safe_cell(row, count_col)),
                        "share": to_float(safe_cell(row, share_col)) if share_col is not None else None,
                        "source_workbook": COLLECTION_BOOKS_WORKBOOK.name,
                    }
                )
                inserted += 1

                if len(batch) >= 1000:
                    conn.execute(insert_sql, batch)
                    batch.clear()

        if batch:
            conn.execute(insert_sql, batch)

    return inserted


def load_self_service(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(SELF_SERVICE_WORKBOOK)
    inserted = 0
    specs = [
        ("self_service_loans_total", "self_service_loans", "self_service_loans_share", 1, 5, 9),
        ("self_service_renewals_total", "self_service_renewals", "self_service_renewals_share", 2, 6, 10),
        ("self_service_returns_total", "self_service_returns", "self_service_returns_share", 3, 7, 11),
    ]

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = workbook.parse(sheet_name=sheet_name, header=None).iloc[5:].copy()

        for _, row in df.iterrows():
            branch_name = normalize_branch_label(row.iloc[0])
            if branch_name is None:
                continue

            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            payload = {"library_id": library_id, "year": year}
            for total_field, subset_field, share_field, total_col, self_col, share_col in specs:
                payload[total_field] = to_int(safe_cell(row, total_col))
                payload[subset_field] = to_int(safe_cell(row, self_col))
                payload[share_field] = to_float(safe_cell(row, share_col))
                inserted += 1

            conn.execute(
                text(
                    """
                    INSERT INTO branch_kpi (
                        library_id,
                        year,
                        self_service_loans_total,
                        self_service_loans,
                        self_service_loans_share,
                        self_service_renewals_total,
                        self_service_renewals,
                        self_service_renewals_share,
                        self_service_returns_total,
                        self_service_returns,
                        self_service_returns_share
                    )
                    VALUES (
                        :library_id,
                        :year,
                        :self_service_loans_total,
                        :self_service_loans,
                        :self_service_loans_share,
                        :self_service_renewals_total,
                        :self_service_renewals,
                        :self_service_renewals_share,
                        :self_service_returns_total,
                        :self_service_returns,
                        :self_service_returns_share
                    )
                    ON CONFLICT (library_id, year)
                    DO UPDATE SET
                        self_service_loans_total = COALESCE(EXCLUDED.self_service_loans_total, branch_kpi.self_service_loans_total),
                        self_service_loans = COALESCE(EXCLUDED.self_service_loans, branch_kpi.self_service_loans),
                        self_service_loans_share = COALESCE(EXCLUDED.self_service_loans_share, branch_kpi.self_service_loans_share),
                        self_service_renewals_total = COALESCE(EXCLUDED.self_service_renewals_total, branch_kpi.self_service_renewals_total),
                        self_service_renewals = COALESCE(EXCLUDED.self_service_renewals, branch_kpi.self_service_renewals),
                        self_service_renewals_share = COALESCE(EXCLUDED.self_service_renewals_share, branch_kpi.self_service_renewals_share),
                        self_service_returns_total = COALESCE(EXCLUDED.self_service_returns_total, branch_kpi.self_service_returns_total),
                        self_service_returns = COALESCE(EXCLUDED.self_service_returns, branch_kpi.self_service_returns),
                        self_service_returns_share = COALESCE(EXCLUDED.self_service_returns_share, branch_kpi.self_service_returns_share);
                    """
                ),
                payload,
            )

    return inserted


def drop_legacy_raw_table(conn) -> None:
    """
    The raw-cell fallback table was only a temporary safety net. Once the
    structured Montreal tables are populated, we remove it so the schema does
    not keep an unlinked dump table around.
    """
    conn.execute(text("DROP TABLE IF EXISTS montreal_excel_cell"))


def drop_legacy_montreal_tables(conn) -> None:
    """
    Remove the temporary Montreal-only tables now that the shared replacements exist.
    """
    conn.execute(
        text(
            """
            DROP TABLE IF EXISTS montreal_self_service_kpi,
            montreal_collection_books,
            montreal_collection_by_language,
            montreal_collection_by_format
            """
        )
    )
    conn.execute(text("DROP TABLE IF EXISTS branch_service_kpi"))


def main() -> None:
    with engine.connect() as conn:
        print("[Montreal collection workbooks] Ensuring tables...", flush=True)
        ensure_tables(conn)
        conn.commit()
        library_lookup = build_library_lookup(conn)

        print("[Montreal collection workbooks] Loading collection-by-format workbook...", flush=True)
        format_rows = load_collection_by_format(conn, library_lookup)
        conn.commit()
        print(f"[Montreal collection workbooks] Format rows committed: {format_rows}", flush=True)

        print("[Montreal collection workbooks] Loading collection-by-language workbook...", flush=True)
        language_rows = load_collection_by_language(conn, library_lookup)
        conn.commit()
        print(f"[Montreal collection workbooks] Language rows committed: {language_rows}", flush=True)

        print("[Montreal collection workbooks] Loading collection-books workbook...", flush=True)
        book_rows = load_collection_books(conn, library_lookup)
        conn.commit()
        print(f"[Montreal collection workbooks] Book rows committed: {book_rows}", flush=True)

        print("[Montreal collection workbooks] Loading self-service workbook...", flush=True)
        self_service_rows = load_self_service(conn, library_lookup)
        conn.commit()
        print(f"[Montreal collection workbooks] Self-service rows committed: {self_service_rows}", flush=True)

        print("[Montreal collection workbooks] Dropping legacy raw workbook table...", flush=True)
        drop_legacy_raw_table(conn)
        drop_legacy_montreal_tables(conn)
        conn.commit()

    print(f"[Montreal collection workbooks] Upserted {format_rows} format rows.")
    print(f"[Montreal collection workbooks] Upserted {language_rows} language rows.")
    print(f"[Montreal collection workbooks] Upserted {book_rows} book rows.")
    print(f"[Montreal collection workbooks] Upserted {self_service_rows} self-service rows.")


if __name__ == "__main__":
    main()
