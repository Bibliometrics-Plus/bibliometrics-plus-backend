from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import engine
from load_montreal_branch_kpis_from_bm_stats import clean_text, resolve_library_id
from load_montreal_open_data_catalog import build_library_lookup


# These workbook loaders use the shared schema where the Montreal workbook data fits:
# - circulation_transaction for aggregated circulation summaries
# - branch_kpi for annual branch-level totals and visits
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw" / "montreal"

PUBLIC_WORKBOOK = DATA_DIR / "pdrpretspublic.xlsx"
FORMAT_WORKBOOK = DATA_DIR / "pdrpretsformat.xlsx"
BOOK_WORKBOOK = DATA_DIR / "pdrpretslivres.xlsx"
TOTAL_WORKBOOK = DATA_DIR / "pret_total_physiquenumerique.xls"
VISITS_WORKBOOK = DATA_DIR / "pdrfrequentation.xls"


def to_int(value) -> int | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def ensure_user_group(conn, label: str) -> int:
    """
    Reuse the existing user_group table for audience-based Montreal circulation summaries.
    """
    conn.execute(
        text(
            """
            INSERT INTO user_group (age_group)
            VALUES (:label)
            ON CONFLICT (age_group) DO NOTHING;
            """
        ),
        {"label": label},
    )

    return conn.execute(
        text("SELECT group_id FROM user_group WHERE age_group = :label LIMIT 1"),
        {"label": label},
    ).scalar()


def delete_existing_loan_type(conn, prefixes: list[str]) -> None:
    """
    circulation_transaction has no natural upsert key, so we delete Montreal rows
    for the loan-type families we are about to reload.
    """
    for prefix in prefixes:
        conn.execute(
            text(
                """
                DELETE FROM circulation_transaction
                WHERE loan_type LIKE :prefix;
                """
            ),
            {"prefix": f"{prefix}%"},
        )


def flush_circulation_batch(conn, batch: list[dict]) -> int:
    """
    Write a batch of circulation summary rows into the shared fact table.
    """
    if not batch:
        return 0

    result = conn.execute(
        text(
            """
            INSERT INTO circulation_transaction (
                item_id,
                group_id,
                library_id,
                borrow_date,
                loan_type,
                loan_count
            )
            VALUES (
                NULL,
                :group_id,
                :library_id,
                make_date(:year, 1, 1),
                :loan_type,
                :loan_count
            );
            """
        ),
        batch,
    )
    batch.clear()
    return result.rowcount or 0


def flush_branch_kpi_batch(conn, batch: list[dict], column_name: str) -> int:
    """
    Upsert one branch_kpi metric family in batches.
    """
    if not batch:
        return 0

    result = conn.execute(
        text(
            f"""
            INSERT INTO branch_kpi (library_id, year, {column_name})
            VALUES (:library_id, :year, :metric_value)
            ON CONFLICT (library_id, year)
            DO UPDATE SET
                {column_name} = COALESCE(EXCLUDED.{column_name}, branch_kpi.{column_name});
            """
        ),
        batch,
    )
    batch.clear()
    return result.rowcount or 0


def load_public_workbook(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(PUBLIC_WORKBOOK)
    inserted = 0
    batch: list[dict] = []
    categories = [
        "Adultes",
        "Jeunes",
        "Aînés",
        "Prêt à domicile",
        "Projets spéciaux",
        "Organismes Adultes",
        "Organismes Jeunes",
        "Dépôt temporaire",
        "Autres",
        "TOTAL",
    ]

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = pd.read_excel(PUBLIC_WORKBOOK, sheet_name=sheet_name, header=[0, 1, 2]).iloc[2:].copy()

        for _, row in df.iterrows():
            branch_label = clean_text(row.iloc[0])
            if branch_label is None:
                continue

            branch_name = branch_label.split(" ", 1)[1] if branch_label.startswith("(") and " " in branch_label else branch_label
            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            for offset, category in enumerate(categories, start=1):
                loan_count = to_int(row.iloc[offset])
                if loan_count is None:
                    continue

                group_id = ensure_user_group(conn, category)
                batch.append(
                    {
                        "group_id": group_id,
                        "library_id": library_id,
                        "year": year,
                        "loan_type": f"MontrealPublic:{category}",
                        "loan_count": loan_count,
                    }
                )

                if len(batch) >= 1000:
                    inserted += flush_circulation_batch(conn, batch)

    inserted += flush_circulation_batch(conn, batch)

    return inserted


def load_format_workbook(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(FORMAT_WORKBOOK)
    inserted = 0
    batch: list[dict] = []
    categories = [
        "Nouveautés",
        "Livres",
        "Périodiques",
        "Audio",
        "Vidéo",
        "Électronique",
        "Multisupports",
        "Autres",
        "TOTAL",
    ]

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = pd.read_excel(FORMAT_WORKBOOK, sheet_name=sheet_name, header=[0, 1, 2]).iloc[2:].copy()

        for _, row in df.iterrows():
            branch_label = clean_text(row.iloc[0])
            if branch_label is None:
                continue

            branch_name = branch_label.split(" ", 1)[1] if branch_label.startswith("(") and " " in branch_label else branch_label
            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            for offset, category in enumerate(categories, start=1):
                loan_count = to_int(row.iloc[offset])
                if loan_count is None:
                    continue

                batch.append(
                    {
                        "group_id": None,
                        "library_id": library_id,
                        "year": year,
                        "loan_type": f"MontrealFormat:{category}",
                        "loan_count": loan_count,
                    }
                )

                if len(batch) >= 1000:
                    inserted += flush_circulation_batch(conn, batch)

    inserted += flush_circulation_batch(conn, batch)

    return inserted


def load_book_workbook(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(BOOK_WORKBOOK)
    inserted = 0
    batch: list[dict] = []
    categories = {
        1: "Prets:Documentaire",
        2: "Prets:Fiction",
        3: "Prets:Autres",
        4: "Prets:Total",
        5: "Renouvellements:Documentaire",
        6: "Renouvellements:Fiction",
        7: "Renouvellements:Autres",
        8: "Renouvellements:Total",
        9: "Total:Documentaire",
        10: "Total:Fiction",
        11: "Total:Autres",
        12: "Total:Global",
    }

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = pd.read_excel(BOOK_WORKBOOK, sheet_name=sheet_name, header=[0, 1, 2]).iloc[3:].copy()

        for _, row in df.iterrows():
            branch_label = clean_text(row.iloc[0])
            if branch_label is None:
                continue

            branch_name = branch_label.split(" ", 1)[1] if branch_label.startswith("(") and " " in branch_label else branch_label
            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            for offset, category in categories.items():
                loan_count = to_int(row.iloc[offset])
                if loan_count is None:
                    continue

                batch.append(
                    {
                        "group_id": None,
                        "library_id": library_id,
                        "year": year,
                        "loan_type": f"MontrealBooks:{category}",
                        "loan_count": loan_count,
                    }
                )

                if len(batch) >= 1000:
                    inserted += flush_circulation_batch(conn, batch)

    inserted += flush_circulation_batch(conn, batch)

    return inserted


def load_total_workbook(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(TOTAL_WORKBOOK)
    processed = 0
    batch: list[dict] = []

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = pd.read_excel(TOTAL_WORKBOOK, sheet_name=sheet_name, header=[0, 1, 2]).iloc[3:].copy()

        for _, row in df.iterrows():
            library_name = clean_text(row.iloc[0])
            if library_name is None:
                continue

            library_id = resolve_library_id(library_lookup, library_name)
            if library_id is None:
                continue

            batch.append(
                {
                    "library_id": library_id,
                    "year": year,
                    "metric_value": to_int(row.iloc[5]),
                }
            )

            if len(batch) >= 500:
                processed += flush_branch_kpi_batch(conn, batch, "circulation")

    processed += flush_branch_kpi_batch(conn, batch, "circulation")

    return processed


def load_visits_workbook(conn, library_lookup) -> int:
    workbook = pd.ExcelFile(VISITS_WORKBOOK)
    processed = 0
    batch: list[dict] = []

    for sheet_name in workbook.sheet_names:
        year = int(sheet_name)
        df = pd.read_excel(VISITS_WORKBOOK, sheet_name=sheet_name, header=[0, 1, 2]).iloc[1:].copy()

        for _, row in df.iterrows():
            branch_label = clean_text(row.iloc[0])
            if branch_label is None:
                continue

            branch_name = branch_label.split(" ", 1)[1] if branch_label.startswith("(") and " " in branch_label else branch_label
            library_id = resolve_library_id(library_lookup, branch_name)
            if library_id is None:
                continue

            batch.append(
                {
                    "library_id": library_id,
                    "year": year,
                    "metric_value": to_int(row.iloc[14]),
                }
            )

            if len(batch) >= 500:
                processed += flush_branch_kpi_batch(conn, batch, "visits")

    processed += flush_branch_kpi_batch(conn, batch, "visits")

    return processed


def main() -> None:
    with engine.connect() as conn:
        library_lookup = build_library_lookup(conn)
        delete_existing_loan_type(
            conn,
            ["MontrealPublic:", "MontrealFormat:", "MontrealBooks:"],
        )
        conn.commit()

        public_rows = load_public_workbook(conn, library_lookup)
        conn.commit()
        format_rows = load_format_workbook(conn, library_lookup)
        conn.commit()
        book_rows = load_book_workbook(conn, library_lookup)
        conn.commit()
        total_rows = load_total_workbook(conn, library_lookup)
        conn.commit()
        visit_rows = load_visits_workbook(conn, library_lookup)
        conn.commit()

    print(f"[Montreal circulation workbooks] Inserted {public_rows} public-summary rows into circulation_transaction.")
    print(f"[Montreal circulation workbooks] Inserted {format_rows} format-summary rows into circulation_transaction.")
    print(f"[Montreal circulation workbooks] Inserted {book_rows} book-summary rows into circulation_transaction.")
    print(f"[Montreal circulation workbooks] Upserted {total_rows} branch circulation rows into branch_kpi.")
    print(f"[Montreal circulation workbooks] Upserted {visit_rows} branch visit rows into branch_kpi.")


if __name__ == "__main__":
    main()
