from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import engine
from load_montreal_open_data_catalog import SYSTEM_NAME, build_library_lookup, normalize_lookup


# This loader maps the consolidated Montreal branch workbook into the shared branch_kpi table.
# It gives the dashboard real Montreal branch-level circulation and visit data without changing
# the Ottawa or Toronto pipelines.
BASE_DIR = Path(__file__).resolve().parent
WORKBOOK_PATH = BASE_DIR / "data" / "raw" / "montreal" / "bm_stats-consol.xlsx"


def clean_text(value) -> str | None:
    if pd.isna(value):
        return None
    cleaned = " ".join(str(value).strip().split())
    return cleaned or None


def to_int(value) -> int | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def resolve_library_id(library_lookup: dict[str, tuple[int, str]], library_name: str | None):
    """
    Match workbook library labels against the cleaned Montreal library table.
    """
    if library_name is None:
        return None

    candidates = [
        library_name,
        f"Bibliothèque {library_name}",
        f"Bibliothèque de {library_name}",
        f"Bibliothèque du {library_name}",
        f"Bibliothèque d'{library_name}",
    ]

    for candidate in candidates:
        match = library_lookup.get(normalize_lookup(candidate))
        if match is not None:
            return match[0]

    aliases = {
        "livromobile": "Bibliobus",
        "haut-anjou": "Bibliothèque du Haut-Anjou",
        "plateau-mont-royal": "Bibliothèque du Plateau-Mont-Royal",
        "plateau mont royal": "Bibliothèque du Plateau-Mont-Royal",
        "l'octogone": "Bibliothèque L'Octogone",
        "l octogone": "Bibliothèque L'Octogone",
        "ahuntsic": "Bibliothèque d'Ahuntsic",
        "cartierville": "Bibliothèque de Cartierville",
        "salaberry": "Bibliothèque de Salaberry",
        "interculturelle": "Bibliothèque interculturelle",
        "côte-des-neiges": "Bibliothèque de Côte-des-Neiges",
        "cote-des-neiges": "Bibliothèque de Côte-des-Neiges",
        "rivière-des-prairies": "Bibliothèque de Rivière-des-Prairies",
        "riviere-des-prairies": "Bibliothèque de Rivière-des-Prairies",
        "pointe-aux-trembles": "Bibliothèque de Pointe-aux-Trembles",
    }

    alias_target = aliases.get(library_name.lower())
    if alias_target:
        match = library_lookup.get(normalize_lookup(alias_target))
        if match is not None:
            return match[0]

    return None


def main() -> None:
    workbook = pd.ExcelFile(WORKBOOK_PATH)
    processed = 0
    skipped = 0

    with engine.begin() as conn:
        library_lookup = build_library_lookup(conn)

        for sheet_name in workbook.sheet_names:
            year = int(sheet_name)
            df = pd.read_excel(WORKBOOK_PATH, sheet_name=sheet_name, header=[0, 1, 2])
            df = df.dropna(how="all")

            library_col = ("Bibliothèque", "Unnamed: 4_level_1", "Unnamed: 4_level_2")
            circulation_col = ("Prêt non numérique", year, "TOTAL")
            visits_col = ("Fréquentation", year, "Unnamed: 19_level_2")
            registrations_col = ("Abonnés", "TOTAL", "Unnamed: 18_level_2")

            for _, row in df.iterrows():
                library_name = clean_text(row.get(library_col))
                if library_name is None:
                    continue

                library_id = resolve_library_id(library_lookup, library_name)
                if library_id is None:
                    skipped += 1
                    continue

                circulation_total = to_int(row.get(circulation_col))
                digital_loans = to_int(row.get(("Prêt numérique", "Unnamed: 14_level_1", "Unnamed: 14_level_2")))
                if circulation_total is not None and digital_loans is not None:
                    circulation_total += digital_loans

                conn.execute(
                    text(
                        """
                        INSERT INTO branch_kpi (library_id, year, visits, registrations, circulation)
                        VALUES (:library_id, :year, :visits, :registrations, :circulation)
                        ON CONFLICT (library_id, year)
                        DO UPDATE SET
                            visits = COALESCE(EXCLUDED.visits, branch_kpi.visits),
                            registrations = COALESCE(EXCLUDED.registrations, branch_kpi.registrations),
                            circulation = COALESCE(EXCLUDED.circulation, branch_kpi.circulation);
                        """
                    ),
                    {
                        "library_id": library_id,
                        "year": year,
                        "visits": to_int(row.get(visits_col)),
                        "registrations": to_int(row.get(registrations_col)),
                        "circulation": circulation_total,
                    },
                )
                processed += 1

    print(f"[Montreal branch KPI] Upserted {processed} rows into branch_kpi.")
    print(f"[Montreal branch KPI] Skipped {skipped} rows without a library match.")


if __name__ == "__main__":
    main()
