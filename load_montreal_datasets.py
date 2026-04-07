from __future__ import annotations

from pathlib import Path

from load_montreal_branch_kpis_from_bm_stats import WORKBOOK_PATH as BM_STATS_WORKBOOK_PATH, main as load_montreal_branch_kpis
from load_montreal_circulation_workbooks import (
    BOOK_WORKBOOK,
    FORMAT_WORKBOOK,
    PUBLIC_WORKBOOK,
    TOTAL_WORKBOOK,
    VISITS_WORKBOOK,
    main as load_montreal_circulation_workbooks,
)
from load_montreal_collection_workbooks import (
    COLLECTION_BOOKS_WORKBOOK,
    COLLECTION_FORMAT_WORKBOOK,
    COLLECTION_LANGUAGE_WORKBOOK,
    SELF_SERVICE_WORKBOOK,
    main as load_montreal_collection_workbooks,
)
from load_libraries_from_montreal_bottin import CSV_PATH as MONTREAL_LIBRARIES_PATH, load_libraries
from load_montreal_census_2021 import CSV_PATH as MONTREAL_CENSUS_PATH, main as load_montreal_census
from load_montreal_open_data_catalog import CSV_PATH as MONTREAL_OPEN_DATA_PATH, main as load_montreal_open_data
from load_montreal_palmares import PALMARES_FILES, load_palmares
from load_montreal_stats_all_years import main as load_montreal_stats_all_years


# This runner is intentionally Montreal-only.
# The Ottawa and Toronto loaders belong to other teammates, so this file avoids
# touching their datasets and keeps this workflow focused on the part you own.
# It only runs loaders that write into the shared comparison tables used by the
# Streamlit app, so Montreal stays aligned with the team schema.
def all_exist(paths: list[Path]) -> bool:
    return all(path.exists() for path in paths)


def any_exist(paths: list[Path]) -> bool:
    return any(path.exists() for path in paths)


def main() -> None:
    loaders = [
        {
            "name": "Montreal libraries from bottin",
            "paths": [MONTREAL_LIBRARIES_PATH],
            "fn": load_libraries,
        },
        {
            "name": "Montreal palmares collection data",
            "paths": PALMARES_FILES,
            "fn": load_palmares,
            "predicate": any_exist,
        },
        {
            "name": "Montreal multi-year summary statistics",
            "paths": list(Path(path) for path in []),
            "fn": load_montreal_stats_all_years,
            "predicate": lambda _paths: True,
        },
        {
            "name": "Montreal open-data catalog",
            "paths": [MONTREAL_OPEN_DATA_PATH],
            "fn": load_montreal_open_data,
        },
        {
            "name": "Montreal census-derived user group stats",
            "paths": [MONTREAL_CENSUS_PATH],
            "fn": load_montreal_census,
        },
        {
            "name": "Montreal branch KPI workbook",
            "paths": [BM_STATS_WORKBOOK_PATH],
            "fn": load_montreal_branch_kpis,
        },
        {
            "name": "Montreal shared collection and service workbooks",
            "paths": [
                COLLECTION_FORMAT_WORKBOOK,
                COLLECTION_LANGUAGE_WORKBOOK,
                COLLECTION_BOOKS_WORKBOOK,
                SELF_SERVICE_WORKBOOK,
            ],
            "fn": load_montreal_collection_workbooks,
        },
        {
            "name": "Montreal circulation summary workbooks",
            "paths": [
                PUBLIC_WORKBOOK,
                FORMAT_WORKBOOK,
                BOOK_WORKBOOK,
                TOTAL_WORKBOOK,
                VISITS_WORKBOOK,
            ],
            "fn": load_montreal_circulation_workbooks,
        },
    ]

    ran = []
    skipped = []

    for loader in loaders:
        paths = loader["paths"]
        predicate = loader.get("predicate", all_exist)

        if not predicate(paths):
            skipped.append(loader["name"])
            print(f"[SKIP] {loader['name']} (source files not present)")
            continue

        print(f"[RUN ] {loader['name']}")
        loader["fn"]()
        ran.append(loader["name"])

    print()
    print("Completed Montreal dataset loading.")
    print("Ran:", ", ".join(ran) if ran else "none")
    print("Skipped:", ", ".join(skipped) if skipped else "none")


if __name__ == "__main__":
    main()
