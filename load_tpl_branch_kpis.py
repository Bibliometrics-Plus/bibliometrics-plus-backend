import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "toronto"

CIRC_PATH = DATA_DIR / "tpl-circulation-annual-by-branch.csv"
VISITS_PATH = DATA_DIR / "tpl-visits-annual-by-branch.csv"
REG_PATH = DATA_DIR / "tpl-card-registrations-annual-by-branch.csv"

SYSTEM = "TPL"


def to_int_or_none(x):
    """Convert pandas numbers to Python int, and NaN -> None for SQL."""
    if pd.isna(x):
        return None
    return int(x)


def main():
    circ = pd.read_csv(CIRC_PATH)[["Year", "BranchCode", "Circulation"]].copy()
    visits = pd.read_csv(VISITS_PATH)[["Year", "BranchCode", "Visits"]].copy()
    regs = pd.read_csv(REG_PATH)[["Year", "BranchCode", "Registrations"]].copy()

    # Normalize key columns
    for d in (circ, visits, regs):
        d["BranchCode"] = d["BranchCode"].astype(str).str.strip()
        d["Year"] = d["Year"].astype(int)

    # Merge all KPIs into one dataframe
    df = circ.merge(visits, on=["Year", "BranchCode"], how="outer") \
             .merge(regs, on=["Year", "BranchCode"], how="outer")

    df = df.dropna(subset=["Year", "BranchCode"]).copy()

    processed = 0
    missing_lib = 0

    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            lib_id = conn.execute(
                text("""
                    SELECT library_id
                    FROM library
                    WHERE system_name = :system_name AND branch_code = :branch_code
                    LIMIT 1;
                """),
                {"system_name": SYSTEM, "branch_code": row.BranchCode},
            ).scalar()

            if lib_id is None:
                missing_lib += 1
                continue

            conn.execute(
                text("""
                    INSERT INTO branch_kpi (library_id, year, visits, registrations, circulation)
                    VALUES (:library_id, :year, :visits, :registrations, :circulation)
                    ON CONFLICT (library_id, year)
                    DO UPDATE SET
                      visits = COALESCE(EXCLUDED.visits, branch_kpi.visits),
                      registrations = COALESCE(EXCLUDED.registrations, branch_kpi.registrations),
                      circulation = COALESCE(EXCLUDED.circulation, branch_kpi.circulation);
                """),
                {
                    "library_id": lib_id,
                    "year": int(row.Year),
                    "visits": to_int_or_none(getattr(row, "Visits", None)),
                    "registrations": to_int_or_none(getattr(row, "Registrations", None)),
                    "circulation": to_int_or_none(getattr(row, "Circulation", None)),
                },
            )
            processed += 1

    print(f"[TPL] Upserted {processed} branch KPI rows into branch_kpi.")
    if missing_lib:
        print(f"[WARN] Skipped {missing_lib} rows because BranchCode was not found in library table.")
        print("       (This usually means the libraries loader didn't run or branch_code didn't load.)")


if __name__ == "__main__":
    main()