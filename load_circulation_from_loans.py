from pathlib import Path
import pandas as pd
from sqlalchemy import text
from db import engine

# Base folder = backend/
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

CSV_LOANS_2022 = DATA_DIR / "Library_Loans_Physical_Material_Circulation_2022.csv"
CSV_LOANS_2024 = DATA_DIR / "Library_Loans_Physical_Material_Circulation_2024.csv"


def normalize_loans(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Expected columns (rename as needed):
      - Branch           -> branch_name
      - AgeGroup         -> age_group
      - Subject          -> subject_name  (if present)
      - LoanType         -> loan_type
      - Month            -> period        (e.g. '2024-03')
      - Loans            -> loan_count
    """
    rename_map = {
        "Branch": "branch_name",
        "AgeGroup": "age_group",
        "Subject": "subject_name",
        "LoanType": "loan_type",
        "Month": "period",
        "Loans": "loan_count",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["year"] = year
    return df


def load_loans(csv_path: Path, year: int):
    df = pd.read_csv(csv_path)
    df = normalize_loans(df, year)

    processed = 0
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO circulation_transaction (
                        item_id,
                        group_id,
                        borrow_date,
                        loan_type,
                        loan_count
                    )
                    VALUES (
                        NULL,          -- no concrete item_id
                        NULL,          -- no age_group info in this dataset
                        to_date(:period, 'YYYY-MM'),
                        :loan_type,
                        :loan_count
                    );
                """),
                {
                    "period": getattr(row, "period", f"{year}-01"),
                    "loan_type": getattr(row, "loan_type", "Physical"),
                    "loan_count": int(getattr(row, "loan_count", 0) or 0),
                },
            )
            processed += 1

    print(f"[{year}] Inserted {processed} circulation summary rows.")


def main():
    load_loans(CSV_LOANS_2022, 2022)
    load_loans(CSV_LOANS_2024, 2024)


if __name__ == "__main__":
    main()
