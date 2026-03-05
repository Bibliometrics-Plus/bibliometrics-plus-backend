import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "toronto"
CSV_PATH = DATA_DIR / "library-circulation-by-cardholder-type.csv"

SYSTEM = "TPL"

def main():
    df = pd.read_csv(CSV_PATH)[["Year", "BranchCode", "CardholderType", "Circulation"]]

    processed = 0

    with engine.begin() as conn:
        for row in df.itertuples(index=False):

            # Ensure user_group exists
            conn.execute(
                text("""
                    INSERT INTO user_group (age_group)
                    VALUES (:label)
                    ON CONFLICT (age_group) DO NOTHING;
                """),
                {"label": row.CardholderType}
            )

            lib_id = conn.execute(
                text("""
                    SELECT library_id
                    FROM library
                    WHERE system_name = :system_name AND branch_code = :branch_code
                    LIMIT 1;
                """),
                {"system_name": SYSTEM, "branch_code": row.BranchCode}
            ).scalar()

            if lib_id is None:
                continue

            conn.execute(
                text("""
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
                        (SELECT group_id FROM user_group WHERE age_group = :cardholder_type LIMIT 1),
                        :library_id,
                        make_date(:year, 1, 1),
                        'AnnualByCardholderType',
                        :loan_count
                    );
                """),
                {
                    "cardholder_type": row.CardholderType,
                    "library_id": lib_id,
                    "year": int(row.Year),
                    "loan_count": row.Circulation,
                }
            )
            processed += 1

    print(f"[TPL] Inserted {processed} circulation rows.")

if __name__ == "__main__":
    main()