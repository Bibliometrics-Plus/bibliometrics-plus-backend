import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "toronto"
CSV_PATH = DATA_DIR / "tpl-branch-general-information-2023.csv"

SYSTEM = "TPL"

def main():
    df = pd.read_csv(CSV_PATH)

    df = df[["BranchCode", "BranchName", "Address", "Lat", "Long"]].copy()

    df["BranchCode"] = df["BranchCode"].astype(str).str.strip()
    df["BranchName"] = df["BranchName"].astype(str).str.strip()

    processed = 0

    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO library (name, address, city, system_name, branch_code, latitude, longitude)
                    VALUES (:name, :address, 'Toronto', :system_name, :branch_code, :latitude, :longitude)
                    ON CONFLICT (system_name, branch_code)
                    DO UPDATE SET
                      name = EXCLUDED.name,
                      address = EXCLUDED.address,
                      city = EXCLUDED.city,
                      latitude = EXCLUDED.latitude,
                      longitude = EXCLUDED.longitude;
                """),
                {
                    "name": row.BranchName,
                    "address": row.Address,
                    "system_name": SYSTEM,
                    "branch_code": row.BranchCode,
                    "latitude": float(row.Lat) if pd.notna(row.Lat) else None,
                    "longitude": float(row.Long) if pd.notna(row.Long) else None,
                }
            )
            processed += 1

    print(f"[TPL] Inserted/Updated {processed} branches.")

if __name__ == "__main__":
    main()