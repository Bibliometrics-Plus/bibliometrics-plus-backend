import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "toronto"
CSV_PATH = DATA_DIR / "tpl-branch-general-information-2023.csv"

SYSTEM = "TPL"


def to_int_or_none(x):
    if pd.isna(x) or x == "":
        return None
    return int(x)


def to_bool_or_none(x):
    if pd.isna(x) or x == "":
        return None
    return str(x).strip() in ["1", "True", "true", "YES", "Yes", "yes"]


def main():
    df = pd.read_csv(CSV_PATH)

    # Keep only the branch and neighbourhood fields we need
    df = df[
        [
            "BranchCode",
            "BranchName",
            "Address",
            "PostalCode",
            "Lat",
            "Long",
            "NBHDNo",
            "NBHDName",
            "TPLNIA",
            "WardNo",
            "WardName",
        ]
    ].copy()

    df["BranchCode"] = df["BranchCode"].astype(str).str.strip()
    df["BranchName"] = df["BranchName"].astype(str).str.strip()

    if "PostalCode" in df.columns:
        df["PostalCode"] = df["PostalCode"].astype(str).str.strip()

    processed = 0

    # Insert or update each Toronto branch in the library table
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO library (
                        name,
                        address,
                        city,
                        system_name,
                        branch_code,
                        latitude,
                        longitude,
                        postal_code,
                        neighbourhood_no,
                        neighbourhood_name,
                        tpl_nia,
                        ward_no,
                        ward_name
                    )
                    VALUES (
                        :name,
                        :address,
                        'Toronto',
                        :system_name,
                        :branch_code,
                        :latitude,
                        :longitude,
                        :postal_code,
                        :neighbourhood_no,
                        :neighbourhood_name,
                        :tpl_nia,
                        :ward_no,
                        :ward_name
                    )
                    ON CONFLICT (system_name, branch_code)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        address = EXCLUDED.address,
                        city = EXCLUDED.city,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        postal_code = EXCLUDED.postal_code,
                        neighbourhood_no = EXCLUDED.neighbourhood_no,
                        neighbourhood_name = EXCLUDED.neighbourhood_name,
                        tpl_nia = EXCLUDED.tpl_nia,
                        ward_no = EXCLUDED.ward_no,
                        ward_name = EXCLUDED.ward_name;
                """),
                {
                    "name": row.BranchName,
                    "address": row.Address,
                    "system_name": SYSTEM,
                    "branch_code": row.BranchCode,
                    "latitude": float(row.Lat) if pd.notna(row.Lat) else None,
                    "longitude": float(row.Long) if pd.notna(row.Long) else None,
                    "postal_code": None if pd.isna(row.PostalCode) else str(row.PostalCode).strip(),
                    "neighbourhood_no": to_int_or_none(row.NBHDNo),
                    "neighbourhood_name": None if pd.isna(row.NBHDName) else str(row.NBHDName).strip(),
                    "tpl_nia": to_bool_or_none(row.TPLNIA),
                    "ward_no": to_int_or_none(row.WardNo),
                    "ward_name": None if pd.isna(row.WardName) else str(row.WardName).strip(),
                }
            )
            processed += 1

    print(f"[TPL] Inserted/Updated {processed} branches.")


if __name__ == "__main__":
    main()