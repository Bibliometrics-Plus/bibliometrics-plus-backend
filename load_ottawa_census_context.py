from pathlib import Path
import pandas as pd
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

SUBAREA_FILE = DATA_DIR / "2021_Census_Sub-Area_25_Long_Form.csv"
WARD_FILE = DATA_DIR / "2021_Long_Form_Census_-_Ward_Data.csv"


def clean_value(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if value in {"", "..", "...", "x", "X", "F"}:
        return None
    return value


def load_subarea():
    print(f"Reading subarea file: {SUBAREA_FILE}")
    df = pd.read_csv(SUBAREA_FILE, encoding="latin1")

    characteristic_col = df.columns[0]

    long_df = df.melt(
        id_vars=[characteristic_col],
        var_name="geography_name",
        value_name="value"
    ).rename(columns={characteristic_col: "characteristic"})

    long_df["characteristic"] = long_df["characteristic"].apply(clean_value)
    long_df["geography_name"] = long_df["geography_name"].apply(clean_value)
    long_df["value"] = long_df["value"].apply(clean_value)

    long_df = long_df.dropna(subset=["characteristic", "geography_name"])
    long_df["geography_type"] = "subarea"
    long_df["census_year"] = 2021
    long_df["source_dataset"] = SUBAREA_FILE.name

    # keep only columns that match Supabase table
    long_df = long_df[
        [
            "geography_type",
            "geography_name",
            "characteristic",
            "value",
            "census_year",
            "source_dataset",
        ]
    ]

    print(f"Subarea melted rows: {len(long_df)}")

    # faster bulk insert
    long_df.to_sql(
        "ottawa_census_subarea_longform",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    print(f"Loaded {len(long_df)} rows into ottawa_census_subarea_longform.")


def load_ward():
    print(f"Reading ward file: {WARD_FILE}")
    df = pd.read_csv(WARD_FILE, encoding="latin1")

    characteristic_col = df.columns[0]

    long_df = df.melt(
        id_vars=[characteristic_col],
        var_name="geography_name",
        value_name="value"
    ).rename(columns={characteristic_col: "characteristic"})

    long_df["characteristic"] = long_df["characteristic"].apply(clean_value)
    long_df["geography_name"] = long_df["geography_name"].apply(clean_value)
    long_df["value"] = long_df["value"].apply(clean_value)

    long_df = long_df.dropna(subset=["characteristic", "geography_name"])
    long_df["geography_type"] = "ward"
    long_df["census_year"] = 2021
    long_df["source_dataset"] = WARD_FILE.name

    # keep only columns that match Supabase table
    long_df = long_df[
        [
            "geography_type",
            "geography_name",
            "characteristic",
            "value",
            "census_year",
            "source_dataset",
        ]
    ]

    print(f"Ward melted rows: {len(long_df)}")

    # faster bulk insert
    long_df.to_sql(
        "ottawa_census_ward_longform",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    print(f"Loaded {len(long_df)} rows into ottawa_census_ward_longform.")


if __name__ == "__main__":
    # optional: clear old staged data before reloading
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE ottawa_census_subarea_longform;"))
        conn.execute(text("TRUNCATE TABLE ottawa_census_ward_longform;"))
        print("Truncated old census context tables.")

    load_subarea()
    load_ward()

    print("Ottawa census context load complete.")