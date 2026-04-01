import pandas as pd
from pathlib import Path
from sqlalchemy import text
from db import engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "toronto"
FILE_PATH = DATA_DIR / "tpl-neighbourhood-profiles-2021.xlsx"
SHEET_NAME = "hd2021_census_profile"

# Load selected Toronto neighbourhood profile indicators from the
# 2021 neighbourhood profiles workbook into Supabase.
# This table is used to add EDI-related neighbourhood context
# to Toronto library branches.

def clean_text(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None


def to_float_or_none(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s in {"", "...", "x"}:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def to_int_or_none(x):
    value = to_float_or_none(x)
    if value is None:
        return None
    return int(round(value))


def normalize_label(x):
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().split())


def find_row_index_by_exact_label(df, label):
    target = normalize_label(label)
    for idx in df.index:
        current = normalize_label(df.iloc[idx, 0])
        if current == target:
            return idx
    raise ValueError(f"Could not find row label: {label}")


def main():
    df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME, header=None)

    # First column = labels
    df.iloc[:, 0] = df.iloc[:, 0].apply(normalize_label)

    # The workbook is in wide format:
    # - column 0 contains row labels / indicator names
    # - row 0 contains neighbourhood names
    # - row 1 contains neighbourhood numbers
    # - row 2 contains TSNS designations

    # Header-style rows from the workbook
    neighbourhood_names = df.iloc[0, 1:]
    neighbourhood_numbers = df.iloc[1, 1:]
    tsns_designations = df.iloc[2, 1:]

    # Only the selected indicators we want for EDI/context
    # These focus on age, income, housing, language diversity,
    # and household structure.
    indicator_rows = {
        "age_0_14_pct": find_row_index_by_exact_label(
            df, "0 to 14 years"
        ),
        "age_65_plus_pct": find_row_index_by_exact_label(
            df, "65 years and over"
        ),
        "median_age": find_row_index_by_exact_label(
            df, "Median age of the population"
        ),
        "median_after_tax_income_2020": find_row_index_by_exact_label(
            df, "Median after-tax income in 2020 among recipients ($)"
        ),
        "low_income_lim_at_pct": find_row_index_by_exact_label(
            df, "Prevalence of low income based on the Low-income measure, after tax (LIM-AT) (%)"
        ),
        "low_income_lico_at_pct": find_row_index_by_exact_label(
            df, "Prevalence of low income based on the Low-income cut-offs, after tax (LICO-AT) (%)"
        ),
        "gini_after_tax_income": find_row_index_by_exact_label(
            df, "Gini index on adjusted household after-tax income"
        ),
        "core_housing_need_pct": find_row_index_by_exact_label(
            df, "% in core housing need"
        ),
        "shelter_cost_30_plus_pct": find_row_index_by_exact_label(
            df, "Spending 30% or more of income on shelter costs"
        ),
        "not_suitable_count": find_row_index_by_exact_label(
            df, "Not suitable"
        ),
        "major_repairs_count": find_row_index_by_exact_label(
            df, "Major repairs needed"
        ),
        "neither_english_nor_french_count": find_row_index_by_exact_label(
            df, "Neither English nor French"
        ),
        "non_official_languages_count": find_row_index_by_exact_label(
            df, "Non-official languages"
        ),
        "one_parent_families_count": find_row_index_by_exact_label(
            df, "Total one-parent families"
        ),
        "one_person_households_count": find_row_index_by_exact_label(
            df, "One-person households"
        ),
    }

    processed = 0

    with engine.begin() as conn:
        for col_idx in range(1, df.shape[1]):
            neighbourhood_name = clean_text(neighbourhood_names.iloc[col_idx - 1])
            neighbourhood_no = to_int_or_none(neighbourhood_numbers.iloc[col_idx - 1])
            tsns_designation = clean_text(tsns_designations.iloc[col_idx - 1])

            if neighbourhood_name is None or neighbourhood_no is None:
                continue

            record = {
                "neighbourhood_no": neighbourhood_no,
                "neighbourhood_name": neighbourhood_name,
                "tsns_designation": tsns_designation,
                "age_0_14_pct": to_float_or_none(df.iloc[indicator_rows["age_0_14_pct"], col_idx]),
                "age_65_plus_pct": to_float_or_none(df.iloc[indicator_rows["age_65_plus_pct"], col_idx]),
                "median_age": to_float_or_none(df.iloc[indicator_rows["median_age"], col_idx]),
                "median_after_tax_income_2020": to_float_or_none(df.iloc[indicator_rows["median_after_tax_income_2020"], col_idx]),
                "low_income_lim_at_pct": to_float_or_none(df.iloc[indicator_rows["low_income_lim_at_pct"], col_idx]),
                "low_income_lico_at_pct": to_float_or_none(df.iloc[indicator_rows["low_income_lico_at_pct"], col_idx]),
                "gini_after_tax_income": to_float_or_none(df.iloc[indicator_rows["gini_after_tax_income"], col_idx]),
                "core_housing_need_pct": to_float_or_none(df.iloc[indicator_rows["core_housing_need_pct"], col_idx]),
                "shelter_cost_30_plus_pct": to_float_or_none(df.iloc[indicator_rows["shelter_cost_30_plus_pct"], col_idx]),
                "not_suitable_count": to_int_or_none(df.iloc[indicator_rows["not_suitable_count"], col_idx]),
                "major_repairs_count": to_int_or_none(df.iloc[indicator_rows["major_repairs_count"], col_idx]),
                "neither_english_nor_french_count": to_int_or_none(df.iloc[indicator_rows["neither_english_nor_french_count"], col_idx]),
                "non_official_languages_count": to_int_or_none(df.iloc[indicator_rows["non_official_languages_count"], col_idx]),
                "one_parent_families_count": to_int_or_none(df.iloc[indicator_rows["one_parent_families_count"], col_idx]),
                "one_person_households_count": to_int_or_none(df.iloc[indicator_rows["one_person_households_count"], col_idx]),
            }

            conn.execute(
                text("""
                    INSERT INTO tpl_neighbourhood_profile (
                        neighbourhood_no,
                        neighbourhood_name,
                        tsns_designation,
                        age_0_14_pct,
                        age_65_plus_pct,
                        median_age,
                        median_after_tax_income_2020,
                        low_income_lim_at_pct,
                        low_income_lico_at_pct,
                        gini_after_tax_income,
                        core_housing_need_pct,
                        shelter_cost_30_plus_pct,
                        not_suitable_count,
                        major_repairs_count,
                        neither_english_nor_french_count,
                        non_official_languages_count,
                        one_parent_families_count,
                        one_person_households_count
                    )
                    VALUES (
                        :neighbourhood_no,
                        :neighbourhood_name,
                        :tsns_designation,
                        :age_0_14_pct,
                        :age_65_plus_pct,
                        :median_age,
                        :median_after_tax_income_2020,
                        :low_income_lim_at_pct,
                        :low_income_lico_at_pct,
                        :gini_after_tax_income,
                        :core_housing_need_pct,
                        :shelter_cost_30_plus_pct,
                        :not_suitable_count,
                        :major_repairs_count,
                        :neither_english_nor_french_count,
                        :non_official_languages_count,
                        :one_parent_families_count,
                        :one_person_households_count
                    )
                    ON CONFLICT (neighbourhood_no)
                    DO UPDATE SET
                        neighbourhood_name = EXCLUDED.neighbourhood_name,
                        tsns_designation = EXCLUDED.tsns_designation,
                        age_0_14_pct = EXCLUDED.age_0_14_pct,
                        age_65_plus_pct = EXCLUDED.age_65_plus_pct,
                        median_age = EXCLUDED.median_age,
                        median_after_tax_income_2020 = EXCLUDED.median_after_tax_income_2020,
                        low_income_lim_at_pct = EXCLUDED.low_income_lim_at_pct,
                        low_income_lico_at_pct = EXCLUDED.low_income_lico_at_pct,
                        gini_after_tax_income = EXCLUDED.gini_after_tax_income,
                        core_housing_need_pct = EXCLUDED.core_housing_need_pct,
                        shelter_cost_30_plus_pct = EXCLUDED.shelter_cost_30_plus_pct,
                        not_suitable_count = EXCLUDED.not_suitable_count,
                        major_repairs_count = EXCLUDED.major_repairs_count,
                        neither_english_nor_french_count = EXCLUDED.neither_english_nor_french_count,
                        non_official_languages_count = EXCLUDED.non_official_languages_count,
                        one_parent_families_count = EXCLUDED.one_parent_families_count,
                        one_person_households_count = EXCLUDED.one_person_households_count;
                """),
                record,
            )

            processed += 1

    print(f"[TORONTO] Inserted/Updated {processed} rows into tpl_neighbourhood_profile.")


if __name__ == "__main__":
    main()