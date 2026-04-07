from __future__ import annotations

import csv
from pathlib import Path

from sqlalchemy import text

from db import engine


# This file derives the census age-band slice that cleanly fits the existing
# shared user_group and user_group_stats tables. We intentionally avoid keeping
# a separate raw census table unless the team decides it is truly needed.
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "raw" / "montreal" / "DONNÉES DU RECENSEMENT DE 2021_AGGLOMÉRATION DE MONTRÉAL_TOTAUX ET POURCENTAGES_0(Recensement2021_Totaux et %).csv"
SOURCE_CITY = "Montreal"
CENSUS_YEAR = 2021
USER_GROUP_INDICATORS = {
    "0 à 14 ans": "Child",
    "15 à 64 ans": "Adult",
    "65 ans et plus": "Senior",
}


def clean_text(value: str | None) -> str | None:
    """
    Normalize whitespace and convert empty strings to None.
    """
    if value is None:
        return None

    normalized = " ".join(str(value).replace("\x96", "-").split()).strip()
    return normalized or None


def ensure_user_group(conn, age_group: str) -> int:
    """
    Reuse the shared user_group table so census age bands can power the same
    comparison views as other cities.
    """
    conn.execute(
        text(
            """
            INSERT INTO user_group (age_group)
            VALUES (:age_group)
            ON CONFLICT (age_group) DO NOTHING;
            """
        ),
        {"age_group": age_group},
    )

    return conn.execute(
        text("SELECT group_id FROM user_group WHERE age_group = :age_group LIMIT 1"),
        {"age_group": age_group},
    ).scalar()


def should_include_geography(geography_name: str | None) -> bool:
    """
    user_group_stats is most useful for sub-city / municipal comparison rows.
    We skip the agglomeration-wide total because it is not a local geography.
    """
    if geography_name is None:
        return False
    return geography_name != "AGGLOMÉRATION DE MONTRÉAL"


def main() -> None:
    with CSV_PATH.open("r", encoding="latin1", newline="") as handle:
        rows = list(csv.reader(handle))

    geography_header = rows[3]
    geography_names = [clean_text(geography_header[idx]) for idx in range(1, len(geography_header), 2)]

    derived_user_group_rows = 0
    current_section = None
    user_group_batch = []
    batch_size = 1000
    user_group_insert_sql = text(
        """
        INSERT INTO user_group_stats (
            group_id,
            neighbourhood,
            year,
            cardholder_count
        )
        VALUES (
            :group_id,
            :neighbourhood,
            :year,
            :cardholder_count
        );
        """
    )

    with engine.connect() as conn:
        user_group_ids = {label: ensure_user_group(conn, label) for label in USER_GROUP_INDICATORS.values()}
        conn.commit()

        # Replace the Montreal 2021 derived age-group slice on rerun so the load stays idempotent.
        for group_id in user_group_ids.values():
            conn.execute(
                text(
                    """
                    DELETE FROM user_group_stats
                    WHERE year = :year
                      AND group_id = :group_id
                      AND neighbourhood LIKE :city_prefix;
                    """
                ),
                {
                    "year": CENSUS_YEAR,
                    "group_id": group_id,
                    "city_prefix": f"{SOURCE_CITY} | %",
                },
            )
        conn.commit()

        for row in rows[5:]:
            indicator_name = clean_text(row[0] if row else None)
            if indicator_name is None:
                continue

            if indicator_name == "Population totale selon le groupe d'âges":
                current_section = indicator_name
                continue

            # Rows that only contain a label in the first column act like section headers.
            if all(clean_text(value) is None for value in row[1:]):
                current_section = indicator_name
                continue

            for geo_index, geography_name in enumerate(geography_names):
                if geography_name is None:
                    continue

                count_col = 1 + (geo_index * 2)
                pct_col = count_col + 1

                value_count = clean_text(row[count_col] if count_col < len(row) else None)
                if value_count is None:
                    continue

                if (
                    current_section == "Population totale selon le groupe d'âges"
                    and indicator_name in USER_GROUP_INDICATORS
                    and should_include_geography(geography_name)
                ):
                    raw_count = value_count.replace(",", "").replace(" ", "") if value_count else None
                    try:
                        parsed_count = int(raw_count) if raw_count else None
                    except Exception:
                        parsed_count = None

                    if parsed_count is not None:
                        user_group_batch.append(
                            {
                                "group_id": user_group_ids[USER_GROUP_INDICATORS[indicator_name]],
                                "neighbourhood": f"{SOURCE_CITY} | {geography_name}",
                                "year": CENSUS_YEAR,
                                "cardholder_count": parsed_count,
                            }
                        )
                        derived_user_group_rows += 1

                        if len(user_group_batch) >= batch_size:
                            conn.execute(user_group_insert_sql, user_group_batch)
                            conn.commit()
                            user_group_batch.clear()

        if user_group_batch:
            conn.execute(user_group_insert_sql, user_group_batch)
            conn.commit()

        # Remove raw census tables so the schema stays aligned with the shared comparison model.
        conn.execute(text("DROP TABLE IF EXISTS census_indicator"))
        conn.execute(text("DROP TABLE IF EXISTS montreal_census_2021_long"))
        conn.commit()

    print(f"[Montreal census] Derived {derived_user_group_rows} age-group rows into user_group_stats.")


if __name__ == "__main__":
    main()
