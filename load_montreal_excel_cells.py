from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import engine


# This loader preserves all Montreal Excel workbook content in a simple raw-cell table.
# It is intentionally generic so every workbook is ingested even when the sheet structure
# differs from file to file.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw" / "montreal"
EXCEL_FILES = sorted([path for path in DATA_DIR.iterdir() if path.suffix.lower() in {".xls", ".xlsx"}])


def clean_text(value) -> str | None:
    """
    Normalize cell text and drop empty values.
    """
    if pd.isna(value):
        return None

    cleaned = " ".join(str(value).strip().split())
    return cleaned or None


def ensure_tables(conn) -> None:
    """
    Create the raw Excel storage table used for the Montreal workbook datasets.
    """
    table_exists = conn.execute(
        text("SELECT to_regclass('public.montreal_excel_cell')")
    ).scalar()
    if table_exists:
        return

    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS montreal_excel_cell (
                cell_id SERIAL PRIMARY KEY,
                workbook_name TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                row_index INTEGER NOT NULL,
                col_index INTEGER NOT NULL,
                cell_value TEXT NOT NULL,
                UNIQUE (workbook_name, sheet_name, row_index, col_index)
            );
            """
        )
    )


def main() -> None:
    inserted_total = 0

    with engine.connect() as conn:
        ensure_tables(conn)
        conn.commit()

        for workbook_path in EXCEL_FILES:
            xl = pd.ExcelFile(workbook_path)

            for sheet_name in xl.sheet_names:
                df = xl.parse(sheet_name=sheet_name, header=None, dtype=str)
                batch = []

                for row_index, row in df.iterrows():
                    for col_index, value in enumerate(row.tolist()):
                        cleaned = clean_text(value)
                        if cleaned is None:
                            continue

                        batch.append(
                            {
                                "workbook_name": workbook_path.name,
                                "sheet_name": sheet_name,
                                "row_index": int(row_index),
                                "col_index": int(col_index),
                                "cell_value": cleaned,
                            }
                        )

                        if len(batch) >= 2000:
                            result = conn.execute(
                                text(
                                    """
                                    INSERT INTO montreal_excel_cell (
                                        workbook_name,
                                        sheet_name,
                                        row_index,
                                        col_index,
                                        cell_value
                                    )
                                    VALUES (
                                        :workbook_name,
                                        :sheet_name,
                                        :row_index,
                                        :col_index,
                                        :cell_value
                                    )
                                    ON CONFLICT (workbook_name, sheet_name, row_index, col_index)
                                    DO UPDATE SET
                                        cell_value = EXCLUDED.cell_value;
                                    """
                                ),
                                batch,
                            )
                            inserted_total += result.rowcount or 0
                            batch.clear()
                            conn.commit()

                if batch:
                    result = conn.execute(
                        text(
                            """
                            INSERT INTO montreal_excel_cell (
                                workbook_name,
                                sheet_name,
                                row_index,
                                col_index,
                                cell_value
                            )
                            VALUES (
                                :workbook_name,
                                :sheet_name,
                                :row_index,
                                :col_index,
                                :cell_value
                            )
                            ON CONFLICT (workbook_name, sheet_name, row_index, col_index)
                            DO UPDATE SET
                                cell_value = EXCLUDED.cell_value;
                            """
                        ),
                        batch,
                    )
                    inserted_total += result.rowcount or 0
                    conn.commit()

                print(f"[Montreal Excel] Loaded workbook={workbook_path.name} sheet={sheet_name}")

    print(f"[Montreal Excel] Raw cell ingest complete. Rows written this run: {inserted_total}")


if __name__ == "__main__":
    main()
