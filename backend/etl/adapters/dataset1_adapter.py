# etl/adapters/dataset1_adapter.py

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

# 🔎 IMPORTANT:
# Find where your base adapter actually lives.
# In VS Code, Ctrl+Shift+F and search for "class BaseDatasetAdapter".
# Then adjust this import to match that path.
#
# Example possibilities (ONLY keep the one that matches your project):
# from etl.adapters.base_adapter import BaseDatasetAdapter
# from etl.base_adapter import BaseDatasetAdapter
# from etl.core.base_adapter import BaseDatasetAdapter
from etl.adapters.base_adapter import BaseDatasetAdapter  # <-- update if needed


class Dataset1Adapter(BaseDatasetAdapter):
    """
    Adapter for Dataset #1 (Ottawa Public Library data).

    This class plugs Dataset #1 into the shared ETL pipeline defined in
    BaseDatasetAdapter. The individual steps are implemented in
    SCRUM-98..SCRUM-103.
    """

    dataset_name = "dataset1"

    def __init__(self, data_dir: Path | None = None) -> None:
        # If caller doesn't pass a data_dir, default to backend/data
        if data_dir is None:
            data_dir = Path(__file__).resolve().parents[2] / "data"

        super().__init__(data_dir=data_dir)

        # Example default raw CSV path (rename this file if yours is different)
        self.raw_path = (
            self.data_dir / "Ottawa_Public_Library_Circulation_2022_2024.csv"
        )

    # SCRUM-98
    def extract(self) -> pd.DataFrame:
        """
        Read raw Dataset #1 data and return a DataFrame.

        For SCRUM-97 this is just a stub; SCRUM-98 will add the real logic.
        """
        # Example of the real thing (for later):
        # df = pd.read_csv(self.raw_path)
        # return df
        raise NotImplementedError("SCRUM-98: implement extract()")

    # SCRUM-99
    def validate_raw_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate that the raw dataframe has the expected columns / types.
        """
        raise NotImplementedError("SCRUM-99: implement validate_raw_schema()")

    # SCRUM-100
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on the raw dataframe (trim strings, normalize values, etc.).
        """
        raise NotImplementedError("SCRUM-100: implement clean()")

    # SCRUM-101 / SCRUM-102
    def map_to_tables(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Map the cleaned dataset into canonical tables (fact + dims).

        Should return something like:
        {
            "circulation_transaction": fact_df,
            "user_group": user_group_df,
            ...
        }
        """
        raise NotImplementedError("SCRUM-101/102: implement map_to_tables()")

    # SCRUM-103
    def load(self, tables: Dict[str, pd.DataFrame]) -> None:
        """
        Load mapped tables into the target database (Supabase/Postgres).
        """
        raise NotImplementedError("SCRUM-103: implement load()")