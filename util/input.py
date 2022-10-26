import pandas as pd
from pathlib import Path
from typing import Optional


class MissingViewException(Exception):
    pass


class CSVInput:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.dataframe = pd.read_csv(file_path, index_col="id")
        self.filtered_views: dict[str, pd.DataFrame] = {}

    def all(self):
        return self.dataframe

    def filter(self, ids: list[str]):
        return self.dataframe[self.dataframe.index.isin(ids)]

    def create_view(self, key: str, ids: list[str]):
        if len(ids) > 0:
            filtered_view = self.filter(ids)
        else:
            filtered_view = self.dataframe

        self.filtered_views[key] = filtered_view

        return filtered_view

    def view(self, key: str):
        if key in self.filtered_views:
            return self.filtered_views[key]

        raise MissingViewException

    def column(self, col_name: str, key: Optional[str] = None):
        if key is None:
            return self.dataframe[col_name]

        return self.view(key)[col_name]

    def row(self, row_index: str, key: Optional[str] = None):
        if key is None:
            return self.dataframe.loc[row_index]

        return self.view(key).loc[row_index]

    def get_ids(self, from_id: Optional[str], to_id: Optional[str], ids: Optional[list[str]]):
        if from_id is None and to_id is None:
            return [] if ids is None else ids
        else:
            from_index = 0
            if from_id is not None:
                from_index = self.dataframe.index.get_loc(from_id)

            to_index = len(self.dataframe.index)
            if to_id is not None:
                to_index = self.dataframe.index.get_loc(to_id)

            filtered_ids = list(self.dataframe.index[from_index:to_index])
            if ids is not None:
                filtered_ids.extend(ids)

            return filtered_ids
