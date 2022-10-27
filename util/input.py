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

    def filter(self, ids: list[str]):
        return self.dataframe[self.dataframe.index.isin(ids)]

    def create_view(self, key: str, ids: list[str]):
        if len(ids) > 0:
            filtered_view = self.filter(ids)
        else:
            filtered_view = self.dataframe

        self.filtered_views[key] = filtered_view

        return filtered_view

    def view(self, key: Optional[str] = None, rows: Optional[list[str]] = None, columns: Optional[list[str]] = None):
        if key is None:
            current_view = self.dataframe
        elif key in self.filtered_views:
            current_view = self.filtered_views[key]
        else:
            raise MissingViewException

        if rows is None:
            if columns is None:
                return current_view
            else:
                return current_view.loc[:, columns]
        else:
            if columns is None:
                return current_view.loc[rows, :]
            else:
                return current_view.loc[rows, columns]

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
