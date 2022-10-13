import logging
from pathlib import Path
from typing import Any

import pandas as pd


class ValueInference:
    def __init__(self, basepath: Path):
        self.basepath = basepath
        self.run_paths = None

    def set_run_paths(self, run_path_pattern: str):
        self.run_paths = list(self.basepath.glob(run_path_pattern))

        return self

    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def reduce_dataframe(self, df: pd.DataFrame) -> Any:
        pass

    def aggregate_run_values(self, values: pd.Series) -> Any:
        pass

    def aggregate_set_values(self, values: pd.Series) -> Any:
        pass

    def infer(self, csv_file_pattern: str):
        if self.run_paths is None:
            raise Exception

        set_values = []
        for run_path in self.run_paths:
            run_values = []
            for csv_file in run_path.glob(csv_file_pattern):
                df = pd.read_csv(csv_file, index_col=False)
                _df = self.filter_dataframe(df)

                if _df.empty:
                    logging.warning(f"No significant values found in {csv_file}."
                                    "Provided filter is probably too aggressive; falling back to full dataframe.")
                    _df = df

                run_value = self.reduce_dataframe(_df)
                run_values.append(run_value)

            run_values = pd.Series(run_values)
            if run_values.empty:
                logging.warning(f"No value infered from {run_path}.")
            else:
                set_value = self.aggregate_run_values(run_values)
                set_values.append(set_value)

        set_values = pd.Series(set_values)
        if set_values.empty:
            raise Exception

        return self.aggregate_set_values(set_values)


class MeanRateInference(ValueInference):
    def __init__(self, basepath: Path, start_time: int, value_column_name="mean_rate", time_column_name="t"):
        super().__init__(basepath)

        self.start_time = start_time
        self.value_column_name = value_column_name
        self.time_column_name = time_column_name

    def filter_dataframe(self, df: pd.DataFrame):
        # Compute relative time
        t0 = df.iloc[0][self.time_column_name]
        df["time"] = df[self.time_column_name] - t0

        return df[df["time"] >= self.start_time]

    def reduce_dataframe(self, df: pd.DataFrame):
        return df[self.value_column_name].max()

    def aggregate_run_values(self, values: pd.Series):
        return values.sum()

    def aggregate_set_values(self, values: pd.Series):
        return values.max()
