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

    def aggregate_values(self, values: pd.Series) -> Any:
        pass

    def infer(self, csv_file_pattern: str):
        if self.run_paths is None:
            raise Exception

        observed_values = []
        for run_path in self.run_paths:
            csv_values = []
            csv_files = run_path.glob(csv_file_pattern)
            for csv_file in csv_files:
                df = pd.read_csv(csv_file, index_col=False)
                df = self.filter_dataframe(df)

                if df.empty:
                    logging.warning(f"No significant values found in {csv_file}."
                                    "Provided filter is probably too aggressive.")
                else:
                    csv_value = self.reduce_dataframe(df)
                    csv_values.append(csv_value)

            csv_values = pd.Series(csv_values)
            if csv_values.empty:
                logging.warning(f"No value infered from {run_path}.")
            else:
                observed_value = self.aggregate_run_values(csv_values)
                observed_values.append(observed_value)

        observed_values = pd.Series(observed_values)
        if observed_values.empty:
            raise Exception

        return self.aggregate_values(observed_values)


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
        return df[self.value_column_name].mean()

    def aggregate_run_values(self, values: pd.Series):
        return values.sum()

    def aggregate_values(self, values: pd.Series):
        return values.mean()
