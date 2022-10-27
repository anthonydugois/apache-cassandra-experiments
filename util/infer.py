import logging
from pathlib import Path
from typing import Any

import pandas as pd

from util.input import CSVInput


class UndefinedInferenceMethodException(Exception):
    pass


class ValueInference:
    def __init__(self, basepath: Path, run_path_pattern: str, csv_file_pattern: str):
        self.basepath = basepath
        self.run_path_pattern = run_path_pattern
        self.csv_file_pattern = csv_file_pattern

    def init(self, *args, **kwargs):
        pass

    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def reduce_dataframe(self, df: pd.DataFrame) -> Any:
        pass

    def aggregate_run_values(self, values: pd.Series) -> Any:
        pass

    def aggregate_set_values(self, values: pd.Series) -> Any:
        pass

    def infer(self):
        set_values = []
        for run_path in self.basepath.glob(self.run_path_pattern):
            run_values = []
            for csv_file in run_path.glob(self.csv_file_pattern):
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
    def __init__(self, basepath: Path):
        super().__init__(basepath, "run-*", "**/*.result.csv")

        self.rate = 1.0
        self.start_time = 0.0
        self.end_time = float("inf")
        self.value_column_name = "mean_rate"
        self.time_column_name = "t"

    def init(self, rate=1.0,
             start_time=0.0,
             end_time=float("inf"),
             value_column_name="mean_rate",
             time_column_name="t"):
        self.rate = rate
        self.start_time = start_time
        self.end_time = end_time
        self.value_column_name = value_column_name
        self.time_column_name = time_column_name

        return self

    def filter_dataframe(self, df: pd.DataFrame):
        # Compute relative time
        t0 = df.iloc[0][self.time_column_name]
        df["time"] = df[self.time_column_name] - t0

        return df[(df["time"] >= self.start_time) & (df["time"] <= self.end_time)]

    def reduce_dataframe(self, df: pd.DataFrame):
        return df[self.value_column_name].max()

    def aggregate_run_values(self, values: pd.Series):
        return values.sum()

    def aggregate_set_values(self, values: pd.Series):
        return self.rate * values.max()


class Infer:
    METHODS = {
        "MeanRate": MeanRateInference
    }

    def __init__(self, csv_input: CSVInput, basepath: Path):
        self.csv_input = csv_input
        self.basepath = basepath

    def infer_from_expr(self, expr: str):
        _method, _id, _args = self.parse_expr(expr)
        row = self.csv_input.view(rows=[_id])
        instance = _method(self.basepath / row["name"])

        return instance.init(*_args).infer()

    @staticmethod
    def parse_expr(expr: str):
        params = expr.split(",")

        method_name = params[0]
        if method_name not in Infer.METHODS:
            raise UndefinedInferenceMethodException

        _method = Infer.METHODS[method_name]
        _id = params[1]
        _args = params[2:] if len(params) > 2 else []

        return _method, _id, _args
