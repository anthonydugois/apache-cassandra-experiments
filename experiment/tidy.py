import logging
import pathlib
import tarfile
from typing import cast, Iterable, Hashable
from io import StringIO

import pandas as pd
import numpy as np
from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent

HIST_MIN = 1
HIST_MAX = 1_000_000_000_000
HIST_DIGITS = 3


def summarize_histogram(hist: HdrHistogram, _id: Hashable, run_index: int, percentiles=None):
    if percentiles is None:
        percentiles = range(1, 100)

    rows = [
        {"id": _id, "run": run_index, "stat_name": "count", "stat_value": hist.get_total_count()},
        {"id": _id, "run": run_index, "stat_name": "min", "stat_value": hist.get_min_value()},
        {"id": _id, "run": run_index, "stat_name": "max", "stat_value": hist.get_max_value()},
        {"id": _id, "run": run_index, "stat_name": "mean", "stat_value": hist.get_mean_value()},
        {"id": _id, "run": run_index, "stat_name": "sd", "stat_value": hist.get_stddev()},
    ]

    for percentile, value in hist.get_percentile_to_value_dict(percentiles).items():
        rows.append({"id": _id, "run": run_index, "stat_name": f"p{percentile}", "stat_value": value})

    return pd.DataFrame(rows)


def tidy(data_path: str,
         start_time: float,
         end_time: float,
         archive: bool):
    _data_path = pathlib.Path(data_path)
    _raw_path = _data_path / "raw"

    _tidy_path = _data_path / "tidy"
    _tidy_path.mkdir(parents=True, exist_ok=True)

    parameters = pd.read_csv(_data_path / "input.csv", index_col="id")

    dfs = {
        "dstat_clients": [],
        "dstat_hosts": [],
        "latency": [],
        "timeseries": []
    }

    for _id, params in parameters.iterrows():
        _name = params["name"]
        _repeat = params["repeat"]
        _set_path = _raw_path / _name
        if not _set_path.exists():
            logging.warning(f"{_set_path} does not exist.")
            continue

        logging.info(f"[{_name}] Processing {_set_path}.")
        logging.info(f"[{_name}] Input parameters:\n\n{params}\n\n")

        for run_index in range(_repeat):
            _run_path = _set_path / f"run-{run_index}"
            _client_path = _run_path / "clients"
            _host_path = _run_path / "hosts"
            if not _run_path.exists():
                logging.warning(f"{_run_path} does not exist.")
                continue

            logging.info(f"[{_name}/run-{run_index}] Processing {_run_path}.")

            # Process Dstat results.
            # We do this for clients and hosts.
            for key in ["clients", "hosts"]:
                _key_path = _run_path / key
                for _path in _key_path.glob("*.grid5000.fr"):
                    _dstat_path = _path / "dstat"
                    _dstat_files = list(_dstat_path.glob("**/*-dstat.csv"))
                    if len(_dstat_files) <= 0:
                        logging.warning(f"[{_name}/run-{run_index}] No Dstat file in {_dstat_path}.")
                        continue

                    for _dstat_file in _dstat_files:
                        with open(_dstat_file, "r") as dstat_file:
                            dstat_lines = dstat_file.readlines()[4:]
                            dstat_headers, dstat_rows = dstat_lines[:2], dstat_lines[2:]

                            dstat_headers = [line.strip("\n") for line in dstat_headers]
                            dstat_rows = [line.strip(",\n") for line in dstat_rows]
                            dstat_content = "\n".join([*dstat_headers, *dstat_rows])

                        # noinspection PyTypeChecker
                        dstat_df = pd.read_csv(StringIO(dstat_content), header=[0, 1])

                        dstat_cols = pd.DataFrame(dstat_df.columns.tolist())
                        dstat_cols.loc[dstat_cols[0].str.startswith("Unnamed:"), 0] = np.nan
                        dstat_cols[0] = dstat_cols[0].fillna(method="ffill")
                        dstat_cols[0] = dstat_cols[0].str.replace("[ /]", "_", regex=True)
                        dstat_cols[1] = dstat_cols[1].str.replace(".+:", "", regex=True)
                        dstat_col_tuples = cast(Iterable[tuple[Hashable, ...]],
                                                dstat_cols.to_records(index=False).tolist())
                        dstat_df.columns = pd.MultiIndex.from_tuples(dstat_col_tuples)
                        dstat_df.rename(columns={"total_cpu_usage": "cpu_usage", "memory_usage": "mem_usage"},
                                        inplace=True)
                        dstat_df.columns = pd.Index(("__".join(col) for col in dstat_df.columns.values))
                        dstat_df.rename(columns={"epoch__epoch": "epoch"}, inplace=True)

                        dstat_df["time"] = dstat_df["epoch"] - dstat_df.iloc[0]["epoch"]
                        dstat_df["id"] = _id
                        dstat_df["run"] = run_index
                        dstat_df["host_address"] = _path.name

                        dfs[f"dstat_{key}"].append(dstat_df)

            # Process Timeseries results.
            for _path in _client_path.glob("*.grid5000.fr"):
                _ts_path = _path / "data"
                _ts_files = list(_ts_path.glob("**/read.result.csv"))
                if len(_ts_files) <= 0:
                    logging.warning(f"[{_name}/run-{run_index}] No Timeseries file in {_ts_path}.")
                    continue

                for _ts_file in _ts_files:
                    ts_df = pd.read_csv(_ts_file, index_col=False)

                    ts_df.rename(columns={"t": "epoch"}, inplace=True)
                    ts_df["time"] = ts_df["epoch"] - ts_df.iloc[0]["epoch"]
                    ts_df["id"] = _id
                    ts_df["run"] = run_index
                    ts_df["host_address"] = _path.name

                    dfs["timeseries"].append(ts_df)

            # Process Latency histogram
            cur_hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)
            for _path in _client_path.glob("*.grid5000.fr"):
                _hist_path = _path / "data"
                _hist_files = list(_hist_path.glob("**/histograms.csv"))
                if len(_hist_files) <= 0:
                    logging.warning(f"[{_name}/run-{run_index}] No Histogram file in {_hist_path}.")
                    continue

                hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)
                for hist_file in _hist_files:
                    logging.info(f"[{_name}/run-{run_index}] {hist_file}")

                    hist_df = pd.read_csv(hist_file, skiprows=3, index_col=0)
                    hist_df.reset_index(drop=True, inplace=True)

                    # Filter histograms in time range
                    hist_df = hist_df[(hist_df["StartTimestamp"] >= start_time) &
                                      ((hist_df["StartTimestamp"] + hist_df["Interval_Length"]) <= end_time)]

                    for _, hist_row in hist_df.iterrows():
                        hist_start_time = hist_row["StartTimestamp"]
                        hist_interval_length = hist_row["Interval_Length"]
                        hist_end_time = hist_start_time + hist_interval_length

                        decoded_hist = HdrHistogram.decode(hist_row["Interval_Compressed_Histogram"])
                        hist_count = decoded_hist.get_total_count()

                        logging.info(f"Getting {hist_count} values from {hist_start_time} to {hist_end_time}"
                                     f" (length: {hist_interval_length}).")

                        if hist_count > 0:
                            hist.add(decoded_hist)
                            logging.info(f"Added histogram.")

                if hist.get_total_count() > 0:
                    cur_hist.add(hist)

            dfs["latency"].append(summarize_histogram(cur_hist, _id, run_index))

    # Save CSV files
    parameters.to_csv(_tidy_path / "input.csv")

    for key in dfs:
        pd.concat(dfs[key]).to_csv(_tidy_path / f"{key}.csv", index=False)

    # Archive results
    if archive:
        _archive_path = ROOT.parent / "archives"
        if not _archive_path.exists():
            _archive_path.mkdir()

        full_name = f"{_data_path.name}-full"
        with tarfile.open(_archive_path / f"{full_name}.tar.gz", mode="w:gz") as file:
            file.add(_data_path, arcname=full_name)
            logging.info(f"Archive successfully created in {file.name}")

        light_name = f"{_data_path.name}-light"
        with tarfile.open(_archive_path / f"{light_name}.tar.gz", mode="w:gz") as file:
            file.add(_tidy_path, arcname=light_name)
            logging.info(f"Archive successfully created in {file.name}")


if __name__ == "__main__":
    import argparse

    from sys import stdout

    logging.basicConfig(stream=stdout, level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")

    parser = argparse.ArgumentParser()

    parser.add_argument("result", type=str)
    parser.add_argument("--start-time", type=float, default=0.0)
    parser.add_argument("--end-time", type=float, default=float("inf"))
    parser.add_argument("--archive", action="store_true")

    args = parser.parse_args()

    tidy(data_path=args.result, start_time=args.start_time, end_time=args.end_time, archive=args.archive)
