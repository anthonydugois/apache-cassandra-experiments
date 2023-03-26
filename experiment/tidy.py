import logging
import pathlib
import tarfile
from typing import cast, Iterable, Hashable
from io import StringIO
from multiprocessing import Pool
from functools import partial

import pandas as pd
import numpy as np
from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent

START_TIME = 120  # 2 minutes


def histogram_aggregator(hist_df, hist_min, hist_max, hist_digits):
    hist = HdrHistogram(hist_min, hist_max, hist_digits)
    filtered_df = hist_df[hist_df["StartTimestamp"] > START_TIME]

    for _, hist_row in filtered_df.iterrows():
        decoded_hist = HdrHistogram.decode(hist_row["Interval_Compressed_Histogram"])

        if decoded_hist.get_total_count() > 0:
            hist.add(decoded_hist)

    return hist.encode()


def summarize_histogram(hist: HdrHistogram, _id: Hashable, run_index: int, percentiles=None):
    if percentiles is None:
        percentiles = [*np.arange(1, 95),
                       *np.arange(95, 100, 0.1).round(2)]

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


def tidy(data_path: str, archive: bool):
    _data_path = pathlib.Path(data_path)
    _raw_path = _data_path / "raw"

    _tidy_path = _data_path / "tidy"
    _tidy_path.mkdir(parents=True, exist_ok=True)

    parameters = pd.read_csv(_data_path / "input.csv", index_col="id")

    dfs = {
        "dstat_clients": [],
        "dstat_hosts": [],
        "latency": [],
        "small_latency": [],
        "large_latency": [],
        "latency_ts": [],
        "small_latency_ts": [],
        "large_latency_ts": [],
        "stretch": [],
        "stretch_ts": []
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

        for run_index in range(1, _repeat + 1):
            _run_path = _set_path / f"run-{run_index}"
            _client_path = _run_path / "clients"
            _host_path = _run_path / "hosts"

            if not _run_path.exists():
                logging.warning(f"{_run_path} does not exist.")
                continue

            logging.info(f"[{_name}/run-{run_index}] Processing {_run_path}.")

            # Process Dstat results.
            for key in ["clients", "hosts"]:
                _key_path = _run_path / key

                for _path in _key_path.glob("*.grid5000.fr"):
                    _dstat_path = _path / "dstat"

                    for _dstat_file in _dstat_path.glob("**/*-dstat.csv"):
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

                for _ts_file in _ts_path.glob("**/read.result-success.csv"):
                    ts_df = pd.read_csv(_ts_file, index_col=False)

                    ts_df.rename(columns={"t": "epoch"}, inplace=True)
                    ts_df["time"] = ts_df["epoch"] - ts_df.iloc[0]["epoch"]
                    ts_df["id"] = _id
                    ts_df["run"] = run_index
                    ts_df["host_address"] = _path.name

                    dfs["latency_ts"].append(ts_df)

                for _ts_file in _ts_path.glob("**/read.small-latency.csv"):
                    ts_df = pd.read_csv(_ts_file, index_col=False)

                    ts_df.rename(columns={"t": "epoch"}, inplace=True)
                    ts_df["time"] = ts_df["epoch"] - ts_df.iloc[0]["epoch"]
                    ts_df["id"] = _id
                    ts_df["run"] = run_index
                    ts_df["host_address"] = _path.name

                    dfs["small_latency_ts"].append(ts_df)

                for _ts_file in _ts_path.glob("**/read.large-latency.csv"):
                    ts_df = pd.read_csv(_ts_file, index_col=False)

                    ts_df.rename(columns={"t": "epoch"}, inplace=True)
                    ts_df["time"] = ts_df["epoch"] - ts_df.iloc[0]["epoch"]
                    ts_df["id"] = _id
                    ts_df["run"] = run_index
                    ts_df["host_address"] = _path.name

                    dfs["large_latency_ts"].append(ts_df)

                for _ts_file in _ts_path.glob("**/read.stretch.csv"):
                    ts_df = pd.read_csv(_ts_file, index_col=False)

                    ts_df.rename(columns={"t": "epoch"}, inplace=True)
                    ts_df["time"] = ts_df["epoch"] - ts_df.iloc[0]["epoch"]
                    ts_df["id"] = _id
                    ts_df["run"] = run_index
                    ts_df["host_address"] = _path.name

                    dfs["stretch_ts"].append(ts_df)

            # Process Histogram results.
            latency_dfs, small_latency_dfs, large_latency_dfs, stretch_dfs = [], [], [], []

            for _path in _client_path.glob("*.grid5000.fr"):
                _hist_path = _path / "data"

                for _hist_file in _hist_path.glob("**/histograms.csv"):
                    hist_df = pd.read_csv(_hist_file, skiprows=3, index_col=0)

                    latency_dfs.append(hist_df[hist_df.index == "Tag=read.result-success"])
                    small_latency_dfs.append(hist_df[hist_df.index == "Tag=read.small-latency"])
                    large_latency_dfs.append(hist_df[hist_df.index == "Tag=read.large-latency"])
                    stretch_dfs.append(hist_df[hist_df.index == "Tag=read.stretch"])

            latency_hist = HdrHistogram(1_000, 10_000_000_000, 5)

            with Pool(processes=len(latency_dfs)) as pool:
                aggregate = partial(histogram_aggregator, hist_min=1_000, hist_max=10_000_000_000, hist_digits=5)

                for encoded_hist in pool.map(aggregate, latency_dfs):
                    decoded_hist = HdrHistogram.decode(encoded_hist)

                    if decoded_hist.get_total_count() > 0:
                        latency_hist.add(decoded_hist)

            dfs["latency"].append(summarize_histogram(latency_hist, _id, run_index))

            small_latency_hist = HdrHistogram(1_000, 10_000_000_000, 5)

            with Pool(processes=len(small_latency_dfs)) as pool:
                aggregate = partial(histogram_aggregator, hist_min=1_000, hist_max=10_000_000_000, hist_digits=5)

                for encoded_hist in pool.map(aggregate, small_latency_dfs):
                    decoded_hist = HdrHistogram.decode(encoded_hist)

                    if decoded_hist.get_total_count() > 0:
                        small_latency_hist.add(decoded_hist)

            dfs["small_latency"].append(summarize_histogram(small_latency_hist, _id, run_index))

            large_latency_hist = HdrHistogram(1_000, 10_000_000_000, 5)

            with Pool(processes=len(large_latency_dfs)) as pool:
                aggregate = partial(histogram_aggregator, hist_min=1_000, hist_max=10_000_000_000, hist_digits=5)

                for encoded_hist in pool.map(aggregate, large_latency_dfs):
                    decoded_hist = HdrHistogram.decode(encoded_hist)

                    if decoded_hist.get_total_count() > 0:
                        large_latency_hist.add(decoded_hist)

            dfs["large_latency"].append(summarize_histogram(large_latency_hist, _id, run_index))

            stretch_hist = HdrHistogram(1, 10_000_000, 5)

            with Pool(processes=len(stretch_dfs)) as pool:
                aggregate = partial(histogram_aggregator, hist_min=1, hist_max=10_000_000, hist_digits=5)

                for encoded_hist in pool.map(aggregate, stretch_dfs):
                    decoded_hist = HdrHistogram.decode(encoded_hist)

                    if decoded_hist.get_total_count() > 0:
                        stretch_hist.add(decoded_hist)

            dfs["stretch"].append(summarize_histogram(stretch_hist, _id, run_index))

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
    parser.add_argument("--archive", action="store_true")

    args = parser.parse_args()

    tidy(data_path=args.result, archive=args.archive)
