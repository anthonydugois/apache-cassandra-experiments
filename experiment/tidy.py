import logging
import pathlib
import tarfile
from typing import cast, Iterable, Hashable

import pandas as pd
import numpy as np
from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent

HIST_MIN = 1
HIST_MAX = 1_000_000_000_000
HIST_DIGITS = 3


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
        "timeseries": [],
        "metrics": []
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
                        dstat_df = pd.read_csv(_dstat_file, skiprows=4, header=[0, 1])

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

            # Process Metrics results
            for _path in _host_path.glob("*.grid5000.fr"):
                _metric_path = _path / "metrics"
                _metric_files = list(_metric_path.glob("**/org.apache.cassandra.metrics.*.csv"))
                if len(_metric_files) <= 0:
                    logging.warning(f"[{_name}/run-{run_index}] No Metrics file in {_metric_path}.")
                    continue

                for _metric_file in _metric_files:
                    metric_df = pd.read_csv(_metric_file, index_col=False)

                    metric_ms = np.concatenate(metric_df
                                               .groupby(["t"])
                                               .count()
                                               .iloc[:, "value"]
                                               .apply(lambda value: np.arange(0, 1, 1 / value))
                                               .values)
                    metric_df["t"] = metric_df["t"] + metric_ms

                    metric_df.rename(columns={"t": "epoch"}, inplace=True)
                    metric_df["id"] = _id
                    metric_df["run"] = run_index
                    metric_df["host_address"] = _path.name
                    metric_df["name"] = _metric_file.stem

                    dfs["metrics"].append(metric_df)

            # Process Latency histogram
            full_hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)
            for _path in _client_path.glob("*.grid5000.fr"):
                _hist_path = _path / "data"
                _hist_files = list(_hist_path.glob("**/histograms.csv"))
                if len(_hist_files) <= 0:
                    logging.warning(f"[{_name}/run-{run_index}] No Histogram file in {_hist_path}.")
                    continue

                hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)
                for hist_file in _hist_files:
                    hist_df = pd.read_csv(hist_file, skiprows=3, index_col=0)
                    hist_df.reset_index(drop=True, inplace=True)

                    # Filter histograms in time range
                    hist_df = hist_df[(hist_df["StartTimestamp"] >= start_time) &
                                      ((hist_df["StartTimestamp"] + hist_df["Interval_Length"]) <= end_time)]

                    hists = hist_df["Interval_Compressed_Histogram"]

                    hist_count = hists.size
                    hist_index = 0

                    for _, encoded_hist in hists.items():
                        decoded_hist = HdrHistogram.decode(encoded_hist)

                        if decoded_hist.get_total_count() > 0:
                            hist.add(decoded_hist)
                            hist_index += 1

                        logging.info(f"[{_name}/run-{run_index}] {hist_file} ({hist_index}/{hist_count})")

                # client_latency_row = dict(count=hist.get_total_count(),
                #                           min=hist.get_min_value(),
                #                           max=hist.get_max_value(),
                #                           mean=hist.get_mean_value(),
                #                           p25=hist.get_value_at_percentile(25),
                #                           p50=hist.get_value_at_percentile(50),
                #                           p75=hist.get_value_at_percentile(75),
                #                           p90=hist.get_value_at_percentile(90),
                #                           p95=hist.get_value_at_percentile(95),
                #                           p98=hist.get_value_at_percentile(98),
                #                           p99=hist.get_value_at_percentile(99),
                #                           p999=hist.get_value_at_percentile(99.9),
                #                           p9999=hist.get_value_at_percentile(99.99),
                #                           id=_id,
                #                           run=run_index,
                #                           host_address=_path.name)
                #
                # dfs["client_latency"].append(pd.DataFrame(client_latency_row, index=[0]))

                if hist.get_total_count() > 0:
                    full_hist.add(hist)

            dfs["latency"].append(
                pd.DataFrame({
                    "count": full_hist.get_total_count(),
                    "min": full_hist.get_min_value(),
                    "max": full_hist.get_max_value(),
                    "mean": full_hist.get_mean_value(),
                    "p25": full_hist.get_value_at_percentile(25),
                    "p50": full_hist.get_value_at_percentile(50),
                    "p75": full_hist.get_value_at_percentile(75),
                    "p90": full_hist.get_value_at_percentile(90),
                    "p95": full_hist.get_value_at_percentile(95),
                    "p98": full_hist.get_value_at_percentile(98),
                    "p99": full_hist.get_value_at_percentile(99),
                    "p999": full_hist.get_value_at_percentile(99.9),
                    "p9999": full_hist.get_value_at_percentile(99.99),
                    "id": _id,
                    "run": run_index
                }, index=[0])
            )

    # Save CSV files
    parameters.to_csv(_tidy_path / "input.csv")

    for key in dfs:
        pd.concat(dfs[key]).to_csv(_tidy_path / f"{key}.csv", index=False)

    # Archive results
    if archive:
        _archive_path = ROOT.parent / "archives"
        if not _archive_path.exists():
            _archive_path.mkdir()

        with tarfile.open(_archive_path / f"{_data_path.name}.tar.gz", mode="w:gz") as file:
            file.add(_data_path, arcname=_data_path.name)

        with tarfile.open(_archive_path / f"{_data_path.name}-light.tar.gz", mode="w:gz") as file:
            file.add(_tidy_path, arcname=f"{_data_path.name}-light")


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
