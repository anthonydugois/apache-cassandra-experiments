import logging
import pathlib
import tarfile

import pandas as pd
from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent

HIST_MIN = 1
HIST_MAX = 1_000_000_000_000
HIST_DIGITS = 3


def post(result_path: str,
         start_time: float,
         end_time: float,
         archive: bool):
    _result_path = pathlib.Path(result_path)
    _raw_path = _result_path / "raw"

    _tidy_path = _result_path / "tidy"
    _tidy_path.mkdir(parents=True, exist_ok=True)

    parameters = pd.read_csv(_result_path / "input.csv", index_col="id")

    dfs = dict(clients=[], hosts=[], timeseries=[], client_latency=[], latency=[])

    for _id, params in parameters.iterrows():
        _name = params["name"]
        _repeat = params["repeat"]
        _set_path = _raw_path / _name
        if _set_path.exists():
            logging.info(f"[{_name}#{_id}] Processing {_set_path}.")
            logging.info(f"[{_name}#{_id}] Input parameters:\n\n{params}\n\n")
            for run_index in range(_repeat):
                _run_path = _set_path / f"run-{run_index}"
                if _run_path.exists():
                    logging.info(f"[{_name}#{_id} - run {run_index}] Processing {_run_path}.")

                    # Process Dstat results
                    for key in ["clients", "hosts"]:
                        _key_path = _run_path / key
                        for _host_path in _key_path.glob("*.grid5000.fr"):
                            _dstat_path = _host_path / "dstat"
                            _dstat_files = list(_dstat_path.glob("**/*-dstat.csv"))
                            if len(_dstat_files) > 0:
                                for _dstat_file in _dstat_files:
                                    dstat_df = pd.read_csv(_dstat_file, skiprows=5, index_col=False)
                                    dstat_df["time"] = dstat_df["epoch"] - dstat_df.iloc[0]["epoch"]
                                    dstat_df["id"] = _id
                                    dstat_df["run"] = run_index
                                    dstat_df["host_address"] = _host_path.name

                                    dfs[key].append(dstat_df)
                            else:
                                logging.warning(f"[{_name}#{_id} - run {run_index}] No Dstat file in {_dstat_path}.")

                    # This histogram will contain histograms of each client
                    global_hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)

                    _client_path = _run_path / "clients"
                    for _host_path in _client_path.glob("*.grid5000.fr"):
                        # Process timeseries results
                        for ts_file in _host_path.glob("**/*.result.csv"):
                            ts_df = pd.read_csv(ts_file, index_col=False)
                            ts_df.rename(columns=dict(t="epoch"), inplace=True)
                            ts_df["time"] = ts_df["epoch"] - ts_df.iloc[0]["epoch"]
                            ts_df["id"] = _id
                            ts_df["run"] = run_index
                            ts_df["host_address"] = _host_path.name

                            dfs["timeseries"].append(ts_df)

                        # Process latency histogram results
                        hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)

                        for hist_file in _host_path.glob("**/histograms.csv"):
                            hist_df = pd.read_csv(hist_file, skiprows=3, index_col=0)
                            hist_df.reset_index(drop=True, inplace=True)

                            # Filter histograms in time range
                            hist_df = hist_df[(hist_df["StartTimestamp"] >= start_time) &
                                              (hist_df["StartTimestamp"] + hist_df["Interval_Length"] <= end_time)]

                            hists = hist_df["Interval_Compressed_Histogram"]

                            hist_count = hists.size
                            hist_index = 0

                            for _, encoded_hist in hists.items():
                                _hist = HdrHistogram.decode(encoded_hist)

                                if _hist.get_total_count() > 0:
                                    hist.add(_hist)
                                    hist_index += 1

                                logging.info(f"[{_name}#{_id}/{run_index}/{_host_path.name}]"
                                             f"{hist_file} ({hist_index}/{hist_count})")

                        client_latency_row = dict(count=hist.get_total_count(),
                                                  min=hist.get_min_value(),
                                                  max=hist.get_max_value(),
                                                  mean=hist.get_mean_value(),
                                                  p25=hist.get_value_at_percentile(25),
                                                  p50=hist.get_value_at_percentile(50),
                                                  p75=hist.get_value_at_percentile(75),
                                                  p90=hist.get_value_at_percentile(90),
                                                  p95=hist.get_value_at_percentile(95),
                                                  p98=hist.get_value_at_percentile(98),
                                                  p99=hist.get_value_at_percentile(99),
                                                  p999=hist.get_value_at_percentile(99.9),
                                                  p9999=hist.get_value_at_percentile(99.99),
                                                  id=_id,
                                                  run=run_index,
                                                  host_address=_host_path.name)

                        dfs["client_latency"].append(pd.DataFrame(client_latency_row, index=[0]))

                        if hist.get_total_count() > 0:
                            global_hist.add(hist)

                    latency_row = dict(count=global_hist.get_total_count(),
                                       min=global_hist.get_min_value(),
                                       max=global_hist.get_max_value(),
                                       mean=global_hist.get_mean_value(),
                                       p25=global_hist.get_value_at_percentile(25),
                                       p50=global_hist.get_value_at_percentile(50),
                                       p75=global_hist.get_value_at_percentile(75),
                                       p90=global_hist.get_value_at_percentile(90),
                                       p95=global_hist.get_value_at_percentile(95),
                                       p98=global_hist.get_value_at_percentile(98),
                                       p99=global_hist.get_value_at_percentile(99),
                                       p999=global_hist.get_value_at_percentile(99.9),
                                       p9999=global_hist.get_value_at_percentile(99.99),
                                       id=_id,
                                       run=run_index)

                    dfs["latency"].append(pd.DataFrame(latency_row, index=[0]))
                else:
                    logging.warning(f"{_run_path} does not exist.")
        else:
            logging.warning(f"{_set_path} does not exist.")

    # Save CSV files
    for key in dfs:
        pd.concat(dfs[key]).to_csv(_tidy_path / f"{key}.csv", index=False)

    # Archive results
    if archive:
        _archive_path = ROOT.parent / "archives"
        if not _archive_path.exists():
            _archive_path.mkdir()

        with tarfile.open(_archive_path / f"{_result_path.name}.tar.xz", mode="w:xz") as file:
            file.add(_result_path, arcname=_result_path.name)


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

    post(result_path=args.result,
         start_time=args.start_time,
         end_time=args.end_time,
         archive=args.archive)
