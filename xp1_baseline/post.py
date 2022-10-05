import logging
import pathlib
import shutil
import tarfile
import pandas as pd

from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent

HIST_MIN = 1
HIST_MAX = 1_000_000_000_000
HIST_DIGITS = 3


def post(input_file: str,
         result_path: str,
         output_path: str,
         start_time: float,
         end_time: float):
    _result_path = pathlib.Path(result_path)
    _output_path = pathlib.Path(output_path)

    _output_path.mkdir(parents=True, exist_ok=True)

    dstat_dfs, timeseries_dfs, latency_dfs = [], [], []

    for row_path in _result_path.iterdir():
        row: pd.Series = pd.read_csv(row_path / "input.csv", index_col="id").iloc[0]

        _id = row.name
        _name = row["name"]

        logging.info(f"[{_name}#{_id}] Processing data in {row_path}.")
        logging.info(f"[{_name}#{_id}] Input parameters:\n\n{row}\n\n")

        # Process Dstat files
        logging.info(f"[{_name}#{_id}] Tidying Dstat data...")

        dstat_dir = row_path / "dstat"
        dstat_files = list(dstat_dir.rglob("*-dstat.csv"))

        if len(dstat_files) > 0:
            dstat_df = pd.read_csv(dstat_files[0], skiprows=5, index_col=False)
            dstat_df["id"] = _id

            dstat_dfs.append(dstat_df)

        # Process timeseries
        logging.info(f"[{_name}#{_id}] Tidying timeseries data...")

        result_file = row_path / "data" / "csv" / f"{ROOT.name}.result.csv"

        ts_df = pd.read_csv(result_file, index_col=False)
        ts_df["id"] = _id
        ts_df["time"] = ts_df["t"] - ts_df.iloc[0]["t"]  # Compute relative time

        timeseries_dfs.append(ts_df)

        # Process histograms
        hist_file = row_path / "data" / "histograms.csv"

        hist_df = pd.read_csv(hist_file, skiprows=3, index_col=0)
        hist_df.reset_index(drop=True, inplace=True)

        hist_df = hist_df[(hist_df["StartTimestamp"] > start_time) &
                          (hist_df["StartTimestamp"] + hist_df["Interval_Length"] < end_time)]

        logging.info(f"[{_name}#{_id}] Aggregating histograms...")

        hists = hist_df["Interval_Compressed_Histogram"]

        hist = HdrHistogram(HIST_MIN, HIST_MAX, HIST_DIGITS)
        hist_count = hists.size
        hist_index = 0

        for _, encoded_hist in hists.items():
            _hist = HdrHistogram.decode(encoded_hist)

            if _hist.get_total_count() > 0:
                hist.add(_hist)
                hist_index += 1

            logging.info(f"[{_name}#{_id}] Added histogram {hist_index}/{hist_count}")

        logging.info(f"[{_name}#{_id}] Saving histogram stats...")

        latency_stats = dict(count=hist.get_total_count(),
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
                             id=_id)

        latency_dfs.append(pd.DataFrame(latency_stats, index=[0]))

    # Save CSV files
    shutil.copy(input_file, _output_path / "input.csv")
    pd.concat(dstat_dfs).to_csv(_output_path / "dstat.csv", index=False)
    pd.concat(timeseries_dfs).to_csv(_output_path / "timeseries.csv", index=False)
    pd.concat(latency_dfs).to_csv(_output_path / "latency.csv", index=False)

    # Compress results
    with tarfile.open(_output_path.parent / "tidy.tar.gz", mode="w:gz") as file:
        file.add(_output_path, arcname=_output_path.name)


if __name__ == "__main__":
    import argparse

    from sys import stdout

    logging.basicConfig(stream=stdout, level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")

    parser = argparse.ArgumentParser()

    parser.add_argument("input", type=str)
    parser.add_argument("--result", type=str, default=str(ROOT / "output" / "raw"))
    parser.add_argument("--output", type=str, default=str(ROOT / "output" / "tidy"))
    parser.add_argument("--start-time", type=float, default=0.0)
    parser.add_argument("--end-time", type=float, default=float("inf"))

    args = parser.parse_args()

    post(input_file=args.input,
         result_path=args.result,
         output_path=args.output,
         start_time=args.start_time,
         end_time=args.end_time)
