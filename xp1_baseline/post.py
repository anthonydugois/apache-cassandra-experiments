import pathlib
import shutil
import tarfile
import pandas as pd

from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent


def post(input_file: str, result_path: str, output_path: str):
    _result_path = pathlib.Path(result_path)
    _output_path = pathlib.Path(output_path)

    _output_path.mkdir(parents=True, exist_ok=True)

    dstat_df, timeseries_df, latency_df = [], [], []

    for row_path in _result_path.iterdir():
        row = pd.read_csv(row_path / "input.csv", index_col=False).iloc[0]

        _id = row["id"]

        # Process Dstat files
        dstat_dir = row_path / "dstat"
        dstat_files = list(dstat_dir.rglob("*-dstat.csv"))

        if len(dstat_files) > 0:
            df = pd.read_csv(dstat_files[0], skiprows=5, index_col=False)
            df["id"] = _id

            dstat_df.append(df)

        # Process timeseries
        result_file = row_path / "data" / "csv" / f"{ROOT.name}.result.csv"

        df = pd.read_csv(result_file, index_col=False)
        df["id"] = _id

        timeseries_df.append(df)

        # Process histograms
        hist_file = row_path / "data" / "histograms.csv"

        df = pd.read_csv(hist_file, skiprows=3, index_col=0)
        df.reset_index(drop=True, inplace=True)

        hists = df["Interval_Compressed_Histogram"]

        global_hist = None
        for index, encoded_hist in hists.items():
            hist = HdrHistogram.decode(encoded_hist)

            if hist.get_total_count() > 0:
                if global_hist is None:
                    global_hist = hist
                else:
                    global_hist.add(hist)

        latency_df.append(pd.DataFrame(dict(count=global_hist.get_total_count(),
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
                                            id=_id), index=[0]))

    # Save input file
    shutil.copy(input_file, _output_path / "input.csv")

    pd.concat(dstat_df).to_csv(_output_path / "dstat.csv", index=False)
    pd.concat(timeseries_df).to_csv(_output_path / "timeseries.csv", index=False)
    pd.concat(latency_df).to_csv(_output_path / "latency.csv", index=False)

    # Compress results
    with tarfile.open(_output_path.parent / "tidy.tar.gz", mode="w:gz") as file:
        file.add(_output_path, arcname=_output_path.name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("input", type=str)
    parser.add_argument("--result", type=str, default=str(ROOT / "output" / "raw"))
    parser.add_argument("--output", type=str, default=str(ROOT / "output" / "tidy"))

    args = parser.parse_args()

    post(input_file=args.input,
         result_path=args.result,
         output_path=args.output)
