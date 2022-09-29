import pathlib
import tarfile
import pandas as pd

from hdrh.histogram import HdrHistogram

ROOT = pathlib.Path(__file__).parent

RAW_RESULTS = ROOT / "results" / "raw"
RESULTS = ROOT / "results" / "tidy"


def post():
    RESULTS.mkdir(parents=True, exist_ok=True)

    dstat_df, timeseries_df, latency_df = [], [], []
    for version_path in RAW_RESULTS.iterdir():
        version = version_path.name
        for throughput_path in version_path.iterdir():
            throughput = throughput_path.name[:-len("-throughput")]

            # Process Dstat files
            dstat_dir = throughput_path / "dstat"
            dstat_files = list(dstat_dir.rglob("*-dstat.csv"))

            if len(dstat_files) > 0:
                df = pd.read_csv(dstat_files[0], skiprows=5, index_col=False)
                df["version"] = version
                df["throughput"] = throughput

                dstat_df.append(df)

            # Process timeseries
            result_file = throughput_path / "data" / "csv" / "main.result.csv"

            df = pd.read_csv(result_file, index_col=False)
            df["version"] = version
            df["throughput"] = throughput

            timeseries_df.append(df)

            # Process histograms
            hist_file = throughput_path / "data" / "histograms.csv"

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
                                                version=version,
                                                throughput=throughput), index=[0]))

    pd.concat(dstat_df).to_csv(RESULTS / "dstat.csv", index=False)
    pd.concat(timeseries_df).to_csv(RESULTS / "timeseries.csv", index=False)
    pd.concat(latency_df).to_csv(RESULTS / "latency.csv", index=False)

    # Compress results
    with tarfile.open(ROOT / "results" / "tidy.tar.gz", mode="w:gz") as archive_file:
        archive_file.add(RESULTS, arcname=RESULTS.name)


if __name__ == "__main__":
    import argparse

    post()
