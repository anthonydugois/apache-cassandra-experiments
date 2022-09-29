import pathlib
import pandas as pd

ROOT = pathlib.Path(__file__).parent

RAW_RESULTS = ROOT / "results" / "raw"
RESULTS = ROOT / "results" / "tidy"


def post():
    RESULTS.mkdir(parents=True, exist_ok=True)

    # Process Dstat files
    dstat_df = []
    for path in RAW_RESULTS.iterdir():
        if path.is_dir():
            dstat_files = list((path / "dstat").rglob("*-dstat.csv"))

            if len(dstat_files) > 0:
                df = pd.read_csv(dstat_files[0], skiprows=5, index_col=False)
                df["version"] = path.name

                dstat_df.append(df)

    pd.concat(dstat_df).to_csv(RESULTS / "dstat.csv", index=False)


if __name__ == "__main__":
    import argparse
