import glob
import os
from pathlib import Path

import pandas as pd
import typer

from utils import cols, with_res_logger

app = typer.Typer()

pd.set_option("display.max_rows", 10)

pandas_types = {"int": "Int64", "float": "Float64", "cat": "category", "str": "object"}


def load(year, nrows):
    kwargs = dict(
        dtype={k: pandas_types[v] for k, v in cols.items() if v != "date"},
        parse_dates=[k for k, v in cols.items() if v == "date"],
        on_bad_lines="warn",
        nrows=nrows,
    )
    p = Path(os.environ["DATA"]) / f"{year}.csv"
    if "*" in year:
        return pd.concat(
            (pd.read_csv(f, **kwargs) for f in glob.glob(str(p))), ignore_index=True
        )
    else:
        return pd.read_csv(p, **kwargs)


@app.command()
@with_res_logger
def top_flop(year: str, nrows: int = None):
    serie = (
        load(year, nrows)
        .groupby(["code_postal"])["id_mutation"]
        .nunique()
        .sort_values(ascending=False)
    )
    print(serie.head(10))
    print(serie.tail(10))
