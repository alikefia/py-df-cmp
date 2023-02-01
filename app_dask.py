import os
from functools import wraps
from pathlib import Path

import dask.dataframe as dd
import typer
from dask.distributed import Client

from app_pandas import pandas_types
from utils import cols, with_res_logger

app = typer.Typer()


def with_client(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with Client():
            return f(*args, **kwargs)

    return wrapper


def load(year):
    return dd.read_csv(
        Path(os.environ["DATA"]) / f"{year}.csv",
        dtype={k: pandas_types[v] for k, v in cols.items() if v != "date"},
        parse_dates=[k for k, v in cols.items() if v == "date"],
        on_bad_lines="warn",
    )


@app.command()
@with_res_logger
@with_client
def top_flop(year: str):
    serie = (
        load(year)
        .groupby(["code_postal"], group_keys=False)["id_mutation"]
        .nunique()
        .compute()
    )
    print(serie.nlargest(10))
    print(serie.nsmallest(10))
