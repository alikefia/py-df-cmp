import os
from pathlib import Path

import polars as pl
import typer

from utils import cols, with_res_logger

pl.Config.set_tbl_rows(10)

app = typer.Typer()


polars_types = {
    "str": pl.Utf8,
    "int": pl.Int64,
    "float": pl.Float64,
    "cat": pl.Categorical,
    "date": pl.Date,
}


def load(year, nrows):
    return pl.scan_csv(
        Path(os.environ["DATA"]) / f"{year}.csv",
        dtypes={k: polars_types[v] for k, v in cols.items()},
        n_rows=nrows,
    )


def load_communes():
    return pl.scan_csv(
        Path(os.environ["DATA"]) / "population" / "Communes.csv",
        dtypes={k: polars_types[v] for k, v in cols.items()},
        sep=";",
        infer_schema_length=10000,
    )


@app.command()
@with_res_logger
def top_flop(year: str, nrows: int = None):
    df = (
        load(year, nrows)
        .select(["code_postal", "id_mutation"])
        .groupby("code_postal")
        .agg([pl.n_unique("id_mutation")])
        .sort("id_mutation", reverse=True)
        .collect()
    )
    print(df[:10])
    print(df[-10:])


@app.command()
@with_res_logger
def join(year: str, nrows: int = None):
    communes = (
        load_communes()
        .rename(
            {
                "DEPCOM": "code_commune",
                "COM": "nom_commune",
                "PTOT": "population_commune",
            }
        )
        .select(["code_commune", "nom_commune", "population_commune"])
    )
    df = load(year, nrows).select(["code_postal", "id_mutation", "code_commune"])
    joined_df = df.join(communes, on="code_commune").collect()
    print(joined_df[:10])
